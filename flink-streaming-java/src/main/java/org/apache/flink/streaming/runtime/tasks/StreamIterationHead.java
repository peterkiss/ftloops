/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.BlockingQueueBroker;
import org.apache.flink.streaming.runtime.io.RecordWriterOutput;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecordSerializer;
import org.apache.flink.types.Either;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

@Internal
public class StreamIterationHead<IN> extends OneInputStreamTask<IN, IN> {

	private static final Logger LOG = LoggerFactory.getLogger(StreamIterationHead.class);

	private volatile boolean running = true;

	/**
	 * A flag that is on during the duration of a checkpoint. While onSnapshot is true the iteration head has to perform
	 * upstream backup of all records in transit within the loop.
	 */
	private volatile boolean onSnapshot = false;

	/**
	 * Flag notifying whether the iteration head has flushed pending
	 */
	private boolean hasFlushed = false;


	private volatile RecordWriterOutput<IN>[] outputs;

	private UpstreamLogger<IN> upstreamLogger;

	private Object lock;

	@Override
	public void init() throws Exception {
		this.lock = getCheckpointLock();
		this.upstreamLogger = new UpstreamLogger(new StreamRecordSerializer<>(getConfiguration().<IN>getTypeSerializerOut(getUserCodeClassLoader())));
		this.headOperator = upstreamLogger;
		headOperator.setup(this, getConfiguration(), operatorChain.getChainEntryPoint());
		operatorChain = new OperatorChain<>(this, headOperator,
			getEnvironment().getAccumulatorRegistry().getReadWriteReporter());
	}

	@Override
	protected void run() throws Exception {

		final String iterationId = getConfiguration().getIterationId();
		if (iterationId == null || iterationId.length() == 0) {
			throw new Exception("Missing iteration ID in the task configuration");
		}
		final String brokerID = createBrokerIdString(getEnvironment().getJobID(), iterationId,
			getEnvironment().getTaskInfo().getIndexOfThisSubtask());
		final long iterationWaitTime = getConfiguration().getIterationWaitTime();
		final boolean shouldWait = iterationWaitTime > 0;

		final BlockingQueue<Either<StreamRecord<IN>, CheckpointBarrier>> dataChannel
			= new ArrayBlockingQueue<>(1);

		// offer the queue for the tail
		BlockingQueueBroker.INSTANCE.handIn(brokerID, dataChannel);
		LOG.info("Iteration head {} added feedback queue under {}", getName(), brokerID);

		// do the work 
		try {
			outputs = (RecordWriterOutput<IN>[]) getStreamOutputs();

			// If timestamps are enabled we make sure to remove cyclic watermark dependencies
			if (getExecutionConfig().areTimestampsEnabled()) {
				for (RecordWriterOutput<IN> output : outputs) {
					output.emitWatermark(new Watermark(Long.MAX_VALUE));
				}
			}

			//emit in-flight events in the upstream log upon initialization
			synchronized (lock) {
				flushAndClearLog();
				hasFlushed = true;
			}

			while (running) {
				Either<StreamRecord<IN>, CheckpointBarrier> nextRecord = shouldWait ?
					dataChannel.poll(iterationWaitTime, TimeUnit.MILLISECONDS) :
					dataChannel.take();

				synchronized (lock) {

					if (nextRecord != null) {

						if (nextRecord.isLeft()) {
							if (onSnapshot) {
								upstreamLogger.log.add(nextRecord.left());
							} else {
								for (RecordWriterOutput<IN> output : outputs) {
									output.collect(nextRecord.left());
								}
							}
						} else {
							//upon barrier from tail
							checkpointStatesInternal(nextRecord.right().getId(), nextRecord.right().getTimestamp());
							flushAndClearLog();
							onSnapshot = false;
						}

					} else {
						flushAndClearLog();
						break;
					}
				}
			}
		} finally {
			// make sure that we remove the queue from the broker, to prevent a resource leak
			BlockingQueueBroker.INSTANCE.remove(brokerID);
			LOG.info("Iteration head {} removed feedback queue under {}", getName(), brokerID);
		}
	}

	private void flushAndClearLog() {
		synchronized (lock) {
			for (StreamRecord<IN> record : upstreamLogger.log) {
				for (RecordWriterOutput<IN> output : outputs) {
					output.collect(record);
				}
			}
			upstreamLogger.log.clear();
		}
	}

	@Override
	protected void cancelTask() {
		running = false;
	}

	@Override
	protected void cleanup() throws Exception {
		//nothing to cleanup
	}


	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	/**
	 * Creates the identification string with which head and tail task find the shared blocking
	 * queue for the back channel. The identification string is unique per parallel head/tail pair
	 * per iteration per job.
	 *
	 * @param jid          The job ID.
	 * @param iterationID  The id of the iteration in the job.
	 * @param subtaskIndex The parallel subtask number
	 * @return The identification string.
	 */
	public static String createBrokerIdString(JobID jid, String iterationID, int subtaskIndex) {
		return jid + "-" + iterationID + "-" + subtaskIndex;
	}

	@Override
	public boolean triggerCheckpoint(long checkpointId, long timestamp) throws Exception {

		//invoked upon barrier from Runtime
		synchronized (lock) {
			operatorChain.broadcastCheckpointBarrier(checkpointId, timestamp);

			if (onSnapshot || !hasFlushed) {
				LOG.debug("Iteration head {} aborting checkpoint {}", getName(), checkpointId);
				return false;
			}
			onSnapshot = true;
			return true;
		}
	}

	/**
	 * Internal operator that solely serves as a state logging facility for persisting and restoring upstream backups
	 */
	public static class UpstreamLogger<IN> extends AbstractStreamOperator<IN> implements OneInputStreamOperator<IN, IN> {


		private final StreamRecordSerializer<IN> serializer;

		/**
		 * The upstreamLog is used to store all records that should be logged throughout the duration of each checkpoint instance.
		 * These are part of the iteration head operator state for that snapshot and represent the records in transit for the backedge of
		 * an iteration cycle.
		 */
		private transient List<StreamRecord<IN>> log;

		private UpstreamLogger(StreamRecordSerializer<IN> streamRecordSerializer) {
			this.serializer = streamRecordSerializer;
			this.log = new ArrayList<>();
		}

		@Override
		public void open() throws Exception {
			super.open();
		}

		@Override
		public StreamTaskState snapshotOperatorState(long checkpointId, long timestamp) throws Exception {
			//TODO add disk spilling
			StreamTaskState taskState = new StreamTaskState();

			final AbstractStateBackend.CheckpointStateOutputStream os = this.getStateBackend().createCheckpointStateOutputStream(
				checkpointId,
				timestamp);
			final DataOutputViewStreamWrapper dov = new DataOutputViewStreamWrapper(os);

			dov.writeInt(log.size());

			for (StreamRecord<IN> streamRecord : log) {
				serializer.serialize(streamRecord, dov);
			}

			taskState.setOperatorState(os.closeAndGetHandle());

			return taskState;
		}

		@Override
		@SuppressWarnings("unchecked")
		public void restoreState(StreamTaskState state, long recoveryTimestamp) throws Exception {
			StreamStateHandle stream = (StreamStateHandle) state.getOperatorState();

			final InputStream is = stream.getState(getUserCodeClassloader());
			final DataInputViewStreamWrapper div = new DataInputViewStreamWrapper(is);

			this.log = new ArrayList<>();

			int logSize = div.readInt();

			for (int i = 0; i < logSize; i++) {
				log.add(serializer.deserialize(div));
			}

			div.close();
		}

		@Override
		public void processElement(StreamRecord<IN> element) throws Exception {
			//nothing to do
		}

		@Override
		public void processWatermark(Watermark mark) throws Exception {
			//nothing to do
		}
	}

}
