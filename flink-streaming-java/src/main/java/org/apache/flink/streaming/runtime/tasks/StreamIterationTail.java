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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.accumulators.AccumulatorRegistry;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.util.event.EventListener;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.BlockingQueueBroker;
import org.apache.flink.streaming.runtime.io.StreamInputProcessor;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.Either;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;

import static org.apache.flink.types.Either.Left;
import static org.apache.flink.types.Either.Right;

@Internal
public class StreamIterationTail<IN> extends StreamTask<IN, OneInputStreamOperator<IN, IN>> {

	private static final Logger LOG = LoggerFactory.getLogger(StreamIterationTail.class);
	private RecordPusher<IN> recordPusher;

	private StreamInputProcessor<IN> inputProcessor;

	private volatile boolean running = true;

	@Override
	public void init() throws Exception {
		StreamConfig configuration = getConfiguration();

		TypeSerializer<IN> inSerializer = configuration.getTypeSerializerIn1(getUserCodeClassLoader());
		InputGate[] inputGates = getEnvironment().getAllInputGates();
		inputProcessor = new StreamInputProcessor<IN>(inputGates, inSerializer,
			new EventListener<CheckpointBarrier>() {
				@Override
				public void onEvent(CheckpointBarrier barrier) {
					try {
						getEnvironment().acknowledgeCheckpoint(barrier.getId());
						recordPusher.processEither(new Right(barrier));
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			},
			configuration.getCheckpointMode(),
			getEnvironment().getIOManager(),
			getExecutionConfig().areTimestampsEnabled());

		// make sure that stream tasks report their I/O statistics
		AccumulatorRegistry registry = getEnvironment().getAccumulatorRegistry();
		AccumulatorRegistry.Reporter reporter = registry.getReadWriteReporter();
		inputProcessor.setReporter(reporter);

		//iteration-specifics initialization
		final String iterationId = getConfiguration().getIterationId();
		if (iterationId == null || iterationId.length() == 0) {
			throw new Exception("Missing iteration ID in the task configuration");
		}
		final String brokerID = StreamIterationHead.createBrokerIdString(getEnvironment().getJobID(), iterationId,
			getEnvironment().getTaskInfo().getIndexOfThisSubtask());
		final long iterationWaitTime = getConfiguration().getIterationWaitTime();

		LOG.info("Iteration tail {} trying to acquire feedback queue under {}", getName(), brokerID);

		@SuppressWarnings("unchecked")
		BlockingQueue<Either<StreamRecord<IN>, CheckpointBarrier>> dataChannel =
			(BlockingQueue<Either<StreamRecord<IN>, CheckpointBarrier>>) BlockingQueueBroker.INSTANCE.get(brokerID);

		LOG.info("Iteration tail {} acquired feedback queue {}", getName(), brokerID);

		this.recordPusher = new RecordPusher<>(dataChannel, iterationWaitTime);
		this.headOperator = recordPusher;
		headOperator.setup(this, getConfiguration(), operatorChain.getChainEntryPoint());
		operatorChain = new OperatorChain<>(this, headOperator,
			getEnvironment().getAccumulatorRegistry().getReadWriteReporter());
	}

	@Override
	protected void run() throws Exception {
		// cache some references on the stack, to make the code more JIT friendly
		final OneInputStreamOperator<IN, IN> operator = this.headOperator;
		final StreamInputProcessor<IN> inputProcessor = this.inputProcessor;
		final Object lock = getCheckpointLock();

		while (running && inputProcessor.processInput(operator, lock)) {
			checkTimerException();
		}
	}

	@Override
	protected void cleanup() throws Exception {
		inputProcessor.cleanup();
	}

	@Override
	protected void cancelTask() {
		running = false;
	}
	

	private static class RecordPusher<IN> extends AbstractStreamOperator<IN> implements OneInputStreamOperator<IN, IN> {

		private static final long serialVersionUID = 1L;

		@SuppressWarnings("NonSerializableFieldInSerializableClass")
		private final BlockingQueue<Either<StreamRecord<IN>, CheckpointBarrier>> dataChannel;

		RecordPusher(BlockingQueue<Either<StreamRecord<IN>, CheckpointBarrier>> dataChannel, long iterationWaitTime) {
			this.dataChannel = dataChannel;
		}

		public void processEither(Either<StreamRecord<IN>, CheckpointBarrier> recordOrBarrier) throws Exception {
			dataChannel.put(recordOrBarrier);
		}

		@Override
		public void processElement(StreamRecord<IN> record) throws Exception {
			processEither(new Left(record));
		}

		@Override
		public void processWatermark(Watermark mark) {
			// ignore
		}
	}
}
