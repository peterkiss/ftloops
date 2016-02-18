/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.checkpointing;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

/**
 * An integration test that runs an iterative streaming topology with checkpointing enabled.
 * <p/>
 * The test triggers a failure after a while and verifies "exactly-once-processing" guarantees.
 * 
 * The correctness of the final state relies on whether all records in transit through the iteration cycle
 * have been processed exactly once by the stateful operator that consumes the feedback stream.
 * 
 */
@SuppressWarnings("serial")
public class StreamIterationCheckpointingITCase extends StreamFaultToleranceTestBase {

	final long NUM_ADDITIONS = 600_000L;

	@Override
	public void testProgram(StreamExecutionEnvironment env) {
		DataStream<ADD> stream = env.addSource(new AddGenerator(NUM_ADDITIONS));

		IterativeStream.ConnectedIterativeStreams<ADD, SUBTRACT> iter = stream.iterate(2000).withFeedbackType(SUBTRACT.class);
		SplitStream<Tuple2<String, Long>> step = iter.flatMap(new LoopCounter()).disableChaining().split(new OutputSelector<Tuple2<String, Long>>() {
			@Override
			public Iterable<String> select(Tuple2<String, Long> value) {
				return Lists.newArrayList(value.f0);
			}
		});
		iter.closeWith(step.select(LoopCounter.FEEDBACK).map(new MapFunction<Tuple2<String, Long>, SUBTRACT>() {
			@Override
			public SUBTRACT map(Tuple2 value) throws Exception {
				return new SUBTRACT(1);
			}
		}));
		step.select(LoopCounter.FORWARD).map(new MapFunction<Tuple2<String, Long>, Long>() {
			@Override
			public Long map(Tuple2<String, Long> value) throws Exception {
				return value.f1;
			}
		}).disableChaining().addSink(new OnceFailingSink(NUM_ADDITIONS));

	}

	@Override
	public void postSubmit() {
		long[] counters = new long[PARALLELISM];
		long[] states = new long[PARALLELISM];

		Arrays.fill(counters, NUM_ADDITIONS);
		Arrays.fill(states, NUM_ADDITIONS);

		assertTrue(OnceFailingSink.hasFailed);
		assertArrayEquals(counters, AddGenerator.counts);
		assertArrayEquals(states, LoopCounter.stateVals);
		assertArrayEquals(counters, OnceFailingSink.stateVals);

	}

	// --------------------------------------------------------------------------------------------
	//  Custom Functions
	// --------------------------------------------------------------------------------------------


	private static class AddGenerator extends RichSourceFunction<ADD>
		implements ParallelSourceFunction<ADD>, Checkpointed<Long> {

		private final long numElements;
		private volatile boolean isRunning = true;
		static final long[] counts = new long[PARALLELISM];
		private int index;
		private long state;

		AddGenerator(long numAdditions) {
			numElements = numAdditions;
		}

		@Override
		public void open(Configuration parameters) throws IOException {
			index = getRuntimeContext().getIndexOfThisSubtask();
		}

		@Override
		public void close() throws Exception {
			counts[index] = state;
		}

		@Override
		public void run(SourceContext<ADD> ctx) throws Exception {
			final Object lockingObject = ctx.getCheckpointLock();

			while (isRunning && state < numElements) {
				synchronized (lockingObject) {
					state++;
					ctx.collect(new ADD(2));
				}
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}

		@Override
		public Long snapshotState(long checkpointId, long checkpointTimestamp) {
			return state;
		}

		@Override
		public void restoreState(Long state) {
			this.state = state;
		}
	}


	private static class LoopCounter extends RichCoFlatMapFunction<ADD, SUBTRACT, Tuple2<String, Long>> implements Checkpointed<Long> {

		public static final String FEEDBACK = "FEEDBACK";
		public static final String FORWARD = "FORWARD";
		static final long[] stateVals = new long[PARALLELISM];
		private long state;
		private int index;

		@Override
		public void open(Configuration parameters) throws Exception {
			this.index = getRuntimeContext().getIndexOfThisSubtask();
		}

		@Override
		public void close() throws Exception {
			stateVals[index] = state;
		}

		@Override
		public void flatMap1(ADD toAdd, Collector<Tuple2<String, Long>> out) throws Exception {
			state += toAdd.val;
			out.collect(new Tuple2<>(FEEDBACK, state));
		}

		@Override
		public void flatMap2(SUBTRACT toSubtract, Collector<Tuple2<String, Long>> out) throws Exception {
			state -= toSubtract.val;
			out.collect(new Tuple2<>(FORWARD, state));
		}

		@Override
		public Long snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
			return state;
		}

		@Override
		public void restoreState(Long recState) throws Exception {
			state = recState;
		}
	}

	public static class ADD implements Serializable {
		public final long val;

		public ADD() {
			this(0);
		}

		public ADD(int val) {
			this.val = val;
		}
	}

	public static class SUBTRACT implements Serializable {
		public final long val;

		public SUBTRACT() {
			this(0);
		}

		public SUBTRACT(int val) {
			this.val = val;
		}
	}

	private static class OnceFailingSink extends RichSinkFunction<Long>
		implements Checkpointed<Long> {

		static final long[] stateVals = new long[PARALLELISM];
		private long state;
		private static volatile boolean hasFailed = false;
		private final long failurePos;
		private int index;
		private int counter;

		OnceFailingSink(long numElements) {
			long failurePosMin = (long) (0.2 * numElements);
			long failurePosMax = (long) (0.4 * numElements);
			failurePos = (new Random().nextLong() % (failurePosMax - failurePosMin)) + failurePosMin;
		}

		@Override
		public void open(Configuration parameters) throws IOException {
			index = getRuntimeContext().getIndexOfThisSubtask();
		}

		@Override
		public void close() throws Exception {
			stateVals[index] = state;
		}

		@Override
		public Long snapshotState(long checkpointId, long checkpointTimestamp) {
			return state;
		}

		@Override
		public void restoreState(Long toRestore) {
			state = toRestore;
		}

		@Override
		public void invoke(Long value) throws Exception {
			counter++;
			if (!hasFailed && counter >= failurePos) {
				hasFailed = true;
				throw new Exception("Intended Exception");
			}
			state++;
		}
	}

}
