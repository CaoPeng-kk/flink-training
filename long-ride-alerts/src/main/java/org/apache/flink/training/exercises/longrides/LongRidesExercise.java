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

package org.apache.flink.training.exercises.longrides;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator;
import org.apache.flink.training.exercises.common.utils.MissingSolutionException;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.time.Duration;

/**
 * The "Long Ride Alerts" exercise.
 *
 * <p>The goal for this exercise is to emit the rideIds for taxi rides with a duration of more than
 * two hours. You should assume that TaxiRide events can be lost, but there are no duplicates.
 *
 * <p>You should eventually clear any state you create.
 */
public class LongRidesExercise {
    private final SourceFunction<TaxiRide> source;
    private final SinkFunction<Long> sink;

    /** Creates a job using the source and sink provided. */
    public LongRidesExercise(SourceFunction<TaxiRide> source, SinkFunction<Long> sink) {
        this.source = source;
        this.sink = sink;
    }

    /**
     * Creates and executes the long rides pipeline.
     *
     * @return {JobExecutionResult}
     * @throws Exception which occurs during job execution.
     */
    public JobExecutionResult execute() throws Exception {

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // start the data generator
        DataStream<TaxiRide> rides = env.addSource(source);

        // the WatermarkStrategy specifies how to extract timestamps and generate watermarks
        WatermarkStrategy<TaxiRide> watermarkStrategy =
                WatermarkStrategy.<TaxiRide>forBoundedOutOfOrderness(Duration.ofSeconds(60))
                        .withTimestampAssigner(
                                (ride, streamRecordTimestamp) -> ride.getEventTimeMillis());

        // create the pipeline
        rides.assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(ride -> ride.rideId)
                .process(new AlertFunction())
                .addSink(sink);

        // execute the pipeline and return the result
        return env.execute("Long Taxi Rides");
    }

    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {
        LongRidesExercise job =
                new LongRidesExercise(new TaxiRideGenerator(), new PrintSinkFunction<>());

        job.execute();
    }

    @VisibleForTesting
    public static class AlertFunction extends KeyedProcessFunction<Long, TaxiRide, Long> {

        // Tuple2<rideId, timestamp>
        private ValueState<Tuple2<Long, Long>> state;

        @Override
        public void open(Configuration config) throws Exception {
//            throw new MissingSolutionException();
            // 初始化status
            TypeInformation<Tuple2<Long, Long>> typeInfo = new TupleTypeInfo<>(TypeInformation.of(Long.class),
                TypeInformation.of(Long.class));
            state = getRuntimeContext()
                .getState(new ValueStateDescriptor<>("rideTimeState", typeInfo));
        }

        @Override
        public void processElement(TaxiRide ride, Context context, Collector<Long> out) throws Exception {
            // start事件时初始化状态
            if (ride.isStart) {
                if (state.value() != null) {
                    // 说明结束事件先被设置状态了
                    boolean rideTimeLessThanTwoHour = state.value().f1 - ride.getEventTimeMillis() < 7200000L;
                    if (rideTimeLessThanTwoHour) {
                        state.clear();
                    } else {
                        out.collect(state.value().f0);
                    }
                    return;
                }
                Tuple2<Long, Long> rideTimeState = updateState(ride);

                // 从开始事件时间开始注册一个2h的定时器
                context.timerService().registerEventTimeTimer(rideTimeState.f1 + 7200000L);
            } else {
                // 车程结束事件
                // 判断结束时间戳减去开始时间戳是否少于两小时，如少于，则删除状态
                Tuple2<Long, Long> value = state.value();
                // 如果结束事件先到了 设置状态
                if (value == null) {
                    updateState(ride);
                    return;
                }
                boolean rideTimeLessThanTwoHour = ride.getEventTimeMillis() - value.f1 < 7200000L;
                if (rideTimeLessThanTwoHour) {
                    state.clear();
                }
            }
        }

        private Tuple2<Long, Long> updateState(TaxiRide ride) throws IOException {
            Tuple2<Long, Long> rideTimeState = new Tuple2<>();
            rideTimeState.f0 = ride.rideId;
            rideTimeState.f1 = ride.getEventTimeMillis();
            state.update(rideTimeState);
            return rideTimeState;
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<Long> out) throws Exception {
            // 定时器触发时检查状态是否存在，如存在则车程时间超过两小时，输出rideId
            Tuple2<Long, Long> value = state.value();
            if (value != null) {
                out.collect(value.f0);
            }
        }
    }
}
