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

package org.apache.flink.training.exercises.hourlytips;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.training.exercises.common.utils.ExerciseBase;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * The "Long Ride Alerts" exercise of the Flink training in the docs.
 *
 * <p>The goal for this exercise is to emit START events for taxi rides that have not been matched
 * by an END event during the first 2 hours of the ride.
 */
public class AccountBalanceLow extends ExerciseBase {

    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    final OutputTag<String> outputTag = new OutputTag<String>("balance-changed-output") {
    };
    private static final Long ACCOUNT_BALANCE_LOW_THRESHOLD = 60L;

    public static void main(String[] args) throws Exception {

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(ExerciseBase.parallelism);

        // start the data generator
        DataStream<AccountUpdatedEvent> eventDataStream = env.fromCollection(getTestEvents())
                .keyBy(AccountUpdatedEvent::getAccountId)
                .flatMap(new UpdateBalanceDataState())
                .keyBy(AccountUpdatedEvent::getAccountId);


        AsyncDataStream.unorderedWait(eventDataStream, new AsyncDatabaseRequest(), 1000, TimeUnit.MILLISECONDS, 100)
                .map(new MapFunction<Tuple2<AccountUpdatedEvent, EnrichedData>, Tuple2<AccountUpdatedEvent, Boolean>>() {
                    @Override
                    public Tuple2<AccountUpdatedEvent, Boolean> map(Tuple2<AccountUpdatedEvent, EnrichedData> value) throws Exception {
                        return Tuple2.of(value.f0, value.f0.balance < value.f1.trendLineValue);
                    }
                })
                .keyBy(d -> d.f0.accountId)
                .flatMap(new UpdateBalanceLowState()).print();


        env.execute("Account balance low");

        //TODO ValueState is probably stored on a per-key basis so MapState is overkill


    }

    private static List<AccountUpdatedEvent> getTestEvents() {
        ArrayList<AccountUpdatedEvent> arrayList = new ArrayList<>();
        arrayList.add(AccountUpdatedEvent.of("123", 100));
        arrayList.add(AccountUpdatedEvent.of("321", 50));
        arrayList.add(AccountUpdatedEvent.of("123", 50));
        arrayList.add(AccountUpdatedEvent.of("321", 50));
        arrayList.add(AccountUpdatedEvent.of("123", 10));
        arrayList.add(AccountUpdatedEvent.of("123", 200));
        return arrayList;
    }

    public static class AccountUpdatedEvent {
        private String accountId;
        private long balance;

        public AccountUpdatedEvent() {
        }

        public static AccountUpdatedEvent of(String accountId, long balance) {
            AccountUpdatedEvent e = new AccountUpdatedEvent();
            e.accountId = accountId;
            e.balance = balance;
            return e;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AccountUpdatedEvent that = (AccountUpdatedEvent) o;
            return balance == that.balance &&
                    Objects.equals(accountId, that.accountId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(accountId, balance);
        }

        public String getAccountId() {
            return accountId;
        }

        public void setAccountId(String accountId) {
            this.accountId = accountId;
        }

        public long getBalance() {
            return balance;
        }

        public void setBalance(long balance) {
            this.balance = balance;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("AccountUpdatedEvent{");
            sb.append("accountId='").append(accountId).append('\'');
            sb.append(", balance=").append(balance);
            sb.append('}');
            return sb.toString();
        }
    }

    public static class UpdateBalanceLowState extends RichFlatMapFunction<Tuple2<AccountUpdatedEvent, Boolean>, Tuple2<AccountUpdatedEvent, Boolean>> {
        private MapState<String, Boolean> accountBalanceLowState;

        @Override
        public void open(Configuration config) {

            MapStateDescriptor<String, Boolean> mapStateDescriptor = new MapStateDescriptor<String, Boolean>(
                    "account-balance-low-state",
                    String.class,
                    Boolean.class

            );
            accountBalanceLowState = getRuntimeContext().getMapState(mapStateDescriptor);
        }

        @Override
        public void flatMap(Tuple2<AccountUpdatedEvent, Boolean> value, Collector<Tuple2<AccountUpdatedEvent, Boolean>> out) throws Exception {
            // If account balance is low and used to be high
            boolean stateChangeHasHappened = !accountBalanceLowState.contains(value.f0.accountId) || accountBalanceLowState.get(value.f0.accountId) == !value.f1;
            // Save state, emit
            if (stateChangeHasHappened) {
                out.collect(value);
            }
            accountBalanceLowState.put(value.f0.accountId, value.f1);
        }
    }

    public static class UpdateBalanceDataState extends RichFlatMapFunction<AccountUpdatedEvent, AccountUpdatedEvent> {
        private MapState<String, Long> accountBalance;

        @Override
        public void open(Configuration config) {

            MapStateDescriptor<String, Long> mapStateDescriptor = new MapStateDescriptor<String, Long>(
                    "account-balance-state",
                    String.class,
                    Long.class

            );
            accountBalance = getRuntimeContext().getMapState(mapStateDescriptor);
        }

        @Override
        public void flatMap(AccountUpdatedEvent value, Collector<AccountUpdatedEvent> out) throws Exception {
            boolean hasBeenUpdated = !accountBalance.contains(value.accountId) || !accountBalance.get(value.accountId).equals(value.balance);
            if (hasBeenUpdated) {
                accountBalance.put(value.accountId, value.balance);
                out.collect(value);
            }
        }
    }

    static class EnrichedData {
        private Long trendLineValue;

        public EnrichedData() {
        }

        public static EnrichedData of(Long trendLineValue) {
            EnrichedData d = new EnrichedData();
            d.trendLineValue = trendLineValue;
            return d;
        }

        public Long getTrendLineValue() {
            return trendLineValue;
        }

        public void setTrendLineValue(Long trendLineValue) {
            this.trendLineValue = trendLineValue;
        }
    }

    static class AsyncDatabaseRequest extends RichAsyncFunction<AccountUpdatedEvent, Tuple2<AccountUpdatedEvent, EnrichedData>> {

        @Override
        public void open(Configuration parameters) throws Exception {
            //client = new DatabaseClient(host, post, credentials);
        }

        @Override
        public void close() throws Exception {
            //client.close();
        }

        @Override
        public void asyncInvoke(AccountUpdatedEvent input, ResultFuture<Tuple2<AccountUpdatedEvent, EnrichedData>> resultFuture) throws Exception {
            resultFuture.complete(Collections.singleton(new Tuple2<>(input, EnrichedData.of(ACCOUNT_BALANCE_LOW_THRESHOLD))));
        }
    }

    public static class DataUpdatedStateChanged extends RichFilterFunction<AccountUpdatedEvent> {

        private MapState<String, Long> accountBalance;

        @Override
        public boolean filter(AccountUpdatedEvent value) throws Exception {
            return !accountBalance.contains(value.accountId) || !accountBalance.get(value.accountId).equals(value.balance);
        }

        public AccountUpdatedEvent updateAccountBalanceState(AccountUpdatedEvent e) throws Exception {
            accountBalance.put(e.accountId, e.balance);
            return e;
        }

        @Override
        public void open(Configuration config) {

            MapStateDescriptor<String, Long> mapStateDescriptor = new MapStateDescriptor<String, Long>(
                    "account-balance-state",
                    String.class,
                    Long.class

            );
            accountBalance = getRuntimeContext().getMapState(mapStateDescriptor);
        }
    }
}
