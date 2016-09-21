/**
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
 **/
package reactor.kafka.receiver;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import reactor.kafka.receiver.internals.ConsumerFactory;

/**
 * Configuration properties for Kafka receiver.
 */
public class ReceiverOptions<K, V> {

    private final Map<String, Object> properties = new HashMap<>();

    private AckMode ackMode = AckMode.AUTO_ACK;
    private Duration pollTimeout = Duration.ofMillis(100);
    private Duration closeTimeout = Duration.ofNanos(Long.MAX_VALUE);
    private Duration commitInterval = ConsumerFactory.INSTANCE.defaultAutoCommitInterval();
    private int commitBatchSize = Integer.MAX_VALUE;
    private int maxAutoCommitAttempts = Integer.MAX_VALUE;

    public ReceiverOptions() {
        setDefaultProperties();
    }

    public ReceiverOptions(Map<String, Object> configProperties) {
        setDefaultProperties();
        this.properties.putAll(configProperties);
    }

    public ReceiverOptions(Properties configProperties) {
        setDefaultProperties();
        configProperties.forEach((name, value) -> this.properties.put((String) name, value));
    }

    public Map<String, Object> consumerProperties() {
        return properties;
    }

    public Object consumerProperty(String name) {
        return properties.get(name);
    }

    public ReceiverOptions<K, V> consumerProperty(String name, Object newValue) {
        this.properties.put(name, newValue);
        return this;
    }

    public AckMode ackMode() {
        return ackMode;
    }

    public ReceiverOptions<K, V> ackMode(AckMode ackMode) {
        this.ackMode = ackMode;
        return this;
    }

    public Duration pollTimeout() {
        return pollTimeout;
    }

    public ReceiverOptions<K, V> pollTimeout(Duration timeout) {
        this.pollTimeout = timeout;
        return this;
    }

    public Duration closeTimeout() {
        return closeTimeout;
    }

    public ReceiverOptions<K, V> closeTimeout(Duration timeout) {
        this.closeTimeout = timeout;
        return this;
    }

    public String groupId() {
        return ConsumerFactory.INSTANCE.groupId(this);
    }

    public Duration heartbeatInterval() {
        return ConsumerFactory.INSTANCE.heartbeatInterval(this);
    }

    public Duration commitInterval() {
        return commitInterval;
    }

    public ReceiverOptions<K, V> commitInterval(Duration interval) {
        this.commitInterval = interval;
        return this;
    }

    public int commitBatchSize() {
        return commitBatchSize;
    }

    public ReceiverOptions<K, V> commitBatchSize(int commitBatchSize) {
        this.commitBatchSize = commitBatchSize;
        return this;
    }

    public int maxAutoCommitAttempts() {
        return maxAutoCommitAttempts;
    }

    public ReceiverOptions<K, V> maxAutoCommitAttempts(int maxRetries) {
        this.maxAutoCommitAttempts = maxRetries;
        return this;
    }

    private void setDefaultProperties() {
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    }

    public ReceiverOptions<K, V> toImmutable() {
        ReceiverOptions<K, V> options = new ReceiverOptions<K, V>(properties) {

            @Override
            public Map<String, Object> consumerProperties() {
                return Collections.unmodifiableMap(super.properties);
            }

            @Override
            public ReceiverOptions<K, V> consumerProperty(String name, Object newValue) {
                throw new java.lang.UnsupportedOperationException("Cannot modify immutable options");
            }

            @Override
            public ReceiverOptions<K, V> ackMode(AckMode ackMode) {
                throw new java.lang.UnsupportedOperationException("Cannot modify immutable options");
            }

            @Override
            public ReceiverOptions<K, V> pollTimeout(Duration timeout) {
                throw new java.lang.UnsupportedOperationException("Cannot modify immutable options");
            }

            @Override
            public ReceiverOptions<K, V> closeTimeout(Duration timeout) {
                throw new java.lang.UnsupportedOperationException("Cannot modify immutable options");
            }

            @Override
            public ReceiverOptions<K, V> commitInterval(Duration interval) {
                throw new java.lang.UnsupportedOperationException("Cannot modify immutable options");
            }

            @Override
            public ReceiverOptions<K, V> commitBatchSize(int commitBatchSize) {
                throw new java.lang.UnsupportedOperationException("Cannot modify immutable options");
            }

            @Override
            public ReceiverOptions<K, V> maxAutoCommitAttempts(int maxRetries) {
                throw new java.lang.UnsupportedOperationException("Cannot modify immutable options");
            }

        };
        options.ackMode = ackMode;
        options.pollTimeout = pollTimeout;
        options.closeTimeout = closeTimeout;
        options.commitInterval = commitInterval;
        options.commitBatchSize = commitBatchSize;
        options.maxAutoCommitAttempts = maxAutoCommitAttempts;
        return options;
    }
}
