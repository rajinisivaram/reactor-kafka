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
package reactor.kafka.inbound;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import reactor.kafka.inbound.internals.ConsumerFactory;

/**
 * Configuration properties for Kafka consumer.
 */
public class InboundOptions<K, V> {

    private final Map<String, Object> properties = new HashMap<>();
    private final List<Consumer<Collection<Partition>>> assignListeners = new ArrayList<>();
    private final List<Consumer<Collection<Partition>>> revokeListeners = new ArrayList<>();

    private AckMode ackMode = AckMode.AUTO_ACK;
    private Duration pollTimeout = Duration.ofMillis(100);
    private Duration closeTimeout = Duration.ofNanos(Long.MAX_VALUE);
    private Duration commitInterval = ConsumerFactory.INSTANCE.defaultAutoCommitInterval();
    private int commitBatchSize = Integer.MAX_VALUE;
    private int maxAutoCommitAttempts = Integer.MAX_VALUE;
    private Collection<String> subscribeTopics;
    private Collection<TopicPartition> assignTopicPartitions;
    private Pattern subscribePattern;

    public static <K, V> InboundOptions<K, V> create() {
        return new InboundOptions<>();
    }

    public static <K, V> InboundOptions<K, V> create(Map<String, Object> configProperties) {
        InboundOptions<K, V> options = create();
        options.properties.putAll(configProperties);
        return options;
    }

    public static <K, V> InboundOptions<K, V> create(Properties configProperties) {
        InboundOptions<K, V> options = create();
        configProperties.forEach((name, value) -> options.properties.put((String) name, value));
        return options;
    }

    private InboundOptions() {
        setDefaultProperties();
    }

    public Map<String, Object> consumerProperties() {
        return properties;
    }

    public Object consumerProperty(String name) {
        return properties.get(name);
    }

    public InboundOptions<K, V> consumerProperty(String name, Object newValue) {
        this.properties.put(name, newValue);
        return this;
    }

    public AckMode ackMode() {
        return ackMode;
    }

    public InboundOptions<K, V> ackMode(AckMode ackMode) {
        this.ackMode = ackMode;
        return this;
    }

    public Duration pollTimeout() {
        return pollTimeout;
    }

    public InboundOptions<K, V> pollTimeout(Duration timeout) {
        this.pollTimeout = timeout;
        return this;
    }

    public Duration closeTimeout() {
        return closeTimeout;
    }

    public InboundOptions<K, V> closeTimeout(Duration timeout) {
        this.closeTimeout = timeout;
        return this;
    }

    /**
     * Adds a listener for partition assignment when group management is used. Applications can
     * use this listener to seek to different offsets of the assigned partitions using
     * any of the seek methods in {@link Partition}.
     */
    public InboundOptions<K, V> addAssignListener(Consumer<Collection<Partition>> onAssign) {
        assignListeners.add(onAssign);
        return this;
    }

    /**
     * Adds a listener for partition revocation when group management is used. Applications
     * can use this listener to commit offsets when ack mode is {@value AckMode#MANUAL_COMMIT}.
     * Acknowledged offsets are committed automatically on revocation for other commit modes.
     */
    public InboundOptions<K, V> addRevokeListener(Consumer<Collection<Partition>> onRevoke) {
        revokeListeners.add(onRevoke);
        return this;
    }

    public InboundOptions<K, V> clearAssignListeners() {
        assignListeners.clear();
        return this;
    }

    public InboundOptions<K, V> clearRevokeListeners() {
        revokeListeners.clear();
        return this;
    }

    public List<Consumer<Collection<Partition>>> assignListeners() {
        return assignListeners;
    }

    public List<Consumer<Collection<Partition>>> revokeListeners() {
        return revokeListeners;
    }

    /**
     * Sets the subscription using group management to the specified topics.
     * This subscription is enabled when a reactive consumer using this options
     * instance is subscribed to. Any existing subscriptions or assignments on this
     * option are deleted.
     */
    public InboundOptions<K, V> subscription(Collection<String> topics) {
        subscribeTopics = new ArrayList<>(topics);
        subscribePattern = null;
        assignTopicPartitions = null;
        return this;
    }

    /**
     * Sets the subscription using group management to the specified pattern.
     * This subscription is enabled when a reactive consumer using this options
     * instance is subscribed to. Any existing subscriptions or assignments on this
     * option are deleted.
     */
    public InboundOptions<K, V> subscription(Pattern pattern) {
        subscribeTopics = null;
        subscribePattern = pattern;
        assignTopicPartitions = null;
        return this;
    }

    /**
     * Sets the subscription using manual assignment to the specified partitions.
     * This assignment is enabled when a reactive consumer using this options
     * instance is subscribed to. Any existing subscriptions or assignments on this
     * option are deleted.
     */
    public InboundOptions<K, V> assignment(Collection<TopicPartition> partitions) {
        subscribeTopics = null;
        subscribePattern = null;
        assignTopicPartitions = new ArrayList<>(partitions);
        return this;
    }

    public Consumer<KafkaConsumer<K, V>> subscriber(ConsumerRebalanceListener listener) {
        if (subscribeTopics != null)
            return consumer -> consumer.subscribe(subscribeTopics, listener);
        else if (subscribePattern != null)
            return consumer -> consumer.subscribe(subscribePattern, listener);
        else if (assignTopicPartitions != null)
            return consumer -> {
                consumer.assign(assignTopicPartitions);
                listener.onPartitionsAssigned(assignTopicPartitions);
            };
        else
            throw new IllegalStateException("No subscriptions have been created");
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

    public InboundOptions<K, V> commitInterval(Duration interval) {
        this.commitInterval = interval;
        return this;
    }

    public int commitBatchSize() {
        return commitBatchSize;
    }

    public InboundOptions<K, V> commitBatchSize(int commitBatchSize) {
        this.commitBatchSize = commitBatchSize;
        return this;
    }

    public int maxAutoCommitAttempts() {
        return maxAutoCommitAttempts;
    }

    public InboundOptions<K, V> maxAutoCommitAttempts(int maxRetries) {
        this.maxAutoCommitAttempts = maxRetries;
        return this;
    }

    private void setDefaultProperties() {
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    }

    public InboundOptions<K, V> toImmutable() {
        InboundOptions<K, V> options = new InboundOptions<K, V>() {

            @Override
            public Map<String, Object> consumerProperties() {
                return Collections.unmodifiableMap(super.properties);
            }

            @Override
            public InboundOptions<K, V> consumerProperty(String name, Object newValue) {
                throw new java.lang.UnsupportedOperationException("Cannot modify immutable options");
            }

            @Override
            public InboundOptions<K, V> addAssignListener(Consumer<Collection<Partition>> onAssign) {
                throw new java.lang.UnsupportedOperationException("Cannot modify immutable options");
            }

            @Override
            public InboundOptions<K, V> addRevokeListener(Consumer<Collection<Partition>> onRevoke) {
                throw new java.lang.UnsupportedOperationException("Cannot modify immutable options");
            }

            @Override
            public InboundOptions<K, V> subscription(Collection<String> topics) {
                throw new java.lang.UnsupportedOperationException("Cannot modify immutable options");
            }

            @Override
            public InboundOptions<K, V> subscription(Pattern pattern) {
                throw new java.lang.UnsupportedOperationException("Cannot modify immutable options");
            }

            @Override
            public InboundOptions<K, V> assignment(Collection<TopicPartition> partitions) {
                throw new java.lang.UnsupportedOperationException("Cannot modify immutable options");
            }

            @Override
            public InboundOptions<K, V> ackMode(AckMode ackMode) {
                throw new java.lang.UnsupportedOperationException("Cannot modify immutable options");
            }

            @Override
            public InboundOptions<K, V> pollTimeout(Duration timeout) {
                throw new java.lang.UnsupportedOperationException("Cannot modify immutable options");
            }

            @Override
            public InboundOptions<K, V> closeTimeout(Duration timeout) {
                throw new java.lang.UnsupportedOperationException("Cannot modify immutable options");
            }

            @Override
            public InboundOptions<K, V> commitInterval(Duration interval) {
                throw new java.lang.UnsupportedOperationException("Cannot modify immutable options");
            }

            @Override
            public InboundOptions<K, V> commitBatchSize(int commitBatchSize) {
                throw new java.lang.UnsupportedOperationException("Cannot modify immutable options");
            }

            @Override
            public InboundOptions<K, V> maxAutoCommitAttempts(int maxRetries) {
                throw new java.lang.UnsupportedOperationException("Cannot modify immutable options");
            }

        };
        options.properties.putAll(properties);
        options.assignListeners.addAll(assignListeners);
        options.revokeListeners.addAll(revokeListeners);
        if (subscribeTopics != null)
            options.subscribeTopics = new ArrayList<>(subscribeTopics);
        if (assignTopicPartitions != null)
            options.assignTopicPartitions = new ArrayList<>(assignTopicPartitions);
        options.subscribePattern = subscribePattern;
        options.ackMode = ackMode;
        options.pollTimeout = pollTimeout;
        options.closeTimeout = closeTimeout;
        options.commitInterval = commitInterval;
        options.commitBatchSize = commitBatchSize;
        options.maxAutoCommitAttempts = maxAutoCommitAttempts;
        return options;
    }
}
