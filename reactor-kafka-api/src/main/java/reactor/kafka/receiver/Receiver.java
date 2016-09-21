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

import java.util.Collection;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import org.apache.kafka.common.TopicPartition;

import reactor.core.publisher.Flux;
import reactor.kafka.receiver.internals.KafkaReceiver;

/**
 * A reactive Kafka consumer that consumes messages from a Kafka cluster.
 *
 * @param <K> incoming message key type
 * @param <V> incoming message value type
 */
public interface Receiver<K, V> {

    /**
     * Creates a reactive receiver with the specified configuration options.
     */
    public static <K, V> Receiver<K, V> create(ReceiverOptions<K, V> options) {
        return new KafkaReceiver<>(options);
    }

    /**
     * Returns a Kafka flux that consumes messages from the specified collection of topics using topic
     * subscription with group management.
     */
    public Flux<ReceiverMessage<K, V>> receive(Collection<String> topics);

    /**
     * Returns a Kafka flux that consumes messages from the topics that match the specified pattern using topic
     * subscription with group management.
     */
    public Flux<ReceiverMessage<K, V>> receive(Pattern pattern);

    /**
     * Returns a Kafka flux that consumes messages from the specified collection of topic partitions using
     * manual partition assignment.
     */
    public Flux<ReceiverMessage<K, V>> receivePartitions(Collection<TopicPartition> topicPartitions);

    /**
     * Adds a listener for partition assignment when group management is used. Applications can
     * use this listener to seek to different offsets of the assigned partitions using
     * any of the seek methods in {@link ReceiverPartition}.
     */
    public Receiver<K, V> doOnPartitionsAssigned(Consumer<Collection<ReceiverPartition>> onAssign);

    /**
     * Adds a listener for partition revocation when group management is used. Applications
     * can use this listener to commit offsets when ack mode is {@value AckMode#MANUAL_COMMIT}.
     * Acknowledged offsets are committed automatically on revocation for other commit modes.
     */
    public Receiver<K, V> doOnPartitionsRevoked(Consumer<Collection<ReceiverPartition>> onRevoke);
}
