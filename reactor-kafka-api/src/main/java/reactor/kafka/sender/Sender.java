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
package reactor.kafka.sender;

import java.util.List;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.kafka.sender.internals.KafkaSender;
import reactor.util.function.Tuple2;

/**
 * Reactive sender that sends messages to Kafka topic partitions. The sender is thread-safe
 * and can be used to send messages to multiple partitions. It is recommended that a single
 * sender is shared for each message type in a client application.
 *
 * @param <K> outgoing message key type
 * @param <V> outgoing message value type
 */
public interface Sender<K, V> {

    /**
     * Creates a Kafka sender that appends messages to Kafka topic partitions.
     */
    public static <K, V> Sender<K, V> create(SenderOptions<K, V> options) {
        return new KafkaSender<>(options);
    }

    /**
     * Sends a sequence of records to Kafka and returns a flux of response record metadata including
     * partition and offset of each send request. Ordering of responses is guaranteed for partitions,
     * but responses from different partitions may be interleaved in a different order from the requests.
     * Additional correlation data may be passed through that is not sent to Kafka, but is included
     * in the response flux to enable matching responses to requests.
     * Example usage:
     * <pre>
     * {@code
     *     sender.send(Flux.range(1, count)
     *                     .map(i -> Tuples.of(new ProducerRecord<>(topic, key(i), message(i)), i)))
     *           .doOnNext(r -> System.out.println("Message #" + r.getT2() + " metadata=" + r.getT1()));
     * }
     * </pre>
     *
     * @param records Records to send to Kafka with additional data of type <T> included in the returned flux
     * @return Flux of Kafka response record metadata along with the corresponding request correlation data
     */
    public <T> Flux<Tuple2<RecordMetadata, T>> send(Publisher<Tuple2<ProducerRecord<K, V>, T>> records);

    /**
     * Sends a sequence of records to Kafka and returns a flux of response record metadata including
     * partition and offset of each send request. Ordering of responses is guaranteed for partitions,
     * but responses from different partitions may be interleaved in a different order from the requests.
     * Additional correlation data may be passed through that is not sent to Kafka, but is included
     * in the response flux to enable matching responses to requests.
     * Example usage:
     * <pre>
     * {@code
     *     source = Flux.range(1, count)
     *                  .map(i -> Tuples.of(new ProducerRecord<>(topic, key(i), message(i)), i));
     *     sender.send(source, Schedulers.newSingle("send"), 1024, false)
     *           .doOnNext(r -> System.out.println("Message #" + r.getT2() + " metadata=" + r.getT1()));
     * }
     * </pre>
     *
     * @param records Sequence of publisher records along with additional data to be included in response
     * @param scheduler Scheduler to publish on
     * @param maxInflight Maximum number of records in flight
     * @param delayError If false, send terminates when a response indicates failure, otherwise send is attempted for all records
     * @return Flux of Kafka response record metadata along with the corresponding request correlation data
     */
    public <T> Flux<Tuple2<RecordMetadata, T>> send(Publisher<Tuple2<ProducerRecord<K, V>, T>> records,
            Scheduler scheduler, int maxInflight, boolean delayError);

    /**
     * Sends a sequence of records to Kafka.
     * @return Mono that completes when all records are delivered to Kafka. The mono fails if any
     *         record could not be successfully delivered to Kafka.
     */
    public Mono<Void> sendAll(Publisher<ProducerRecord<K, V>> records);

    /**
     * Returns partition information for the specified topic. This is useful for
     * choosing partitions to which records are sent if default partition assignor is not used.
     */
    public Mono<List<PartitionInfo>> partitionsFor(String topic);

    /**
     * Closes this producer and releases all resources allocated to it.
     */
    public void close();

}
