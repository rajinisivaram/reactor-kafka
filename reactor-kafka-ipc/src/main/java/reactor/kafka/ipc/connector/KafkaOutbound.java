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
package reactor.kafka.ipc.connector;


import org.apache.kafka.clients.producer.ProducerRecord;
import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.ipc.connector.Outbound;
import reactor.kafka.sender.Sender;
import reactor.kafka.sender.SenderOptions;

public class KafkaOutbound<K, V> implements Outbound<V> {

    private final Sender<K, V> sender;
    private final String topic;
    private final K key;

    public static <K, V> KafkaOutbound<K, V> create(SenderOptions<K, V> options, String topic, K key) {
        return new KafkaOutbound<>(options, topic, key);
    }

    private KafkaOutbound(SenderOptions<K, V> options, String topic, K key) {
        this.sender = Sender.create(options);
        this.topic = topic;
        this.key = key;
    }

    @Override
    public Mono<Void> send(Publisher<? extends V> dataStream) {
        return sender.send(Flux.from(dataStream)
                                .map(value -> new ProducerRecord<>(topic, key, value)));
    }

    public void close() {
        sender.close();
    }
}
