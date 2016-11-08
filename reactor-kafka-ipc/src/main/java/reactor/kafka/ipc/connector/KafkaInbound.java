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

import reactor.core.publisher.Flux;
import reactor.ipc.connector.Inbound;
import reactor.kafka.receiver.Receiver;
import reactor.kafka.receiver.ReceiverOptions;

public class KafkaInbound<K, V> implements Inbound<V> {

    private final Receiver<K, V> receiver;

    public static <K, V> KafkaInbound<K, V> create(ReceiverOptions<K, V> options) {
        return new KafkaInbound<>(options);
    }


    private KafkaInbound(ReceiverOptions<K, V> receiverOptions) {
        receiver = Receiver.create(receiverOptions);
    }

    @Override
    public Flux<V> receive() {
        return receiver.receive()
                       .map(r -> r.record().value());
    }
}
