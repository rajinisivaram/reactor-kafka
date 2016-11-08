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

import java.util.function.BiFunction;

import org.reactivestreams.Publisher;

import reactor.core.Cancellation;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.ipc.connector.Inbound;
import reactor.ipc.connector.Outbound;
import reactor.ipc.connector.Connector;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.SenderOptions;

public class KafkaConnector<K, IN, OUT> implements Connector<IN, OUT, Inbound<IN>, Outbound<OUT>> {

    private final ReceiverOptions<K, IN> receiverOptions;
    private final SenderOptions<K, OUT> senderOptions;
    private final String senderTopic;
    private final K senderKey;

    /**
     * Creates a reactive receiver and sender with the specified configuration options.
     */
    public static <K, IN, OUT> KafkaConnector<K, IN, OUT> create(ReceiverOptions<K, IN> receiverOptions, SenderOptions<K, OUT> senderOptions, String senderTopic, K senderKey) {
        return new KafkaConnector<>(receiverOptions, senderOptions, senderTopic, senderKey);
    }

    protected KafkaConnector(ReceiverOptions<K, IN> receiverOptions, SenderOptions<K, OUT> senderOptions, String senderTopic, K senderKey) {
        this.receiverOptions = receiverOptions == null ? null : receiverOptions.toImmutable();
        this.senderOptions = senderOptions == null ? null : senderOptions.toImmutable();
        this.senderTopic = senderTopic;
        this.senderKey = senderKey;
    }

    @Override
    public Mono<? extends Cancellation> newHandler(BiFunction<? super Inbound<IN>, ? super Outbound<OUT>, ? extends Publisher<Void>> ioHandler) {

        return Mono.create(sink -> {
                KafkaInbound<K, IN> receiver = receiverOptions != null ? KafkaInbound.create(receiverOptions) : null;
                KafkaOutbound<K, OUT> sender = senderOptions  != null ?  KafkaOutbound.create(senderOptions, senderTopic, senderKey) : null;
                sink.success(new Cancellation() {
                    @Override
                    public void dispose() {
                        close(receiver, sender);
                    }
                });
                Publisher<Void> closing = ioHandler.apply(receiver, sender);
                Flux.from(closing)
                    .subscribe(null,
                            t -> tryClose(receiver, sender, sink, t),
                            () -> tryClose(receiver, sender, sink, null));
            });
    }

    private void tryClose(KafkaInbound<K, IN> inbound, KafkaOutbound<K, OUT> outbound, MonoSink<? extends Cancellation> sink, Throwable t) {
        close(inbound, outbound);
        if (t == null)
            sink.success();
        else
            sink.error(t);
    }

    private void close(KafkaInbound<K, IN> inbound, KafkaOutbound<K, OUT> outbound) {
        // Inbound connections are closed when consumer is no longer consuming, no additional cleanup required
        try {
            if (outbound != null)
                outbound.close();
        } catch (Exception e) {
            // Ignore
        }
    }
}
