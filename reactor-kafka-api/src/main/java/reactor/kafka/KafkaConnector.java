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
package reactor.kafka;

import java.util.function.BiFunction;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.ipc.connector.Inbound;
import reactor.ipc.connector.Outbound;
import reactor.ipc.connector.ConnectedState;
import reactor.ipc.connector.Connector;
import reactor.kafka.inbound.internals.KafkaReceiver;
import reactor.kafka.inbound.InboundOptions;
import reactor.kafka.inbound.InboundRecord;
import reactor.kafka.outbound.OutboundOptions;
import reactor.kafka.outbound.internals.KafkaSender;

public class KafkaConnector<K, V> implements
    Connector<InboundRecord<K, V>, ProducerRecord<K, V>, Inbound<InboundRecord<K, V>>, Outbound<ProducerRecord<K, V>>> {

    private final InboundOptions<K, V> inboundOptions;
    private final OutboundOptions<K, V> outboundOptions;

    /**
     * Creates a reactive receiver and sender with the specified configuration options.
     */
    public static <K, V> KafkaConnector<K, V> create(InboundOptions<K, V> inboundOptions, OutboundOptions<K, V> outboundOptions) {
        return new KafkaConnector<>(inboundOptions, outboundOptions);
    }

    KafkaConnector(InboundOptions<K, V> inboundOptions, OutboundOptions<K, V> outboundOptions) {
        this.inboundOptions = inboundOptions == null ? null : inboundOptions.toImmutable();
        this.outboundOptions = outboundOptions == null ? null : outboundOptions.toImmutable();
    }

    /**
     * Creates a reactive receiver with the specified configuration options.
     */
    public static <K, V> KafkaConnector<K, V> createInbound(InboundOptions<K, V> options) {
        return new KafkaConnector<>(options, null);
    }

    /**
     * Creates a reactive sender with the specified configuration options.
     */
    public static <K, V> KafkaConnector<K, V> createOutbound(OutboundOptions<K, V> options) {
        return new KafkaConnector<>(null, options);
    }

    @Override
    public Mono<? extends ConnectedState> newHandler(BiFunction<? super Inbound<InboundRecord<K, V>>, ? super Outbound<ProducerRecord<K, V>>, ? extends Publisher<Void>> channelHandler) {
        return Mono.create(sink -> {
                KafkaReceiver<K, V> inbound = inboundOptions != null ? new KafkaReceiver<>(inboundOptions) : null;
                KafkaSender<K, V> outbound = outboundOptions  != null ?  new KafkaSender<>(outboundOptions) : null;
                Publisher<Void> closing = channelHandler.apply(inbound, outbound);
                Flux.from(closing)
                    .subscribe(null,
                            t -> tryClose(inbound, outbound, sink, t),
                            () -> tryClose(inbound, outbound, sink, null));
            });
    }

    private void tryClose(KafkaReceiver<K, V> inbound, KafkaSender<K, V> outbound, MonoSink<? extends ConnectedState> sink, Throwable t) {
        try {
            if (inbound != null)
                inbound.close();
        } catch (Exception e) {
            // Ignore
        }
        try {
            if (outbound != null)
                outbound.close();
        } catch (Exception e) {
            // Ignore
        }
        if (t == null)
            sink.success();
        else
            sink.error(t);
    }
}
