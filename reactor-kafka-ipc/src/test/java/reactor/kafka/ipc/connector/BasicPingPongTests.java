/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.kafka.ipc.connector;

import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.Rule;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import reactor.core.Cancellation;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.ipc.stream.Ipc;
import reactor.ipc.stream.StreamContext;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.SenderOptions;
import reactor.util.Logger;
import reactor.util.Loggers;

public class BasicPingPongTests {

    static final Logger log = Loggers.getLogger(BasicPingPongTests.class);

    private String clientTopic = "clientTopic";
    private String serverTopic = "serverTopic";
    @Rule
    public KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, 1, clientTopic, serverTopic);

    public SenderOptions<Integer, byte[]> createSenderOptions() {
        Map<String, Object> props = KafkaTestUtils.producerProps(embeddedKafka);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        return SenderOptions.create(props);
    }

    public ReceiverOptions<Integer, byte[]> createReceiveOptions(String groupId, String topic) {
        Map<String, Object> props = KafkaTestUtils.consumerProps("", "false", embeddedKafka);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        ReceiverOptions<Integer, byte[]> receiverOptions = ReceiverOptions.create(props);
        receiverOptions.subscription(Collections.singleton(topic));
        return receiverOptions;
    }

    public interface PingPongClientAPI extends Cancellation {

        @Ipc
        Mono<Integer> pong2(Mono<Integer> ping);

        @Ipc
        Flux<Integer> pong(Publisher<Integer> ping);

        @Ipc
        void send(Publisher<Integer> values);

        @Ipc
        void send2(Publisher<Integer> values);

        @Ipc
        Flux<Integer> receive();

        @Ipc
        Mono<Integer> receive2();

        @Ipc
        void umap(Function<Flux<Integer>, Publisher<Integer>> mapper);

        @Ipc
        void send3(Publisher<Integer> v);

        @Ipc
        Mono<Integer> receive3();

        @Ipc
        Mono<Integer> map3(Flux<Integer> v1);
    }

    public static class PingPongServerAPI {

        @Ipc
        public void send3(StreamContext<Void> ctx, Mono<Integer> v1) {
            v1.log("send3").subscribe();
        }

        @Ipc
        public Mono<Integer> receive3(StreamContext<Void> ctx) {
            log.info("Server: receive3()");
            return Mono.just(33);
        }

        @Ipc
        public Publisher<Integer> map3(StreamContext<Void> ctx, Flux<Integer> v1) {
            log.info("Server: map3()");
            return v1.scan((p, n) -> p + n);
        }

        @Ipc
        public Publisher<Integer> pong2(StreamContext<Void> ctx, Publisher<Integer> ping) {
            log.info("Server: pong2()");
            return Flux.from(ping).map(v -> v + 1);
        }

        @Ipc
        public Publisher<Integer> pong(StreamContext<Void> ctx, Publisher<Integer> ping) {
            log.info("Server: pong()");
            return Flux.from(ping).map(v -> v + 1);
        }

        @Ipc
        public void send(StreamContext<Void> ctx, Publisher<Integer> values) {
            log.info("Server: send()");
            Flux.from(values).subscribe(v -> {
                    log.info("Server: " + v);
                }, Throwable::printStackTrace);
        }

        @Ipc
        public void send2(StreamContext<Void> ctx, Publisher<Integer> values) {
            send(ctx, values);
        }

        @Ipc
        public Publisher<Integer> receive(StreamContext<Void> ctx) {
            log.info("Server: receive()");
            return Flux.range(1, 1000);
        }

        @Ipc
        public Publisher<Integer> receive2(StreamContext<Void> ctx) {
            return receive(ctx);
        }

        @Ipc
        public Publisher<Integer> umap(StreamContext<Void> ctx, Flux<Integer> values) {
            log.info("Server: umap()");
            values.subscribe(v -> {
                    log.info("Server: " + v);
                }, Throwable::printStackTrace);

            return Flux.just(50);
        }
    }

    static void print(Publisher<?> p) {
        log.info(Flux.from(p).blockLast().toString());
    }

    @Test
    public void pingPong() throws Exception {

        Cancellation c = KafkaClient.create(createReceiveOptions("server", serverTopic), createSenderOptions(), clientTopic)
                .newReceiver(PingPongServerAPI::new).block();
        PingPongClientAPI api = KafkaClient.create(createReceiveOptions("client", clientTopic), createSenderOptions(), serverTopic)
                .newProducer(PingPongClientAPI.class).block();

        api.send3(Flux.just(1, 2));

        Thread.sleep(1000);

        log.info(api.receive3().block().toString());

        log.info(api.map3(Flux.just(1, 2)).block().toString());

        log.info("-----------");

        log.info("Map:");
        print(api.pong(Flux.just(1)));

        log.info("Sync map:");
        log.info(api.pong2(Mono.just(2)).block().toString());

        log.info("Send:");
        api.send(Flux.just(20));

        Thread.sleep(200);

        log.info("Send:");
        api.send2(Mono.just(25));

        Thread.sleep(200);

        log.info("Receive:");
        long t = System.currentTimeMillis();
        print(api.receive());

        log.info("t = " + (System.currentTimeMillis() - t));

        log.info("Receive sync:");
        log.info(api.receive2().block().toString());

        log.info("Umap:");
        api.umap(o -> o.map(v -> -v));

        Thread.sleep(5000);

        api.dispose();

        c.dispose();
    }

    public interface StreamPerfClientAPI extends Cancellation {

        @Ipc
        Publisher<Integer> range(Publisher<Integer> count);
    }

    public static final class StreamPerfServerAPI {

        @Ipc
        public Publisher<Integer> range(StreamContext<?> ctx, Publisher<Integer> count) {
            // log.info("Server: range");
            return Flux.from(count).concatMap(v -> Flux.range(1, v));
        }
    }

    @Test
    public void streamPerf() throws Exception {

        Cancellation c = KafkaClient.create(createReceiveOptions("server", serverTopic), createSenderOptions(), clientTopic)
                .newReceiver(StreamPerfServerAPI::new).block();
        StreamPerfClientAPI api = KafkaClient.create(createReceiveOptions("client", clientTopic), createSenderOptions(), serverTopic)
                .newProducer(StreamPerfClientAPI.class).block();

        int n = 100_000;

        for (int i = 1; i <= n; i *= 10) {

            System.out.printf("%6d | %n", i);

            for (int j = 0; j < 10; j++) {
                long t = System.nanoTime();

                long count = Flux.from(api.range(Flux.just(i))).publishOn(Schedulers.immediate()).count().block();

                t = System.nanoTime() - t;

                System.out.printf("-> %6d", count);
                System.out.printf("          %.3f ms/op%n", t / 1024d / 1024d);
            }
        }

        api.dispose();
        c.dispose();
    }
}
