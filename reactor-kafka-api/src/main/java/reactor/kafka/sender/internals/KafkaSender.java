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
package reactor.kafka.sender.internals;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.sender.Sender;
import reactor.kafka.sender.SenderOptions;
import reactor.util.concurrent.QueueSupplier;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

/**
 * Reactive sender that sends messages to Kafka topic partitions. The sender is thread-safe
 * and can be used to send messages to multiple partitions. It is recommended that a single
 * producer is shared for each message type in a client.
 *
 * @param <K> outgoing message key type
 * @param <V> outgoing message value type
 */
public class KafkaSender<K, V> implements Sender<K, V> {

    private static final Logger log = LoggerFactory.getLogger(KafkaSender.class.getName());

    private final Mono<KafkaProducer<K, V>> producerMono;
    private final AtomicBoolean hasProducer = new AtomicBoolean();
    private final SenderOptions<K, V> senderOptions;
    private final Scheduler scheduler = Schedulers.single();

    /**
     * Constructs a sender with the specified configuration properties. All Kafka
     * producer properties are supported.
     */
    public KafkaSender(SenderOptions<K, V> options) {
        this.senderOptions = options.toImmutable();
        this.producerMono = Mono.fromCallable(() -> {
                return ProducerFactory.createProducer(senderOptions);
            })
            .cache()
            .doOnSubscribe(s -> hasProducer.set(true));
    }

    /*
     * (non-Javadoc)
     * @see reactor.kafka.sender.Sender#send(org.reactivestreams.Publisher)
     */
    @Override
    public <T> Flux<Tuple2<RecordMetadata, T>> send(Publisher<Tuple2<ProducerRecord<K, V>, T>> records) {
        Flux<Tuple2<RecordMetadata, T>> flux = outboundFlux(records, false);
        return flux.publishOn(scheduler, QueueSupplier.SMALL_BUFFER_SIZE);
    }

    /*
     * (non-Javadoc)
     * @see reactor.kafka.sender.Sender#send(org.reactivestreams.Publisher, reactor.core.scheduler.Scheduler, int, boolean)
     */
    @Override
    public <T> Flux<Tuple2<RecordMetadata, T>> send(Publisher<Tuple2<ProducerRecord<K, V>, T>> records,
            Scheduler scheduler, int maxInflight, boolean delayError) {
        return outboundFlux(records, delayError).publishOn(scheduler, maxInflight);
    }

    /*
     * (non-Javadoc)
     * @see reactor.kafka.sender.Sender#sendAll(org.reactivestreams.Publisher)
     */
    @Override
    public Mono<Void> sendAll(Publisher<ProducerRecord<K, V>> records) {
        // TODO: Check that Mono can't block sender network thread
        return new Mono<Void>() {
            @Override
            public void subscribe(Subscriber<? super Void> s) {
                records.subscribe(new SendSubscriberMono(s));
            }

        }.publishOn(scheduler);
    }

    /*
     * (non-Javadoc)
     * @see reactor.kafka.sender.Sender#partitionsFor(java.lang.String)
     */
    @Override
    public Flux<PartitionInfo> partitionsFor(String topic) {
        return producerMono
                .flatMap(producer -> Flux.fromIterable(producer.partitionsFor(topic)));
    }

    /**
     * Closes this producer and releases all resources allocated to it.
     */
    public void close() {
        if (hasProducer.getAndSet(false))
            producerMono.block().close(senderOptions.closeTimeout().toMillis(), TimeUnit.MILLISECONDS);
    }

    private <T> Flux<Tuple2<RecordMetadata, T>> outboundFlux(Publisher<Tuple2<ProducerRecord<K, V>, T>> records, boolean delayError) {
        return new Flux<Tuple2<RecordMetadata, T>>() {
            @Override
            public void subscribe(Subscriber<? super Tuple2<RecordMetadata, T>> s) {
                records.subscribe(new SendSubscriber<T>(s, delayError));
            }
        };
    }

    private enum SubscriberState {
        INIT,
        ACTIVE,
        OUTBOUND_DONE,
        COMPLETE,
        FAILED
    }

    private abstract class AbstractSendSubscriber<Q, S, C> implements Subscriber<Q> {
        protected final Subscriber<? super S> actual;
        private final boolean delayError;
        private KafkaProducer<K, V> producer;
        private AtomicInteger inflight = new AtomicInteger();
        private SubscriberState state;
        private AtomicReference<Throwable> firstException = new AtomicReference<>();

        AbstractSendSubscriber(Subscriber<? super S> actual, boolean delayError) {
            this.actual = actual;
            this.delayError = delayError;
            this.state = SubscriberState.INIT;
        }

        @Override
        public void onSubscribe(Subscription s) {
            this.state = SubscriberState.ACTIVE;
            producer = producerMono.block();
            actual.onSubscribe(s);
        }

        @Override
        public void onNext(Q m) {
            if (state == SubscriberState.FAILED)
                return;
            else if (state == SubscriberState.COMPLETE) {
                Operators.onNextDropped(m);
                return;
            }
            inflight.incrementAndGet();
            C correlator = correlator(m);
            try {
                producer.send(producerRecord(m), (metadata, exception) -> {
                        boolean complete = inflight.decrementAndGet() == 0 && state == SubscriberState.OUTBOUND_DONE;
                        try {
                            if (exception == null) {
                                handleResponse(metadata, correlator);
                                if (complete)
                                    complete();
                            } else
                                error(metadata, exception, correlator, complete);
                        } catch (Exception e) {
                            error(metadata, e, correlator, complete);
                        }
                    });
            } catch (Exception e) {
                inflight.decrementAndGet();
                error(null, e, correlator, true);
            }
        }

        @Override
        public void onError(Throwable t) {
            if (state == SubscriberState.FAILED)
                return;
            else if (state == SubscriberState.COMPLETE) {
                Operators.onErrorDropped(t);
                return;
            }
            state = SubscriberState.FAILED;
            actual.onError(t);
        }

        @Override
        public void onComplete() {
            if (state == SubscriberState.COMPLETE)
                return;
            state = SubscriberState.OUTBOUND_DONE;
            if (inflight.get() == 0) {
                complete();
            }
        }

        private void complete() {
            Throwable exception = firstException.getAndSet(null);
            if (delayError && exception != null) {
                onError(exception);
            } else {
                state = SubscriberState.COMPLETE;
                actual.onComplete();
            }
        }

        public void error(RecordMetadata metadata, Throwable t, C correlator, boolean complete) {
            log.error("error {}", t);
            firstException.compareAndSet(null, t);
            if (delayError)
                handleResponse(metadata, correlator);
            if (!delayError || complete)
                onError(t);
        }

        protected abstract void handleResponse(RecordMetadata metadata, C correlator);
        protected abstract ProducerRecord<K, V> producerRecord(Q request);
        protected abstract C correlator(Q request);
    }

    private class SendSubscriber<T> extends AbstractSendSubscriber<Tuple2<ProducerRecord<K, V>, T>, Tuple2<RecordMetadata, T>, T> {

        SendSubscriber(Subscriber<? super Tuple2<RecordMetadata, T>> actual, boolean delayError) {
           super(actual, delayError);
        }

        @Override
        protected void handleResponse(RecordMetadata metadata, T correlator) {
            actual.onNext(Tuples.of(metadata, correlator));
        }

        @Override
        protected T correlator(Tuple2<ProducerRecord<K, V>, T> request) {
            return request.getT2();
        }

        @Override
        protected ProducerRecord<K, V> producerRecord(Tuple2<ProducerRecord<K, V>, T> request) {
            return request.getT1();
        }

    }

    private class SendSubscriberMono extends AbstractSendSubscriber<ProducerRecord<K, V>, Void, Void> {

        SendSubscriberMono(Subscriber<? super Void> actual) {
           super(actual, false);
        }

        @Override
        protected void handleResponse(RecordMetadata metadata, Void correlator) {
        }

        @Override
        protected Void correlator(ProducerRecord<K, V> request) {
            return null;
        }

        @Override
        protected ProducerRecord<K, V> producerRecord(ProducerRecord<K, V> request) {
            return request;
        }
    }
}
