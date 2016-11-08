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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.ipc.connector.Inbound;
import reactor.ipc.connector.Outbound;
import reactor.ipc.stream.StreamConnector;
import reactor.ipc.stream.StreamOperations;
import reactor.ipc.stream.StreamOutbound;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.SenderOptions;

public final class KafkaClient extends KafkaConnector<Integer, byte[], byte[]> implements StreamConnector<byte[], byte[], Inbound<byte[]>, Outbound<byte[]>>,
        BiConsumer<Inbound<byte[]>, StreamOperations>, Function<Outbound<byte[]>, StreamOutbound> {

    public static final int TYPE_NEW = 1;
    public static final int TYPE_CANCEL = 2;
    public static final int TYPE_NEXT = 3;
    public static final int TYPE_ERROR = 4;
    public static final int TYPE_COMPLETE = 5;
    public static final int TYPE_REQUEST = 6;

    public static final byte PAYLOAD_OBJECT = 0;
    public static final byte PAYLOAD_INT = 1;
    public static final byte PAYLOAD_LONG = 2;
    public static final byte PAYLOAD_STRING = 3;
    public static final byte PAYLOAD_BYTES = 4;

    static Scheduler scheduler = Schedulers.fromExecutorService(Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "test-client-pool");
            t.setDaemon(true);
            return t;
        }));

    static public KafkaClient create(ReceiverOptions<Integer, byte[]> receiverOptions, SenderOptions<Integer, byte[]> senderOptions, String topic) {
        return new KafkaClient(receiverOptions, senderOptions, topic);
    }

    final ReceiverOptions<Integer, byte[]> receiverOptions;
    final SenderOptions<Integer, byte[]> senderOptions;
    final String topic;

    KafkaClient(ReceiverOptions<Integer, byte[]> receiverOptions, SenderOptions<Integer, byte[]> senderOptions, String topic) {
        super(receiverOptions, senderOptions, topic, 0);
        this.receiverOptions = receiverOptions;
        this.senderOptions = senderOptions;
        this.topic = topic;
    }

    @Override
    public void accept(Inbound<byte[]> inbound, StreamOperations endpoint) {
        if (inbound != null) {
            inbound.receive().subscribe(d -> onNext(endpoint, d));
        }
    }

    void onNext(StreamOperations endpoint, byte[] d) {
        try {
            ByteArrayInputStream bytesIn = new ByteArrayInputStream(d);
            DataInputStream in = new DataInputStream(bytesIn);
            long streamId = in.readLong();
            int type = in.readInt();
            int datatype = in.readInt();
            Object o = null;
            switch (datatype) {
                case PAYLOAD_OBJECT:
                    try (ObjectInputStream oin = new ObjectInputStream(bytesIn)) {
                        o = oin.readObject();
                    }
                    break;
                case PAYLOAD_INT:
                    o = in.readInt();
                    break;
                case PAYLOAD_LONG:
                    o = in.readLong();
                    break;
                case PAYLOAD_STRING:
                    o = in.readUTF();
                    break;
                case PAYLOAD_BYTES:
                    byte[] bytes = new byte[in.available()];
                    in.read(bytes);
                    o = bytes;
                    break;
            }

            if (!endpoint.isClosed()) {
                switch (type) {
                    case TYPE_NEW:
                        endpoint.onNew(streamId, (String) o);
                        break;
                    case TYPE_NEXT:
                        endpoint.onNext(streamId, o);
                        break;
                    case TYPE_REQUEST:
                        endpoint.onRequested(streamId, (Long) o);
                        break;
                    case TYPE_CANCEL:
                        endpoint.onCancel(streamId, (String) o);
                        break;
                    case TYPE_ERROR:
                        endpoint.onError(streamId, (String) o);
                        break;
                    case TYPE_COMPLETE:
                        endpoint.onComplete(streamId);
                        break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    byte[] encode(long streamId, int type, Object o) {
        try {
            ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytesOut);
            out.writeLong(streamId);
            out.writeInt(type);
            if (o instanceof Integer) {
                out.writeInt(PAYLOAD_INT);
                out.writeInt((Integer) o);
            } else if (o instanceof Long) {
                out.writeInt(PAYLOAD_LONG);
                out.writeLong((Long) o);
            } else if (o instanceof String) {
                out.writeInt(PAYLOAD_STRING);
                out.writeUTF((String) o);
            } else if (o instanceof byte[]) {
                out.writeInt(PAYLOAD_BYTES);
                out.write((byte[]) o);
            } else {
                out.writeInt(PAYLOAD_OBJECT);
                try (ObjectOutputStream oout = new ObjectOutputStream(bytesOut)) {
                    oout.writeObject(o);
                }
            }
            return bytesOut.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public StreamOutbound apply(Outbound<byte[]> outbound) {
        return new StreamOutbound() {

            @Override
            public void sendRequested(long streamId, long n) {
                if (outbound != null)
                    outbound.send(Mono.just(encode(streamId, TYPE_REQUEST, n))).subscribe();
            }

            @Override
            public void sendNext(long streamId, Object o) throws IOException {
                if (outbound != null)
                    outbound.send(Mono.just(encode(streamId, TYPE_NEXT, o))).subscribe();
            }

            @Override
            public void sendNew(long streamId, String function) {
                if (outbound != null)
                    outbound.send(Mono.just(encode(streamId, TYPE_NEW, function))).subscribe();
            }

            @Override
            public void sendError(long streamId, Throwable e) {
                if (outbound != null)
                    outbound.send(Mono.just(encode(streamId, TYPE_ERROR, e))).subscribe();
            }

            @Override
            public void sendComplete(long streamId) {
                if (outbound != null)
                    outbound.send(Mono.just(encode(streamId, TYPE_COMPLETE, ""))).subscribe();
            }

            @Override
            public void sendCancel(long streamId, String reason) {
                if (outbound != null)
                    outbound.send(Mono.just(encode(streamId, TYPE_CANCEL, reason))).subscribe();
            }

            @Override
            public boolean isClosed() {
                return false;
            }
        };
    }

    @Override
    public <API> Mono<API> newBidirectional(Supplier<?> receiverSupplier, Class<? extends API> api) {
        return newStreamSupport(receiverSupplier, api, this, this);
    }
}
