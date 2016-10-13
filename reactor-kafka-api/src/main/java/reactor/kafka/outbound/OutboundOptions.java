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
package reactor.kafka.outbound;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Configuration properties for reactive Kafka producer.
 */
public class OutboundOptions<K, V> {

    private final Map<String, Object> properties = new HashMap<>();

    private Duration closeTimeout = Duration.ofMillis(Long.MAX_VALUE);

    public static <K, V> OutboundOptions<K, V> create() {
        return new OutboundOptions<>();
    }

    public static <K, V> OutboundOptions<K, V> create(Map<String, Object> configProperties) {
        OutboundOptions<K, V> options = create();
        options.properties.putAll(configProperties);
        return options;
    }

    public static <K, V> OutboundOptions<K, V> create(Properties configProperties) {
        OutboundOptions<K, V> options = create();
        configProperties.forEach((name, value) -> options.properties.put((String) name, value));
        return options;
    }

    private OutboundOptions() {
    }

    public Map<String, Object> producerProperties() {
        return properties;
    }

    public Object producerProperty(String name) {
        return properties.get(name);
    }

    public OutboundOptions<K, V> producerProperty(String name, Object value) {
        properties.put(name, value);
        return this;
    }

    public Duration closeTimeout() {
        return closeTimeout;
    }

    public OutboundOptions<K, V> closeTimeout(Duration timeout) {
        this.closeTimeout = timeout;
        return this;
    }

    public OutboundOptions<K, V> toImmutable() {
        OutboundOptions<K, V> options = new OutboundOptions<K, V>() {

            @Override
            public Map<String, Object> producerProperties() {
                return Collections.unmodifiableMap(super.properties);
            }

            @Override
            public OutboundOptions<K, V> producerProperty(String name, Object value) {
                throw new java.lang.UnsupportedOperationException("Cannot modify immutable options");
            }

            @Override
            public OutboundOptions<K, V> closeTimeout(Duration timeout) {
                throw new java.lang.UnsupportedOperationException("Cannot modify immutable options");
            }

        };
        options.properties.putAll(properties);
        options.closeTimeout = closeTimeout;
        return options;
    }
}
