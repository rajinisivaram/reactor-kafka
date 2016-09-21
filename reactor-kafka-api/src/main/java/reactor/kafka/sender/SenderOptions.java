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

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Configuration properties for reactive Kafka sender.
 */
public class SenderOptions<K, V> {

    private final Map<String, Object> properties = new HashMap<>();

    private Duration closeTimeout = Duration.ofMillis(Long.MAX_VALUE);

    public SenderOptions() {
    }

    public SenderOptions(Map<String, Object> configProperties) {
        this.properties.putAll(configProperties);
    }

    public SenderOptions(Properties configProperties) {
        configProperties.forEach((name, value) -> this.properties.put((String) name, value));
    }

    public Map<String, Object> producerProperties() {
        return properties;
    }

    public Object producerProperty(String name) {
        return properties.get(name);
    }

    public SenderOptions<K, V> producerProperty(String name, Object value) {
        properties.put(name, value);
        return this;
    }

    public Duration closeTimeout() {
        return closeTimeout;
    }

    public SenderOptions<K, V> closeTimeout(Duration timeout) {
        this.closeTimeout = timeout;
        return this;
    }

    public SenderOptions<K, V> toImmutable() {
        SenderOptions<K, V> options = new SenderOptions<K, V>(properties) {

            @Override
            public Map<String, Object> producerProperties() {
                return Collections.unmodifiableMap(super.properties);
            }

            @Override
            public SenderOptions<K, V> producerProperty(String name, Object value) {
                throw new java.lang.UnsupportedOperationException("Cannot modify immutable options");
            }

            @Override
            public SenderOptions<K, V> closeTimeout(Duration timeout) {
                throw new java.lang.UnsupportedOperationException("Cannot modify immutable options");
            }

        };
        options.closeTimeout = closeTimeout;
        return options;
    }
}
