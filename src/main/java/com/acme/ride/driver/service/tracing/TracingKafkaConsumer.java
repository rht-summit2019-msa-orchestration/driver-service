package com.acme.ride.driver.service.tracing;

import java.util.HashMap;
import java.util.Map;

import io.opentracing.Tracer;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaReadStream;
import io.vertx.kafka.client.consumer.impl.KafkaConsumerImpl;

public interface TracingKafkaConsumer<K, V> extends KafkaConsumer<K, V> {

    static <K, V> KafkaConsumer<K, V> create(Vertx vertx, Map<String, String> config, Tracer tracer) {
        KafkaReadStream<K, V> stream = KafkaReadStream.create(vertx, new HashMap<>(config));
        return new TracingKafkaConsumerImpl<>(new KafkaConsumerImpl<>(stream).registerCloseHook(), tracer);
    }

}
