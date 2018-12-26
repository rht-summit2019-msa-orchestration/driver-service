package com.acme.ride.driver.service;

import java.util.HashMap;
import java.util.Map;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;

public class MessageConsumerVerticle extends AbstractVerticle {

    private final static Logger log = LoggerFactory.getLogger("MessageConsumer");

    private KafkaConsumer<String, String> kafkaConsumer;

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        Map<String, String> kafkaConfig = new HashMap<>();
        kafkaConfig.put("bootstrap.servers", config().getString("kafka.bootstrap.servers"));
        kafkaConfig.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaConfig.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaConfig.put("group.id", config().getString("kafka.groupid"));
        kafkaConfig.put("enable.auto.commit", "false");
        kafkaConsumer = KafkaConsumer.create(vertx, kafkaConfig);
        kafkaConsumer.handler(this::handleMessage);
        kafkaConsumer.subscribe(config().getString("kafka.topic.driver-command"));

        startFuture.complete();
    }

    private void handleMessage(KafkaConsumerRecord<String, String> msg) {
        JsonObject message = new JsonObject(msg.value());

        if (message.isEmpty()) {
            log.warn("Message " + msg.key() + " has no contents. Ignoring message");
            return;
        }
        String messageType = message.getString("messageType");
        if (!("AssignDriverCommand".equals(messageType))) {
            log.debug("Unexpected message type '" + messageType + "' in message " + message + ". Ignoring message");
            return;
        }
        log.debug("Consumed 'AssignDriverCommand' message. Ride: " + message.getJsonObject("payload").getString("rideId")
            + " , topic: " + msg.topic() + " ,  partition: " + msg.partition());

        // send message to producer verticle
        vertx.eventBus().<JsonObject>send("message-producer", message);

        //commit message
        kafkaConsumer.commit();
    }

    @Override
    public void stop(Future<Void> stopFuture) throws Exception {
        if (kafkaConsumer != null) {
            kafkaConsumer.commit();
            kafkaConsumer.unsubscribe();
            kafkaConsumer.close();
        }
        stopFuture.complete();
    }
}
