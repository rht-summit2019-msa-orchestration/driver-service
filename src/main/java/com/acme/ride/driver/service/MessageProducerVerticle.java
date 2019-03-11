package com.acme.ride.driver.service;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.acme.ride.driver.service.tracing.TracingKafkaUtils;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;

public class MessageProducerVerticle extends AbstractVerticle {

    private final static Logger log = LoggerFactory.getLogger("MessageProducer");

    private KafkaProducer<String, String> kafkaProducer;

    private Tracer tracer;

    private int minDelayBeforeDriverAssignedEvent;

    private int maxDelayBeforeDriverAssignedEvent;

    private int minDelayBeforeRideStartedEvent;

    private int maxDelayBeforeRideStartedEvent;

    private int minDelayBeforeRideEndedEvent;

    private int maxDelayBeforeRideEndedEvent;

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        tracer = GlobalTracer.get();

        Map<String, String> kafkaConfig = new HashMap<>();
        kafkaConfig.put("bootstrap.servers", config().getString("kafka.bootstrap.servers"));
        kafkaConfig.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaConfig.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaConfig.put("acks", "1");
        kafkaProducer = KafkaProducer.create(vertx, kafkaConfig);

        minDelayBeforeDriverAssignedEvent = config().getInteger("driver.assigned.min.delay", 1);
        maxDelayBeforeDriverAssignedEvent = config().getInteger("driver.assigned.max.delay", 3);
        minDelayBeforeRideStartedEvent = config().getInteger("ride.started.min.delay", 5);
        maxDelayBeforeRideStartedEvent = config().getInteger("ride.started.max.delay", 10);
        minDelayBeforeRideEndedEvent = config().getInteger("ride.ended.min.delay", 5);
        maxDelayBeforeRideEndedEvent = config().getInteger("ride.ended.max.delay", 10);

        vertx.eventBus().consumer("message-producer", this::handleMessage);

        startFuture.complete();
    }

    private void handleMessage(Message<JsonObject> msg) {

        if (dropAssignDriverEvent(getRideId(msg.body()))) {
            msg.reply(null);
            return;
        }
        sendDriverAssignedMessage(msg, getDriverId());
    }

    private boolean dropAssignDriverEvent(String rideId) {
        try {
            UUID uuid = UUID.fromString(rideId);
            long leastSignificantBits = uuid.getLeastSignificantBits() & 0x0000000F;
            if (leastSignificantBits == 15) {
                log.info("Dropping 'AssignedDriverEvent' for rideId " + rideId);
                return true;
            }
        } catch (IllegalArgumentException e) {
            // rideId is not an UUID
            return false;
        }
        return false;
    }

    private boolean dropRideStartedEvent(String rideId) {
        try {
            UUID uuid = UUID.fromString(rideId);
            long leastSignificantBits = uuid.getLeastSignificantBits() & 0x0000000F;
            if (leastSignificantBits == 14) {
                log.info("Dropping 'RideStartedEvent' for rideId " + rideId);
                return true;
            }
        } catch (IllegalArgumentException e) {
            // rideId is not an UUID
            return false;
        }
        return false;
    }

    private String getDriverId() {
        return "driver" + randomInteger(100, 200).intValue();
    }

    private void sendDriverAssignedMessage(final Message<JsonObject> msgIn, final String driverId) {
        vertx.setTimer(delayBeforeMessage(minDelayBeforeDriverAssignedEvent, maxDelayBeforeDriverAssignedEvent), i -> {
            doSendDriverAssignedMessage(msgIn, driverId);
            sendRideStartedEventMessage(msgIn);
        });
    }

    private void sendRideStartedEventMessage(final Message<JsonObject> msgIn) {

        if (dropRideStartedEvent(getRideId(msgIn.body()))) {
            return;
        }
        vertx.setTimer(delayBeforeMessage(minDelayBeforeRideStartedEvent, maxDelayBeforeRideStartedEvent), i -> {
            doSendRideStartedEventMessage(msgIn);
            sendRideEndedEventMessage(msgIn);
        });
    }

    private void sendRideEndedEventMessage(final Message<JsonObject> msgIn) {
        vertx.setTimer(delayBeforeMessage(minDelayBeforeRideEndedEvent, maxDelayBeforeRideEndedEvent), i -> {
            doSendRideEndedEventMessage(msgIn);
        });
    }

    private int delayBeforeMessage(int min, int max) {
        return randomInteger(min, max).intValue()*1000;
    }

    private BigInteger randomInteger(int min, int max) {
        BigInteger s = BigInteger.valueOf(min);
        BigInteger e = BigInteger.valueOf(max);

        return s.add(new BigDecimal(Math.random()).multiply(new BigDecimal(e.subtract(s))).toBigInteger());
    }

    private void doSendDriverAssignedMessage(Message<JsonObject> msg, String driverId) {
        JsonObject msgIn = msg.body();
        JsonObject msgOut = new JsonObject();
        msgOut.put("messageType","DriverAssignedEvent");
        msgOut.put("id", UUID.randomUUID().toString());
        msgOut.put("traceId", msgIn.getString("traceId"));
        msgOut.put("sender", "DriverServiceSimulator");
        msgOut.put("timestamp", Instant.now().toEpochMilli());
        JsonObject payload = new JsonObject();
        String rideId = msgIn.getJsonObject("payload").getString("rideId");
        payload.put("rideId", rideId);
        payload.put("driverId", driverId);
        msgOut.put("payload", payload);

        sendMessageToTopic(config().getString("kafka.topic.driver-event"), rideId, msgOut.toString(), msg);
        log.debug("Sent 'DriverAssignedEvent' message for ride " + rideId);
    }

    private void doSendRideStartedEventMessage(Message<JsonObject> msg) {
        JsonObject msgIn = msg.body();
        JsonObject msgOut = new JsonObject();
        msgOut.put("messageType","RideStartedEvent");
        msgOut.put("id", UUID.randomUUID().toString());
        msgOut.put("traceId", msgIn.getString("traceId"));
        msgOut.put("sender", "DriverServiceSimulator");
        msgOut.put("timestamp", Instant.now().toEpochMilli());
        JsonObject payload = new JsonObject();
        String rideId = msgIn.getJsonObject("payload").getString("rideId");
        payload.put("rideId", rideId);
        payload.put("timestamp", Instant.now().toEpochMilli());
        msgOut.put("payload", payload);

        sendMessageToTopic(config().getString("kafka.topic.ride-event"), rideId, msgOut.toString(), msg);
        log.debug("Sent 'RideStartedEvent' message for ride " + rideId);
    }

    private void doSendRideEndedEventMessage(Message<JsonObject> msg) {
        JsonObject msgIn = msg.body();
        JsonObject msgOut = new JsonObject();
        msgOut.put("messageType","RideEndedEvent");
        msgOut.put("id", UUID.randomUUID().toString());
        msgOut.put("traceId", msgIn.getString("traceId"));
        msgOut.put("sender", "DriverServiceSimulator");
        msgOut.put("timestamp", Instant.now().toEpochMilli());
        JsonObject payload = new JsonObject();
        String rideId = msgIn.getJsonObject("payload").getString("rideId");
        payload.put("rideId", rideId);
        payload.put("timestamp", Instant.now().toEpochMilli());
        msgOut.put("payload", payload);
        sendMessageToTopic(config().getString("kafka.topic.ride-event"), rideId, msgOut.toString(), msg);
        log.debug("Sent 'RideEndedEvent' message for ride " + rideId);
    }


    private void sendMessageToTopic(String topic, String key, String value, Message<JsonObject> msg) {
        KafkaProducerRecord<String, String> record = KafkaProducerRecord.create(topic, key, value);
        Span span = TracingKafkaUtils.buildAndInjectSpan(record, tracer, msg);
        try {
            kafkaProducer.write(record);
        } finally {
            span.finish();
        }
    }

    private String getRideId(JsonObject message) {
        return message.getJsonObject("payload").getString("rideId");
    }

    @Override
    public void stop(Future<Void> stopFuture) throws Exception {
        if (kafkaProducer != null) {
            kafkaProducer.close();
        }
        stopFuture.complete();
    }
}
