package com.acme.ride.driver.service.tracing;

import java.util.List;
import java.util.Map;
import java.util.Set;

import io.opentracing.Tracer;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.kafka.client.common.PartitionInfo;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.KafkaConsumerRecords;
import io.vertx.kafka.client.consumer.KafkaReadStream;
import io.vertx.kafka.client.consumer.OffsetAndMetadata;
import io.vertx.kafka.client.consumer.OffsetAndTimestamp;
import io.vertx.kafka.client.consumer.impl.KafkaConsumerImpl;
import io.vertx.kafka.client.consumer.impl.KafkaConsumerRecordImpl;
import org.apache.kafka.clients.consumer.Consumer;

public class TracingKafkaConsumerImpl<K, V> implements KafkaConsumer<K, V> {

    private final KafkaConsumerImpl<K, V> consumer;

    private final Tracer tracer;

    public TracingKafkaConsumerImpl(KafkaConsumerImpl<K, V> consumer, Tracer tracer) {
        this.consumer = consumer;
        this.tracer = tracer;
    }

    @Override
    public KafkaConsumer<K, V> exceptionHandler(Handler<Throwable> handler) {
        consumer.exceptionHandler(handler);
        return this;
    }

    @Override
    public KafkaConsumer<K, V> handler(Handler<KafkaConsumerRecord<K, V>> handler) {
        if (handler != null) {
            consumer.asStream().handler(record -> {
                TracingKafkaUtils.buildAndFinishChildSpan(record, tracer);
                handler.handle(new KafkaConsumerRecordImpl<>(record));
            });
        } else {
            consumer.handler(null);
        }
        return this;
    }

    @Override
    public KafkaConsumer<K, V> pause() {
        consumer.pause();
        return this;
    }

    @Override
    public KafkaConsumer<K, V> resume() {
        consumer.resume();
        return this;
    }

    @Override
    public KafkaConsumer<K, V> endHandler(Handler<Void> endHandler) {
        consumer.endHandler(endHandler);
        return this;
    }

    @Override
    public KafkaConsumer<K, V> subscribe(String topic) {
        consumer.subscribe(topic);
        return this;
    }

    @Override
    public KafkaConsumer<K, V> subscribe(Set<String> topics) {
        consumer.subscribe(topics);
        return this;
    }

    @Override
    public KafkaConsumer<K, V> subscribe(String topic, Handler<AsyncResult<Void>> completionHandler) {
        consumer.subscribe(topic, completionHandler);
        return this;
    }

    @Override
    public KafkaConsumer<K, V> subscribe(Set<String> topics, Handler<AsyncResult<Void>> completionHandler) {
        consumer.subscribe(topics, completionHandler);
        return this;
    }

    @Override
    public KafkaConsumer<K, V> assign(TopicPartition topicPartition) {
        consumer.assign(topicPartition);
        return this;
    }

    @Override
    public KafkaConsumer<K, V> assign(Set<TopicPartition> topicPartitions) {
        consumer.assign(topicPartitions);
        return this;
    }

    @Override
    public KafkaConsumer<K, V> assign(TopicPartition topicPartition, Handler<AsyncResult<Void>> completionHandler) {
        consumer.assign(topicPartition, completionHandler);
        return this;
    }

    @Override
    public KafkaConsumer<K, V> assign(Set<TopicPartition> topicPartitions, Handler<AsyncResult<Void>> completionHandler) {
        consumer.assign(topicPartitions, completionHandler);
        return this;
    }

    @Override
    public KafkaConsumer<K, V> assignment(Handler<AsyncResult<Set<TopicPartition>>> handler) {
        consumer.assignment(handler);
        return this;
    }

    @Override
    public KafkaConsumer<K, V> listTopics(Handler<AsyncResult<Map<String, List<PartitionInfo>>>> handler) {
        consumer.listTopics(handler);
        return this;
    }

    @Override
    public KafkaConsumer<K, V> unsubscribe() {
        consumer.unsubscribe();
        return this;
    }

    @Override
    public KafkaConsumer<K, V> unsubscribe(Handler<AsyncResult<Void>> completionHandler) {
        consumer.unsubscribe(completionHandler);
        return this;
    }

    @Override
    public KafkaConsumer<K, V> subscription(Handler<AsyncResult<Set<String>>> handler) {
        consumer.subscription(handler);
        return this;
    }

    @Override
    public KafkaConsumer<K, V> pause(TopicPartition topicPartition) {
        consumer.pause(topicPartition);
        return this;
    }

    @Override
    public KafkaConsumer<K, V> pause(Set<TopicPartition> topicPartitions) {
        consumer.pause(topicPartitions);
        return this;
    }

    @Override
    public KafkaConsumer<K, V> pause(TopicPartition topicPartition, Handler<AsyncResult<Void>> completionHandler) {
        consumer.pause(topicPartition, completionHandler);
        return this;
    }

    @Override
    public KafkaConsumer<K, V> pause(Set<TopicPartition> topicPartitions, Handler<AsyncResult<Void>> completionHandler) {
        consumer.pause(topicPartitions, completionHandler);
        return this;
    }

    @Override
    public void paused(Handler<AsyncResult<Set<TopicPartition>>> handler) {
        consumer.paused(handler);
    }

    @Override
    public KafkaConsumer<K, V> resume(TopicPartition topicPartition) {
        consumer.resume(topicPartition);
        return this;
    }

    @Override
    public KafkaConsumer<K, V> resume(Set<TopicPartition> topicPartitions) {
        consumer.resume(topicPartitions);
        return this;
    }

    @Override
    public KafkaConsumer<K, V> resume(TopicPartition topicPartition, Handler<AsyncResult<Void>> completionHandler) {
        consumer.resume(topicPartition, completionHandler);
        return this;
    }

    @Override
    public KafkaConsumer<K, V> resume(Set<TopicPartition> topicPartitions, Handler<AsyncResult<Void>> completionHandler) {
        consumer.resume(topicPartitions, completionHandler);
        return this;
    }

    @Override
    public KafkaConsumer<K, V> partitionsRevokedHandler(Handler<Set<TopicPartition>> handler) {
        consumer.partitionsRevokedHandler(handler);
        return this;
    }

    @Override
    public KafkaConsumer<K, V> partitionsAssignedHandler(Handler<Set<TopicPartition>> handler) {
        consumer.partitionsAssignedHandler(handler);
        return this;
    }

    @Override
    public KafkaConsumer<K, V> seek(TopicPartition topicPartition, long offset) {
        consumer.seek(topicPartition, offset);
        return this;
    }

    @Override
    public KafkaConsumer<K, V> seek(TopicPartition topicPartition, long offset, Handler<AsyncResult<Void>> completionHandler) {
        consumer.seek(topicPartition, offset, completionHandler);
        return this;
    }

    @Override
    public KafkaConsumer<K, V> seekToBeginning(TopicPartition topicPartition) {
        consumer.seekToBeginning(topicPartition);
        return this;
    }

    @Override
    public KafkaConsumer<K, V> seekToBeginning(Set<TopicPartition> topicPartitions) {
        consumer.seekToBeginning(topicPartitions);
        return this;
    }

    @Override
    public KafkaConsumer<K, V> seekToBeginning(TopicPartition topicPartition, Handler<AsyncResult<Void>> completionHandler) {
        consumer.seekToBeginning(topicPartition, completionHandler);
        return this;
    }

    @Override
    public KafkaConsumer<K, V> seekToBeginning(Set<TopicPartition> topicPartitions, Handler<AsyncResult<Void>> completionHandler) {
        consumer.seekToBeginning(topicPartitions, completionHandler);
        return this;
    }

    @Override
    public KafkaConsumer<K, V> seekToEnd(TopicPartition topicPartition) {
        consumer.seekToEnd(topicPartition);
        return this;
    }

    @Override
    public KafkaConsumer<K, V> seekToEnd(Set<TopicPartition> topicPartitions) {
        consumer.seekToEnd(topicPartitions);
        return this;
    }

    @Override
    public KafkaConsumer<K, V> seekToEnd(TopicPartition topicPartition, Handler<AsyncResult<Void>> completionHandler) {
        consumer.seekToEnd(topicPartition, completionHandler);
        return this;
    }

    @Override
    public KafkaConsumer<K, V> seekToEnd(Set<TopicPartition> topicPartitions, Handler<AsyncResult<Void>> completionHandler) {
        consumer.seekToEnd(topicPartitions, completionHandler);
        return this;
    }

    @Override
    public void commit() {
        consumer.commit();
    }

    @Override
    public void commit(Handler<AsyncResult<Void>> completionHandler) {
        consumer.commit(completionHandler);
    }

    @Override
    public void commit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        consumer.commit(offsets);
    }

    @Override
    public void commit(Map<TopicPartition, OffsetAndMetadata> offsets, Handler<AsyncResult<Map<TopicPartition, OffsetAndMetadata>>> completionHandler) {
        consumer.commit(offsets, completionHandler);
    }

    @Override
    public void committed(TopicPartition topicPartition, Handler<AsyncResult<OffsetAndMetadata>> handler) {
        consumer.committed(topicPartition, handler);
    }

    @Override
    public KafkaConsumer<K, V> partitionsFor(String topic, Handler<AsyncResult<List<PartitionInfo>>> handler) {
        consumer.partitionsFor(topic, handler);
        return this;
    }

    @Override
    public KafkaConsumer<K, V> batchHandler(Handler<KafkaConsumerRecords<K, V>> handler) {
        consumer.batchHandler(handler);
        return this;
    }

    @Override
    public void close(Handler<AsyncResult<Void>> completionHandler) {
        consumer.close(completionHandler);
    }

    @Override
    public void position(TopicPartition partition, Handler<AsyncResult<Long>> handler) {
        consumer.position(partition, handler);
    }

    @Override
    public void offsetsForTimes(Map<TopicPartition, Long> topicPartitionTimestamps, Handler<AsyncResult<Map<TopicPartition, OffsetAndTimestamp>>> handler) {
        consumer.offsetsForTimes(topicPartitionTimestamps, handler);
    }

    @Override
    public void offsetsForTimes(TopicPartition topicPartition, Long timestamp, Handler<AsyncResult<OffsetAndTimestamp>> handler) {
        consumer.offsetsForTimes(topicPartition, timestamp, handler);
    }

    @Override
    public void beginningOffsets(Set<TopicPartition> topicPartitions, Handler<AsyncResult<Map<TopicPartition, Long>>> handler) {
        consumer.beginningOffsets(topicPartitions, handler);
    }

    @Override
    public void beginningOffsets(TopicPartition topicPartition, Handler<AsyncResult<Long>> handler) {
        consumer.beginningOffsets(topicPartition, handler);
    }

    @Override
    public void endOffsets(Set<TopicPartition> topicPartitions, Handler<AsyncResult<Map<TopicPartition, Long>>> handler) {
        consumer.endOffsets(topicPartitions, handler);
    }

    @Override
    public void endOffsets(TopicPartition topicPartition, Handler<AsyncResult<Long>> handler) {
        consumer.endOffsets(topicPartition, handler);
    }

    @Override
    public KafkaReadStream<K, V> asStream() {
        return consumer.asStream();
    }

    @Override
    public Consumer<K, V> unwrap() {
        return consumer.unwrap();
    }

    @Override
    public KafkaConsumer<K, V> pollTimeout(long timeout) {
        consumer.pollTimeout(timeout);
        return this;
    }
}
