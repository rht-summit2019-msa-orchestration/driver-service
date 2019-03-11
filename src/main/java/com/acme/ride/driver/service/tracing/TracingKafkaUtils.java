package com.acme.ride.driver.service.tracing;

import io.opentracing.References;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import io.opentracing.tag.Tags;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;

public class TracingKafkaUtils {

    private static final String TO_PREFIX = "To_";
    private static final String FROM_PREFIX = "From_";
    private static final String COMPONENT_NAME = "vertx-kafka";
    private static final String COMPONENT = "driver-service";


    public static Span buildAndInjectSpan(KafkaProducerRecord<String, String> record, Tracer tracer, Message<? extends Object> msg) {
        Tracer.SpanBuilder spanBuilder = tracer.buildSpan(TO_PREFIX + record.topic())
                .ignoreActiveSpan()
                .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_PRODUCER);

        SpanContext parent = extract(msg, tracer);
        if (parent != null) {
            spanBuilder.asChildOf(parent);
        }
        Span span = spanBuilder.start();
        Tags.COMPONENT.set(span, TracingKafkaUtils.COMPONENT_NAME);
        SpanDecorator.onSend(record.record(), span);
        inject(span, record, tracer);

        return span;
    }

    public static DeliveryOptions inject(DeliveryOptions options, Tracer tracer) {
        Span span = tracer.activeSpan();
        if (span != null) {
            options.addHeader("opentracing.span", span.context().toString());
        }
        return options;
    }

    private static SpanContext extract(Headers headers, Tracer tracer) {
        SpanContext spanContext = tracer.extract(Format.Builtin.TEXT_MAP, new KafkaHeadersExtractAdapter(headers, false));
        if (spanContext != null) {
            return spanContext;
        }

        Span span = tracer.activeSpan();
        if (span != null) {
            return span.context();
        }

        return null;
    }

    private static SpanContext extract(Message<? extends Object> msg, Tracer tracer) {
        String spanContextAsString = msg.headers().get("opentracing.span");
        if (spanContextAsString != null) {
            return io.jaegertracing.SpanContext.contextFromString(spanContextAsString);
        }
        Span span = tracer.activeSpan();
        if (span != null) {
            return span.context();
        }
        return null;
    }

    static void inject(Span span, KafkaProducerRecord<String, String> record, Tracer tracer) {
        tracer.inject(span.context(), Format.Builtin.TEXT_MAP,
                new KafkaHeadersInjectAdapter(record.record().headers(),false));
    }

    static void injectSecond(SpanContext spanContext, Headers headers,
                             Tracer tracer) {
        tracer.inject(spanContext, Format.Builtin.TEXT_MAP,
                new KafkaHeadersInjectAdapter(headers, true));
    }

    public static SpanContext extractSpanContext(Headers headers, Tracer tracer) {
        return tracer
                .extract(Format.Builtin.TEXT_MAP, new KafkaHeadersExtractAdapter(headers, true));
    }

    static <K,V> void buildAndFinishChildSpan(ConsumerRecord<K, V> record, Tracer tracer) {
        SpanContext parentContext = extract(record.headers(), tracer);

        if (parentContext != null) {

            String consumerOper = FROM_PREFIX + record.topic(); // <====== It provides better readability in the UI
            Tracer.SpanBuilder spanBuilder = tracer.buildSpan(consumerOper)
                    .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CONSUMER);

            spanBuilder.addReference(References.FOLLOWS_FROM, parentContext);

            Span span = spanBuilder.start();
            SpanDecorator.onResponse(record, span);
            span.finish();

            // Inject created span context into record headers for extraction by client to continue span chain
            TracingKafkaUtils.injectSecond(span.context(), record.headers(), tracer);
        }
    }

    public static Scope buildChildSpan(String operationName, KafkaConsumerRecord<String, String> record, Tracer tracer) {

        SpanContext spanContext = extractSpanContext(record.record().headers(), tracer);

        if (spanContext != null) {
            return spanBuilder(operationName, tracer).addReference(References.CHILD_OF, spanContext)
                    .startActive(true);
        }
        return null;
    }

    private static Tracer.SpanBuilder spanBuilder(String operationName, Tracer tracer) {
        return  tracer.buildSpan(operationName).ignoreActiveSpan()
                .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CONSUMER)
                .withTag(Tags.COMPONENT.getKey(), COMPONENT);
    }


}
