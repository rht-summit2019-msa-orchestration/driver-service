package com.acme.ride.driver.service.tracing;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import io.opentracing.propagation.TextMap;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

public class KafkaHeadersExtractAdapter implements TextMap {

    private final Map<String, String> map = new HashMap<>();

    public KafkaHeadersExtractAdapter(Headers headers, boolean second) {
        for (Header header : headers) {
            if (second) {
                if (header.key().startsWith("second_span_")) {
                    map.put(header.key().replaceFirst("^second_span_", ""),
                            new String(header.value(), StandardCharsets.UTF_8));
                }
            } else {
                map.put(header.key(), new String(header.value(), StandardCharsets.UTF_8));
            }
        }
    }

    @Override
    public Iterator<Map.Entry<String, String>> iterator() {
        return map.entrySet().iterator();
    }

    @Override
    public void put(String key, String value) {
        throw new UnsupportedOperationException(
                "KafkaHeadersExtractAdapter should only be used with Tracer.extract()");
    }
}
