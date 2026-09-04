package dev.thompgt.enricher.core;

import java.util.Objects;

/**
 * Identifies a source partition.
 *
 * <p>This exists instead of Kafka's {@code TopicPartition} because of invariant 3
 * in {@code CLAUDE.md}: {@code enricher-core} holds the offset watermark and the
 * in-flight window, which are the two places a bug loses data without raising an
 * error. They need property tests over thousands of randomised interleavings in
 * milliseconds, and that stops being possible the moment constructing one of them
 * requires the Kafka client on the classpath.
 *
 * <p>{@code enricher-kafka} adapts at the boundary.
 */
public record PartitionRef(String topic, int partition) implements Comparable<PartitionRef> {

    public PartitionRef {
        Objects.requireNonNull(topic, "topic");
        if (topic.isBlank()) {
            throw new IllegalArgumentException("topic must not be blank");
        }
        if (partition < 0) {
            throw new IllegalArgumentException("partition must not be negative: " + partition);
        }
    }

    /** Parses the {@code topic-partition} form used in logs and dead-letter headers. */
    public static PartitionRef parse(String s) {
        Objects.requireNonNull(s, "s");
        int dash = s.lastIndexOf('-');
        if (dash <= 0 || dash == s.length() - 1) {
            throw new IllegalArgumentException("not a topic-partition: " + s);
        }
        try {
            return new PartitionRef(s.substring(0, dash), Integer.parseInt(s.substring(dash + 1)));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("not a topic-partition: " + s, e);
        }
    }

    @Override
    public int compareTo(PartitionRef other) {
        int byTopic = topic.compareTo(other.topic);
        return byTopic != 0 ? byTopic : Integer.compare(partition, other.partition);
    }

    @Override
    public String toString() {
        return topic + "-" + partition;
    }
}
