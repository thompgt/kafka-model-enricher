package dev.thompgt.enricher.core;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.NotBlank;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PartitionRefTest {

    @Test
    void rendersAsTopicDashPartition() {
        assertThat(new PartitionRef("events.raw", 3)).hasToString("events.raw-3");
    }

    @Test
    void parsesTheRenderedForm() {
        assertThat(PartitionRef.parse("events.raw-3"))
                .isEqualTo(new PartitionRef("events.raw", 3));
    }

    @Test
    void splitsOnTheLastDashSoHyphenatedTopicNamesSurvive() {
        assertThat(PartitionRef.parse("events-raw-dlq-12"))
                .isEqualTo(new PartitionRef("events-raw-dlq", 12));
    }

    @Test
    void rejectsNegativePartition() {
        assertThatThrownBy(() -> new PartitionRef("events.raw", -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("partition");
    }

    @Test
    void rejectsBlankTopic() {
        assertThatThrownBy(() -> new PartitionRef("  ", 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsInputWithoutAPartitionNumber() {
        assertThatThrownBy(() -> PartitionRef.parse("events.raw-"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> PartitionRef.parse("events.raw"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> PartitionRef.parse("-3"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * Round-tripping matters because the dead-letter envelope records the source
     * partition as a header string, and invariant 5 requires that envelope to be
     * replayable on its own. A parse that silently mangles a hyphenated topic
     * name would send replayed records to the wrong place.
     */
    @Property(tries = 2000)
    void parseRoundTripsToString(@ForAll @NotBlank String topic,
                                 @ForAll @IntRange(min = 0, max = 4096) int partition) {
        PartitionRef ref = new PartitionRef(topic.strip().isEmpty() ? "t" : topic, partition);
        assertThat(PartitionRef.parse(ref.toString())).isEqualTo(ref);
    }
}
