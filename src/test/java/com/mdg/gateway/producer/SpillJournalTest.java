package com.mdg.gateway.producer;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SpillJournalTest {

    @TempDir
    Path dir;

    private SpillJournal open(long maxBytes, long segmentBytes) throws IOException {
        return new SpillJournal(dir, maxBytes, segmentBytes, false);
    }

    private static byte[] record(int i) {
        return ("{\"n\":" + i + "}").getBytes(StandardCharsets.UTF_8);
    }

    private static List<String> strings(List<byte[]> records) {
        return records.stream().map(r -> new String(r, StandardCharsets.UTF_8)).toList();
    }

    private long segmentFiles() throws IOException {
        try (Stream<Path> files = Files.list(dir)) {
            return files.filter(f -> f.toString().endsWith(".spill")).count();
        }
    }

    @Test
    @DisplayName("records come back in the order they went in, and stay until committed")
    void fifoWithTwoPhaseRead() throws IOException {
        try (SpillJournal journal = open(1 << 20, 1 << 16)) {
            for (int i = 0; i < 5; i++) {
                assertThat(journal.append(record(i))).isTrue();
            }
            assertThat(journal.pending()).isEqualTo(5);

            assertThat(strings(journal.peek(3))).containsExactly("{\"n\":0}", "{\"n\":1}", "{\"n\":2}");
            // Peeking removes nothing: an undelivered batch is simply read again.
            assertThat(strings(journal.peek(3))).containsExactly("{\"n\":0}", "{\"n\":1}", "{\"n\":2}");

            journal.commit(2);
            assertThat(journal.pending()).isEqualTo(3);
            assertThat(strings(journal.peek(10))).containsExactly("{\"n\":2}", "{\"n\":3}", "{\"n\":4}");
        }
    }

    @Test
    @DisplayName("rolls across segments, deletes drained ones, and returns the disk when empty")
    void segmentsRollAndAreReclaimed() throws IOException {
        // ~10 bytes a record, 64-byte segments: forces many segments.
        try (SpillJournal journal = open(1 << 20, 64)) {
            for (int i = 0; i < 100; i++) {
                journal.append(record(i));
            }
            assertThat(segmentFiles()).isGreaterThan(5);

            List<String> drained = new ArrayList<>();
            while (journal.pending() > 0) {
                List<byte[]> batch = journal.peek(7);
                drained.addAll(strings(batch));
                journal.commit(batch.size());
            }
            assertThat(drained).containsExactlyElementsOf(
                    IntStream.range(0, 100).mapToObj(i -> "{\"n\":" + i + "}").toList());
            assertThat(journal.bytes()).isZero();
            assertThat(segmentFiles()).isEqualTo(1);

            // And it is still usable afterwards.
            journal.append(record(100));
            assertThat(strings(journal.peek(5))).containsExactly("{\"n\":100}");
        }
    }

    @Test
    @DisplayName("survives a restart: undrained records are still there, committed ones are not")
    void recoversAcrossRestart() throws IOException {
        try (SpillJournal journal = open(1 << 20, 64)) {
            for (int i = 0; i < 20; i++) {
                journal.append(record(i));
            }
            journal.commit(journal.peek(8).size());
        }

        try (SpillJournal reopened = open(1 << 20, 64)) {
            assertThat(reopened.pending()).isEqualTo(12);
            assertThat(strings(reopened.peek(3))).containsExactly("{\"n\":8}", "{\"n\":9}", "{\"n\":10}");
            reopened.append(record(20));
            List<byte[]> rest = reopened.peek(100);
            assertThat(strings(rest)).hasSize(13).endsWith("{\"n\":20}");
        }
    }

    @Test
    @DisplayName("a crash before commit re-delivers the batch instead of losing it")
    void uncommittedBatchIsRedelivered() throws IOException {
        try (SpillJournal journal = open(1 << 20, 1 << 16)) {
            for (int i = 0; i < 4; i++) {
                journal.append(record(i));
            }
            journal.peek(4);   // handed to the broker, process dies before commit
        }
        try (SpillJournal reopened = open(1 << 20, 1 << 16)) {
            assertThat(strings(reopened.peek(10))).hasSize(4).startsWith("{\"n\":0}");
        }
    }

    @Test
    @DisplayName("a torn last line from a crash mid-append is discarded on recovery")
    void tornTailIsTruncated() throws IOException {
        try (SpillJournal journal = open(1 << 20, 1 << 16)) {
            journal.append(record(1));
            journal.append(record(2));
        }
        Path segment;
        try (Stream<Path> files = Files.list(dir)) {
            segment = files.filter(f -> f.toString().endsWith(".spill")).findFirst().orElseThrow();
        }
        Files.writeString(segment, "{\"n\":3,\"half", StandardOpenOption.APPEND);

        try (SpillJournal reopened = open(1 << 20, 1 << 16)) {
            assertThat(reopened.pending()).isEqualTo(2);
            reopened.append(record(4));
            assertThat(strings(reopened.peek(10))).containsExactly("{\"n\":1}", "{\"n\":2}", "{\"n\":4}");
        }
    }

    @Test
    @DisplayName("refuses to grow past its limit, and accepts again once drained")
    void boundedBySize() throws IOException {
        try (SpillJournal journal = open(50, 1 << 16)) {
            int accepted = 0;
            while (journal.append(record(accepted))) {
                accepted++;
            }
            assertThat(accepted).isBetween(4, 6);
            assertThat(journal.bytes()).isLessThanOrEqualTo(50);
            assertThat(journal.pending()).isEqualTo(accepted);

            journal.commit(journal.peek(100).size());
            assertThat(journal.append(record(99))).isTrue();
        }
    }

    @Test
    @DisplayName("reads a record larger than its read chunk")
    void largeRecord() throws IOException {
        byte[] big = ("{\"raw\":\"" + "x".repeat(700_000) + "\"}").getBytes(StandardCharsets.UTF_8);
        try (SpillJournal journal = open(4L << 20, 2L << 20)) {
            journal.append(record(1));
            journal.append(big);
            journal.append(record(2));
            List<byte[]> all = journal.peek(10);
            assertThat(all).hasSize(3);
            assertThat(all.get(1)).isEqualTo(big);
        }
    }

    @Test
    @DisplayName("rejects a record containing a newline, and a commit beyond what was peeked")
    void guardsItsInvariants() throws IOException {
        try (SpillJournal journal = open(1 << 20, 1 << 16)) {
            assertThatThrownBy(() -> journal.append("a\nb".getBytes(StandardCharsets.UTF_8)))
                    .isInstanceOf(IllegalArgumentException.class);
            journal.append(record(1));
            journal.peek(1);
            assertThatThrownBy(() -> journal.commit(2)).isInstanceOf(IllegalStateException.class);
        }
    }
}
