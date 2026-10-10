package com.mdg.gateway.producer;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

/**
 * A bounded, append-only FIFO of records on local disk.
 *
 * <p>Deliberately small and dumb: no index, no compaction, no framing beyond a newline. A
 * record is one line (JSON never contains a raw newline), so a torn write after a crash is
 * simply an unterminated last line, and recovery is "truncate to the last newline".
 *
 * <h2>Layout</h2>
 * <pre>
 *   00000000000000000007.spill   segments, appended in order, rolled at segmentBytes
 *   00000000000000000008.spill
 *   checkpoint                   "segment offset" of the first record not yet drained
 * </pre>
 * Reading is two-phase — {@link #peek} then {@link #commit} — so a record leaves the journal
 * only after the caller has confirmed it was delivered. A crash between the two re-delivers
 * the batch on restart: at-least-once, never lost. Segments are deleted as soon as they are
 * fully drained, so the directory shrinks back to almost nothing once the backlog is gone.
 *
 * <h2>Locking</h2>
 * One {@link ReentrantLock} guards the files, deliberately not {@code synchronized}: callers
 * are virtual threads, and on Java 21 a virtual thread that blocks inside a {@code synchronized}
 * block pins its carrier thread. An {@code fsync} can take milliseconds, and with only as many
 * carriers as CPU cores, a few pinned ones would stall every other virtual thread in the
 * process. A {@code ReentrantLock} lets a waiting or blocked virtual thread unmount instead.
 *
 * <p>{@link #pending()} and {@link #bytes()} do not take the lock at all. {@code pending()} is
 * read for every event the gateway handles (it is how the pipeline asks "is anything waiting
 * on disk?"), so it must never queue behind a disk write.
 */
public final class SpillJournal implements Closeable {

    private static final String SEGMENT_SUFFIX = ".spill";
    private static final String CHECKPOINT = "checkpoint";
    private static final int READ_CHUNK = 256 * 1024;

    private final Path directory;
    private final long maxBytes;
    private final long segmentBytes;
    private final boolean fsync;

    /** Segment sequence number to its size in bytes. */
    private final TreeMap<Long, Long> segments = new TreeMap<>();
    /** (segment, end offset) of each record returned by the last {@link #peek}. */
    private final ArrayDeque<long[]> peeked = new ArrayDeque<>();
    private final ReentrantLock lock = new ReentrantLock();

    private FileChannel writer;
    private long writeSegment;
    private long readSegment;
    private long readOffset;
    /** Written only under {@link #lock}; volatile so readers need not take it. */
    private volatile long pending;
    private volatile long totalBytes;

    public SpillJournal(Path directory, long maxBytes, long segmentBytes, boolean fsync) throws IOException {
        this.directory = directory;
        this.maxBytes = maxBytes;
        this.segmentBytes = segmentBytes;
        this.fsync = fsync;
        recover();
    }

    /**
     * Appends one record.
     *
     * @return {@code false} if the journal is at its size limit — the caller must handle the
     *         record some other way; nothing was written
     */
    public boolean append(byte[] record) {
        lock.lock();
        try {
            return appendLocked(record);
        } finally {
            lock.unlock();
        }
    }

    private boolean appendLocked(byte[] record) {
        for (byte b : record) {
            if (b == '\n') {
                throw new IllegalArgumentException("A spill record must be a single line");
            }
        }
        long length = record.length + 1L;
        if (totalBytes + length > maxBytes) {
            return false;
        }
        try {
            if (segments.get(writeSegment) > 0 && segments.get(writeSegment) + length > segmentBytes) {
                roll();
            }
            ByteBuffer buffer = ByteBuffer.allocate((int) length);
            buffer.put(record).put((byte) '\n').flip();
            while (buffer.hasRemaining()) {
                writer.write(buffer);
            }
            if (fsync) {
                // The whole point of the journal is to outlive a crash while the broker is down.
                writer.force(false);
            }
            segments.merge(writeSegment, length, Long::sum);
            totalBytes += length;
            pending++;
            return true;
        } catch (IOException ex) {
            throw new UncheckedIOException("Could not append to spill journal in " + directory, ex);
        }
    }

    /** Up to {@code max} oldest undrained records, in order, without removing them. */
    public List<byte[]> peek(int max) {
        lock.lock();
        try {
            return peekLocked(max);
        } finally {
            lock.unlock();
        }
    }

    private List<byte[]> peekLocked(int max) {
        peeked.clear();
        List<byte[]> records = new ArrayList<>();
        long segment = readSegment;
        long offset = readOffset;
        try {
            while (records.size() < max) {
                long size = segments.get(segment);
                if (offset >= size) {
                    Long next = segments.higherKey(segment);
                    if (next == null) {
                        break;
                    }
                    segment = next;
                    offset = 0;
                    continue;
                }
                byte[] chunk = readChunk(segment, offset, size);
                int start = 0;
                for (int i = 0; i < chunk.length && records.size() < max; i++) {
                    if (chunk[i] == '\n') {
                        records.add(Arrays.copyOfRange(chunk, start, i));
                        start = i + 1;
                        peeked.add(new long[] {segment, offset + start});
                    }
                }
                if (start == 0) {
                    break;   // no complete record left in this segment
                }
                offset += start;
            }
        } catch (IOException ex) {
            throw new UncheckedIOException("Could not read spill journal in " + directory, ex);
        }
        return records;
    }

    /** Marks the first {@code count} records of the last {@link #peek} as delivered. */
    public void commit(int count) {
        lock.lock();
        try {
            commitLocked(count);
        } finally {
            lock.unlock();
        }
    }

    private void commitLocked(int count) {
        if (count <= 0) {
            return;
        }
        for (int i = 0; i < count; i++) {
            long[] position = peeked.poll();
            if (position == null) {
                throw new IllegalStateException("commit(" + count + ") exceeds the records last peeked");
            }
            readSegment = position[0];
            readOffset = position[1];
            pending--;
        }
        peeked.clear();
        try {
            // Whole segments behind the read position are finished with.
            while (!segments.isEmpty() && segments.firstKey() < readSegment) {
                deleteSegment(segments.firstKey());
            }
            while (readSegment < writeSegment && readOffset >= segments.get(readSegment)) {
                long finished = readSegment;
                readSegment = segments.higherKey(finished);
                readOffset = 0;
                deleteSegment(finished);
            }
            if (pending == 0 && segments.get(writeSegment) > 0) {
                // Fully drained: start a fresh segment so the disk goes back to empty.
                long finished = writeSegment;
                roll();
                readSegment = writeSegment;
                readOffset = 0;
                deleteSegment(finished);
            }
            writeCheckpoint();
        } catch (IOException ex) {
            throw new UncheckedIOException("Could not advance spill journal in " + directory, ex);
        }
    }

    /** Records appended and not yet committed. Lock-free: see the class comment. */
    public long pending() {
        return pending;
    }

    /** Bytes currently on disk, drained-but-not-yet-deleted data included. Lock-free. */
    public long bytes() {
        return totalBytes;
    }

    @Override
    public void close() throws IOException {
        lock.lock();
        try {
            if (writer != null && writer.isOpen()) {
                writer.force(true);
                writer.close();
            }
        } finally {
            lock.unlock();
        }
    }

    // ------------------------------------------------------------------

    private void recover() throws IOException {
        Files.createDirectories(directory);
        try (Stream<Path> files = Files.list(directory)) {
            for (Path file : files.filter(f -> f.getFileName().toString().endsWith(SEGMENT_SUFFIX)).toList()) {
                String name = file.getFileName().toString();
                segments.put(Long.parseLong(name.substring(0, name.length() - SEGMENT_SUFFIX.length())), Files.size(file));
            }
        }

        long[] checkpoint = readCheckpoint();
        if (checkpoint != null) {
            while (!segments.isEmpty() && segments.firstKey() < checkpoint[0]) {
                deleteSegment(segments.firstKey());
            }
        }
        if (segments.isEmpty()) {
            writeSegment = checkpoint == null ? 0 : checkpoint[0] + 1;
            segments.put(writeSegment, 0L);
            Files.createFile(segmentPath(writeSegment));
        } else {
            writeSegment = segments.lastKey();
            truncateTornTail(writeSegment);
        }
        readSegment = segments.firstKey();
        readOffset = checkpoint != null && checkpoint[0] == readSegment
                ? Math.min(checkpoint[1], segments.get(readSegment))
                : 0;

        writer = FileChannel.open(segmentPath(writeSegment), StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        pending = countRecords();
        totalBytes = segments.values().stream().mapToLong(Long::longValue).sum();
    }

    /** A crash mid-append leaves a last line with no terminator. It was never acknowledged to anyone. */
    private void truncateTornTail(long segment) throws IOException {
        byte[] content = Files.readAllBytes(segmentPath(segment));
        int end = content.length;
        while (end > 0 && content[end - 1] != '\n') {
            end--;
        }
        if (end != content.length) {
            try (FileChannel channel = FileChannel.open(segmentPath(segment), StandardOpenOption.WRITE)) {
                channel.truncate(end);
                channel.force(true);
            }
            segments.put(segment, (long) end);
        }
    }

    private long countRecords() throws IOException {
        long count = 0;
        for (long segment : segments.keySet()) {
            long offset = segment == readSegment ? readOffset : 0;
            long size = segments.get(segment);
            try (FileChannel channel = FileChannel.open(segmentPath(segment), StandardOpenOption.READ)) {
                ByteBuffer buffer = ByteBuffer.allocate(READ_CHUNK);
                while (offset < size) {
                    buffer.clear();
                    int read = channel.read(buffer, offset);
                    if (read <= 0) {
                        break;
                    }
                    for (int i = 0; i < read; i++) {
                        if (buffer.get(i) == '\n') {
                            count++;
                        }
                    }
                    offset += read;
                }
            }
        }
        return count;
    }

    /** Reads from {@code offset}, growing the chunk until it holds at least one whole record. */
    private byte[] readChunk(long segment, long offset, long size) throws IOException {
        try (FileChannel channel = FileChannel.open(segmentPath(segment), StandardOpenOption.READ)) {
            int length = READ_CHUNK;
            while (true) {
                int toRead = (int) Math.min(length, size - offset);
                ByteBuffer buffer = ByteBuffer.allocate(toRead);
                while (buffer.hasRemaining() && channel.read(buffer, offset + buffer.position()) > 0) {
                    // keep filling
                }
                byte[] chunk = Arrays.copyOf(buffer.array(), buffer.position());
                boolean hasRecord = false;
                for (byte b : chunk) {
                    if (b == '\n') {
                        hasRecord = true;
                        break;
                    }
                }
                if (hasRecord || toRead >= size - offset) {
                    return chunk;
                }
                length *= 2;
            }
        }
    }

    private void roll() throws IOException {
        if (fsync) {
            writer.force(true);
        }
        writer.close();
        writeSegment++;
        segments.put(writeSegment, 0L);
        writer = FileChannel.open(segmentPath(writeSegment),
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
    }

    private void deleteSegment(long segment) throws IOException {
        Files.deleteIfExists(segmentPath(segment));
        Long size = segments.remove(segment);
        if (size != null) {
            totalBytes -= size;
        }
    }

    private Path segmentPath(long segment) {
        return directory.resolve(String.format("%020d%s", segment, SEGMENT_SUFFIX));
    }

    private void writeCheckpoint() throws IOException {
        // Write-then-rename, so a crash leaves either the old checkpoint or the new one.
        Path temp = directory.resolve(CHECKPOINT + ".tmp");
        Files.writeString(temp, readSegment + " " + readOffset, StandardCharsets.UTF_8);
        Files.move(temp, directory.resolve(CHECKPOINT),
                StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    private long[] readCheckpoint() throws IOException {
        Path file = directory.resolve(CHECKPOINT);
        if (!Files.exists(file)) {
            return null;
        }
        try {
            String[] parts = Files.readString(file, StandardCharsets.UTF_8).trim().split(" ");
            return new long[] {Long.parseLong(parts[0]), Long.parseLong(parts[1])};
        } catch (RuntimeException unreadable) {
            // Fall back to the start of the oldest segment: re-delivery, not loss.
            return null;
        }
    }
}
