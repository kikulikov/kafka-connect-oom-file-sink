package io.michelin.connect;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CheckpointFileSinkTask extends SinkTask {
  private static final Logger log = LoggerFactory.getLogger(CheckpointFileSinkTask.class);

  private String tmpFilePath;
  private String destFilePath;
  private String checkpoint;
  private String filePrefix;
  private String destFilePermissions;

  public CheckpointFileSinkTask() {}

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    this.tmpFilePath = props.get(CheckpointFileSinkConfig.TMP_FILE_PATH);
    this.destFilePath = props.get(CheckpointFileSinkConfig.DEST_FILE_PATH);
    this.checkpoint = props.get(CheckpointFileSinkConfig.CHECKPOINT_RECORD);
    this.filePrefix = props.get(CheckpointFileSinkConfig.FILE_PREFIX);
    this.destFilePermissions = props.get(CheckpointFileSinkConfig.DEST_FILE_PERMISSIONS);
  }

  static class CandidateKey {

    private final String key;
    private final boolean complete;
    private final long offset;

    public CandidateKey(String key, boolean complete, long offset) {
      this.key = key;
      this.complete = complete;
      this.offset = offset;
    }

    @Override
    public boolean equals(Object obj) {
      return EqualsBuilder.reflectionEquals(this, obj);
    }

    @Override
    public int hashCode() {
      return new HashCodeBuilder().append(key).append(complete).toHashCode();
    }

    public String getKey() {
      return key;
    }

    public boolean isComplete() {
      return complete;
    }

    public long getOffset() {
      return offset;
    }
  }

  /** partition -> list[key, status, offset] */
  private Map<TopicPartition, Set<CandidateKey>> trackedKeys = new HashMap<>();

  @Override
  public void put(Collection<SinkRecord> sinkRecords) {
    for (SinkRecord record : sinkRecords) {
      final String filename = makeFilename(tmpFilePath, record.key().toString());
      final TopicPartition tp = new TopicPartition(record.topic(), record.kafkaPartition());

      if ((checkpoint == null && record.value() == null)
          || (checkpoint != null && checkpoint.equals(record.value()))) {
        log.debug(
            "Processing checkpoint record {}: {}: {}",
            record.kafkaPartition(),
            record.key(),
            record.value());

        trackRecord(tp, record, true); // marks the key as complete

      } else {
        log.debug("Writing line to {}", filename);

        try (final PrintStream printer = makePrintStream(filename)) {
          printer.println(record.value()); // writes to the temporary file

          trackRecord(tp, record, false); // appends the key as incomplete
        }
      }
    }
  }

  private void trackRecord(TopicPartition tp, SinkRecord record, boolean completed) {
    final Set<CandidateKey> keys =
        trackedKeys.getOrDefault(tp, new HashSet<>()).stream()
            // TODO describe in detail
            .filter(can -> !can.key.equals(record.key().toString()))
            .collect(Collectors.toSet());
    keys.add(new CandidateKey(record.key().toString(), completed, record.kafkaOffset()));
    trackedKeys.put(tp, keys);
  }

  static boolean allComplete(Set<CandidateKey> candidates) {
    return candidates.stream().reduce(true, (acc, key) -> acc && key.complete, Boolean::logicalAnd);
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(
      Map<TopicPartition, OffsetAndMetadata> currentOffsets) {

    final Map<TopicPartition, Set<CandidateKey>> completed =
        trackedKeys.entrySet().stream()
            .filter(entry -> allComplete(entry.getValue()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    completed.values().stream()
        .flatMap(Collection::stream) // flattens the set of set of candidates
        .forEach(
            candidate -> {
              final Path src = Paths.get(makeFilename(tmpFilePath, candidate.key));
              final Path dest = Paths.get(makeFilename(destFilePath, candidate.key));

              try {
                Files.move(src, dest, StandardCopyOption.REPLACE_EXISTING);
              } catch (IOException e) {
                throw new ConnectException(
                    String.format("Failed to move %s to %s", src.toString(), dest.toString()), e);
              }

              if (this.destFilePermissions != null) {
                try {
                  Set<PosixFilePermission> ownerWritable =
                      PosixFilePermissions.fromString(this.destFilePermissions);

                  Files.setPosixFilePermissions(dest, ownerWritable); // moves the file
                } catch (Exception e) {
                  throw new ConnectException(
                      String.format(
                          "Failed to set permissions %s on %s",
                          this.destFilePermissions, dest.toString()),
                      e);
                }
              }
            });

    completed
        .keySet()
        .forEach(
            tp -> {
              trackedKeys.remove(tp);
            });

    currentOffsets =
        completed.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> new OffsetAndMetadata(lastOffset(entry.getValue()))));

    return super.preCommit(currentOffsets);
  }

  private long lastOffset(Set<CandidateKey> candidates) {
    return candidates.stream()
        .mapToLong(candidateKey -> candidateKey.getOffset() + 1)
        .max()
        .orElseThrow(() -> new RuntimeException("Something bad has happened"));
  }

  String makeFilename(String path, String key) {
    return (path == null ? "" : path) + "/" + (filePrefix == null ? "" : filePrefix) + key;
  }

  private PrintStream makePrintStream(String filename) {
    try {
      return new PrintStream(
          Files.newOutputStream(
              Paths.get(filename), StandardOpenOption.CREATE, StandardOpenOption.APPEND),
          true);
    } catch (Exception e) {
      throw new ConnectException("Couldn't find or create file '" + filename + "'", e);
    }
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
    // Nothing to do here.
  }

  @Override
  public void stop() {
    // Nothing to do here.
  }
}
