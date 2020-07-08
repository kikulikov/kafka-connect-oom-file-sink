package io.michelin.connect;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.stream.Collectors;

public class CheckpointFileSinkTask extends SinkTask {
  private static final Logger log = LoggerFactory.getLogger(CheckpointFileSinkTask.class);

  private String tmpFilePath;
  private String destFilePath;
  private String checkpoint;
  private String filePrefix;

  public CheckpointFileSinkTask() {
  }

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
  }

  static class CandidateKey {

    private final String key;
    private final boolean complete;
    private final long offset = 0L;

    public CandidateKey(String key, boolean complete) {
      this.key = key;
      this.complete = complete;
    }

    @Override
    public boolean equals(Object obj) {
      return EqualsBuilder.reflectionEquals(this, obj);
    }

    @Override
    public int hashCode() {
      return new HashCodeBuilder().append(key).append(complete).toHashCode();
    }
  }

  /**
   * partition -> offset
   */
  private Map<TopicPartition, Long> trackedOffsets = new HashMap<>();

  /**
   * partition -> list[key, status]
   */
  private Map<TopicPartition, Set<CandidateKey>> trackedKeys = new HashMap<>();

  @Override
  public void put(Collection<SinkRecord> sinkRecords) {
    for (SinkRecord record : sinkRecords) {
      final var filename = makeFilename(tmpFilePath, record.key().toString());
      final var tp = new TopicPartition(record.topic(), record.kafkaPartition());

      if ((checkpoint == null && record.value() == null) || (checkpoint != null && checkpoint.equals(record.value()))) {
        log.debug("Received checkpoint record {}: {}: {}", record.kafkaPartition(), record.key(), record.value());

        final var keys = trackedKeys.getOrDefault(tp, new HashSet<>()).stream()
            .filter(can -> !can.key.equals(record.key().toString())).collect(Collectors.toSet());
        keys.add(new CandidateKey(record.key().toString(), true)); // marks the key as complete
        trackedKeys.put(tp, keys);

      } else {
        final var printer = makePrintStream(filename);
        log.trace("Writing line to {}: {}", filename, record.value());

        try (printer) {
          printer.println(record.value()); // writes to the file

          final var keys = trackedKeys.getOrDefault(tp, new HashSet<>()).stream()
              .filter(can -> !can.key.equals(record.key().toString())).collect(Collectors.toSet());
          keys.add(new CandidateKey(record.key().toString(), false)); // appends the key as incomplete
          trackedKeys.put(tp, keys);
        }
      }

      trackedOffsets.put(tp, record.kafkaOffset()); // tracks the offset for topic-partition-key
    }
  }

  static boolean allComplete(Set<CandidateKey> candidates) {
    return candidates.stream().reduce(true, (acc, key) -> acc && key.complete, Boolean::logicalAnd);
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {


    var partitions = trackedKeys.entrySet().stream()
        .filter(entry -> allComplete(entry.getValue()))
        .map(Map.Entry::getKey)
        .collect(Collectors.toCollection(HashSet::new));

    var offsets = trackedOffsets.entrySet().stream()
        .filter(e -> partitions.contains(e.getKey()))
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            e -> new OffsetAndMetadata(e.getValue())
        ));

    offsets.keySet().stream()
        .flatMap(tp -> trackedKeys.getOrDefault(tp, Collections.emptySet()).stream())
        .forEach(tp -> {
          var src = Paths.get(makeFilename(tmpFilePath, tp.key));
          var dest = Paths.get(makeFilename(destFilePath, tp.key));

          try {
            Files.move(src, dest, StandardCopyOption.REPLACE_EXISTING);
          } catch (IOException e) {
            throw new ConnectException(String.format("Failed to move %s to %s", src.toString(), dest.toString()), e);
          }
        });

    offsets.keySet().forEach(tp -> {
      trackedKeys.remove(tp);
      trackedOffsets.remove(tp);
    });

    return super.preCommit(offsets);
  }

  String makeFilename(String path, String key) {
    return (path == null ? "" : path) + "/" + (filePrefix == null ? "" : filePrefix) + key;
  }

  private PrintStream makePrintStream(String filename) {
    try {
      return new PrintStream(Files.newOutputStream(Paths.get(filename),
          StandardOpenOption.CREATE, StandardOpenOption.APPEND), true);
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