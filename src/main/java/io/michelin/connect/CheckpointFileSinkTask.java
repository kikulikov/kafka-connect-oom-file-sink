package io.michelin.connect;

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

  // TODO IO streams open/close/pool???
  // TODO TopicPartition[topic, partition], OffsetKey[key, offset, status]
  // TODO partition: Integer -> TopicPartition

  /**
   * partition -> offset
   */
  private Map<Integer, Long> trackedOffsets = new HashMap<>();

  /**
   * partition -> list[key]
   */
  private Map<Integer, Set<String>> trackedKeys = new HashMap<>();

  /**
   * partition -> list[key]
   */
  private Map<Integer, Set<String>> candidateKeys = new HashMap<>();

  // private Set<Integer> partitionCandidates = new HashSet<>();

  @Override
  public void put(Collection<SinkRecord> sinkRecords) {
    for (SinkRecord record : sinkRecords) {
      final var filename = makeFilename(tmpFilePath, record.key().toString());

      if ((checkpoint == null && record.value() == null) || (checkpoint != null && checkpoint.equals(record.value()))) {
        log.debug("Received checkpoint record {}: {}: {}", record.kafkaPartition(), record.key(), record.value());

        var keys = trackedKeys.getOrDefault(record.kafkaPartition(), new HashSet<>());
        keys.remove(record.key().toString()); // drop the key from the list
        trackedKeys.put(record.kafkaPartition(), keys);

        var kk = candidateKeys.getOrDefault(record.kafkaPartition(), new HashSet<>());
        kk.add(record.key().toString());
        candidateKeys.put(record.kafkaPartition(), kk);

      } else {
        final var printer = makePrintStream(filename);
        log.trace("Writing line to {}: {}", filename, record.value());

        try (printer) {
          printer.println(record.value());

          // track keys
          var keys = trackedKeys.getOrDefault(record.kafkaPartition(), new HashSet<>());
          keys.add(record.key().toString());
          trackedKeys.put(record.kafkaPartition(), keys);
        }
      }

      // track offsets
      trackedOffsets.put(record.kafkaPartition(), record.kafkaOffset());
    }
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    var partitions = trackedKeys.entrySet().stream()
        .filter(e -> e.getValue().isEmpty())
        .map(Map.Entry::getKey)
        .collect(Collectors.toCollection(HashSet::new));

    // TODO use TopicPartition instead
    var topic = currentOffsets.keySet().stream().findFirst().get().topic();

    var offsets = trackedOffsets.entrySet().stream()
        .filter(e -> partitions.contains(e.getKey()))
        .collect(Collectors.toMap(
            e -> new TopicPartition(topic, e.getKey()),
            e -> new OffsetAndMetadata(e.getValue())
        ));

    offsets.keySet().stream()
        .flatMap(k -> candidateKeys.getOrDefault(k.partition(), Collections.emptySet()).stream())
        .forEach(k -> {
          var src = Paths.get(makeFilename(tmpFilePath, k));
          var dest = Paths.get(makeFilename(destFilePath, k));

          try {
            Files.move(src, dest, StandardCopyOption.REPLACE_EXISTING);
          } catch (IOException e) {
            e.printStackTrace(); // TODO error handling
          }
        });

    offsets.keySet().stream().forEach(p -> {
      trackedKeys.remove(p.partition());
      trackedOffsets.remove(p.partition());
      candidateKeys.remove(p.partition());
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