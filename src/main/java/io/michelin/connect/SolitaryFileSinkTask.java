package io.michelin.connect;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Map;

public class SolitaryFileSinkTask extends SinkTask {
  private static final Logger log = LoggerFactory.getLogger(SolitaryFileSinkTask.class);

  private String filePath;
  private String filePrefix;

  public SolitaryFileSinkTask() {
  }

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    this.filePath = props.get(SolitaryFileSinkConfig.FILE_PATH);
    this.filePrefix = props.get(SolitaryFileSinkConfig.FILE_PREFIX);
  }

  @Override
  public void put(Collection<SinkRecord> sinkRecords) {
    for (SinkRecord record : sinkRecords) {
      final var filename = makeFilename(record.key().toString());

      log.debug("Writing line to {}", filename);

      try (final var printer = makePrintStream(filename);) {
        printer.println(record.value());
      }
    }
  }

  String makeFilename(String key) {
    return (filePath == null ? "" : filePath) + "/" + (filePrefix == null ? "" : filePrefix) + key;
  }

  private PrintStream makePrintStream(String filename) {
    try {
      return new PrintStream(Files.newOutputStream(Paths.get(filename),
          StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING), true);
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