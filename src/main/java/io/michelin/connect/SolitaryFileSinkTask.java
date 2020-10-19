package io.michelin.connect;

import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolitaryFileSinkTask extends SinkTask {
  private static final Logger log = LoggerFactory.getLogger(SolitaryFileSinkTask.class);

  private String filePath;
  private String filePrefix;
  private String destFilePermissions;

  public SolitaryFileSinkTask() {}

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    this.filePath = props.get(SolitaryFileSinkConfig.FILE_PATH);
    this.filePrefix = props.get(SolitaryFileSinkConfig.FILE_PREFIX);
    this.destFilePermissions = props.get(SolitaryFileSinkConfig.DEST_FILE_PERMISSIONS);
  }

  @Override
  public void put(Collection<SinkRecord> sinkRecords) {
    for (SinkRecord record : sinkRecords) {
      final String filename = makeFilename(record.key().toString());

      log.debug("Writing line to {}", filename);

      try (final PrintStream printer = makePrintStream(filename); ) {
        printer.println(record.value());
      }

      if (this.destFilePermissions != null) {
        try {
          Set<PosixFilePermission> permissions =
              PosixFilePermissions.fromString(this.destFilePermissions);

          Files.setPosixFilePermissions(Paths.get(filename), permissions); // moves the file
        } catch (Exception e) {
          throw new ConnectException(
              String.format(
                  "Failed to set permissions %s on %s", this.destFilePermissions, filename),
              e);
        }
      }
    }
  }

  String makeFilename(String key) {
    return (filePath == null ? "" : filePath) + "/" + (filePrefix == null ? "" : filePrefix) + key;
  }

  private PrintStream makePrintStream(String filename) {
    try {
      return new PrintStream(
          Files.newOutputStream(
              Paths.get(filename), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING),
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
