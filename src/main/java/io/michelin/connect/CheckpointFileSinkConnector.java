package io.michelin.connect;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CheckpointFileSinkConnector extends SinkConnector {

  private static final Logger log = LoggerFactory.getLogger(CheckpointFileSinkConnector.class);
  private static final String CONNECTOR_NAME = CheckpointFileSinkConnector.class.getSimpleName();
  private CheckpointFileSinkConfig config;

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  /** This will be executed once per connector. This can be used to handle connector level setup. */
  @Override
  public void start(Map<String, String> map) {
    log.info("The {} has been started.", CONNECTOR_NAME);
    this.config = new CheckpointFileSinkConfig(map);

    deleteFiles(
        map.get(CheckpointFileSinkConfig.TMP_FILE_PATH)); // deletes temporary files before tasks
  }

  private void deleteFiles(String dir) {
    for (File file : Paths.get(dir).toFile().listFiles()) {
      if (!file.isDirectory()) {
        file.delete();
      }
    }
  }

  /** Returns your task implementation. */
  @Override
  public Class<? extends Task> taskClass() {
    return CheckpointFileSinkTask.class;
  }

  /** Defines the individual task configurations that will be executed. */
  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    final List<Map<String, String>> configs = new ArrayList<>();

    for (int i = 0; i < maxTasks; i++) {
      configs.add(
          this.config.values().entrySet().stream()
              .filter(entry -> entry.getValue() != null)
              .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString())));
    }

    return configs;
  }

  @Override
  public void stop() {
    log.info("The {} has been stopped.", CONNECTOR_NAME);
  }

  @Override
  public ConfigDef config() {
    return CheckpointFileSinkConfig.CONFIG_DEF;
  }
}
