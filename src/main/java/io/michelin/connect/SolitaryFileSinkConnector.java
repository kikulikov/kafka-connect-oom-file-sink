package io.michelin.connect;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SolitaryFileSinkConnector extends SinkConnector {

  private static final Logger log = LoggerFactory.getLogger(SolitaryFileSinkConnector.class);
  private static final String CONNECTOR_NAME = SolitaryFileSinkConnector.class.getSimpleName();
  private SolitaryFileSinkConfig config;

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  /**
   * This will be executed once per connector. This can be used to handle connector level setup.
   */
  @Override
  public void start(Map<String, String> map) {
    log.info("The {} has been started.", CONNECTOR_NAME);
    this.config = new SolitaryFileSinkConfig(map);
  }

  /**
   * Returns your task implementation.
   */
  @Override
  public Class<? extends Task> taskClass() {
    return SolitaryFileSinkTask.class;
  }

  /**
   * Defines the individual task configurations that will be executed.
   */
  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    final var configs = new ArrayList<Map<String, String>>();

    for (int i = 0; i < maxTasks; i++) {
      configs.add(this.config.values().entrySet().stream().collect(
          Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString())));
    }

    return configs;
  }

  @Override
  public void stop() {
    log.info("The {} has been stopped.", CONNECTOR_NAME);
  }

  @Override
  public ConfigDef config() {
    return SolitaryFileSinkConfig.CONFIG_DEF;
  }
}