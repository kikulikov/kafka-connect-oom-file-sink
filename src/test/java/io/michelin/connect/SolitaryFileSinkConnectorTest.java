package io.michelin.connect;

import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.sink.SinkConnector;
import org.easymock.EasyMockSupport;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class SolitaryFileSinkConnectorTest extends EasyMockSupport {

  private static final String MULTIPLE_TOPICS = "test1,test2";
  private static final String FILE_PREFIX_VALUE = "oom_";

  private SolitaryFileSinkConnector connector;
  private Map<String, String> sinkProperties;

  @Before
  public void setup() throws Exception {
    connector = new SolitaryFileSinkConnector();

    final ConnectorContext context = createMock(ConnectorContext.class);
    connector.initialize(context);

    final var topDir = new TemporaryFolder();
    topDir.create();

    final var outputDir = topDir.newFolder("file-stream-sink-" + UUID.randomUUID().toString());

    sinkProperties = new HashMap<>();
    sinkProperties.put(SinkConnector.TOPICS_CONFIG, MULTIPLE_TOPICS);
    sinkProperties.put(SolitaryFileSinkConfig.FILE_PATH, outputDir.getAbsolutePath());
    sinkProperties.put(SolitaryFileSinkConfig.FILE_PREFIX, FILE_PREFIX_VALUE);
  }

  @Test
  public void testSinkTasks() {
    replayAll();

    connector.start(sinkProperties);

    final var oneTaskConfig = connector.taskConfigs(1);
    assertEquals(1, oneTaskConfig.size());

    final var manyTaskConfigs = connector.taskConfigs(2);
    assertEquals(2, manyTaskConfigs.size());

    verifyAll();
  }

  @Test
  public void testTaskClass() {
    replayAll();

    connector.start(sinkProperties);
    assertEquals(SolitaryFileSinkTask.class, connector.taskClass());

    verifyAll();
  }
}
