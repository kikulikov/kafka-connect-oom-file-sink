package io.michelin.connect;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.sink.SinkConnector;
import org.easymock.EasyMockSupport;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class CheckpointFileSinkConnectorTest extends EasyMockSupport {

  private static final String MULTIPLE_TOPICS = "test1,test2";
  private static final String FILE_PREFIX_VALUE = "oom_";
  private static final String CHECKPOINT = "STOP";

  private CheckpointFileSinkConnector connector;
  private Map<String, String> sinkProperties;

  @Before
  public void setup() throws Exception {
    connector = new CheckpointFileSinkConnector();

    final ConnectorContext context = createMock(ConnectorContext.class);
    connector.initialize(context);

    final TemporaryFolder topDir = new TemporaryFolder();
    topDir.create();

    final File tmpPath = topDir.newFolder("file-stream-sink-" + UUID.randomUUID().toString());
    final File destPath = topDir.newFolder("file-stream-sink-" + UUID.randomUUID().toString());

    sinkProperties = new HashMap<>();
    sinkProperties.put(SinkConnector.TOPICS_CONFIG, MULTIPLE_TOPICS);
    sinkProperties.put(CheckpointFileSinkConfig.TMP_FILE_PATH, tmpPath.getAbsolutePath());
    sinkProperties.put(CheckpointFileSinkConfig.DEST_FILE_PATH, destPath.getAbsolutePath());
    sinkProperties.put(CheckpointFileSinkConfig.FILE_PREFIX, FILE_PREFIX_VALUE);
    sinkProperties.put(CheckpointFileSinkConfig.CHECKPOINT_RECORD, CHECKPOINT);
  }

  @Test
  public void testSinkTasks() {
    replayAll();

    connector.start(sinkProperties);

    final List<Map<String, String>> oneTaskConfig = connector.taskConfigs(1);
    assertEquals(1, oneTaskConfig.size());

    final List<Map<String, String>> manyTaskConfigs = connector.taskConfigs(2);
    assertEquals(2, manyTaskConfigs.size());

    verifyAll();
  }

  @Test
  public void testTaskClass() {
    replayAll();

    connector.start(sinkProperties);
    assertEquals(CheckpointFileSinkTask.class, connector.taskClass());

    verifyAll();
  }
}
