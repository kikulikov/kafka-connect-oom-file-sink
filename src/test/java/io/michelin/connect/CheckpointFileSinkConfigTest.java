package io.michelin.connect;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.sink.SinkConnector;
import org.easymock.EasyMockSupport;
import org.junit.Before;
import org.junit.Test;

public class CheckpointFileSinkConfigTest extends EasyMockSupport {

  private static final String MULTIPLE_TOPICS = "test1,test2";
  private static final String TMP_PATH = "/bunch/of/bananas";
  private static final String DEST_PATH = "/bunch/of/monkeys";
  private static final String FILE_PREFIX_VALUE = "oom_";
  private static final String CHECKPOINT = "STOP";

  private Map<String, String> sinkProperties;

  @Before
  public void setup() throws Exception {
    sinkProperties = new HashMap<>();
    sinkProperties.put(SinkConnector.TOPICS_CONFIG, MULTIPLE_TOPICS);
    sinkProperties.put(CheckpointFileSinkConfig.TMP_FILE_PATH, TMP_PATH);
    sinkProperties.put(CheckpointFileSinkConfig.DEST_FILE_PATH, DEST_PATH);
    sinkProperties.put(CheckpointFileSinkConfig.FILE_PREFIX, FILE_PREFIX_VALUE);
    sinkProperties.put(CheckpointFileSinkConfig.CHECKPOINT_RECORD, CHECKPOINT);
  }

  @Test
  public void testConfigValidationWhenOK() {
    replayAll();

    final List<ConfigValue> configValues =
        CheckpointFileSinkConfig.CONFIG_DEF.validate(sinkProperties);
    for (ConfigValue val : configValues) {
      assertEquals("Config property errors: " + val.errorMessages(), 0, val.errorMessages().size());
    }

    verifyAll();
  }

  @Test
  public void testConfigValidationWhenFailure() {
    replayAll();

    final Map<String, String> props = new HashMap<>(sinkProperties);
    props.remove(CheckpointFileSinkConfig.TMP_FILE_PATH);

    final List<ConfigValue> configValues = CheckpointFileSinkConfig.CONFIG_DEF.validate(props);
    final List<ConfigValue> errors =
        configValues.stream()
            .filter(c -> !c.errorMessages().isEmpty())
            .collect(Collectors.toList());

    assertEquals(1, errors.size());

    verifyAll();
  }
}
