package io.michelin.connect;

import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.sink.SinkConnector;
import org.easymock.EasyMockSupport;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class SolitaryFileSinkConfigTest extends EasyMockSupport {

  private static final String MULTIPLE_TOPICS = "test1,test2";
  private static final String FILE_PATH = "/bunch/of/monkeys";
  private static final String FILE_PREFIX_VALUE = "oom_";

  private Map<String, String> sinkProperties;

  @Before
  public void setup() throws Exception {
    sinkProperties = new HashMap<>();
    sinkProperties.put(SinkConnector.TOPICS_CONFIG, MULTIPLE_TOPICS);
    sinkProperties.put(SolitaryFileSinkConfig.FILE_PATH, FILE_PATH);
    sinkProperties.put(SolitaryFileSinkConfig.FILE_PREFIX, FILE_PREFIX_VALUE);
  }

  @Test
  public void testConfigValidationWhenOK() {
    replayAll();

    final var configValues = SolitaryFileSinkConfig.CONFIG_DEF.validate(sinkProperties);
    for (ConfigValue val : configValues) {
      assertEquals("Config property errors: " + val.errorMessages(), 0, val.errorMessages().size());
    }

    verifyAll();
  }

  @Test
  public void testConfigValidationWhenFailure() {
    replayAll();

    final var props = new HashMap<>(sinkProperties);
    props.remove(SolitaryFileSinkConfig.FILE_PATH);

    final var configValues = SolitaryFileSinkConfig.CONFIG_DEF.validate(props);
    final var errors = configValues.stream().filter(c -> !c.errorMessages().isEmpty()).collect(Collectors.toList());

    assertEquals(1, errors.size());

    verifyAll();
  }
}