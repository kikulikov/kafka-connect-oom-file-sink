package io.michelin.connect;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

public class SolitaryFileSinkConfig extends AbstractConfig {

  public static final String FILE_PATH = "path";
  private static final String FILE_PATH_DOC = "Destination path.";

  public static final String FILE_PREFIX = "prefix";
  private static final String FILE_PREFIX_DOC = "File prefix.";

  public static final String DEST_FILE_PERMISSIONS = "file.dest.permissions";
  private static final String DEST_FILE_PERMISSIONS_DOC = "File permissions posix style.";

  public static final ConfigDef CONFIG_DEF =
      new ConfigDef()
          .define(FILE_PATH, Type.STRING, Importance.HIGH, FILE_PATH_DOC)
          .define(FILE_PREFIX, Type.STRING, null, Importance.LOW, FILE_PREFIX_DOC)
          .define(DEST_FILE_PERMISSIONS, Type.STRING, null, Importance.LOW, DEST_FILE_PERMISSIONS_DOC);

  public SolitaryFileSinkConfig(Map<String, String> parsedConfig) {
    super(CONFIG_DEF, parsedConfig);
  }
}
