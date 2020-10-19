package io.michelin.connect;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

public class CheckpointFileSinkConfig extends AbstractConfig {

  public static final String TMP_FILE_PATH = "tmp.path";
  private static final String TMP_FILE_PATH_DOC = "Temporary file's location.";

  public static final String DEST_FILE_PATH = "dest.path";
  private static final String DEST_FILE_PATH_DOC = "Final file's location.";

  public static final String CHECKPOINT_RECORD = "checkpoint.record";
  private static final String CHECKPOINT_RECORD_DOC = "Checkpoint message value. Null by default.";

  public static final String FILE_PREFIX = "file.prefix";
  private static final String FILE_PREFIX_DOC = "File prefix. Empty by default.";

  public static final String DEST_FILE_PERMISSIONS = "file.dest.permissions";
  private static final String DEST_FILE_PERMISSIONS_DOC = "File permissions posix style.";

  public static final ConfigDef CONFIG_DEF =
      new ConfigDef()
          .define(TMP_FILE_PATH, Type.STRING, Importance.HIGH, TMP_FILE_PATH_DOC)
          .define(DEST_FILE_PATH, Type.STRING, Importance.HIGH, DEST_FILE_PATH_DOC)
          .define(CHECKPOINT_RECORD, Type.STRING, null, Importance.MEDIUM, CHECKPOINT_RECORD_DOC)
          .define(FILE_PREFIX, Type.STRING, null, Importance.LOW, FILE_PREFIX_DOC)
          .define(
              DEST_FILE_PERMISSIONS, Type.STRING, null, Importance.LOW, DEST_FILE_PERMISSIONS_DOC);

  public CheckpointFileSinkConfig(Map<String, String> parsedConfig) {
    super(CONFIG_DEF, parsedConfig);
  }
}
