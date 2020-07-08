package io.michelin.connect;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.*;
import java.nio.file.Paths;
import java.util.*;

import static io.michelin.connect.CheckpointFileSinkTask.allComplete;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class CheckpointFileSinkTaskTest {

  private static final String FILE_PREFIX_VALUE = "oom_";
  private static final String CHECKPOINT = "STOP";

  private CheckpointFileSinkTask task;
  private File tmpPath;
  private File destPath;

  @Before
  public void setup() throws Exception {
    this.task = new CheckpointFileSinkTask();

    final var topDir = new TemporaryFolder();
    topDir.create();

    this.tmpPath = topDir.newFolder("file-stream-sink-" + UUID.randomUUID().toString());
    this.destPath = topDir.newFolder("file-stream-sink-" + UUID.randomUUID().toString());
  }

  @Test
  public void testPutFlushWhenTombstoneIsNull() throws Exception {
    final var sinkProperties = new HashMap<String, String>();
    sinkProperties.put(CheckpointFileSinkConfig.TMP_FILE_PATH, tmpPath.getAbsolutePath());
    sinkProperties.put(CheckpointFileSinkConfig.DEST_FILE_PATH, destPath.getAbsolutePath());
    sinkProperties.put(CheckpointFileSinkConfig.FILE_PREFIX, FILE_PREFIX_VALUE);
    sinkProperties.put(CheckpointFileSinkConfig.CHECKPOINT_RECORD, null);

    task.start(sinkProperties);

    // Testing Scenario
    //
    // >>> Partition=1 <<<
    // key=xxx,offset=1 -> key=xxx,offset=100 (STOP)

    // TODO final var currentOffsets = new HashMap<TopicPartition, OffsetAndMetadata>();

    // STEP #1 (produce)

    task.put(Arrays.asList(
        new SinkRecord("t1", 1, Schema.STRING_SCHEMA, "xxx",
            Schema.STRING_SCHEMA, "key=xxx,offset=1", 1)
    ));

    // TODO currentOffsets.put(new TopicPartition("t1", 1), new OffsetAndMetadata(1));
    final var offsets1 = task.preCommit(Collections.emptyMap());
    assertThat(offsets1.size(), equalTo(0));

    final var tmp1 = Arrays.asList(tmpPath.list());
    assertThat(tmp1.size(), equalTo(1));
    assertThat(tmp1, hasItems("oom_xxx"));

    final var dest1 = Arrays.asList(destPath.list());
    assertThat(dest1.size(), equalTo(0));

    // STEP #2 (commit)

    task.put(Arrays.asList(
        new SinkRecord("t1", 1, Schema.STRING_SCHEMA, "xxx",
            Schema.STRING_SCHEMA, null, 100)
    ));

    // TODO currentOffsets.put(new TopicPartition("t1", 1), new OffsetAndMetadata(100));
    final var offsets2 = task.preCommit(Collections.emptyMap());
    assertThat(offsets2.size(), equalTo(1));
    assertTrue(offsets2.values().stream().filter(p -> p.offset() == 100).count() == 1);

    final var tmp2 = Arrays.asList(tmpPath.list());
    assertThat(tmp2.size(), equalTo(0));

    final var dest2 = Arrays.asList(destPath.list());
    assertThat(dest2.size(), equalTo(1));
    assertThat(dest2, hasItems("oom_xxx"));

    final var msg_xxx = readFromInputStream(new FileInputStream(
        Paths.get(destPath.getAbsolutePath(), "oom_xxx").toFile()));
    assertThat(msg_xxx, is("key=xxx,offset=1"));
  }

  @Test
  public void testPutFlushWhenOK() throws Exception {
    final var sinkProperties = new HashMap<String, String>();
    sinkProperties.put(CheckpointFileSinkConfig.TMP_FILE_PATH, tmpPath.getAbsolutePath());
    sinkProperties.put(CheckpointFileSinkConfig.DEST_FILE_PATH, destPath.getAbsolutePath());
    sinkProperties.put(CheckpointFileSinkConfig.FILE_PREFIX, FILE_PREFIX_VALUE);
    sinkProperties.put(CheckpointFileSinkConfig.CHECKPOINT_RECORD, CHECKPOINT);

    task.start(sinkProperties);

    // Testing Scenario
    //
    // >>> Partition=0 <<<
    // key=101,offset=1 -> key=201,offset=2 -> key=101,offset=3 -> key=101,offset=4 (STOP) -> key=201,offset=5 (STOP)
    //
    // >>> Partition=1 <<<
    // key=xxx,offset=1 -> key=xxx,offset=100 (STOP)

    final var currentOffsets = new HashMap<TopicPartition, OffsetAndMetadata>();

    task.put(Arrays.asList(
        new SinkRecord("t1", 0, Schema.STRING_SCHEMA, "101",
            Schema.STRING_SCHEMA, "key=101,offset=1", 1),
        new SinkRecord("t1", 1, Schema.STRING_SCHEMA, "xxx",
            Schema.STRING_SCHEMA, "key=xxx,offset=1", 1)
    ));

    currentOffsets.put(new TopicPartition("t1", 0), new OffsetAndMetadata(1));
    currentOffsets.put(new TopicPartition("t1", 1), new OffsetAndMetadata(1));
    final var offsets1 = task.preCommit(currentOffsets);
    assertThat(offsets1.size(), equalTo(0));

    final var tmp1 = Arrays.asList(tmpPath.list());
    assertThat(tmp1.size(), equalTo(2));
    assertThat(tmp1, hasItems("oom_101"));
    assertThat(tmp1, hasItems("oom_xxx"));

    final var dest1 = Arrays.asList(destPath.list());
    assertThat(dest1.size(), equalTo(0));

    task.put(Arrays.asList(
        new SinkRecord("t1", 0, Schema.STRING_SCHEMA, "201",
            Schema.STRING_SCHEMA, "key=201,offset=2", 2),
        new SinkRecord("t1", 0, Schema.STRING_SCHEMA, "101",
            Schema.STRING_SCHEMA, "key=101,offset=3", 3),
        new SinkRecord("t1", 0, Schema.STRING_SCHEMA, "101",
            Schema.STRING_SCHEMA, "STOP", 4)
    ));

    currentOffsets.put(new TopicPartition("t1", 0), new OffsetAndMetadata(4));
    currentOffsets.put(new TopicPartition("t1", 1), new OffsetAndMetadata(1));
    final var offsets2 = task.preCommit(currentOffsets);
    assertThat(offsets2.size(), equalTo(0));

    final var tmp2 = Arrays.asList(tmpPath.list());
    assertThat(tmp2.size(), equalTo(3));
    assertThat(tmp2, hasItems("oom_101"));
    assertThat(tmp2, hasItems("oom_201"));
    assertThat(tmp2, hasItems("oom_xxx"));

    final var dest2 = Arrays.asList(destPath.list());
    assertThat(dest2.size(), equalTo(0));

    task.put(Arrays.asList(
        new SinkRecord("t1", 0, Schema.STRING_SCHEMA, "201",
            Schema.STRING_SCHEMA, "STOP", 5)
    ));

    currentOffsets.put(new TopicPartition("t1", 0), new OffsetAndMetadata(5));
    currentOffsets.put(new TopicPartition("t1", 1), new OffsetAndMetadata(1));
    final var offsets3 = task.preCommit(currentOffsets);
    assertThat(offsets3.size(), equalTo(1));
    assertTrue(offsets3.values().stream().filter(p -> p.offset() == 5).count() == 1);

    final var tmp3 = Arrays.asList(tmpPath.list());
    assertThat(tmp3.size(), equalTo(1));
    assertThat(tmp3, hasItems("oom_xxx"));

    final var dest3 = Arrays.asList(destPath.list());
    assertThat(dest3.size(), equalTo(2));
    assertThat(dest3, hasItems("oom_101"));
    assertThat(dest3, hasItems("oom_201"));

    task.put(Arrays.asList(
        new SinkRecord("t1", 1, Schema.STRING_SCHEMA, "xxx",
            Schema.STRING_SCHEMA, "STOP", 100)
    ));

    currentOffsets.put(new TopicPartition("t1", 0), new OffsetAndMetadata(5));
    currentOffsets.put(new TopicPartition("t1", 1), new OffsetAndMetadata(100));
    final var offsets4 = task.preCommit(currentOffsets);
    assertThat(offsets4.size(), equalTo(1));
    assertTrue(offsets4.values().stream().filter(p -> p.offset() == 100).count() == 1);

    final var tmp4 = Arrays.asList(tmpPath.list());
    assertThat(tmp4.size(), equalTo(0));

    final var dest4 = Arrays.asList(destPath.list());
    assertThat(dest4.size(), equalTo(3));
    assertThat(dest4, hasItems("oom_101"));
    assertThat(dest4, hasItems("oom_201"));
    assertThat(dest4, hasItems("oom_xxx"));

    final var msg101 = readFromInputStream(new FileInputStream(
        Paths.get(destPath.getAbsolutePath(), "oom_101").toFile()));
    assertThat(msg101, is("key=101,offset=1\nkey=101,offset=3"));

    final var msg201 = readFromInputStream(new FileInputStream(
        Paths.get(destPath.getAbsolutePath(), "oom_201").toFile()));
    assertThat(msg201, is("key=201,offset=2"));

    final var msg_xxx = readFromInputStream(new FileInputStream(
        Paths.get(destPath.getAbsolutePath(), "oom_xxx").toFile()));
    assertThat(msg_xxx, is("key=xxx,offset=1"));
  }

  private String readFromInputStream(InputStream inputStream) throws IOException {
    final var resultStringBuilder = new StringBuilder();
    try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
      String line;
      while ((line = br.readLine()) != null) {
        if (resultStringBuilder.length() > 0) {
          resultStringBuilder.append("\n");
        }
        resultStringBuilder.append(line);
      }
    }
    return resultStringBuilder.toString();
  }

  @Test(expected = NullPointerException.class)
  public void testPutFlushWhenKeyIsNull() {
    final var sinkProperties = new HashMap<String, String>();
    sinkProperties.put(CheckpointFileSinkConfig.TMP_FILE_PATH, tmpPath.getAbsolutePath());
    sinkProperties.put(CheckpointFileSinkConfig.DEST_FILE_PATH, destPath.getAbsolutePath());
    sinkProperties.put(CheckpointFileSinkConfig.FILE_PREFIX, FILE_PREFIX_VALUE);
    sinkProperties.put(CheckpointFileSinkConfig.CHECKPOINT_RECORD, CHECKPOINT);

    task.start(sinkProperties);

    task.put(Collections.singletonList(
        new SinkRecord("t1", 0, null, null, Schema.STRING_SCHEMA, "line101", 1)
    ));
  }

  @Test
  public void testFilenameWhenOK() {
    final var sinkProperties = new HashMap<String, String>();
    sinkProperties.put(CheckpointFileSinkConfig.TMP_FILE_PATH, "/tmp");
    sinkProperties.put(CheckpointFileSinkConfig.DEST_FILE_PATH, null);
    sinkProperties.put(CheckpointFileSinkConfig.FILE_PREFIX, "oom_");
    sinkProperties.put(CheckpointFileSinkConfig.CHECKPOINT_RECORD, null);

    task.start(sinkProperties);

    final var uuid = UUID.randomUUID().toString();
    final var filename = task.makeFilename("/tmp", uuid);
    Assert.assertEquals("/tmp/oom_" + uuid, filename);
  }

  @Test
  public void testFilenameWhenPrefixIsNull() {
    final var sinkProperties = new HashMap<String, String>();
    sinkProperties.put(CheckpointFileSinkConfig.TMP_FILE_PATH, "/tmp");
    sinkProperties.put(CheckpointFileSinkConfig.DEST_FILE_PATH, null);
    sinkProperties.put(CheckpointFileSinkConfig.FILE_PREFIX, null);
    sinkProperties.put(CheckpointFileSinkConfig.CHECKPOINT_RECORD, null);

    task.start(sinkProperties);

    final var uuid = UUID.randomUUID().toString();
    final var filename = task.makeFilename("/tmp", uuid);
    Assert.assertEquals("/tmp/" + uuid, filename);
  }

  @Test
  public void testAllCompleteWhenAllComplete() {
    final var candidateKeys = new HashSet<CheckpointFileSinkTask.CandidateKey>();
    candidateKeys.add(new CheckpointFileSinkTask.CandidateKey("monkeys", true, 0L));
    candidateKeys.add(new CheckpointFileSinkTask.CandidateKey("bananas", true, 0L));

    Assert.assertEquals(true, allComplete(candidateKeys));
  }

  @Test
  public void testAllCompleteWhenAllIncomplete() {
    final var candidateKeys = new HashSet<CheckpointFileSinkTask.CandidateKey>();
    candidateKeys.add(new CheckpointFileSinkTask.CandidateKey("monkeys", false, 0L));
    candidateKeys.add(new CheckpointFileSinkTask.CandidateKey("bananas", false, 0L));

    Assert.assertEquals(false, allComplete(candidateKeys));
  }

  @Test
  public void testAllCompleteWhenSomeIncomplete() {
    final var candidateKeys = new HashSet<CheckpointFileSinkTask.CandidateKey>();
    candidateKeys.add(new CheckpointFileSinkTask.CandidateKey("monkeys", true, 0L));
    candidateKeys.add(new CheckpointFileSinkTask.CandidateKey("bananas", false, 0L));

    Assert.assertEquals(false, allComplete(candidateKeys));
  }
}