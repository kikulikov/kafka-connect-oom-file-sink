package io.michelin.connect;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.*;
import java.util.*;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SolitaryFileSinkTaskTest {

  private SolitaryFileSinkTask task;
  private File outputDir;

  @Before
  public void setup() throws Exception {
    this.task = new SolitaryFileSinkTask();

    final TemporaryFolder topDir = new TemporaryFolder();
    topDir.create();

    this.outputDir = topDir.newFolder("file-stream-sink-" + UUID.randomUUID().toString());
  }

  @Test
  public void testPutFlushWhenOK() throws Exception {
    final Map<String, String> sinkProperties = new HashMap<>();
    sinkProperties.put(SolitaryFileSinkConfig.FILE_PATH, outputDir.getAbsolutePath());
    sinkProperties.put(SolitaryFileSinkConfig.FILE_PREFIX, "oom_");

    task.start(sinkProperties);

    task.put(
        Collections.singletonList(
            new SinkRecord(
                "t1", 0, Schema.STRING_SCHEMA, "101", Schema.STRING_SCHEMA, "line101", 1)));

    final List<String> result1 = Arrays.asList(outputDir.list());
    assertThat(result1, hasItems("oom_101"));

    final String msg101 = readFromInputStream(new FileInputStream(outputDir.listFiles()[0]));
    assertThat(msg101, is("line101"));

    task.put(
        Arrays.asList(
            new SinkRecord(
                "t1", 1, Schema.STRING_SCHEMA, "201", Schema.STRING_SCHEMA, "line201", 1),
            new SinkRecord("t1", 0, Schema.STRING_SCHEMA, "101", Schema.STRING_SCHEMA, "xxx", 1)));

    final List<String> result2 = Arrays.asList(Objects.requireNonNull(outputDir.list()));
    assertThat(result2, hasItems("oom_101", "oom_201"));

    final String msg101xxx =
        readFromInputStream(
            new FileInputStream(
                outputDir.listFiles(pathname -> "oom_101".equals(pathname.getName()))[0]));
    assertThat(msg101xxx, is("xxx"));

    final String msg201 =
        readFromInputStream(
            new FileInputStream(
                outputDir.listFiles(pathname -> "oom_201".equals(pathname.getName()))[0]));
    assertThat(msg201, is("line201"));
  }

  private String readFromInputStream(InputStream inputStream) throws IOException {
    final StringBuilder resultStringBuilder = new StringBuilder();
    try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
      String line;
      while ((line = br.readLine()) != null) {
        resultStringBuilder.append(line);
      }
    }
    return resultStringBuilder.toString();
  }

  @Test(expected = NullPointerException.class)
  public void testPutFlushWhenKeyIsNull() {
    final Map<String, String> sinkProperties = new HashMap<String, String>();
    sinkProperties.put(SolitaryFileSinkConfig.FILE_PATH, outputDir.getAbsolutePath());
    sinkProperties.put(SolitaryFileSinkConfig.FILE_PREFIX, "oom_");

    task.start(sinkProperties);

    task.put(
        Collections.singletonList(
            new SinkRecord("t1", 0, null, null, Schema.STRING_SCHEMA, "line101", 1)));
  }

  @Test
  public void testFilenameWhenOK() {
    final Map<String, String> sinkProperties = new HashMap<String, String>();
    sinkProperties.put(SolitaryFileSinkConfig.FILE_PATH, "/tmp");
    sinkProperties.put(SolitaryFileSinkConfig.FILE_PREFIX, "oom_");

    task.start(sinkProperties);

    final String uuid = UUID.randomUUID().toString();
    final String filename = task.makeFilename(uuid);
    Assert.assertEquals("/tmp/oom_" + uuid, filename);
  }

  @Test
  public void testFilenameWhenPrefixIsNull() {
    final Map<String, String> sinkProperties = new HashMap<String, String>();
    sinkProperties.put(SolitaryFileSinkConfig.FILE_PATH, "/tmp");
    sinkProperties.put(SolitaryFileSinkConfig.FILE_PREFIX, null);

    task.start(sinkProperties);

    final String uuid = UUID.randomUUID().toString();
    final String filename = task.makeFilename(uuid);
    Assert.assertEquals("/tmp/" + uuid, filename);
  }
}
