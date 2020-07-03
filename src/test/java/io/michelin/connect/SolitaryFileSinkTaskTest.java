package io.michelin.connect;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.*;
import java.util.*;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class SolitaryFileSinkTaskTest {

  private SolitaryFileSinkTask task;
  private File outputDir;

  @Before
  public void setup() throws Exception {
    this.task = new SolitaryFileSinkTask();

    final var topDir = new TemporaryFolder();
    topDir.create();

    this.outputDir = topDir.newFolder("file-stream-sink-" + UUID.randomUUID().toString());
  }

  @Test
  public void testPutFlushWhenOK() throws Exception {
    final var props = new HashMap<String, String>();
    props.put(SolitaryFileSinkConfig.FILE_PATH, outputDir.getAbsolutePath());
    props.put(SolitaryFileSinkConfig.FILE_PREFIX, "oom_");

    task.start(props);

    task.put(Collections.singletonList(
        new SinkRecord("t1", 0, Schema.STRING_SCHEMA, "101", Schema.STRING_SCHEMA, "line101", 1)
    ));

    final var result1 = Arrays.asList(outputDir.list());
    assertThat(result1, hasItems("oom_101"));

    final var msg101 = readFromInputStream(new FileInputStream(outputDir.listFiles()[0]));
    assertThat(msg101, is("line101"));

    task.put(Arrays.asList(
        new SinkRecord("t1", 1, Schema.STRING_SCHEMA, "201", Schema.STRING_SCHEMA, "line201", 1),
        new SinkRecord("t1", 0, Schema.STRING_SCHEMA, "101", Schema.STRING_SCHEMA, "xxx", 1)
    ));

    final var result2 = Arrays.asList(Objects.requireNonNull(outputDir.list()));
    assertThat(result2, hasItems("oom_101", "oom_201"));

    final var msg101xxx = readFromInputStream(new FileInputStream(
        outputDir.listFiles(pathname -> "oom_101".equals(pathname.getName()))[0]));
    assertThat(msg101xxx, is("xxx"));

    final var msg201 = readFromInputStream(new FileInputStream(
        outputDir.listFiles(pathname -> "oom_201".equals(pathname.getName()))[0]));
    assertThat(msg201, is("line201"));
  }

  private String readFromInputStream(InputStream inputStream) throws IOException {
    final var resultStringBuilder = new StringBuilder();
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
    final var props = new HashMap<String, String>();
    props.put(SolitaryFileSinkConfig.FILE_PATH, outputDir.getAbsolutePath());
    props.put(SolitaryFileSinkConfig.FILE_PREFIX, "oom_");

    task.start(props);

    task.put(Collections.singletonList(
        new SinkRecord("t1", 0, null, null, Schema.STRING_SCHEMA, "line101", 1)
    ));
  }

  @Test
  public void testFilenameWhenOK() {
    final var props = new HashMap<String, String>();
    props.put(SolitaryFileSinkConfig.FILE_PATH, "/tmp");
    props.put(SolitaryFileSinkConfig.FILE_PREFIX, "oom_");

    final var task = new SolitaryFileSinkTask();
    task.start(props);

    final var uuid = UUID.randomUUID().toString();
    final var filename = task.makeFilename(uuid);
    Assert.assertEquals("/tmp/oom_" + uuid, filename);
  }

  @Test
  public void testFilenameWhenPrefixIsNull() {
    final var props = new HashMap<String, String>();
    props.put(SolitaryFileSinkConfig.FILE_PATH, "/tmp");
    props.put(SolitaryFileSinkConfig.FILE_PREFIX, null);

    final var task = new SolitaryFileSinkTask();
    task.start(props);

    final var uuid = UUID.randomUUID().toString();
    final var filename = task.makeFilename(uuid);
    Assert.assertEquals("/tmp/" + uuid, filename);
  }
}