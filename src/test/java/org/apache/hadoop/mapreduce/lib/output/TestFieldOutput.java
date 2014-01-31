package org.apache.hadoop.mapreduce.lib.output;

import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestFieldOutput {
  private static Path workDir = new Path(new Path(System.getProperty(
      "test.build.data", "."), "data"), "TestFieldOutputFormat");
  private static String attempt = "attempt_200707121733_0001_m_000000_0";
  private static String partFile = "part-m-00000";
  private static TaskAttemptID taskID = TaskAttemptID.forName(attempt);
  
  private static void cleanup() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = workDir.getFileSystem(conf);
    fs.delete(workDir, true);
  }
  
  @Before
  public void setUp() throws Exception {
    cleanup();
  }

  @After
  public void tearDown() throws Exception {
    cleanup();
  }

  @Test
  public void test() {
    //fail("Not yet implemented");
  }

}
