package org.apache.hadoop.mapreduce.lib.output;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FieldWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestFieldOutput {
  private static Path workDir = new Path(new Path(System.getProperty(
      "test.build.data", "."), "data"), "TestFieldOutputFormat");
  private static String attempt = "attempt_200707121733_0001_m_000000_0";
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
 
  public String readOutputFile(Configuration conf, Path path) throws IOException{
    FileSystem localFs = FileSystem.getLocal(conf);
    int len = (int) localFs.getFileStatus(path).getLen();
    byte[] buf = new byte[len];
    InputStream in = localFs.open(path);
    String content;
    try {
      in.read(buf, 0, len);
      content = new String(buf, "UTF-8");
    } finally {
      in.close();
    }
    return content;
  }

  @Test
  public void testFileOutputCommitter() throws 
    NoSuchFieldException, IllegalArgumentException, IllegalAccessException, IOException, InterruptedException {
    Job job = Job.getInstance();
    FieldOutputFormat.setOutputPath(job, workDir);
    Configuration conf = job.getConfiguration();
    
    conf.set("mapreduce.task.attempt.id", attempt); // might fail on other environment?
    JobContext jContext = new JobContextImpl(conf, taskID.getJobID());
    TaskAttemptContext tContext = new TaskAttemptContextImpl(conf, taskID);
    FieldOutputFormat fof = new FieldOutputFormat();
    FieldOutputCommitter committer = (FieldOutputCommitter) fof.getOutputCommitter(tContext);
    
    // setup
    committer.setupJob(jContext);
    committer.setupTask(tContext);
    
    FieldWritable fw = new FieldWritable("col1\tcol2");
    fw.set("abc\tdef");
    

    RecordWriter<FieldWritable, NullWritable> rw = fof.getRecordWriter(tContext);
    rw.write(fw, NullWritable.get());
    rw.close(tContext);

    committer.commitTask(tContext);
    committer.commitJob(jContext);
    
    String output = readOutputFile(conf, new Path(workDir, "part-m-00000"));
    assertEquals("abc\tdef\n", output);
    
    String header = readOutputFile(conf, new Path(new Path(workDir, "_logs"), "header.tsv"));
    assertEquals("col1\tcol2", header);
  }

}
