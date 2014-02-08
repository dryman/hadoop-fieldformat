package org.apache.hadoop.mapreduce.lib.output;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FieldWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FieldInputFormat;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestFieldOutput {
  private static Path outDir = new Path(new Path(System.getProperty(
      "test.build.data", "."), "data"), "TestFieldOutputFormat_out");
  private static Path inDir = new Path(new Path(System.getProperty(
      "test.build.data", "."), "data"), "TestFieldOutputFormat_in");
  private static String attempt = "attempt_200707121733_0001_m_000000_0";
  private static TaskAttemptID taskID = TaskAttemptID.forName(attempt);
  
  private static void cleanup() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = outDir.getFileSystem(conf);
    fs.delete(outDir, true);
    fs.delete(inDir,true);
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
    FieldOutputFormat.setOutputPath(job, outDir);
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
    
    String output = readOutputFile(conf, new Path(outDir, "part-m-00000"));
    assertEquals("abc\tdef\n", output);
    
    String header = readOutputFile(conf, new Path(new Path(outDir, "_logs"), "header.tsv"));
    assertEquals("col1\tcol2", header);
  }
  
  private void createInputDir () throws IOException{
    Job job = Job.getInstance();
    Configuration conf = job.getConfiguration();
    FileSystem localFs = FileSystem.getLocal(conf);
    Path logs_path = new Path(inDir, "_logs");
    Path header_path = new Path(logs_path, "header.tsv");
    Path file = new Path(inDir, "part-r-00000");
    if (!localFs.exists(logs_path)){
      localFs.mkdirs(logs_path);
    }
    Writer writer = new OutputStreamWriter(localFs.create(header_path));
    writer.write("col1\tcol2");
    writer.close();
    
    writer = new OutputStreamWriter(localFs.create(file));
    writer.write("abc\tdef\nghi\tjkl");
    writer.close();
  }
  
  public static class MOReducer extends Reducer<LongWritable, FieldWritable, FieldWritable, NullWritable>{
    private MultipleOutputs<FieldWritable, NullWritable> mo;
    public void setup(Context context){
      mo = new MultipleOutputs<FieldWritable, NullWritable>(context);
    }
    public void cleanup(Context context) throws IOException, InterruptedException{
      mo.close();
    }
    public void reduce(LongWritable keyIn, Iterable<FieldWritable> valIn, Context context) throws IOException, InterruptedException{
      for (FieldWritable f : valIn){
        System.out.println("getting data: " + f);
        context.write(f.clone(), NullWritable.get());
      }
    }
  }

  
  @Test
  public void testMultipleOutputs () throws IOException, InterruptedException, ClassNotFoundException{
    createInputDir();
    Configuration conf = new Configuration();
    conf.set("mapreduce.framework.name", "local");
    Job job = new Job(conf);
    FieldOutputFormat.setOutputPath(job, outDir);    
    
    FieldInputFormat.setInputPaths(job, inDir);
    job.setInputFormatClass(FieldInputFormat.class);
    FileOutputFormat.setOutputPath(job, outDir);
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(FieldWritable.class);
    job.setReducerClass(MOReducer.class);
    job.setOutputFormatClass(FieldOutputFormat.class);
    job.submit();
    job.waitForCompletion(true);
    
    String output = readOutputFile(conf, new Path(outDir, "part-r-00000"));
    assertEquals("abc\tdef\nghi\tjkl\n", output);
    
    String header = readOutputFile(conf, new Path(new Path(outDir, "_logs"), "header.tsv"));
    assertEquals("col1\tcol2", header);
  }

}
