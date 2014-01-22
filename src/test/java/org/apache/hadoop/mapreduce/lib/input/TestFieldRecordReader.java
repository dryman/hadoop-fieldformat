/**
 * 
 */
package org.apache.hadoop.mapreduce.lib.input;

import static org.junit.Assert.assertEquals;

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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.After;
import org.junit.Test;

/**
 * Test for FieldRecordReader
 * it is basically as same as LineRecordReader
 */
public class TestFieldRecordReader {
  private static Path workDir = new Path(new Path(System.getProperty(
      "test.build.data", "."), "data"), "TestFieldInputFormat");
  private static Path inputDir = new Path(workDir, "input");
  private static Path outputDir = new Path(workDir, "output");
  

  
  public void createInputDir(Configuration conf) throws IOException{
    FileSystem localFs = FileSystem.getLocal(conf);
    Path file = new Path(inputDir, "part-r-00000");
    Writer writer = new OutputStreamWriter(localFs.create(file));
    writer.write("abc\tdef\nghi\tjkl");
    writer.close();
    Path header = new Path(new Path(inputDir, "_logs"), "header.tsv");
    writer = new OutputStreamWriter(localFs.create(header));
    writer.write("col1\tcol2");
    writer.close();
  }
  
  public String readOutputFile(Configuration conf) throws IOException{
    FileSystem localFs = FileSystem.getLocal(conf);
    Path file = new Path(outputDir, "part-m-00000");
    int len = (int) localFs.getFileStatus(file).getLen();
    byte[] buf = new byte[len];
    InputStream in = localFs.open(file);
    String content;
    try {
      in.read(buf, 0, len);
      content = new String(buf, "UTF-8");
    } finally {
      in.close();
    }
    return content;
  }
  
  public void createAndRunjob(Configuration conf) throws IOException, InterruptedException, ClassNotFoundException{
    Job job = new Job(conf);
    job.setJarByClass(TestFieldRecordReader.class);
    job.setMapperClass(RecordMapper.class);
    job.setNumReduceTasks(0);
    job.setInputFormatClass(FieldInputFormat.class);
    FieldInputFormat.addInputPath(job, inputDir);
    FileOutputFormat.setOutputPath(job, outputDir);
    job.waitForCompletion(true);
  }
  
  public static class RecordMapper extends Mapper<LongWritable, FieldWritable, Text, NullWritable>{
    public void map (LongWritable key, FieldWritable val, Context context) throws IOException, InterruptedException{
      context.write(new Text(val.get("col2")+"\t"+val.get("col1")), NullWritable.get());
    }
  }
  

  @Test
  public void testFieldRecordReader() throws IOException, InterruptedException, ClassNotFoundException {
    Configuration conf = new Configuration();
    FileSystem localFs = FileSystem.getLocal(conf);
    localFs.delete(workDir, true);
    createInputDir(conf);
    createAndRunjob(conf);
    String expected = "def\tabc\njkl\tghi\n";
    assertEquals(expected, readOutputFile(conf));
  }
  
  @After
  public void tearDown() throws IOException{
    Configuration conf = new Configuration();
    FileSystem localFs = FileSystem.getLocal(conf);
    localFs.delete(new Path(System.getProperty(
        "test.build.data", "."), "data"), true);
  }

}
