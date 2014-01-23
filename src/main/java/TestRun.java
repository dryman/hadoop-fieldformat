import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FieldWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FieldInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FieldOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class TestRun extends Configured implements Tool {

  /**
   * @param args
   * @throws Exception 
   */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new Configuration(), new TestRun(), args));
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    //conf.set("mapreduce.fieldoutput.header", "ct_audit,ct_action");
    Job job = new Job(conf);
    job.setJobName("test fieldInput");
    job.setJarByClass(TestRun.class);
    MultipleInputs.addInputPath(job, new Path(args[0]), FieldInputFormat.class, CTMapper.class);
    job.setNumReduceTasks(0);
    FieldOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setOutputFormatClass(FieldOutputFormat.class);
    job.submit();
    job.waitForCompletion(true);
    return 0;
  }

  public static class CTMapper extends Mapper<LongWritable, FieldWritable, FieldWritable, NullWritable>{
    public void map(LongWritable key, FieldWritable map, Context context) throws IOException, InterruptedException{
      String out = map.get("ct_audit") + "\t" + map.get("ct_action");
//      String fields[] = map.toString().split("\\t");
//      String out = fields[0] + "\t" + fields[2];
      context.write(new FieldWritable("ct_audit\tct_action",out), NullWritable.get());
    }
  }
}
