hadoop-fieldformat
==================

Structured input, output, and writable classes for map-reduce

DONE
----

0. `FieldWritable` Working implementation with tests. It captures failure aggresively, but the performance need to be enhanced.
1. `FieldInputFormat` Ok implementation but not tests yet.
2. `FieldRecordReader` Same as above.

TODO
----

1. Rationale
2. Implementations
3. Test against MR1, MR2 apis
4. Setting tests on MiniDFSCluster


`FieldInputFormat` class reads the meta-data from /_logs/header.tsv, and turn the Text object into a Map instead of plain
text representation. Also, `FieldOutputFormat` will insert the header information into the /_logs/header.tsv after
map-reduce program succeed.

Example program:

```java

public int run (String[] args) throws Exception {
  Job job = new Job(getConf());

  job.setInputFormatClass(FieldInputFormat.class);
  job.setMapperClass(ExampleMapper.class);
  job.setOutputKeyClass(FieldWritable.class);
  job.setOutputValueClass(NullWritable.class);
  job.setOutputFormatClass(FieldOutputFormat.class);
  job.setNumReduceTasks(0);

  job.submit();
}

public static class ExampleMapper extends Mapper<LongWritable, FieldWritable, FieldWritable, NullWritable> {

  public void map (LongWritable key, FieldWritable fields, Context context) throws IOException, InterruptedException{
    String ip = fields.get("ip");
    String user_agent = fields.get("user_agent");
    String cookie = fields.get("cookie");

    String [] header = {"ip", "user_agent", "cookie"};
    String [] body = {ip, user_agent, cookie};
    FieldWritable new_fields = new FieldWritable(header, body);

    context.write(new_fields, NullWritable.get());
  }
}
```
