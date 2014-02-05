hadoop-fieldformat (beta)
==================

Map Reduce utilities for flat tables.

RATIONALE
---------

Hadoop is build to process semi-structured data; however, in many uses cases we found we still need the data to be more *structured*, such as querying data, ETL, doing data science, etc. Many popular hadoop tools propose different solutions: [Pig][pig] and [Cascading][cascading] uses runtime schema layout to determine fields; Hive uses external MySQL database to save header meta information. In contrast, this project is trying another approach: make the Map-Reduce API be able to process the fields without using external source. Header information is attached into the data itself, and the field mapping can be read/write by mappers and reducers. It is done by rewriting `TextInputFormat` and `TextOutputFormat` classes, so you don't need to change any of your Map-Reduce code, just needt to use different input/output classes and it's done. Currently it is only available to raw Map-Reduce API, but it shouldn't be difficult to integrate into other batch processing tools like [Hive][hive], [Pig][pig], and [Cascading][cascading].

[hive]: http://hive.apache.org
[pig]: https://pig.apache.org
[cascading]: http://www.cascading.org

SYNOPSIS
--------

Before you use the classes, you'll need to know how header is stored in HDFS. The trick is, store the `header.tsv` in `<source directory>/_logs/header.tsv`. When running Map-Reduce jobs, anything that is in `_logs` won't be read, thus it is compatible to other map-reduce ecosystem.

### Reading fields in Mapper

To use this library, just setup `job.setInputFormatClass(FieldInputFormat.class)` instead of the default `TextInputFormat.class`. 

```java

public int run (String[] args) throws Exception {
  Job job = new Job(getConf());

  FileInputFormat.addInputPaths(job, args[0]);
  job.setInputFormatClass(FieldInputFormat.class);
  job.setMapperClass(ExampleMapper.class);
  job.setNumReduceTasks(0)
  FileOutputFormat.setOutputPath(job, new Path(args[1]));

  job.submit();
}

public static class ExampleMapper extends Mapper<LongWritable, FieldWritable, Text, NullWritable> {

  public void map (LongWritable key, FieldWritable fields, Context context) throws IOException, InterruptedException{
    String ip = fields.get("ip");
    String user_agent = fields.get("user_agent");
    String cookie = fields.get("cookie");

    context.write(new Text(ip+"\t"+user_agent+"\t"+cookie), NullWritable.get());
  }
}
```

If you use wildcard characters in your path. `FieldInputFormat` will read different headers in different paths.


### Write header information to output

Output is as simple as input, just specify the output format class to be `FieldOutputFormat.class`.

```java
    job.setOutputFormatClass(FieldOutputFormat.class);

    String [] header = {"ip", "user_agent", "cookie"};
    String [] body = {ip, user_agent, cookie};
    FieldWritable new_fields = new FieldWritable(header, body);

    context.write(new_fields, NullWritable.get());
```

INSTALL
-------

Hadoop-FieldFormat uses maven for dependency management. To use it in your project, add the following to your `pom.xml` file.

```xml
  <repositories>
    <repository>
        <id>hadoop-fieldformat-mvn-repo</id>
        <url>https://raw.github.com/dryman/hadoop-fieldformat/mvn-repo/</url>
        <snapshots>
            <enabled>true</enabled>
            <updatePolicy>always</updatePolicy>
        </snapshots>
    </repository>
  </repositories>

  <dependencies>
    <dependency>
      <groupId>org.apache.hadoop-contrib</groupId>
      <artifactId>hadoop-fieldformat</artifactId>
      <version>0.3.12</version>
    </dependency>
  </dependencies>
```

ADVANTAGES
----------

1. Useful for long-term aggregation, because map-reduce jobs only need to use string to refer the field, not by column numbers (which may change by time).
2. Gives more semantic on data level.

TODOS
-----

1. Test on YARN environment
2. Setup TravisCI or Jenkins-CI
3. Integrate into other hadoop tools

LICENSE
-------

Copyright (c) 2014 Felix Chern

Distributed under the Apache License Version 2.0
