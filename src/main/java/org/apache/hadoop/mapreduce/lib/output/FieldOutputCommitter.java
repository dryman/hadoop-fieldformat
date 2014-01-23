/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapreduce.lib.output;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class FieldOutputCommitter extends FileOutputCommitter {
  private Path outputPath = null;
  public static String FIELD_HEADER = "mapreduce.fieldoutput.header";
  private String header;
  
  public String getHeader() {
    return header;
  }

  public void setHeader(String header) {
    System.out.println("setting header");
    this.header = header;
  }

  public FieldOutputCommitter(Path outputPath, TaskAttemptContext context)
      throws IOException {
    super(outputPath, context);
    this.outputPath = outputPath;
  }
  
  @Override
  public void commitTask(TaskAttemptContext context) 
  throws IOException {
    super.commitTask(context);
    System.out.println("task attempt id: " + context.getTaskAttemptID());
  }
  
  @Override
  public void commitJob(JobContext context) throws IOException{
    Configuration conf = context.getConfiguration();
    super.commitJob(context);
    if (this.outputPath != null){
      Path headerPath = new Path(new Path(this.outputPath, "_logs"), "header.tsv");
      System.out.println("header path: "+ headerPath);
      FileSystem fs = headerPath.getFileSystem(conf);
      System.out.println("header is: " + header);
      if (header != null) {
        Writer writer = new OutputStreamWriter(fs.create(headerPath));
        writer.write(header);
        writer.close();
      }
    }
  }
}
