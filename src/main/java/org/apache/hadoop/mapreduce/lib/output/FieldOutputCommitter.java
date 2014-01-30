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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class FieldOutputCommitter extends FileOutputCommitter {
  private Path outputPath = null;
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
    if (context.getTaskAttemptID().getTaskID().getId() == 0){
      FileSystem fs = outputPath.getFileSystem(context.getConfiguration());
      Path headerPath = new Path(new Path(outputPath, "_logs"), "header.tsv");
      if (fs.exists(headerPath)){
        if (!fs.delete(headerPath, true)){
          throw new IOException("Could not delete " + headerPath);
        }
      }
      if (header != null) {
        Writer writer = new OutputStreamWriter(fs.create(headerPath));
        writer.write(header);
        writer.close();
      }
    }
  }
}
