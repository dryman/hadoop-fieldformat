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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FieldWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

/** An {@link OutputFormat} that writes plain text files. */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class FieldOutputFormat extends TextOutputFormat<FieldWritable, NullWritable> {
  public static String FIELD_HEADER = "mapreduce.fieldoutput.header";
  private FieldOutputCommitter committer = null;
  protected static class FieldRecordWriter extends TextOutputFormat.LineRecordWriter<FieldWritable, NullWritable>{
    private TaskAttemptContext job;
    private boolean isHeaderSet = false;
    private static final String utf8 = "UTF-8";
    private static final byte[] newline;
    static {
      try {
        newline = "\n".getBytes(utf8);
      } catch (UnsupportedEncodingException uee) {
        throw new IllegalArgumentException("can't find " + utf8 + " encoding");
      }
    }
    public FieldRecordWriter(DataOutputStream out, TaskAttemptContext job) {
      super(out);
      this.job = job;
    }
    
    public synchronized void write(FieldWritable key, NullWritable value)
        throws IOException {
        if (!isHeaderSet){
          if (job.getConfiguration().get(FIELD_HEADER) == null){
            String header = StringUtils.join(key.getHeader(), ",");
            job.getConfiguration().set(FIELD_HEADER, header);
          }
          isHeaderSet=true;
        }
        out.write(key.getBytes(), 0, key.getLength());
        out.write(newline);
      }
  }
  
  @Override
  public RecordWriter<FieldWritable, NullWritable> getRecordWriter(TaskAttemptContext job) throws IOException{
    Configuration conf = job.getConfiguration();
    boolean isCompressed = getCompressOutput(job);
    CompressionCodec codec = null;
    String extension = "";
    if (isCompressed) {
      Class<? extends CompressionCodec> codecClass = 
        getOutputCompressorClass(job, GzipCodec.class);
      codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
      extension = codec.getDefaultExtension();
    }
    Path file = getDefaultWorkFile(job, extension);
    FileSystem fs = file.getFileSystem(conf);

    if (!isCompressed) {
      FSDataOutputStream fileOut = fs.create(file, false);
      return new FieldRecordWriter(fileOut, job);
    } else {
      FSDataOutputStream fileOut = fs.create(file, false);
      return new FieldRecordWriter(new DataOutputStream
                                        (codec.createOutputStream(fileOut)),
                                         job);
    }
  }
  
  @Override
  public synchronized 
  OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
    System.out.println("get field output committer");
    if (committer == null) {
      Path output = getOutputPath(context);
      // maybe setup a delegate to this class (which contains record writer)?
      committer = new FieldOutputCommitter(output, context);
    }
    return committer;
  }
  
}
