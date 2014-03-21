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

package org.apache.hadoop.mapreduce.lib.input;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FieldWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Treats keys as offset in file and value as line. 
 */
public class FieldRecordReader extends LineRecordReader {
  private static final Log LOG = LogFactory.getLog(FieldRecordReader.class);
  private static Path headerPath = null;

  private FieldWritable value = null;
  private String header = null;

  public FieldRecordReader() {
  }

  public FieldRecordReader(byte[] recordDelimiter) {
    super(recordDelimiter);
  }

  public void initialize(InputSplit genericSplit,
                         TaskAttemptContext context) throws IOException {
    FileSplit split = (FileSplit) genericSplit;
    
    super.initialize(genericSplit, context);
    
    Configuration job = context.getConfiguration();
    Path file = split.getPath();
    FileSystem fs = file.getFileSystem(job);
    if (fs.isFile(split.getPath())){
      headerPath = new Path(split.getPath().getParent().toString() + "/_logs/header.tsv");
    } else{
      headerPath = new Path(split.getPath().toString() + "/_logs/header.tsv");
    }
    if (fs.exists(headerPath)){
      LOG.debug("FieldReocrdReader reading header path: "+ headerPath);  
      FSDataInputStream headerIn = fs.open(headerPath);
      BufferedReader br = new BufferedReader(new InputStreamReader(headerIn));
      header = br.readLine();
      br.close();
    } else {
      LOG.debug("Couldn't find header path at: "+ headerPath);  
    }

  }

  @Override
  public boolean nextKeyValue() throws IOException {
    if(super.nextKeyValue()){
      if (value == null) {
        if (header == null){
          throw new IOException("FieldInputFormat header is null, couldn't find it at path: "+headerPath);
        }
        value = new FieldWritable(header); 
      }
      value.set(super.getCurrentValue());
      return true;
    } else {
      value = null;
      return false;
    }
  }

  @Override
  public FieldWritable getCurrentValue() {
    return value;
  }

}
