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

package org.apache.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.avro.reflect.Stringable;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;


/** This class stores the header and field content using UTF8 encoding,
 * and exposes a map interface for user to query the content fields using
 * header key. <p>Each header key can only contain word characters [a-zA-Z_0-9] 
 * so that it can be easily processed by downstream programs. <p>This class also
 * extends BinaryComparable and has fast run-time performance during shuffling.
 */
@Stringable
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class FieldWritable extends Text
    implements WritableComparable<BinaryComparable>, Map<String, String>{
  
  private HashMap<String, String> instance;
  private Text content;
  private String [] header;  
  
  public FieldWritable() {
    instance = new HashMap<String, String>();
    content = new Text();
  }
  
  public FieldWritable(String header) {
    this.header = header.split("\\t");
    for (String key : this.header){
      if (!key.matches("\\w+")) throw new IllegalArgumentException("header must be word characters [a-zA-Z_0-9]");
    }
    instance = new HashMap<String,String>();
  }
  
  public FieldWritable(String [] header) {
    this.header = header;
    for (String key : this.header){
      if (!key.matches("\\w+")) throw new IllegalArgumentException("header must be word characters [a-zA-Z_0-9]");
    }
    instance = new HashMap<String,String>();
  }
  
  /** Construct fields from two String arrays
   * @param headers should only contain word characters [a-zA-Z_0-9], 
   * so that it can be easily processed by downstream programs.
   * @param contents should not contain tabs
   */
  public FieldWritable(String [] headers, String [] contents){
    instance = new HashMap<String, String>();
    content = new Text();
    header = null;
    set(headers, contents);
  }
  
  /**
   * Construct fields with header string and content string.
   * String will be split to fields by split_regex
   * @param header should only contain word characters [a-zA-Z_0-9],
   * so that it can be easily processed by downstream programs.
   * @param content each content field should not contain tabs.
   * Tabs are used for separation for internal use. Even if you use comma
   * separation, you cannot contain tab in each field.
   * @param split_regex Regex for splitting strings.
   */
  public FieldWritable(String header, String content, String split_regex){
    this(header.split(split_regex), content.split(split_regex));
  }
  
  /**
   * Construct fields with header string and content string.
   * String will be split to fields by tabs
   * @param header should only contain word characters [a-zA-Z_0-9],
   * so that it can be easily processed by downstream programs.
   * @param content each content field should not contain tabs.
   */
  public FieldWritable(String header, String content){
    this(header,content, "\\t");
  }
  
  /** Constructor helper class
   * @param headers
   * @param contents
   */
  public void set(String [] headers, String [] contents){
    if (headers.length != contents.length) {
      throw new IllegalArgumentException("FieldWritable header & field lenth don't match. header: " 
         +headers.length + " content: " + contents.length );
    }
    this.header = headers;
    for (int i = 0; i < this.header.length; i++) {
      if (!headers[i].matches("\\w+")) 
        throw new IllegalArgumentException("header \"" + headers[i] +"\" must be word characters [a-zA-Z_0-9]");
      put(headers[i], contents[i]);
    }
  }
  
  public void setContent(Text content){
    this.content = content;
    String [] fields = this.content.toString().split("\\t");
    if (header.length != fields.length) {
      throw new IllegalArgumentException("FieldWritable header & field lenth don't match. header: " 
         +header.length + " content: " + fields.length );
    }
    int i;
    for (i=0; i < header.length; i++){
      instance.put(header[i], fields[i]);
    }
  }
  
  /**
   * Get the copy of content field's byte. Used by fast compareTo method
   * in {@link org.apache.hadoop.io.BinaryComparable#getBytes}
   */
  @Override
  public byte[] getBytes() {
    return content.getBytes();
  }

  /**
   * Get the copy of content field's length. Used by fast compareTo method
   * in {@link org.apache.hadoop.io.BinaryComparable#getLength}
   */
  @Override
  public int getLength() {
    return content.getLength();
  }
  
  /**
   * Get a copy of the header string array
   */
  public String [] getHeader (){
    return header.clone();
  }
  
  /**
   * Return the content fields (without header)
   */
  @Override
  public String toString(){
    return content.toString();
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    header = Text.readString(in).split("\\t");
    content.readFields(in);
    String [] fields = content.toString().split("\\t");
    if (fields.length != header.length) throw new IllegalArgumentException("FieldWritable header & field lenth don't match");
    for (int i = 0; i < header.length; i++){
      String h = header[i], f = fields[i];
      // we may not need to check value here?
      // maybe it's better to check it on the write side
      if (!h.matches("\\w+"))
        throw new IllegalArgumentException("FieldWritable header can contains only word characters [a-zA-Z_0-9]");
      instance.put(h, f);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, StringUtils.join(header, "\t"));
    content.write(out);
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException("No put operation supported for unmodifiable map");
  }

  @Override
  public boolean containsKey(Object key) {
    return instance.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return instance.containsValue(value);
  }

  @Override
  public Set<java.util.Map.Entry<String, String>> entrySet() {
    return instance.entrySet();
  }

  @Override
  public String get(Object key) {
    return instance.get(key);
  }

  @Override
  public boolean isEmpty() {
    return instance.isEmpty();
  }

  @Override
  public Set<String> keySet() {
    return instance.keySet();
  }

  /**
   * throw new UnsupportedOperationException("No put operation supported for unmodifiable map");
   */
  @Override
  public String put(String key, String value) {
    throw new UnsupportedOperationException("No put operation supported for unmodifiable map");
  }

  /**
   * throw new UnsupportedOperationException("No put operation supported for unmodifiable map");
   */
  @Override
  public void putAll(Map<? extends String, ? extends String> m) {
    throw new UnsupportedOperationException("No put operation supported for unmodifiable map");
  }

  /**
   * Removes a header-content pair.
   */
  @Override
  public String remove(Object key) {
    throw new UnsupportedOperationException("No put operation supported for unmodifiable map");
  }

  @Override
  public int size() {
    return header.length;
  }

  @Override
  public Collection<String> values() {
    return instance.values();
  }

}
