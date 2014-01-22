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
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
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
public class FieldWritable extends BinaryComparable
    implements WritableComparable<BinaryComparable>, Map<String, String>{
  
  private HashMap<String, String> instance;
  private Text content;
  private String [] header;  
  
  public FieldWritable() {
    instance = new HashMap<String, String>();
    content = new Text();
  }
  
  public FieldWritable(String [] header) {
    this.header = header;
    for (String key : this.header){
      if (key.matches("\\W")) throw new IllegalArgumentException("header must be word characters [a-zA-Z_0-9]");
    }
    instance = new HashMap<String,String>();
  }
  
  /** Construct fields from two String arrays
   * @param headers should only contain word characters [a-zA-Z_0-9], 
   * so that it can be easily processed by downstream programs.
   * @param contents should not contain tabs
   */
  public FieldWritable(String [] headers, String [] contents){
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
    String [] headers = header.split(split_regex);
    String [] contents = content.split(split_regex);
    set(headers, contents);
  }
  
  /**
   * Construct fields with header string and content string.
   * String will be split to fields by tabs
   * @param header should only contain word characters [a-zA-Z_0-9],
   * so that it can be easily processed by downstream programs.
   * @param content each content field should not contain tabs.
   */
  public FieldWritable(String header, String content){
    String split_regex = "\\t";
    String [] headers = header.split(split_regex);
    String [] contents = content.split(split_regex);
    set(headers, contents);
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
    for (int i = 0; i< headers.length; i++)
      put(headers[i], contents[i]);
    // A lazy way to construct the class, but we'll just keep it simple first
  }
  
  public void updateContent(Text content){
    this.content = content; // actually this is the same Text object...
    String [] fields = this.content.toString().split("\\t");
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
      if (h.matches("\\W"))
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
    header = null;
    content = new Text();
    instance.clear();
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
   * Append new header field and content field
   * If header field exist, it will replace the old content field with
   * new content.
   * The field's order is as same as the insertion order
   * @param key the new header field
   * @param value the new content field
   */
  @Override
  public String put(String key, String value) {
    // TODO, need document the behavior
    if (key.matches("\\W")) throw new IllegalArgumentException("header must be word characters [a-zA-Z_0-9]");
    if (value.matches("\\t")) throw new IllegalArgumentException("field cannot contain tabs");
    if (value.equals("")) value = "\\N";
    if (!instance.containsKey(key)){      
      String new_header = (header == null) ? key : StringUtils.join(header, "\t") + "\t" + key;
      header = new_header.split("\\t");
      try {
        ByteBuffer bb;
        if (content.toString().equals("")){
          bb = Text.encode(value);
        }else {
          bb = Text.encode("\t"+ value);
        }
        content.append(bb.array(), 0, bb.limit());
      } catch (CharacterCodingException e) {
        e.printStackTrace();
      }
    } else {
      int i = 0;
      for (; i < header.length && !header[i].equals(key); i++){;}
      String [] content_arr = content.toString().split("\\t");
      content_arr[i] = value;
      content = new Text(StringUtils.join(content_arr, "\t"));
    }   
    return instance.put(key, value);
  }

  /**
   * For each key-value pair, runs @{put}
   * Since we don't know exactly what the order of the map would be,
   * this is not a recommended way to insert fields.
   */
  @Override
  public void putAll(Map<? extends String, ? extends String> m) {
    // TODO, document bad behavior
    for (Map.Entry<? extends String, ? extends String> entry : m.entrySet()){
      put(entry.getKey(), entry.getValue());
    } 
  }

  /**
   * Removes a header-content pair.
   */
  @Override
  public String remove(Object key) {
    // TODO: document bad behavior
    if (instance.containsKey(key)){
      ArrayList<String> header_lst = new ArrayList<String>(), content_lst = new ArrayList<String>();
      for (String s : header)
        header_lst.add(s);
      for (String s : content.toString().split("\\t"))
        content_lst.add(s);
      
      int i = 0;
      for (; i < header.length && !header[i].equals(key); i++);
      header_lst.remove(i);
      content_lst.remove(i);
      
      header = new String[header_lst.size()];
      header = header_lst.toArray(header);
      content = new Text(StringUtils.join(content_lst, "\t"));
    }
    return instance.remove(key);
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
