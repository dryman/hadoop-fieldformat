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

import org.apache.commons.lang.StringUtils;

public class FieldWritable extends BinaryComparable
    implements WritableComparable<BinaryComparable>, Map<String, String>{
  
  private HashMap<String, String> instance;
  private Text content;
  private String [] header;  
  
  public FieldWritable() {
    instance = new HashMap<String, String>();
    content = new Text();
  }
  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.io.BinaryComparable#getBytes()
   * Need to add constructors to make it easy to use.
   */

  @Override
  public byte[] getBytes() {
    return content.getBytes();
  }

  @Override
  public int getLength() {
    return content.getLength();
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    header = Text.readString(in).split("\\t");
    content.readFields(in);
    String [] fields = content.toString().split("\\t");
    if (fields.length != header.length) throw new IllegalArgumentException("FieldWritable header & field lenth don't match");
    for (int i = 0; i < header.length; i++){
      String h = header[i], f = fields[i];
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

  @Override
  public String put(String key, String value) {
    // TODO, need document the behavior
    if (key.matches("\\W")) throw new IllegalArgumentException("header must be word characters [a-zA-Z_0-9]");
    if (value.matches("\\t")) throw new IllegalArgumentException("field cannot contain tabs");
    if (!instance.containsKey(key)){
      String new_header = StringUtils.join(header, "\t") + "\t" + key;
      header = new_header.split("\\t");
      try {
        ByteBuffer bb = Text.encode(value);
        content.append(bb.array(), 0, bb.limit());
      } catch (CharacterCodingException e) {
        e.printStackTrace();
      }
    } else {
      int i = 0;
      for (; !header[i].equals(key); i++);
      String [] content_arr = content.toString().split("\\t");
      content_arr[i] = value;
      content = new Text(StringUtils.join(content_arr, "\t"));
    }   
    return instance.put(key, value);
  }

  @Override
  public void putAll(Map<? extends String, ? extends String> m) {
    // TODO, document bad behavior
    for (Map.Entry<? extends String, ? extends String> entry : m.entrySet()){
      put(entry.getKey(), entry.getValue());
    } 
  }

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
      for (; !header[i].equals(key); i++);
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
