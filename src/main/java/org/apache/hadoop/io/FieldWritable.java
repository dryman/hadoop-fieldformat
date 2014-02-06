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
public class FieldWritable extends Text implements Map<String, String>, Cloneable{
  
  private HashMap<String, String> instance;
  private String [] header;
  private boolean isDirty = false;
  
  public FieldWritable() {
    super();
    header = null;
    instance = new HashMap<String, String>();
  }
  
  /**
   * Construct FieldWritable that only contains header
   * @param header tab delimited header string
   */
  public FieldWritable(String header) {
    super();
    this.header = header.split("\\t");
    for (String key : this.header){
      if (!key.matches("\\w+")) throw new IllegalArgumentException("header must be word characters [a-zA-Z_0-9]");
    }
    instance = new HashMap<String,String>();
  }
  
  /**
   * Construct FieldWritable that only contains header
   * @param header header string array
   */
  public FieldWritable(String [] header) {
    super();
    this.header = header;
    for (String key : this.header){
      if (!key.matches("\\w+")) throw new IllegalArgumentException("header must be word characters [a-zA-Z_0-9]");
    }
    instance = new HashMap<String,String>();
  }
  
  /**
   * Creates a new FieldWritable from old one.
   * @param old
   */
  public FieldWritable(FieldWritable old){
    super();    
    instance = new HashMap<String,String>();
    this.header = old.header;
    super.set(old.getBytes(), 0, old.getLength());
    if (header != null){
      for (String h : header) {
        instance.put(h, old.get(h));
      }
    }
    isDirty = false;    
  }
  
  /** Construct FieldWritable with header and array
   * @param headers should only contain word characters [a-zA-Z_0-9], 
   * so that it can be easily processed by downstream programs.
   * @param contents should not contain tabs
   */
  public FieldWritable(String [] headers, String [] contents){
    super();
    instance = new HashMap<String, String>();
    header = null;
    if (headers.length != contents.length) {
      throw new IllegalArgumentException("FieldWritable header & field lenth don't match. header: " 
         +headers.length + " content: " + contents.length );
    }
    this.header = headers;
    for (int i = 0; i < this.header.length; i++) {
      if (!headers[i].matches("\\w+")) 
        throw new IllegalArgumentException("header \"" + headers[i] +"\" must be word characters [a-zA-Z_0-9]");
      instance.put(headers[i], contents[i]);
    }
    isDirty = true;
  }
  
  /**
   * Construct FieldWritable with header string and content string.
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
   * Construct FieldWritable with header string and content string.
   * String will be split to fields by tabs
   * @param header should only contain word characters [a-zA-Z_0-9],
   * so that it can be easily processed by downstream programs.
   * @param content each content field should not contain tabs.
   */
  public FieldWritable(String header, String content){
    this(header,content, "\\t");
  }
  
  /**
   * Set the content fields. Will raise IllegalArgumentException if header
   * and content fields count don't match 
   * @param contents
   */
  public void set(String [] contents){
    if (header.length != contents.length) {
      throw new IllegalArgumentException("FieldWritable header & field lenth don't match. header: " 
         +header.length + " content: " + contents.length );
    }
    for (int i = 0; i < this.header.length; i++) {
      if (!header[i].matches("\\w+")) 
        throw new IllegalArgumentException("header \"" + header[i] +"\" must be word characters [a-zA-Z_0-9]");
      instance.put(header[i], contents[i]);
    }
    isDirty = true;
  }
  
  /**
   * Set the content by tab delimited string.
   * Will raise IllegalArgumentException if header
   * and content fields count don't match 
   */
  @Override
  public void set(String content){
    set(content.split("\\t"));
  }
  
  /**
   * Set the content by tab delimited utf8 bytes.
   * Will raise IllegalArgumentException if header
   * and content fields count don't match 
   */
  @Override
  public void set(byte[] utf8){
    Text txt = new Text(utf8);
    set(txt.toString().split("\\t"));
  }
  
  /**
   * Set the content by tab delimited Text object
   * Will raise IllegalArgumentException if header
   * and content fields count don't match 
   */
  @Override
  public void set(Text txt){
    set(txt.toString().split("\\t"));
  }
  
  /**
   * Set the content by another FieldWritable format
   * Will raise IllegalArgumentException if header
   * and content fields count don't match 
   */
  public void set(FieldWritable that){
    set(that.toString().split("\\t"));
  }
  
  public FieldWritable clone(){
    if (isDirty) refreshContent();
    return new FieldWritable(this);
  }
  
  /** WritableComparator optimized for Text keys. */
  public static class FieldComparator extends WritableComparator {
    public FieldComparator() {
      super(FieldWritable.class);
    }

    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {
      int n1 = WritableUtils.decodeVIntSize(b1[s1]);
      int n2 = WritableUtils.decodeVIntSize(b2[s2]);
      return compareBytes(b1, s1+n1, l1-n1, b2, s2+n2, l2-n2);
    }
  }
  
  static {
    // register this comparator
    WritableComparator.define(FieldWritable.class, new FieldComparator());
  }
  
  @Override
  public byte[] copyBytes(){
    if (isDirty) refreshContent();
    return super.copyBytes();
  }
  
  @Override
  public byte[] getBytes(){
    if (isDirty) refreshContent();
    return super.getBytes();
  }
  
  @Override
  public int getLength (){
    if (isDirty) refreshContent();
    return super.getLength();
  }
  
  @Override
  public int charAt (int position){
    if (isDirty) refreshContent();
    return super.charAt(position);
  }
  
  
  
  /**
   * Get a copy of the header string array
   */
  public String [] getHeader (){
    return header.clone();
  }
  

  @Override
  public void readFields(DataInput in) throws IOException {
    header = Text.readString(in).split("\\t");
    super.readFields(in);
    String [] fields = super.toString().split("\\t");
    if (fields.length != header.length) throw new IllegalArgumentException("FieldWritable header & field lenth don't match header: " 
         + header.length + " content: " + fields.length + "\ncontent: "+super.toString());
    for (int i = 0; i < header.length; i++){
      String h = header[i], f = fields[i];
      if (!h.matches("\\w+"))
        throw new IllegalArgumentException("FieldWritable header can contains only word characters [a-zA-Z_0-9]");
      instance.put(h, f);
    }
    isDirty = false;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    if (isDirty) refreshContent();
    String h = StringUtils.join(header, "\t");
    Text.writeString(out, h);
    if (getLength()==0 && header!=null)
      throw new IllegalArgumentException("Writing empty content with header: "+ h);
    super.write(out);
  }
  
  @Override
  public String toString(){
    if (isDirty) refreshContent();
    return super.toString();
  }
  
  @Override 
  public boolean equals(Object o){
    if (isDirty) refreshContent();
    if (o instanceof FieldWritable)
      return super.equals(o);
    return false;
  }
  
  @Override
  public int hashCode() {
    if (isDirty) refreshContent();
    return super.hashCode();
  }
  
  
  private void refreshContent(){
    StringBuilder content = new StringBuilder();
    if (header != null){
      for (String h : header){
        String c = instance.get(h);
        content.append(c+"\t");
      }
      content.deleteCharAt(content.length()-1);
      super.set(content.toString());
    } else {
      super.clear();
    }
    isDirty=false;
  }
  
   

  @Override
  public void clear() {
    throw new UnsupportedOperationException("No put operation supported for unmodifiable map");
  }
  
//map interfaces

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
    if (instance.containsKey(key)){
      isDirty = true;
      return instance.put(key, value);
    } else {
      throw new UnsupportedOperationException("Cannot insert new key-value pair");  
    }
  }

  /**
   * throw new UnsupportedOperationException("No put operation supported for unmodifiable map");
   */
  @Override
  public void putAll(Map<? extends String, ? extends String> m) {
    for (Entry<? extends String, ? extends String> e : m.entrySet()){
      put(e.getKey(), e.getValue());
    }
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
