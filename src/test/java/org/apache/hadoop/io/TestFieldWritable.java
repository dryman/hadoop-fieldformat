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

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;


public class TestFieldWritable {

  @Test
  public void testConstructorCheck1() {
    try {
      new FieldWritable("abc", "def\tghi");
      fail("header and content field count should be the same");
    }catch(Exception e) {}
    assert(true);
  }
  
  @Test
  public void testConstructorCheck2() {
    try {
      new FieldWritable("abc.", "def");
      fail("header should not contain non word character");
    }catch(Exception e) {}
    assert(true);
  }

  @Test
  public void testHeaderConstructor(){
    FieldWritable f = new FieldWritable("col1\tcol2");
    f.set("abc\tdef");
    assertArrayEquals(f.getHeader(), new String[]{"col1", "col2"});
    assertEquals(f.get("col1"), "abc");
    assertEquals(f.get("col2"), "def");
    
  }
  
  @Test
  public void testHeaderConstructorCount(){
    FieldWritable f = new FieldWritable("col1\tcol2");
    try {
      f.set("abc\tdef\tghi");
      fail("when there's different col count in header and content, should fail");
    } catch(Exception e){}
  }
  
  @Test
  public void testEmptyContent(){
    FieldWritable f = new FieldWritable("col1\tcol2");
    try {
      f.set("\t");
      fail("content that is empty should behave weird");
    } catch(Exception e){}
  }
  
  @Test
  public void testIO() throws IOException{
    DataOutputBuffer out = new DataOutputBuffer();
    DataInputBuffer in = new DataInputBuffer();
    
    out.reset();
    FieldWritable before = new FieldWritable("col1\tcol2");
    before.set("abc\tdef");
    before.write(out);
    
    in.reset(out.getData(), out.getLength());
    FieldWritable after = new FieldWritable();
    after.readFields(in);
    assertTrue(before.equals(after));
  }
  
  @Test
  public void testEquals() {
    FieldWritable a = new FieldWritable("abc", "def");
    FieldWritable b = new FieldWritable("abc", "def");
    assertEquals(b, a);
    assertEquals(0, a.compareTo(b));
  }

  @Test
  public void testClone() {
    FieldWritable a = new FieldWritable("abc", "def");
    FieldWritable b = a.clone();
    assertEquals(b, a);
  }
  
  @Test
  public void testCompare() {
    FieldWritable a = new FieldWritable("abc", "def");
    FieldWritable b = new FieldWritable("abc", "def");    
    int cmp = a.compareTo(b);
    assertEquals(0, cmp);
  }
  
  @Test
  public void testEmptyClone() {
    FieldWritable f = new FieldWritable();
    FieldWritable c = f.clone();
    assertEquals(f, c);
  }

  @Test
  public void testToString(){
    FieldWritable f = new FieldWritable("abc", "def");
    assertEquals("def", f.toString());
  }
}
