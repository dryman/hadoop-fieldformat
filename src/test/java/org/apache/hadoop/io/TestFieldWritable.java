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

import org.junit.Test;

/**
 * 
 *
 */
public class TestFieldWritable {

  @Test
  public void testConstructor1() {
    try {
      FieldWritable f = new FieldWritable("abc", "def\tghi");
      System.out.println(f);
      fail("header and content field count should be the same");
    }catch(Exception e) {}
    assert(true);
  }
  
  @Test
  public void testConstructor2() {
    try {
      FieldWritable f = new FieldWritable("abc.", "def");
      System.out.println(f);
      fail("header should not contain non word character");
    }catch(Exception e) {}
    assert(true);
  }

  // TODO: need to write more test cases
  // like insert, ordering, toString...etc.
}
