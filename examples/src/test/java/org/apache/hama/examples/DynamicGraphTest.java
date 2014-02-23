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
package org.apache.hama.examples;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hama.HamaConfiguration;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link org.apache.hama.examples.DynamicGraph}
 */
public class DynamicGraphTest {
  private static String OUTPUT = "/tmp/page-out";
  private Configuration conf = new HamaConfiguration();
  private FileSystem fs;

  private void deleteTempDirs() throws Exception {
      if (fs.exists(new Path(OUTPUT)))
        fs.delete(new Path(OUTPUT), true);
  }

  private void verifyResult() throws IOException {
    FileStatus[] globStatus = fs.globStatus(new Path(OUTPUT + "/part-*"));
    for (FileStatus fts : globStatus) {
      BufferedReader reader = new BufferedReader(new InputStreamReader(
          fs.open(fts.getPath())));
      String line = null;
      while ((line = reader.readLine()) != null) {
        String[] split = line.split("\t");
        assertTrue(split[0].equals("sum"));
        assertTrue(split[1].equals("11"));
        System.out.println(split[0] + " : " + split[1]);
      }
    }
  }

  @Before
  public void setUp() throws Exception {
    fs = FileSystem.get(conf);
  }

  @Test
  public void testGraphGeneration() throws Exception {
    try {
      DynamicGraph.main(new String[] { getClass().getResource("/dg.txt").getFile(), OUTPUT });
      verifyResult();
    } finally {
      deleteTempDirs();
    }
  }

}
