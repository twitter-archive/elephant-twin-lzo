/**
 * Copyright 2012 Twitter, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>. 
 */
package com.twitter.elephanttwin.lzo.retrieval;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.hadoop.compression.lzo.LzoCodec;
import com.hadoop.compression.lzo.LzoIndex;
import com.hadoop.compression.lzo.LzopCodec;
import com.twitter.elephantbird.mapreduce.io.ThriftBlockWriter;
import com.twitter.elephantbird.mapreduce.io.ThriftWritable;
import com.twitter.elephantbird.mapreduce.output.LzoBinaryB64LineRecordWriter;
import com.twitter.elephantbird.mapreduce.output.LzoBinaryBlockRecordWriter;
import com.twitter.elephantbird.pig.load.ThriftPigLoader;
import com.twitter.elephanttwin.gen.ExciteLog;
import com.twitter.elephanttwin.retrieval.IndexedPigLoader;

/**
 * Testing lzo block level indexing and use PigServer to test using the
 * IndexedLZOPigLoader to automatically use the indexes.
 * Need native lzo library in order to run the tests.
 <pre>
 {@code
 ant test -Dnoivy=true
          -Dtest.library.path=/path/to/native/Mac_OS_X-x86_64-64
          -Dtestcase=TestIndexing
           -Dtest.output=yes
}
</pre>
 */

public class TestLzoIndexing {

  //root directory for the testing
  private static String TESTDIR = System.getProperty("test.build.data") +
      "/TestIndexing/";

  // event names to be repeated and stored in lzo files;
  private static String[] uids = new String[] { "k1", "k2", "k3", "k4"};

  private static String INPUTDIR = "input/";
  //directory to store lzo files to be created and indexed
  private static File inputDir = new File(TESTDIR, "input");

  //directory under root dir to store generated indexes;
  private static String INDEXDIR = "indexes/";
  private static PigServer pigServer=null;
  private static Configuration conf;
  private static LzopCodec codec;

  // total number of times each key appear in the files;
  //used to check agains Pig query result;
  private static int repeatFactor;

  private static long globsCnt;      // used to compare pig query using globs.
  private static Long cnt;
  /**
   * first create a .lzo input file which contains ExciteLog data, then index it
   * to then create a .lzo.index file finally start an indexing job on
   * uid.
   *
   * @throws Exception
   */
  @BeforeClass
  public static void setUp() throws Exception {

    conf = new Configuration();

    conf.set("io.compression.codecs", "com.hadoop.compression.lzo.LzopCodec");
    conf.setInt(LzoCodec.LZO_BUFFER_SIZE_KEY, 256);
    codec = new LzopCodec();
    codec.setConf(conf);
    FileUtil.fullyDelete( new File(TESTDIR));
    inputDir.mkdirs();

    // close any FileySystem from previous tests:
    FileSystem.get(conf).close();

    //create 3 files to test globs and test on single lzo block in a split;
    //create File 1, which has only one lzo block.
    FileSystem fs = FileSystem.get(conf);
    String baseFilePath = TESTDIR + INPUTDIR;
    LzoIndex index ;
    int repeatFactor1 = 1;
    createLZOFile(baseFilePath+"11.lzo", repeatFactor1, true);
    index = LzoIndex.readIndex(fs, new Path(baseFilePath+"11.lzo"));
    if(index.getNumberOfBlocks() > 1)
      throw new RuntimeException(baseFilePath+"11.lzo has more than one " +
          "lzo block" );

    //create File 2, which has more than 1 lzo blocks.
    int repeatFactor2 = 10;
    createLZOFile(baseFilePath+"21.lzo", repeatFactor2, true);
    index = LzoIndex.readIndex(fs, new Path(baseFilePath+"21.lzo"));
    if(index.getNumberOfBlocks() < 2)
      throw new RuntimeException(baseFilePath+"21.lzo has only one lzo block" );
    //index the created lzo files without combining lzo blocks;
    String[] args = new String[] {
        "-jobpoolsize=1",
        "-index=" + TESTDIR + INDEXDIR,
        "-input=" + TESTDIR + INPUTDIR,
        "-inputformat=com.twitter.elephantbird.mapreduce.input." +
            "LzoThriftB64LineInputFormat",
        "-value_class=com.twitter.elephanttwin.gen.ExciteLog",
        "-columnname=uid", "-num_partitions=1", "-sleeptime=1"};

    GenericOptionsParser optParser = new GenericOptionsParser(args);
    ToolRunner.run(conf, new LZOBlockLevelIndexingJobs(),
        optParser.getRemainingArgs());

    //create a new lzo file 3 to test combining lzo blocks.

    int repeatFactor3 = 30;
    createLZOFile(baseFilePath + "31.lzo", repeatFactor3, true); //b64 format
    index = LzoIndex.readIndex(fs, new Path(baseFilePath+"31.lzo"));
    if(index.getNumberOfBlocks() < 2)
      throw new RuntimeException(baseFilePath+"31.lzo has only one lzo block" );


    //create lzo binary block format input files.

    int repeatFactor4 = 1;
    createLZOFile(baseFilePath + "b11.lzo", repeatFactor4, false);
    index = LzoIndex.readIndex(fs, new Path(baseFilePath + "b11.lzo"));
    if(index.getNumberOfBlocks() > 1)
      throw new RuntimeException(baseFilePath+"b11.lzo has more than one " +
          "lzo block" );

    //create File 2, which has more than 1 lzo blocks.
    int repeatFactor5 = 10;
    createLZOFile(baseFilePath + "b21.lzo", repeatFactor5, false);
    index = LzoIndex.readIndex(fs, new Path(baseFilePath + "b21.lzo"));
    if(index.getNumberOfBlocks() < 2)
      throw new RuntimeException(baseFilePath + "b21.lzo has only one lzo block" );

    int repeatFactor6 = 30;
    createLZOFile(baseFilePath + "b31.lzo", repeatFactor6, false); //b64 format
    index = LzoIndex.readIndex(fs, new Path(baseFilePath + "b31.lzo"));
    if(index.getNumberOfBlocks() < 2)
      throw new RuntimeException(baseFilePath+"b31.lzo has only one lzo block" );

    //index the created lzo files, and allow combining lzo blocks;
    //notice the allowedgapsize parameter, and the nooverwrite flag.

    args = new String[] {
        "java", "-cp", "./target/classes/",
        "-DLZOBlocOffsetMapper.allowedgapsize=100",
        "-jobpoolsize=1",
        "-index=" + TESTDIR + INDEXDIR,
        "-input=" + TESTDIR + INPUTDIR,
        "-inputformat=com.twitter.elephantbird.mapreduce.input." +
            "LzoThriftB64LineInputFormat",
        "-value_class=com.twitter.elephanttwin.gen.ExciteLog",
        "-columnname=uid", "-num_partitions=1", "-sleeptime=1",
        "-overwrite=false"};
    ProcessBuilder pb = new ProcessBuilder(args);
    Process proc = pb.start();
    proc.waitFor();
/*
    optParser = new GenericOptionsParser(args);
    ToolRunner.run(new Configuration(), new LZOBlockLevelIndexingJobs(),
        optParser.getRemainingArgs());
    //index the file, and combine lzo blocks.
*/
    // the number of each key appears in all files
    repeatFactor = repeatFactor1 + repeatFactor2 + repeatFactor3 +
        repeatFactor4 + repeatFactor5 + repeatFactor6 ;
    // number of rows has the same unique key in two files matching *1.lzo globs
    globsCnt = repeatFactor;

    pigServer = new PigServer(ExecType.LOCAL);
    // set lzo codec:
    pigServer.getPigContext().getProperties().setProperty(
        "io.compression.codecs", "com.hadoop.compression.lzo.LzopCodec");
    pigServer.getPigContext().getProperties().setProperty(
        "io.compression.codec.lzo.class", "com.hadoop.compression.lzo.LzoCodec");
  }


  // create a lzo file and its index file.
  //Repeat the uids array repeatTimes times.
  //create base64 format input files if b64format is true, otherwise create
  //binary block format files.
  private static void createLZOFile(String fileName, int repeatTimes, boolean b64format )
      throws Exception {

    File file = new File(fileName);
    // first create the file, duplicate the strings as many times as needed
    // to make rowCnt lines in the generated file
    if (file.exists()) {
      file.delete();
    }
    DataOutputStream os = new DataOutputStream(
        codec.createOutputStream(new FileOutputStream(file)));

    RecordWriter<ExciteLog, ThriftWritable<ExciteLog>> writer = null;

    if(b64format)
      //LzoBinaryB64LineRecordWriter<ExciteLog, ThriftWritable<ExciteLog>>
      writer = LzoBinaryB64LineRecordWriter.newThriftWriter(ExciteLog.class, os);
    else
      //LzoBinaryBlockRecordWriter<ExciteLog, ThriftWritable<ExciteLog>>
      writer  =  new LzoBinaryBlockRecordWriter<ExciteLog, ThriftWritable<ExciteLog>>(new ThriftBlockWriter<ExciteLog>(os, ExciteLog.class, 10));

    ThriftWritable<ExciteLog> thriftWritable = ThriftWritable.newInstance(ExciteLog.class);
    for (int i = 0; i < repeatTimes; i++)
      for (int j = 0; j < uids.length; j++) {
        ExciteLog ExciteLog = new ExciteLog();
        ExciteLog.setUid(uids[j]);
        thriftWritable.set(ExciteLog);
        writer.write(null, thriftWritable);
      }

    writer.close(null);
    // need to create an LZO index file for it in order to use it.
    LzoIndex.createIndex(FileSystem.get(conf), new Path(fileName));
  }

  /**
   * Search each unique key using IndexedLZOPigLoader,
   * do a grouping and count the number of rows in the results,
   * and then compare the result to the actual number of rows in the input file.
   * @throws IOException
   */
  @Test
  public void testSearchAllKeys() throws IOException {


    // make sure each key appears repeatFactor times in the indexed file

    pigServer.registerQuery(String.format(
        "T1 = load '%s' using %s('%s','%s','%s');\n",
        inputDir.toURI().toString(),
        IndexedPigLoader.class.getName(),
        ThriftPigLoader.class.getName(),
        ExciteLog.class.getName(),TESTDIR + INDEXDIR));

    for (String searchValue : uids) {
      pigServer.registerQuery(String.format("T2 = filter T1 by uid" +
          " == '%s' ;\n",searchValue));
      Iterator<Tuple> rows = pigServer.openIterator("T2");

      Assert.assertNotNull(rows);
      cnt=0L;
      while(rows.hasNext()){
        cnt++;
        rows.next();
      }
      Assert.assertEquals("searching on key "+searchValue+" failed to produce " +
          "correct number of rows"  + cnt + "!=" + repeatFactor,
          (long)repeatFactor, (long)cnt) ;
    }
  }

  /**
   * Search key which doesn't exist in the input file.
   * @throws IOException
   */
  @Test
  public void testEmptySearch() throws IOException {

    pigServer.registerQuery(String.format(
        "T1 = load '%s' using %s('%s','%s','%s');\n",
        inputDir.toURI().toString(),
        IndexedPigLoader.class.getName(),
        ThriftPigLoader.class.getName(),
        ExciteLog.class.getName(),TESTDIR + INDEXDIR));
    pigServer.registerQuery(String.format("T2 = filter T1 by uid == " +
        "'%s' ;\n","nosuchkey" ));
    Iterator<Tuple> rows = pigServer.openIterator("T2");
    Assert.assertNotNull(rows);
    Assert.assertFalse(rows.hasNext());
  }


  /**
   * Test filtering conditions like  'abcd' == column_name; test that it is
   * pushed down and use the index files.
   */
  @Test
  public void testSearchValueEqualsColumn() throws IOException {

    pigServer.registerQuery(String.format(
        "T1 = load '%s' using %s('%s','%s','%s');\n",
        inputDir.toURI().toString(),
        IndexedPigLoader.class.getName(),
        ThriftPigLoader.class.getName(),
        ExciteLog.class.getName(),TESTDIR + INDEXDIR));
    pigServer.registerQuery(String.format("T2 = filter T1 by '%s' == uid " +
        " ;\n", uids[0]));
    Iterator<Tuple> rows = pigServer.openIterator("T2");
    Assert.assertNotNull(rows);
    cnt=0L;
    while(rows.hasNext()){
      cnt++;
      rows.next();
    }
    Assert.assertEquals(String.format("searching on '%s' == uid" +
        " failed to produce correct number of rows:" + cnt + "!=" +
        repeatFactor, uids[0]), (long)repeatFactor, (long)cnt) ;;
  }

  /**
   * Test Globs patterns like  *1.lzo.
   */
  @Test
  public void testGlobsQuery() throws IOException {

    pigServer.registerQuery(String.format(
        "T1 = load '%s' using %s('%s','%s','%s');\n",
        inputDir.toURI().toString()+"*1.lzo",
        IndexedPigLoader.class.getName(),
        ThriftPigLoader.class.getName(),
        ExciteLog.class.getName(),TESTDIR + INDEXDIR));
    pigServer.registerQuery(String.format("T2 = filter T1 by '%s' == " +
        "uid  ;\n", uids[0]));
    Iterator<Tuple> rows = pigServer.openIterator("T2");
    Assert.assertNotNull(rows);
    cnt=0L;
    while(rows.hasNext()){
      cnt++;
      rows.next();
    }
    Assert.assertEquals(String.format("searching on '%s' == uid " +
        "failed " +	"to produce correct number of rows"  + cnt + "!="
        + globsCnt, uids[0]), (long)repeatFactor, (long)cnt);
  }

  /**
   * Test OR filter condition
   */
  @Test
  public void testOrCondition() throws IOException {

    pigServer.registerQuery(String.format(
        "T1 = load '%s' using %s('%s','%s','%s');\n",
        inputDir.toURI().toString(),
        IndexedPigLoader.class.getName(),
        ThriftPigLoader.class.getName(),
        ExciteLog.class.getName(),
        TESTDIR + INDEXDIR));
    pigServer.registerQuery(String.format("T2 = filter T1 by '%s' == " +
        "uid  or  '%s' == uid  or  '%s' == uid;\n",
        uids[0], uids[1], uids[2]));
    Iterator<Tuple> rows = pigServer.openIterator("T2");
    Assert.assertNotNull(rows);
    cnt=0L;
    while(rows.hasNext()){
      cnt++;
      rows.next();
    }
    Assert.assertEquals("searching  on OR condition failed to produce" +
        " correct number of rows " + cnt + "!=" + repeatFactor*3,
        (long)repeatFactor *3, (long)cnt);
  }

  /**
   * Test AND filter condition
   */

  @Test
  public void testAndCondition() throws IOException {

    pigServer.registerQuery(String.format(
        "T1 = load '%s' using %s('%s','%s','%s');\n",
        inputDir.toURI().toString(),
        IndexedPigLoader.class.getName(),
        ThriftPigLoader.class.getName(),
        ExciteLog.class.getName(),TESTDIR + INDEXDIR));
    pigServer.registerQuery(String.format("T2 = filter T1 by '%s' == uid"
        +	"  and  '%s' == uid  and  '%s' == uid;\n",
        uids[0],uids[1],uids[2] ));
    Iterator<Tuple> rows = pigServer.openIterator("T2");
    Assert.assertNotNull(rows);
    cnt=0L;
    while(rows.hasNext()){
      cnt++;
      rows.next();
    }
    Assert.assertEquals("searching  on OR condition failed to produce " +
        "correct " +	"number of rows " + cnt + "!=" + 0, 0L, (long)cnt);
  }

  @Test
  public void testAndCondition2() throws IOException {

    pigServer.registerQuery(String.format(
        "T1 = load '%s' using %s('%s','%s','%s');\n",
        inputDir.toURI().toString(),
        IndexedPigLoader.class.getName(),
        ThriftPigLoader.class.getName(),
        ExciteLog.class.getName(),TESTDIR + INDEXDIR));
    pigServer.registerQuery(String.format("T2 = filter T1 by '%s' == uid"
        + "  and  '%s' == uid  and  '%s' == uid;\n",
        uids[1],uids[1],uids[1] ));
    Iterator<Tuple> rows = pigServer.openIterator("T2");
    Assert.assertNotNull(rows);
    cnt=0L;
    while(rows.hasNext()){
      cnt++;
      rows.next();
    }
    Assert.assertEquals("searching  on OR condition failed to produce " +
        "correct " +  "number of rows " + cnt + "!=" + 0, repeatFactor, (long)cnt);
  }


  /**
   * Test OR in AND filter condition
   */


  @Test
  public void testORAndCondition() throws IOException {

    pigServer.registerQuery(String.format(
        "T1 = load '%s' using %s('%s','%s','%s');\n",
        inputDir.toURI().toString(),
        IndexedPigLoader.class.getName(),
        ThriftPigLoader.class.getName(),
        ExciteLog.class.getName(),
        TESTDIR + INDEXDIR));
    pigServer.registerQuery(String.format("T2 = filter T1 by ('%s' ==" +
        " uid" + " or  '%s' == uid)  and  ('%s' == uid " +
        " or '%s' == uid);\n", uids[0], uids[1],
        uids[1], uids[2]));
    Iterator<Tuple> rows = pigServer.openIterator("T2");
    Assert.assertNotNull(rows);
    cnt=0L;
    while(rows.hasNext()){
      cnt++;
      rows.next();
    }
    Assert.assertEquals("searching  on OR condition produced " +
        " number of rows: " + cnt + "!=" + repeatFactor, (long)repeatFactor,
        (long)cnt);
  }


  /**
   * Test nested filter condition
   */
  @Test
  public void testNestedCondition() throws IOException {

    pigServer.registerQuery(String.format(
        "T1 = load '%s' using %s('%s','%s','%s');\n",
        inputDir.toURI().toString(),
        IndexedPigLoader.class.getName(),
        ThriftPigLoader.class.getName(),
        ExciteLog.class.getName(),
        TESTDIR + INDEXDIR));
    pigServer.registerQuery(String.format("T2 = filter T1 by" +
        " ( '%s' == uid or  '%s' == uid)  " +
        " and  ('%s' == uid  or '%s' == uid ); \n",
        uids[0], uids[1],  uids[1], uids[2]));
    Iterator<Tuple> rows = pigServer.openIterator("T2");
    Assert.assertNotNull(rows);
    cnt=0L;
    while(rows.hasNext()){
      cnt++;
      rows.next();
    }
    Assert.assertEquals("searching  on AND condition produced " +
        " number of rows: " + cnt + "!= expected: " + repeatFactor,
        (long)cnt, (long)repeatFactor);
  }

  /**
   * Test unsupported filter condition. Instead of creating a subclass for
   * this test case, just use IOException.
   * @throws IOException
   */
  //@Test(expected=IOException.class)
  public void testUnSupportedCondition() throws IOException {
    pigServer.registerQuery(String.format(
        "T1 = load '%s' using %s('%s','%s','%s');\n",
        inputDir.toURI().toString()+"*1.lzo",
        IndexedPigLoader.class.getName(),
        ThriftPigLoader.class.getName(),
        ExciteLog.class.getName(),TESTDIR + INDEXDIR));
    pigServer.registerQuery("T2 = filter T1 by uid != 'abcd' ;");
    pigServer.openIterator("T2");
  }

  @AfterClass
  public static void tearDown() throws IOException {
    if(pigServer !=null) pigServer.shutdown();
    FileUtil.fullyDelete( new File(TESTDIR));
  }
}
