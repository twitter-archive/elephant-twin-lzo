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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.twitter.common.args.Arg;
import com.twitter.common.args.ArgFilters;
import com.twitter.common.args.ArgScanner;
import com.twitter.common.args.CmdLine;
import com.twitter.common.args.constraints.NotNull;
import com.twitter.elephantbird.mapreduce.input.LzoInputFormat;
import com.twitter.elephantbird.mapreduce.io.ThriftWritable;
import com.twitter.elephanttwin.gen.DocType;
import com.twitter.elephanttwin.gen.FileIndexDescriptor;
import com.twitter.elephanttwin.gen.IndexType;
import com.twitter.elephanttwin.gen.IndexedField;
import com.twitter.elephanttwin.indexing.MapFileIndexingReducer;
import com.twitter.elephanttwin.io.ListLongPair;
import com.twitter.elephanttwin.io.LongPairWritable;
import com.twitter.elephanttwin.io.TextLongPairWritable;
import com.twitter.elephanttwin.retrieval.BlockIndexedFileInputFormat;
import com.twitter.elephanttwin.util.HdfsUtils;
import com.twitter.elephanttwin.util.YamlConfig;

/**
 * LZOBlockLevelIndexingJobs takes in a list of input paths, finds all lzo
 * files from the input paths (recursively), indexes each file using user
 *  provided InputFormat class and value class
 *   (can be either Thrift or Protocol Buffer class)
 *  on the Column (user provided). The indexed files are stored under user
 * provided IndexDir directory. The actual indexes are stored in MapFile format.
 * There is also an index meta file for each indexed file which records what
 * columns have been indexed for an input file. <p>
 *
 * The user can specify how many parallel index jobs to be used via: <br>
 *  -jobpoolsize=10 <br>
 *  An example indexing job:<br>
 <pre>
 {@code
 hadoop jar my.jar
        com.twitter.elephanttwin.retrieval.LZOBlockLevelIndexingJobs
        -jobpoolsize=10
        -index=/user/user1/index/hdfs/index001
        -input=/logs/events/02/17
        -inputformat=
        com.twitter.elephantbird.mapreduce.input.LzoThriftB64LineInputFormat
        -value_class=com.mycompany.thrift.gen.LogEvent
        -columnname=event_name
        -num_partitions=4
 }
</pre>
 */
public class LZOBlockLevelIndexingJobs extends Configured implements Tool {

  protected Logger LOG =Logger
      .getLogger(LZOBlockLevelIndexingJobs.class);
  public static final String YAML_INPUT_DIR = "input_file_base_directory";


  @NotNull @CmdLine(name = "input", help = "one or more paths to input data, comma" +
      " separated.")
  public static  Arg<List<String>> input = Arg.create(null);

  @NotNull @CmdLine(name = "index", help = "index location")
  public static  Arg<String> index = Arg.create();

  @NotNull @CmdLine(name = "inputformat", help = "actual inputformat class name to" +
      " be used (example com.twitter.elephantbird.mapreduce." +
      "input.LzoThriftB64LineInputFormat")
  public static  Arg<String> inputformat = Arg.create();

  @NotNull @CmdLine(name = "value_class", help = "class name used to read values " +
      "from HDFS (example com.twitter.clientapp.gen.LogEvent")
  public static  Arg<String> value_class = Arg.create();

  @NotNull @CmdLine(name = "columnname", help = "column name to be indexed")
  public static  Arg<String> columnname = Arg.create();

  @CmdLine(name = "keyvalue", help = "searched value on column")
  public static  Arg<String> keyvalue = Arg.create();

  @CmdLine(name = "num_partitions", help = "number of reducers to use to " +
      "create partial index files")
  public static  Arg<Integer> numPartitions = Arg.create();

  @CmdLine(name = "sleeptime", help = "# of seconds (in a loop) to wait" +
      " for all index jobs to finish")
  public static  Arg<Integer> sleepTime = Arg.create(3);

  @CmdLine(name = "configfile", help = "Path to yaml configuration file")
  private static  Arg<String> configFile = Arg.create();

  @CmdLine(name = "overwrite", help = " reindex files (from -input) even " +
      "if previous indexes exist, true by default ")
  private static  Arg<Boolean> overwrite = Arg.create(true);

  @CmdLine(name = "dryrun", help = "won't submit mapreduce jobs, just" +
      " print information before submitting MR jobs")
  private static  Arg<Boolean> dryrun = Arg.create(false);

  @CmdLine(name = "jobpoolsize", help = "the number of concurrent" +
      " MR jobs to be used pool")
  private static  Arg<Integer> jobPoolSize = Arg.create(10);

  int totalInputFiles = 0;
  int totalFiles2Index = 0;
  Boolean noBatchJob;
  YamlConfig config;
  AtomicInteger finishedJobs = new AtomicInteger(0);
  AtomicInteger indexedFiles = new AtomicInteger(0);
  List<String> failedFiles = Collections
      .synchronizedList(new ArrayList<String>());

  /**
   * Index all lzo files (recursively) from user provided input paths. <br>
   * Currently each file is indexed separately.
   */
  @Override
  public int run(String[] args) throws Exception {
    ArgScanner scanner = new ArgScanner();
    scanner.parse(ArgFilters.selectClass(getClass()), Arrays.asList(args));
    return work(null, null, 0);
  }

  /**
   * Create a FileIndexDescriptor to describe what columns have been indexed
   * @param path
   *          the path to the directory where index files are stored for the
   *          input file
   * @return FileIndexDescriptor
   * @throws IOException
   */

  protected void createIndexDescriptors(FileStatus inputFile, FileSystem fs)
      throws IOException {
    Path indexFilePath = new Path(index
        + inputFile.getPath().toUri().getRawPath());

    FileIndexDescriptor fid = new FileIndexDescriptor();
    fid.setSourcePath(inputFile.getPath().toString());
    fid.setDocType(getExpectedDocType());
    LOG.info("getting checksum from:" + inputFile.getPath());
    FileChecksum cksum = fs.getFileChecksum(inputFile.getPath());
    com.twitter.elephanttwin.gen.FileChecksum fidCksum = null;
    if (cksum != null)
      fidCksum = new com.twitter.elephanttwin.gen.FileChecksum(
          cksum.getAlgorithmName(), ByteBuffer.wrap(cksum.getBytes()),
          cksum.getLength());
    fid.setChecksum(fidCksum);
    fid.setIndexedFields(getIndexedFields());
    fid.setIndexType(getIndexType());
    fid.setIndexVersion(getIndexVersion());

    Path idxPath = new Path(indexFilePath + "/"
        + BlockIndexedFileInputFormat.INDEXMETAFILENAME);
    FSDataOutputStream os = fs.create(idxPath, true);
    @SuppressWarnings("unchecked")
    ThriftWritable<FileIndexDescriptor> writable =
    (ThriftWritable<FileIndexDescriptor>) ThriftWritable
    .newInstance(fid.getClass());
    writable.set(fid);
    writable.write(os);
    os.close();
  }

  protected String getJobName() {
    return "LZOBlockLevelIndexingJobs:input="
        + Joiner.on(",").join(input.get());
  }

  protected int getIndexVersion() {
    return 1;
  }

  protected List<IndexedField> getIndexedFields() {
    List<IndexedField> indexed_fields = new ArrayList<IndexedField>();
    indexed_fields
    .add(new IndexedField(columnname.get(), false, false, false));
    return indexed_fields;
  }

  protected DocType getExpectedDocType() {
    return DocType.BLOCK;
  }

  protected IndexType getIndexType() {
    return IndexType.MAPFILE;
  }

  public static void main(String[] args) throws Exception {
    GenericOptionsParser optParser = new GenericOptionsParser(args);
    ToolRunner.run(optParser.getConfiguration(),
        new LZOBlockLevelIndexingJobs(), optParser.getRemainingArgs());
  }

  private class IndexingWorker implements Runnable {
    private FileStatus stat;
    private Job job;
    private FileSystem fs;

    IndexingWorker(Job job, FileStatus file, FileSystem fs) {
      stat = file;
      this.job = job;
      this.fs = fs;
    }

    @Override
    public void run() {
      try {
        Path outputDir = new Path(index.get()
            + stat.getPath().toUri().getRawPath() + "/" + columnname.get());
        fs.delete(outputDir, true);
        long startTime = System.currentTimeMillis();

        MapFileOutputFormat.setOutputPath(job, outputDir);

        LOG.info("Job " + job.getJobName() + " started.");
        job.waitForCompletion(true);

        if (job.isSuccessful()) {
          // create an index meta file to record what columns have
          // been indexed.
          createIndexDescriptors(stat, fs);
          indexedFiles.incrementAndGet();
          LOG.info(job.getJobName() + ": indexing " + stat.getPath()
              + " succeeded and finished in "
              + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
          LOG.info("Indexed " + indexedFiles.get() + " files");

        } else {
          synchronized (failedFiles) {
            failedFiles.add(stat.getPath().toString());
          }
          LOG.info(job.getJobName() + ": indexing " + stat.getPath()
              + " failed,  " + (System.currentTimeMillis() - startTime)
              / 1000.0 + " seconds");
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        finishedJobs.incrementAndGet();
      }
    }
  }

  public int work(Calendar start, Calendar end, int batchId) {

    LOG.info("Starting up indexer...");
    LOG.info(" - input: " + Joiner.on(" ").join(input.get()));
    LOG.info(" - index: " + index);
    LOG.info(" - number of reducers: " + numPartitions);

    Configuration conf = getConf();
    conf.setBoolean("mapred.map.tasks.speculative.execution", false);
    conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

    totalInputFiles = 0; // total number files from input directories

    try {
      ExecutorService pool = Executors.newFixedThreadPool(jobPoolSize.get());

      for (String s : input.get()) {
        Path spath = new Path(s);
        FileSystem fs = spath.getFileSystem(conf);

        List<FileStatus> stats = Lists.newArrayList();

        // get all lzo files from the input paths/directories
        HdfsUtils.addInputPathRecursively(stats, fs, spath,
            HdfsUtils.hiddenDirectoryFilter, HdfsUtils.lzoFileFilter);

        totalInputFiles += stats.size();
        LOG.info(" total files under " + s + ":" + stats.size());

        if (dryrun.get()) {
          continue;
        }

        int filesToIndex = 0;
        for (FileStatus stat : stats) {
          // if the input file has no lzo index file, we wont' index it for now.
          // Later
          // we may create lzo index on the fly for it and then index it.
          if (!hasLZOIndex(stat, fs))
            continue;
          if (!overwrite.get()) {
            if (hasPreviosIndex(stat, fs))
              continue;
          }
          filesToIndex++;
          totalFiles2Index++;
          Job job = new Job(new Configuration(conf));
          job.setJarByClass(getClass());
          job.setInputFormatClass(BlockIndexedFileInputFormat.class);
          job.setReducerClass(MapFileIndexingReducer.class);
          job.setMapOutputKeyClass(TextLongPairWritable.class);
          job.setMapOutputValueClass(LongPairWritable.class);
          job.setOutputKeyClass(Text.class);
          job.setOutputValueClass(ListLongPair.class);
          job.setPartitionerClass(TextLongPairWritable.Parititioner.class);
          job.setSortComparatorClass(TextLongPairWritable.PairComparator.class);
          job.setGroupingComparatorClass(TextLongPairWritable.KeyOnlyComparator.class);
          job.setOutputFormatClass(MapFileOutputFormat.class);
          job.setNumReduceTasks(numPartitions.get());
          BlockIndexedFileInputFormat.setIndexOptions(job, inputformat.get(),
              value_class.get(), index.get(), columnname.get());
          job.setMapperClass(LZOBlockOffsetMapper.class);
          job.getConfiguration().set(LZOBlockOffsetMapper.CLASSNAME_CONF,
              value_class.get());
          job.getConfiguration().set(LZOBlockOffsetMapper.COLUMNNAME_CONF,
              columnname.get());
          LzoInputFormat.setInputPaths(job, stat.getPath());
          job.setJobName(getJobName() + ":" + stat.getPath());
          Thread.sleep(3000);
          pool.execute(new IndexingWorker(job, stat, fs));
        }

        LOG.info("total files submitted for indexing under" + s + ":"
            + filesToIndex);
      }

      if (dryrun.get()) {
        return 0;
      }

      int sleepTimeVal = sleepTime.get() * 1000;
      while (finishedJobs.get() < totalFiles2Index) {
        Thread.sleep(sleepTimeVal);
      }
      LOG.info(" total number of files from input directories: "
          + totalInputFiles);
      LOG.info(" total number of files submitted for indexing job: "
          + totalFiles2Index);
      LOG.info(" number of files successfully indexed is: " + indexedFiles);
      if (failedFiles.size() > 0)
        LOG.info(" these files were not indexed:"
            + Arrays.toString(failedFiles.toArray()));
      else
        LOG.info(" all files have been successfully indexed");
      pool.shutdown();
    } catch (Exception e) {
      LOG.error(e);
      return -1;
    }

    if (totalFiles2Index == 0)
      return 0;
    else if (totalFiles2Index != indexedFiles.get() )
      return -1;
    else
      return 1;
  }

  private boolean hasLZOIndex(FileStatus stat, FileSystem fs)
      throws IOException {
    Path lzoIndexFile = stat.getPath().suffix(".index");
    return fs.exists(lzoIndexFile);
  }

  private boolean hasPreviosIndex(FileStatus stat, FileSystem fs)
      throws IOException {
    Path indexDir = new Path(index.get() + stat.getPath().toUri().getRawPath()
        + "/" + columnname.get());
    return fs.exists(indexDir);
  }
}