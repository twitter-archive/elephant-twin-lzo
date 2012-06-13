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
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.google.common.base.Joiner;
import com.twitter.common.args.Arg;
import com.twitter.common.args.ArgFilters;
import com.twitter.common.args.ArgScanner;
import com.twitter.common.args.CmdLine;
import com.twitter.common.args.constraints.NotNull;
import com.twitter.elephanttwin.indexing.AbstractBlockIndexingJob;
import com.twitter.elephanttwin.util.HdfsUtils;

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
public class LZOBlockLevelIndexingJobs extends AbstractBlockIndexingJob implements Tool {

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

  @Override
  protected String getJobName() {
    return "LZOBlockLevelIndexingJobs:input="
        + Joiner.on(",").join(input.get());
  }

  public static void main(String[] args) throws Exception {
    GenericOptionsParser optParser = new GenericOptionsParser(args);
    ToolRunner.run(optParser.getConfiguration(),
        new LZOBlockLevelIndexingJobs(), optParser.getRemainingArgs());
  }

  /**
   * Currently, we skip files that don't have regular lzo indexes.
   * In the future we may want to index them on the fly.
   */
  @Override
  protected boolean fileIsOk(FileStatus stat, FileSystem fs)
      throws IOException {
    Path lzoIndexFile = stat.getPath().suffix(".index");
    return fs.exists(lzoIndexFile);
  }

  @Override
  protected Job setMapper(Job job) {
    job.setMapperClass(LZOBlockOffsetMapper.class);
    job.getConfiguration().set(LZOBlockOffsetMapper.CLASSNAME_CONF,
        value_class.get());
    job.getConfiguration().set(LZOBlockOffsetMapper.COLUMNNAME_CONF,
        columnname.get());
    return job;
  }

  @Override
  protected Boolean doOverwrite() {
    return overwrite.get();
  }

  @Override
  protected String getColumnName() {
    return columnname.get();
  }

  @Override
  protected String getIndex() {
    return index.get();
  }

  @Override
  protected List<String> getInput() {
    return input.get();
  }

  @Override
  protected String getInputFormat() {
    return inputformat.get();
  }

  @Override
  protected Integer getJobPoolSize() {
   return jobPoolSize.get();
  }

  @Override
  protected String getKeyValue() {
    return keyvalue.get();
  }

  @Override
  protected Integer getNumPartitions() {
    return numPartitions.get();
  }

  @Override
  protected Integer getSleepTime() {
    return sleepTime.get();
  }

  @Override
  protected String getValueClass() {
    return value_class.get();
  }

  @Override
  protected Boolean isDryRun() {
    return dryrun.get();
  }

  @Override
  protected PathFilter getFileFilter() {
    return HdfsUtils.lzoFileFilter;
  }
}