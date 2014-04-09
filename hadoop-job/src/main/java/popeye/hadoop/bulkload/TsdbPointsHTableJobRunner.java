package popeye.hadoop.bulkload;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import java.io.IOException;

import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;

public class TsdbPointsHTableJobRunner {
  public static final String HBASE_CONF_QUORUM = "popeye.hadoop.bulkload.hbase.conf.hbase.zookeeper.quorum";
  public static final String HBASE_CONF_QUORUM_PORT = "popeye.hadoop.bulkload.hbase.conf.hbase.zookeeper.clientPort";
  public static final String POINTS_TABLE_NAME = "popeye.hadoop.bulkload.hbase.points.table.name";
  public static final String UNIQUE_ID_TABLE_NAME = "popeye.hadoop.bulkload.hbase.uniqueid.table.name";
  public static final String UNIQUE_ID_CACHE_SIZE = "popeye.hadoop.bulkload.hbase.uniqueid.cachesize";
  public static final String MAX_PENDING_POINTS = "popeye.hadoop.bulkload.max.pending.points";

  public void configureJob(Job job, HTable table) throws IOException, ClassNotFoundException, InterruptedException {
//    TotalOrderPartitioner.setPartitionFile(conf, new Path(""));

    HFileOutputFormat2.configureIncrementalLoad(job, table);

    job.setJarByClass(TsdbPointsHTableJobRunner.class);

    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);

    job.setMapperClass(TestMapper.class);
//    job.setPartitionerClass(Tes.class);
    job.setReducerClass(TestReducer.class);
//
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
//

    FileInputFormat.setInputPaths(job, new Path("file:////tmp/hadoop/input"));
    TextOutputFormat.setOutputPath(job, new Path("file:////tmp/hadoop/output"));
    job.waitForCompletion(true);
  }


  public static class TestMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
  }

  public static class TestReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
  }

}
