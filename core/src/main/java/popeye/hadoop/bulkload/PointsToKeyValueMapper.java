package popeye.hadoop.bulkload;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

public class PointsToKeyValueMapper extends Mapper<NullWritable, List<KeyValue>, ImmutableBytesWritable, KeyValue> {

  @Override
  protected void map(NullWritable key, List<KeyValue> keyValues, Context context) throws IOException, InterruptedException {
    for (KeyValue keyValue : keyValues) {
      context.getCounter(Counters.MAPPED_KEYVALUES).increment(keyValues.size());
      context.write(new ImmutableBytesWritable((keyValue.getRow())), keyValue);
    }
  }
}
