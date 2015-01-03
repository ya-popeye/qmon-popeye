package popeye.rollup;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;

import java.io.IOException;

public class RollupMapper extends TableMapper<ImmutableBytesWritable, KeyValue> {

  private RollupMapperEngine engine = null;
  private HTablePool hTablePool = null;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    hTablePool = RollupMapperEngine.createHTablePool(context.getConfiguration());
    engine = RollupMapperEngine.createFromConfiguration(context.getConfiguration(), hTablePool);
  }

  @Override
  protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
    for (RollupMapperOutput out : engine.map(value)) {
      context.write(out.row(), out.keyValue());
    }
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    for (RollupMapperOutput out : engine.cleanup()) {
      context.write(out.row(), out.keyValue());
    }
    hTablePool.close();
  }
}
