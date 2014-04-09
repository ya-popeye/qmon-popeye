package popeye.hadoop.bulkload;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import popeye.javaapi.storage.hbase.PartiallyConvertedPoints;
import popeye.javaapi.storage.hbase.PartiallyConvertedPointsAndKeyValues;
import popeye.javaapi.storage.hbase.TsdbFormat;
import popeye.proto.Message;
import popeye.storage.hbase.BytesKey;
import popeye.storage.hbase.HBaseStorage;
import popeye.javaapi.storage.hbase.UniqueIdStorage;

import static popeye.hadoop.bulkload.TsdbPointsHTableJobRunner.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class PointsToKeyValueMapper extends Mapper<NullWritable, Message.Point, ImmutableBytesWritable, KeyValue> {

  private UniqueIdStorage uniqueIdStorage;
  private Map<HBaseStorage.QualifiedName, BytesKey> uniqueIdCache;
  private List<Message.Point> pendingPoints;
  private TsdbFormat tsdbFormat = new TsdbFormat();
  HTablePool tablePool;
  private int maxPendingPoints;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    Configuration hbaseConf = HBaseConfiguration.create();
    String hbaseQuorum = context.getConfiguration().get(HBASE_CONF_QUORUM);
    int hbaseQuorumPort = context.getConfiguration().getInt(HBASE_CONF_QUORUM_PORT, 2181);
    String uniqueIdTableName = context.getConfiguration().get(UNIQUE_ID_TABLE_NAME);
    int cacheCapacity = context.getConfiguration().getInt(UNIQUE_ID_CACHE_SIZE, 100000);
    maxPendingPoints = context.getConfiguration().getInt(MAX_PENDING_POINTS, 10000);
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, hbaseQuorum);
    hbaseConf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, hbaseQuorumPort);
    tablePool = new HTablePool(hbaseConf, 2);
    uniqueIdStorage = new UniqueIdStorage(uniqueIdTableName, tablePool);
    uniqueIdCache = createIdCache(cacheCapacity);
    pendingPoints = new ArrayList<Message.Point>(maxPendingPoints + 1);
  }

  @Override
  protected void map(NullWritable key, Message.Point value, Context context) throws IOException, InterruptedException {
    List<HBaseStorage.QualifiedName> names = tsdbFormat.getAllQualifiedNames(value);
    if (isAllIdsInCache(names)) {
      tsdbFormat.convertToKeyValue(value, uniqueIdCache);
    } else {
      pendingPoints.add(value);
      if (pendingPoints.size() >= maxPendingPoints) {
        flushPendingPoints(context);
      }
    }
  }

  private void flushPendingPoints(Context context) throws IOException, InterruptedException {
    PartiallyConvertedPointsAndKeyValues result = tsdbFormat.convertToKeyValues(pendingPoints, uniqueIdCache);
    writeKeyValues(result.keyValues(), context);
    PartiallyConvertedPoints partialPoints = result.partialPoints();
    Map<HBaseStorage.QualifiedName, BytesKey> idsMap = uniqueIdStorage.findByNames(partialPoints.unresolvedNames());
    List<KeyValue> newKeyValues = partialPoints.convert(idsMap);
    writeKeyValues(newKeyValues, context);
    uniqueIdCache.putAll(idsMap);
    pendingPoints.clear();
  }

  private void writeKeyValues(List<KeyValue> keyValues, Context context) throws IOException, InterruptedException {
    for (KeyValue keyValue : keyValues) {
      context.write(new ImmutableBytesWritable((keyValue.getRow())), keyValue);
    }
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    flushPendingPoints(context);
    tablePool.close();
  }

  private boolean isAllIdsInCache(List<HBaseStorage.QualifiedName> names) {
    for (HBaseStorage.QualifiedName name : names) {
      if (uniqueIdCache.get(name) == null) {
        return false;
      }
    }
    return true;
  }

  private Map<HBaseStorage.QualifiedName, BytesKey> createIdCache(final int capacity) {
    return new LinkedHashMap<HBaseStorage.QualifiedName, BytesKey>() {
      @Override
      protected boolean removeEldestEntry(Map.Entry<HBaseStorage.QualifiedName, BytesKey> eldest) {
        return size() > capacity + 1;
      }
    };
  }
}
