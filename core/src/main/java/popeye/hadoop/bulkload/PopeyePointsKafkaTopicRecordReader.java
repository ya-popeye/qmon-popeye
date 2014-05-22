package popeye.hadoop.bulkload;

import kafka.consumer.SimpleConsumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import popeye.javaapi.hadoop.bulkload.TsdbKeyValueIterator;
import popeye.javaapi.kafka.hadoop.KafkaInput;
import popeye.kafka.KafkaSimpleConsumerFactory;
import popeye.storage.hbase.BytesKey;
import popeye.storage.hbase.TimeRangeIdMapping;
import popeye.storage.hbase.TsdbFormat;

import static popeye.hadoop.bulkload.BulkLoadConstants.*;

import java.io.IOException;
import java.util.List;

public class PopeyePointsKafkaTopicRecordReader extends RecordReader<NullWritable, List<KeyValue>> {

  private HTablePool hTablePool;
  private KafkaPointsIterator pointsIterator;
  private TsdbKeyValueIterator keyValueIterator;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    KafkaInput kafkaInput = ((KafkaInputSplit) split).getInput();
    Configuration conf = context.getConfiguration();
    String brokersString = conf.get(KAFKA_BROKERS);
    int consumerTimeout = conf.getInt(KAFKA_CONSUMER_TIMEOUT, 5000);
    int consumerBufferSize = conf.getInt(KAFKA_CONSUMER_BUFFER_SIZE, 100000);
    int fetchSize = conf.getInt(KAFKA_CONSUMER_FETCH_SIZE, 2000000);
    String clientId = conf.get(KAFKA_CLIENT_ID);
    KafkaSimpleConsumerFactory consumerFactory = new KafkaSimpleConsumerFactory(brokersString, consumerTimeout, consumerBufferSize, clientId);
    SimpleConsumer consumer = consumerFactory.startConsumer(kafkaInput.topic(), kafkaInput.partition());
    pointsIterator = new KafkaPointsIterator(consumer, kafkaInput, fetchSize, clientId);

    Configuration hbaseConf = HBaseConfiguration.create();
    String hbaseQuorum = context.getConfiguration().get(HBASE_CONF_QUORUM);
    int hbaseQuorumPort = context.getConfiguration().getInt(HBASE_CONF_QUORUM_PORT, 2181);
    String uniqueIdTableName = context.getConfiguration().get(UNIQUE_ID_TABLE_NAME);
    int cacheCapacity = context.getConfiguration().getInt(UNIQUE_ID_CACHE_SIZE, 100000);
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, hbaseQuorum);
    hbaseConf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, hbaseQuorumPort);
    hTablePool = new HTablePool(hbaseConf, 1);
    TimeRangeIdMapping timeRangeIdMapping = new TimeRangeIdMapping() {

      @Override
      public BytesKey getRangeId(long timestampInSeconds) {
        return new BytesKey(new byte[]{0, 0});
      }
    };
    TsdbFormat tsdbFormat = new TsdbFormat(timeRangeIdMapping);
    keyValueIterator = TsdbKeyValueIterator.create(
      pointsIterator,
      tsdbFormat,
      uniqueIdTableName,
      hTablePool,
      cacheCapacity
    );
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    return keyValueIterator.hasNext();
  }

  @Override
  public NullWritable getCurrentKey() throws IOException, InterruptedException {
    return NullWritable.get();
  }

  @Override
  public List<KeyValue> getCurrentValue() throws IOException, InterruptedException {
    return keyValueIterator.next();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return keyValueIterator.getProgress();
  }

  @Override
  public void close() throws IOException {
    if (hTablePool != null) {
      hTablePool.close();
    }
    if (pointsIterator != null) {
      pointsIterator.close();
    }
  }
}
