package popeye.hadoop.bulkload;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
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
import popeye.storage.hbase.TsdbFormat;
import popeye.storage.hbase.TsdbFormatConfig$;
import popeye.util.ZkConnect;
import popeye.util.ZkConnect$;
import popeye.util.hbase.HBaseConfigured;

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

    String uniqueIdTableName = conf.get(UNIQUE_ID_TABLE_NAME);
    int cacheCapacity = conf.getInt(UNIQUE_ID_CACHE_SIZE, 100000);
    String hbaseZkConnectString = conf.get(HBASE_ZK_CONNECT);
    ZkConnect hbaseZkConnect = ZkConnect$.MODULE$.parseString(hbaseZkConnectString);
    Configuration hbaseConf = new HBaseConfigured(ConfigFactory.empty(), hbaseZkConnect).hbaseConfiguration();
    hTablePool = new HTablePool(hbaseConf, 1);
    Config tsdbFormatConfig = ConfigFactory.parseString(conf.get(TSDB_FORMAT_CONFIG));
    TsdbFormat tsdbFormat = TsdbFormatConfig$.MODULE$.parseConfig(tsdbFormatConfig).tsdbFormat();
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
