package popeye.hadoop.bulkload;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;
import popeye.javaapi.kafka.hadoop.KafkaInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PopeyePointsKafkaTopicInputFormat extends InputFormat<NullWritable, List<KeyValue>> {

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    String kafkaInputsString = context.getConfiguration().get(BulkLoadConstants.KAFKA_INPUTS);
    List<KafkaInput> inputs = KafkaInput.parseStringAsInputJavaList(kafkaInputsString);
    List<InputSplit> splits = new ArrayList<InputSplit>();
    for (KafkaInput input : inputs) {
      splits.add(new KafkaInputSplit(input));
    }
    return splits;
  }

  @Override
  public RecordReader<NullWritable, List<KeyValue>> createRecordReader(
    InputSplit split,
    TaskAttemptContext context
  ) throws IOException, InterruptedException {
    return new PopeyePointsKafkaTopicRecordReader();
  }
}
