package popeye.hadoop.bulkload;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import popeye.javaapi.kafka.hadoop.KafkaInput;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class KafkaInputSplit extends InputSplit implements Writable {
  private KafkaInput input;


  // default constructor for deserialization (Writable)
  public KafkaInputSplit() {
  }

  public KafkaInputSplit(KafkaInput input) {
    this.input = input;
  }

  public KafkaInput getInput() {
    return input;
  }

  @Override
  public long getLength() throws IOException, InterruptedException {
    return input.stopOffset() - input.startOffset();
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    return new String[0];
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeUTF(KafkaInput.renderInputString(input));
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    input = KafkaInput.parseStringAsInput(dataInput.readUTF());
  }
}
