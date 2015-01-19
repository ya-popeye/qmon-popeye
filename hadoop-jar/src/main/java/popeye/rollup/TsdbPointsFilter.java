package popeye.rollup;

import com.google.common.base.Preconditions;
import com.google.common.primitives.UnsignedBytes;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import popeye.storage.hbase.TsdbFormat;

import java.io.*;

public class TsdbPointsFilter extends FilterBase {

  private final int baseTimeStartSeconds;
  private final int baseTimeStopSeconds;
  private final byte[] generationId;
  private final byte valueTypeId;
  private boolean done = false;

  public TsdbPointsFilter(short generationId, byte valueTypeId, int baseTimeStartSeconds, int baseTimeStopSeconds) {
    this.valueTypeId = valueTypeId;
    this.generationId = Bytes.toBytes(generationId);
    checkAlignment(baseTimeStartSeconds, "start time");
    checkAlignment(baseTimeStopSeconds, "stop time");
    Preconditions.checkArgument(baseTimeStartSeconds < baseTimeStopSeconds, "start time should be less than stop time");
    this.baseTimeStartSeconds = baseTimeStartSeconds;
    this.baseTimeStopSeconds = baseTimeStopSeconds;
  }

  private void checkAlignment(int baseTimeStopSeconds, String variableName) {
    Preconditions.checkArgument(
      baseTimeStopSeconds % TsdbFormat.MAX_TIMESPAN()== 0,
      variableName + " (" + baseTimeStopSeconds + ") should be divisible by " + TsdbFormat.MAX_TIMESPAN()
    );
  }

  @Override
  public ReturnCode filterKeyValue(Cell cell) throws IOException {
    byte[] rowArray = cell.getRowArray();
    int rowOffset = cell.getRowOffset();
    int generationIdWidth = TsdbFormat.uniqueIdGenerationWidth();
    int generationComparison = KeyValue.COMPARATOR.compareRows(
      rowArray, rowOffset, generationIdWidth,
      generationId, 0, generationIdWidth
    );
    if (generationComparison == 0) {
      return filterKeyValueBaseCase(rowArray, rowOffset);
    } else if (generationComparison < 0) {
      return ReturnCode.SEEK_NEXT_USING_HINT;
    } else {
      done = true;
      return ReturnCode.NEXT_ROW;
    }
  }

  private ReturnCode filterKeyValueBaseCase(byte[] rowArray, int rowOffset) {
    byte pointValueTypeId = rowArray[rowOffset + TsdbFormat.valueTypeIdOffset()];
    boolean valueTypeMatches = pointValueTypeId == valueTypeId;
    int pointBaseTime = Bytes.toInt(rowArray, rowOffset + TsdbFormat.baseTimeOffset(), TsdbFormat.baseTimeWidth());
    boolean timestampIsInRange = baseTimeStartSeconds <= pointBaseTime && pointBaseTime < baseTimeStopSeconds;
    if (valueTypeMatches && timestampIsInRange) {
      return ReturnCode.INCLUDE;
    } else {
      return ReturnCode.SEEK_NEXT_USING_HINT;
    }
  }

  @Override
  public Cell getNextCellHint(Cell cell) throws IOException {
    byte[] rowArray = cell.getRowArray();
    int rowOffset = cell.getRowOffset();
    int generationIdWidth = TsdbFormat.uniqueIdGenerationWidth();
    int generationComparison = KeyValue.COMPARATOR.compareRows(
      rowArray, rowOffset, generationIdWidth,
      generationId, 0, generationIdWidth
    );
    if (generationComparison == 0) {
      return getNextCellHintBaseCase(rowArray, rowOffset);
    } else if (generationComparison < 0) {
      // skip to desired generation
      return KeyValue.createFirstOnRow(generationId);
    } else {
      throw new IOException(new AssertionError());
    }
  }

  private Cell getNextCellHintBaseCase(byte[] rowArray, int rowOffset) throws IOException {
    byte pointValueTypeId = rowArray[rowOffset + TsdbFormat.valueTypeIdOffset()];
    int valueTypeComparison = UnsignedBytes.compare(pointValueTypeId, valueTypeId);
    if (valueTypeComparison == 0) {
      int pointBaseTime = Bytes.toInt(rowArray, rowOffset + TsdbFormat.baseTimeOffset(), TsdbFormat.baseTimeWidth());
      if (pointBaseTime < baseTimeStartSeconds) {
        byte[] rowHint = Bytes.copy(rowArray, rowOffset, TsdbFormat.attributesOffset());
        Bytes.putInt(rowHint, TsdbFormat.baseTimeOffset(), baseTimeStartSeconds);
        return KeyValue.createFirstOnRow(rowHint);
      } else if (pointBaseTime >= baseTimeStopSeconds) {
        return nextMetricRow(rowArray, rowOffset);
      } else {
        throw new IOException(new AssertionError());
      }
    } else if (valueTypeComparison < 0) {
      byte[] rowHint = Bytes.copy(rowArray, rowOffset, TsdbFormat.shardIdOffset());
      rowHint[TsdbFormat.valueTypeIdOffset()] = valueTypeId;
      return KeyValue.createFirstOnRow(rowHint);
    } else {
      return nextMetricRow(rowArray, rowOffset);
    }
  }

  private Cell nextMetricRow(byte[] rowArray, int rowOffset) {
    byte[] rowHint = Bytes.copy(rowArray, rowOffset, TsdbFormat.shardIdOffset());
    incrementBytes(rowHint, TsdbFormat.metricOffset(), TsdbFormat.metricWidth());
    rowHint[TsdbFormat.valueTypeIdOffset()] = valueTypeId;
    return KeyValue.createFirstOnRow(rowHint);
  }

  @Override
  public boolean filterAllRemaining() throws IOException {
    return done;
  }

  @Override
  public String toString() {
    return super.toString();
  }

  @Override
  public byte[] toByteArray() throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(outputStream);
    writeTo(dataOut);
    dataOut.flush();
    return outputStream.toByteArray();
  }

  private void writeTo(DataOutputStream stream) throws IOException {
    stream.writeShort(Bytes.toShort(generationId));
    stream.writeByte(valueTypeId);
    stream.writeInt(baseTimeStartSeconds);
    stream.writeInt(baseTimeStopSeconds);
  }

  public static TsdbPointsFilter parseFrom(final byte[] bytes) {
    ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
    DataInputStream dataIn = new DataInputStream(inputStream);
    try {
      short generationId = dataIn.readShort();
      byte valueTypeId = dataIn.readByte();
      int baseTimeStartSeconds = dataIn.readInt();
      int baseTimeStopSeconds = dataIn.readInt();
      return new TsdbPointsFilter(generationId, valueTypeId, baseTimeStartSeconds, baseTimeStopSeconds);
    } catch (IOException e) {
      // impossible
      throw new AssertionError();
    }
  }

  public static void incrementBytes(byte[] bytes, int offset, int length) {
    for (int i = length - 1; i >= 0; i--) {
      byte maxUnsignedValue = -1; //0xff
      if (bytes[offset + i] != maxUnsignedValue) {
        bytes[offset + i] += 1;
        return;
      } else {
        bytes[offset + i] = 0;
      }
    }
  }
}
