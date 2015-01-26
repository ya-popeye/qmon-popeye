package popeye.rollup;

import com.google.common.base.Preconditions;
import com.google.common.primitives.UnsignedBytes;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import popeye.storage.hbase.TsdbFormat;

import java.io.*;
import java.util.Arrays;

public class TsdbPointsFilter extends FilterBase {

  private final byte[] generationId;
  private final int downsamplingResolutionId;
  private final byte valueTypeId;
  private final int baseTimeStartSeconds;
  private final int baseTimeStopSeconds;

  private boolean done = false;

  public TsdbPointsFilter(short generationId,
                          int downsamplingResolutionId,
                          byte valueTypeId,
                          int baseTimeStartSeconds,
                          int baseTimeStopSeconds) {
    this.generationId = Bytes.toBytes(generationId);
    this.valueTypeId = valueTypeId;
    this.downsamplingResolutionId = downsamplingResolutionId;
    checkAlignment(baseTimeStartSeconds, downsamplingResolutionId, "start time");
    checkAlignment(baseTimeStopSeconds, downsamplingResolutionId, "stop time");
    Preconditions.checkArgument(baseTimeStartSeconds < baseTimeStopSeconds, "start time should be less than stop time");
    this.baseTimeStartSeconds = baseTimeStartSeconds;
    this.baseTimeStopSeconds = baseTimeStopSeconds;
  }

  private static void checkAlignment(int baseTimeStopSeconds, int downsamplingResolutionId, String variableName) {
    int timespan = TsdbFormat.getTimespanByDownsamplingId(downsamplingResolutionId);
    Preconditions.checkArgument(
      baseTimeStopSeconds % timespan == 0,
      variableName + " (" + baseTimeStopSeconds + ") should be divisible by " + timespan
    );
  }

  @Override
  public ReturnCode filterKeyValue(Cell cell) throws IOException {
    byte[] rowArray = cell.getRowArray();
    int rowOffset = cell.getRowOffset();
    int generationAndDownsamplingKeyComparison = compareGenerationAndDownsamplingKey(rowArray, rowOffset);
    if (generationAndDownsamplingKeyComparison == 0) {
      return filterKeyValueBaseCase(rowArray, rowOffset);
    } else if (generationAndDownsamplingKeyComparison < 0) {
      return ReturnCode.SEEK_NEXT_USING_HINT;
    } else {
      done = true;
      return ReturnCode.NEXT_ROW;
    }
  }

  private int compareGenerationAndDownsamplingKey(byte[] rowArray, int rowOffset) {
    int generationIdWidth = TsdbFormat.uniqueIdGenerationWidth();
    int generationComparison = KeyValue.COMPARATOR.compareRows(
      rowArray, rowOffset, generationIdWidth,
      generationId, 0, generationIdWidth
    );
    if (generationComparison == 0) {
      int downsamplingByteIndex = rowOffset + TsdbFormat.downsamplingQualByteOffset();
      return compareDownsamplingKey(rowArray[downsamplingByteIndex]);
    } else {
      return generationComparison;
    }
  }

  private int compareDownsamplingKey(byte downsamplingByte) {
    return Integer.compare(
      TsdbFormat.parseDownsamplingResolution(downsamplingByte),
      this.downsamplingResolutionId
    );
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
    int generationAndDownsamplingKeyComparison = compareGenerationAndDownsamplingKey(rowArray, rowOffset);
    if (generationAndDownsamplingKeyComparison == 0) {
      return getNextCellHintBaseCase(rowArray, rowOffset);
    } else if (generationAndDownsamplingKeyComparison < 0) {
      // skip to desired generation
      return generationIdAndDsByteHint();
    } else {
      throw new IOException(new AssertionError());
    }
  }

  private Cell generationIdAndDsByteHint() {
    byte dsByte = TsdbFormat.renderDownsamplingByteFromIds(this.downsamplingResolutionId, 0);
    byte[] generationIdAndDsByte = appendByte(generationId, dsByte);
    return KeyValue.createFirstOnRow(generationIdAndDsByte);
  }

  private byte[] appendByte(byte[] array, byte b) {
    byte[] newArray = Arrays.copyOf(array, array.length + 1);
    newArray[array.length] = b;
    return newArray;
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
    return "TsdbPointsFilter{" +
      "generationId=" + Arrays.toString(generationId) +
      ", downsamplingResolutionId=" + downsamplingResolutionId +
      ", valueTypeId=" + valueTypeId +
      ", baseTimeStartSeconds=" + baseTimeStartSeconds +
      ", baseTimeStopSeconds=" + baseTimeStopSeconds +
      ", done=" + done +
      '}';
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
    stream.writeInt(downsamplingResolutionId);
    stream.writeByte(valueTypeId);
    stream.writeInt(baseTimeStartSeconds);
    stream.writeInt(baseTimeStopSeconds);
  }

  public static TsdbPointsFilter parseFrom(final byte[] bytes) {
    ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
    DataInputStream dataIn = new DataInputStream(inputStream);
    try {
      short generationId = dataIn.readShort();
      int downsamplingResolutionId = dataIn.readInt();
      byte valueTypeId = dataIn.readByte();
      int baseTimeStartSeconds = dataIn.readInt();
      int baseTimeStopSeconds = dataIn.readInt();
      return new TsdbPointsFilter(
        generationId,
        downsamplingResolutionId,
        valueTypeId,
        baseTimeStartSeconds,
        baseTimeStopSeconds
      );
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
