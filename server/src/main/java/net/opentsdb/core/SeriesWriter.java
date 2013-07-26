package net.opentsdb.core;

import com.stumbleupon.async.Deferred;
import org.hbase.async.Bytes;
import org.hbase.async.PutRequest;
import popeye.transport.proto.Message;

import java.util.Arrays;

class SeriesWriter {

  public static class TsdbPut implements Comparable<TsdbPut> {
    public final byte[] row;
    public final Message.Event event;

    public TsdbPut(byte[] row, Message.Event event) {
      this.row = row;
      this.event = event;
    }

    @Override
    public int compareTo(TsdbPut o) {
      final int c = Bytes.memcmp(this.row, o.row);
      if (c != 0)
        return c;
      long thisVal = event.getTimestamp();
      long anotherVal = o.event.getTimestamp();
      return (thisVal<anotherVal ? -1 : (thisVal==anotherVal ? 0 : 1));
    }
  }

  private final TSDB tsdb;

  /**
   * The row key.
   * 3 bytes for the metric name, 4 bytes for the base timestamp, 6 bytes per
   * tag (3 for the name, 3 for the value).
   */
  private byte[] row;

  /**
   * Qualifiers for individual data points.
   * The last Const.FLAG_BITS bits are used to store flags (the type of the
   * data point - integer or floating point - and the size of the data point
   * in bytes).  The remaining MSBs store a delta in seconds from the base
   * timestamp stored in the row key.
   */
  private short[] qualifiers;

  /**
   * Each value in the row.
   */
  private long[] values;

  /**
   * Number of data points in this row.
   */
  private short size;

  SeriesWriter(TSDB tsdb) {
    this.tsdb = tsdb;
    this.qualifiers = new short[3];
    this.values = new long[3];
  }

  public void startSeries(byte[] row) {
    this.row = row;
    size = 0;
  }

  /**
   * Updates the base time in the row key.
   *
   * @param timestamp The timestamp from which to derive the new base time.
   * @return The updated base time.
   */
  private long updateBaseTime(final long timestamp) {
    // We force the starting timestamp to be on a MAX_TIMESPAN boundary
    // so that all TSDs create rows with the same base time.  Otherwise
    // we'd need to coordinate TSDs to avoid creating rows that cover
    // overlapping time periods.
    final long base_time = timestamp - (timestamp % Const.MAX_TIMESPAN);
    // Clone the row key since we're going to change it.  We must clone it
    // because the HBase client may still hold a reference to it in its
    // internal datastructures.
    row = Arrays.copyOf(row, row.length);
    Bytes.setInt(row, (int) base_time, tsdb.metrics.width());
//    tsdb.scheduleForCompaction(row, (int) base_time);
    return base_time;
  }

  Deferred<Object> addPoint(final long timestamp, final long value) {
    final short flags = 0x7;  // An int stored on 8 bytes.
    return addPointInternal(timestamp, Bytes.fromLong(value), flags);
  }

  Deferred<Object> addPoint(final long timestamp, final float value) {
    final short flags = Const.FLAG_FLOAT | 0x3;  // A float stored on 4 bytes.
    return addPointInternal(timestamp,
            Bytes.fromInt(Float.floatToRawIntBits(value)),
            flags);
  }


  /**
   * Implements {@link #addPoint} by storing a value with a specific flag.
   *
   * @param timestamp The timestamp to associate with the value.
   * @param value     The value to store.
   * @param flags     Flags to store in the qualifier (size and type of the data
   *                  point).
   * @return A deferred object that indicates the completion of the request.
   */
  private Deferred<Object> addPointInternal(final long timestamp, final byte[] value,
                                            final short flags) {
    // This particular code path only expects integers on 8 bytes or floating
    // point values on 4 bytes.
    assert value.length == 8 || value.length == 4 : Bytes.pretty(value);
    if (row == null) {
      throw new IllegalStateException("setSeries() never called!");
    }
    if ((timestamp & 0xFFFFFFFF00000000L) != 0) {
      // => timestamp < 0 || timestamp > Integer.MAX_VALUE
      throw new IllegalArgumentException((timestamp < 0 ? "negative " : "bad")
              + " timestamp=" + timestamp
              + " when trying to add value=" + Arrays.toString(value) + " to " + this);
    }

    long base_time;
    if (size > 0) {
      base_time = baseTime();
      final long last_ts = base_time + (delta(qualifiers[size - 1]));
      if (timestamp <= last_ts) {
        throw new IllegalArgumentException("New timestamp=" + timestamp
                + " is less than previous=" + last_ts
                + " when trying to add value=" + Arrays.toString(value)
                + " to " + this);
      } else if (timestamp - base_time >= Const.MAX_TIMESPAN) {
        // Need to start a new row as we've exceeded Const.MAX_TIMESPAN.
        base_time = updateBaseTime(timestamp);
        size = 0;
        //LOG.info("Starting a new row @ " + this);
      }
    } else {
      // This is the first data point, let's record the starting timestamp.
      base_time = updateBaseTime(timestamp);
      Bytes.setInt(row, (int) base_time, tsdb.metrics.width());
    }

    if (values.length == size) {
      grow();
    }

    // Java is so stupid with its auto-promotion of int to float.
    final short qualifier = (short) ((timestamp - base_time) << Const.FLAG_BITS
            | flags);
    qualifiers[size] = qualifier;
    values[size] = (value.length == 8
            ? Bytes.getLong(value)
            : Bytes.getInt(value) & 0x00000000FFFFFFFFL);
    size++;

    final PutRequest point = new PutRequest(tsdb.table, row, TSDB.FAMILY,
            Bytes.fromShort(qualifier),
            value);
    point.setDurable(true);
    return tsdb.client.put(point)/*.addBoth(cb)*/;
  }

  private void grow() {
    // We can't have more than 1 value per second, so MAX_TIMESPAN values.
    final int new_size = Math.min(size * 2, Const.MAX_TIMESPAN);
    if (new_size == size) {
      throw new AssertionError("Can't grow " + this + " larger than " + size);
    }
    values = Arrays.copyOf(values, new_size);
    qualifiers = Arrays.copyOf(qualifiers, new_size);
  }

  /**
   * Extracts the base timestamp from the row key.
   */
  private long baseTime() {
    return Bytes.getUnsignedInt(row, tsdb.metrics.width());
  }

  private short delta(final short qualifier) {
    return (short) ((qualifier & 0xFFFF) >>> Const.FLAG_BITS);
  }

  public long timestamp(final int i) {
    checkIndex(i);
    return baseTime() + (delta(qualifiers[i]) & 0xFFFF);
  }

  public boolean isInteger(final int i) {
    checkIndex(i);
    return (qualifiers[i] & Const.FLAG_FLOAT) == 0x0;
  }

  public long longValue(final int i) {
    // Don't call checkIndex(i) because isInteger(i) already calls it.
    if (isInteger(i)) {
      return values[i];
    }
    throw new ClassCastException("value #" + i + " is not a long in " + this);
  }

  public double doubleValue(final int i) {
    // Don't call checkIndex(i) because isInteger(i) already calls it.
    if (!isInteger(i)) {
      return Float.intBitsToFloat((int) values[i]);
    }
    throw new ClassCastException("value #" + i + " is not a float in " + this);
  }

  /**
   * @throws IndexOutOfBoundsException if {@code i} is out of bounds.
   */
  private void checkIndex(final int i) {
    if (i > size) {
      throw new IndexOutOfBoundsException("index " + i + " > " + size
              + " for this=" + this);
    }
    if (i < 0) {
      throw new IndexOutOfBoundsException("negative index " + i
              + " for this=" + this);
    }
  }

}
