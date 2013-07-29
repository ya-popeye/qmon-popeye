package net.opentsdb.core;

import com.google.common.base.Throwables;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import net.opentsdb.uid.NoSuchUniqueName;
import org.hbase.async.Bytes;
import org.hbase.async.HBaseClient;
import org.hbase.async.HBaseRpc;
import org.hbase.async.PleaseThrottleException;
import org.hbase.async.PutRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import popeye.transport.proto.Message;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Andrey Stepachev
 */
public abstract class EventPersistFuture implements Callback<Object, Object>, Future<Object> {

  static Field clientField;
  static Field metricsField;

  static {
    try {
      clientField = TSDB.class.getDeclaredField("client");
      clientField.setAccessible(true);
      metricsField = TSDB.class.getDeclaredField("metrics");
      metricsField.setAccessible(true);
    } catch (NoSuchFieldException e) {
      throw new IllegalStateException("Can't find client field in TSDB", e);
    }
  }


  static HBaseClient stealHBaseClient(TSDB tsdb) {
    try {
      return (HBaseClient) clientField.get(tsdb);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Very bad, can't read HBaseClient field 'client'", e);
    }
  }

  private Logger logger = LoggerFactory.getLogger(EventPersistFuture.class);

  private TSDB tsdb;
  private HBaseClient client;
  private long startMillis;
  private long finishMillis;
  private AtomicBoolean canceled = new AtomicBoolean(false);
  private AtomicBoolean batched = new AtomicBoolean(false);
  private volatile boolean throttle = false;
  private AtomicInteger batchedEvents = new AtomicInteger();

  public EventPersistFuture(TSDB tsdb, Message.Point[] batch) {
    this.tsdb = tsdb;
    this.client = stealHBaseClient(tsdb);
    try {
      writeDataPoints(batch);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public boolean isCancelled() {
    return canceled.get();
  }

  @Override
  public boolean isDone() {
    return !canceled.get() && batched.get() && batchedEvents.get() == 0;
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    if (isDone())
      return false;
    canceled.set(true);
    return true;
  }

  public long getStartMillis() {
    return startMillis;
  }

  public long getFinishMillis() {
    return finishMillis;
  }

  @Override
  public Object get() throws InterruptedException, ExecutionException {
    return null;
  }

  @Override
  public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
    return null;
  }

  private void writeDataPoints(Message.Point[] data) throws Exception {
    startMillis = System.currentTimeMillis();
    if (data.length == 0) {
      batched.set(true);
      tryComplete();
      return;
    }
    final SeriesWriter.TsdbPut[] tsdbPuts = new SeriesWriter.TsdbPut[data.length];
    final HashSet<String> errors = new HashSet<String>();
    int validEvents = 0;
    for (Message.Point point : data) {
      try {
        checkEvent(point);
        final byte[] row = rowKeyTemplate(point.getMetric(), point.getAttributesList());
        tsdbPuts[validEvents++] = new SeriesWriter.TsdbPut(row, point);
      } catch (IllegalArgumentException ie) {
        errors.add(ie.getMessage());
      }
    }
    Arrays.sort(tsdbPuts, 0, validEvents);

    final SeriesWriter seriesWriter = new SeriesWriter(tsdb);
    int prevRow = 0;
    int cnt = 0;
    for (int i = 0; i < validEvents; i++) {
      if (Bytes.memcmp(tsdbPuts[prevRow].row, tsdbPuts[i].row) != 0) {
        seriesWriter.startSeries(tsdbPuts[i].row);
        for(int j = prevRow; j < i; j++) {
          try {
            final Deferred<Object> d;
            final Message.Point pointData = tsdbPuts[j].point;
            if (pointData.hasIntValue()) {
              d = seriesWriter.addPoint(pointData.getTimestamp(), pointData.getIntValue());
            } else if (pointData.hasFloatValue()) {  // floating point value
              d = seriesWriter.addPoint(pointData.getTimestamp(), pointData.getFloatValue());
            } else {
              throw new IllegalArgumentException("Metric doesn't have either int no float value");
            }
            batchedEvents.incrementAndGet();
            d.addBoth(this);
            cnt++;
            if (throttle)
              throttle(d);
          } catch (IllegalArgumentException ie) {
            errors.add(ie.getMessage());
          }
        }
        prevRow = i;
      }
    }
    batched.set(true);
    tryComplete();
    if (errors.size() > 0) {
      logger.error("Points " + cnt + " of " + data.length
              + " imported with " + errors.toString() + " IllegalArgumentExceptions");
    }
  }

  public Object call(final Object arg) {
    if (arg instanceof PleaseThrottleException) {
      if (canceled.get()) {
        // don't try to recend in case of cancelled batch,
        // silently skip
        return null;
      }
      final PleaseThrottleException e = (PleaseThrottleException) arg;
      logger.warn("Need to throttle, HBase isn't keeping up.", e);
      throttle = true;
      final HBaseRpc rpc = e.getFailedRpc();
      if (rpc instanceof PutRequest) {
        client.put((PutRequest) rpc)
                .addBoth(this);  // Don't lose edits.
      }
    } else if (arg instanceof Throwable) {
      fail((Throwable) arg);
    } else {
      batchedEvents.decrementAndGet();
      tryComplete();
    }
    return null;
  }

  private void tryComplete() {
    int awaits = batchedEvents.get();
    if (awaits == 0 && batched.get()) {
      finishMillis = System.currentTimeMillis();
      complete();
    }
  }

  protected abstract void complete();

  protected abstract void fail(Throwable arg);

  /**
   * Helper method, implements throttle.
   * Sleeps, until throttle will be switch off
   * by successful operation.
   *
   * @param deferred what to throttle
   */
  private void throttle(Deferred deferred) {
    logger.info("Throttling...");
    long throttle_time = System.nanoTime();
    try {
      deferred.join();
    } catch (Exception e) {
      throw new RuntimeException("Should never happen", e);
    }
    throttle_time = System.nanoTime() - throttle_time;
    if (throttle_time < 1000000000L) {
      logger.info("Got throttled for only " + throttle_time + "ns, sleeping a bit now");
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        throw new RuntimeException("interrupted", e);
      }
    }
    logger.info("Done throttling...");
    throttle = false;
  }

  /**
   * Validates the given metric and attributes.
   *
   * @throws IllegalArgumentException if any of the arguments aren't valid.
   */
  static void checkEvent(Message.Point point) {
    if (!point.hasIntValue() && !point.hasFloatValue())
      throw new IllegalArgumentException("Point doesn't have either int no float value: " + point);
    if (point.hasFloatValue()) {
      final float value = point.getFloatValue();
      if (Float.isNaN(value) || Float.isInfinite(value)) {
        throw new IllegalArgumentException("value is NaN or Infinite: " + value
                + " for " + point.toString());
      }
    }
    if (point.getAttributesCount() <= 0) {
      throw new IllegalArgumentException("Need at least one attributes " + point.toString());
    } else if (point.getAttributesCount() > Const.MAX_NUM_TAGS) {
      throw new IllegalArgumentException("Too many attributes " + point.toString() + " need not more then " + Const.MAX_NUM_TAGS);
    }

    Tags.validateString("metric name", point.getMetric());
    for (int i = 0; i < point.getAttributesCount(); i++) {
      Message.Attribute attribute = point.getAttributes(i);
      Tags.validateString("attribute name", attribute.getName());
      Tags.validateString("attribute value", attribute.getValue());
    }
  }

  /**
   * Returns a partially initialized row key for this metric and these attributes.
   * The only thing left to fill in is the base timestamp.
   */
  byte[] rowKeyTemplate(final String metric,
                               final List<Message.Attribute> attributes) {
    final short metric_width = tsdb.metrics.width();
    final short attribute_name_width = tsdb.tag_names.width();
    final short attribute_value_width = tsdb.tag_values.width();
    final short num_attributes = (short) attributes.size();

    int row_size = (metric_width + Const.TIMESTAMP_BYTES
            + attribute_name_width * num_attributes
            + attribute_value_width * num_attributes);
    final byte[] row = new byte[row_size];

    short pos = 0;

    copyInRowKey(row, pos, tsdb.metrics.getOrCreateId(metric));
    pos += metric_width;

    pos += Const.TIMESTAMP_BYTES;

    copyAttributes(row, pos, attributes);
    return row;
  }

  void copyAttributes(byte[] row, short pos, final List<Message.Attribute> attributes)
          throws NoSuchUniqueName {
    final ArrayList<byte[]> attribute_ids = new ArrayList<byte[]>(attributes.size());
    for (Message.Attribute attribute : attributes) {
      final byte[] attribute_id = tsdb.tag_names.getOrCreateId(attribute.getName());
      final byte[] value_id = tsdb.tag_values.getOrCreateId(attribute.getValue());
      final byte[] thisattribute = new byte[attribute_id.length + value_id.length];
      System.arraycopy(attribute_id, 0, thisattribute, 0, attribute_id.length);
      System.arraycopy(value_id, 0, thisattribute, attribute_id.length, value_id.length);
      attribute_ids.add(thisattribute);
    }
    // Now sort the attributes.
    Collections.sort(attribute_ids, Bytes.MEMCMP);
    for (final byte[] attribute : attribute_ids) {
      copyInRowKey(row, pos, attribute);
      pos += attribute.length;
    }
  }


  /**
   * Copies the specified byte array at the specified offset in the row key.
   *
   * @param row    The row key into which to copy the bytes.
   * @param offset The offset in the row key to start writing at.
   * @param bytes  The bytes to copy.
   */
  private static void copyInRowKey(final byte[] row, final short offset, final byte[] bytes) {
    System.arraycopy(bytes, 0, row, offset, bytes.length);
  }

}
