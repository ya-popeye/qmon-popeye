package net.opentsdb.core;

import com.google.common.base.Throwables;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueId;
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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

  public static final Comparator<Message.Event> EVENT_COMPARATOR = orderByTimestamp();
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

  static UniqueId stealMetrics(TSDB tsdb) {
    try {
      return (UniqueId) metricsField.get(tsdb);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Very bad, can't read UniqueId field 'metric'", e);
    }
  }

  private Logger logger = LoggerFactory.getLogger(EventPersistFuture.class);

  private TSDB tsdb;
  private HBaseClient client;
  private final UniqueId metric;
  private long startMillis;
  private long finishMillis;
  private AtomicBoolean canceled = new AtomicBoolean(false);
  private AtomicBoolean batched = new AtomicBoolean(false);
  private volatile boolean throttle = false;
  private AtomicInteger batchedEvents = new AtomicInteger();

  public EventPersistFuture(TSDB tsdb, Message.Event[] batch) {
    this.tsdb = tsdb;
    this.client = stealHBaseClient(tsdb);
    this.metric = stealMetrics(tsdb);
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

  private void writeDataPoints(Message.Event[] data) throws Exception {
    startMillis = System.currentTimeMillis();
    if (data.length == 0) {
      batched.set(true);
      tryComplete();
      return;
    }
    final SeriesWriter.TsdbPut[] tsdbPuts = new SeriesWriter.TsdbPut[data.length];
    final HashSet<String> errors = new HashSet<String>();
    int validEvents = 0;
    for (Message.Event event : data) {
      try {
        checkEvent(event);
        final byte[] row = rowKeyTemplate(event.getMetric(), event.getTagsList());
        tsdbPuts[validEvents++] = new SeriesWriter.TsdbPut(row, event);
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
        seriesWriter.startSeries(tsdbPuts[prevRow].row);
        for(int j = prevRow; j < i; j++) {
          try {
            final Deferred<Object> d;
            final Message.Event eventData = tsdbPuts[j].event;
            if (eventData.hasIntValue()) {
              d = seriesWriter.addPoint(eventData.getTimestamp(), eventData.getIntValue());
            } else if (eventData.hasFloatValue()) {  // floating point value
              d = seriesWriter.addPoint(eventData.getTimestamp(), eventData.getFloatValue());
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


  private Map<String, String> toMap(List<Message.Tag> tagsList) {
    Map<String, String> tagsMap = new HashMap<String, String>();
    for (Message.Tag tag : tagsList) {
      tagsMap.put(tag.getName(), tag.getValue());
    }
    return tagsMap;
  }

  public static Comparator<Message.Event> orderByTimestamp() {
    return new Comparator<Message.Event>() {
      public int compare(Message.Event o1, Message.Event o2) {
        return (o1.getTimestamp() < o2.getTimestamp()) ? -1 : (o1.getTimestamp() == o2.getTimestamp() ? 0 : 1);
      }
    };
  }

  /**
   * Validates the given metric and tags.
   *
   * @throws IllegalArgumentException if any of the arguments aren't valid.
   */
  static void checkEvent(Message.Event event) {
    if (!event.hasIntValue() && !event.hasFloatValue())
      throw new IllegalArgumentException("Event doesn't have either int no float value: " + event);
    if (event.hasFloatValue()) {
      final float value = event.getFloatValue();
      if (Float.isNaN(value) || Float.isInfinite(value)) {
        throw new IllegalArgumentException("value is NaN or Infinite: " + value
                + " for " + event.toString());
      }
    }
    if (event.getTagsCount() <= 0) {
      throw new IllegalArgumentException("Need at least one tags " + event.toString());
    } else if (event.getTagsCount() > Const.MAX_NUM_TAGS) {
      throw new IllegalArgumentException("Too many tags " + event.toString() + " need not more then " + Const.MAX_NUM_TAGS);
    }

    Tags.validateString("metric name", event.getMetric());
    for (int i = 0; i < event.getTagsCount(); i++) {
      Message.Tag tag = event.getTags(i);
      Tags.validateString("tag name", tag.getName());
      Tags.validateString("tag value", tag.getValue());
    }
  }

  /**
   * Returns a partially initialized row key for this metric and these tags.
   * The only thing left to fill in is the base timestamp.
   */
  byte[] rowKeyTemplate(final String metric,
                               final List<Message.Tag> tags) {
    final short metric_width = tsdb.metrics.width();
    final short tag_name_width = tsdb.tag_names.width();
    final short tag_value_width = tsdb.tag_values.width();
    final short num_tags = (short) tags.size();

    int row_size = (metric_width + Const.TIMESTAMP_BYTES
            + tag_name_width * num_tags
            + tag_value_width * num_tags);
    final byte[] row = new byte[row_size];

    short pos = 0;

    copyInRowKey(row, pos, tsdb.metrics.getOrCreateId(metric));
    pos += metric_width;

    pos += Const.TIMESTAMP_BYTES;

    copyTags(row, pos, tags);
    return row;
  }

  void copyTags(byte[] row, short pos, final List<Message.Tag> tags)
          throws NoSuchUniqueName {
    final ArrayList<byte[]> tag_ids = new ArrayList<byte[]>(tags.size());
    for (Message.Tag tag : tags) {
      final byte[] tag_id = tsdb.tag_names.getOrCreateId(tag.getName());
      final byte[] value_id = tsdb.tag_values.getOrCreateId(tag.getValue());
      final byte[] thistag = new byte[tag_id.length + value_id.length];
      System.arraycopy(tag_id, 0, thistag, 0, tag_id.length);
      System.arraycopy(value_id, 0, thistag, tag_id.length, value_id.length);
      tag_ids.add(thistag);
    }
    // Now sort the tags.
    Collections.sort(tag_ids, Bytes.MEMCMP);
    for (final byte[] tag : tag_ids) {
      copyInRowKey(row, pos, tag);
      pos += tag.length;
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
