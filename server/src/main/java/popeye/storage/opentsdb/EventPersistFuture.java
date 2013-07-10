package popeye.storage.opentsdb;

import com.google.common.base.Throwables;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.WritableDataPoints;
import org.hbase.async.HBaseClient;
import org.hbase.async.HBaseRpc;
import org.hbase.async.PleaseThrottleException;
import org.hbase.async.PutRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import popeye.transport.proto.Message;
import popeye.transport.proto.Storage;

import java.lang.reflect.Field;
import java.util.ArrayList;
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

  static Field field;

  static {
    try {
      field = TSDB.class.getDeclaredField("client");
      field.setAccessible(true);

    } catch (NoSuchFieldException e) {
      throw new IllegalStateException("Can't find client field in TSDB", e);
    }
  }

  static HBaseClient stealHBaseClient(TSDB tsdb) {
    try {
      return (HBaseClient) field.get(tsdb);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Very bad, can't read HBaseClient field", e);
    }
  }

  private Logger logger = LoggerFactory.getLogger(EventPersistFuture.class);

  private TSDB tsdb;
  private HBaseClient client;
  private long batchId;
  private AtomicBoolean canceled = new AtomicBoolean(false);
  private AtomicBoolean batched = new AtomicBoolean(false);
  private volatile boolean throttle = false;
  private AtomicInteger batchedEvents = new AtomicInteger();

  public EventPersistFuture(TSDB tsdb, Storage.Ensemble batch) {
    this.tsdb = tsdb;
    this.batchId = batch.getBatchId();
    this.client = stealHBaseClient(tsdb);
    try {
      writeDataPoints(batch);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public long getBatchId() {
    return batchId;
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

  @Override
  public Object get() throws InterruptedException, ExecutionException {
    return null;
  }

  @Override
  public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
    return null;
  }

  private void writeDataPoints(Storage.Ensemble data) throws Exception {
    if (data.getEventsCount() < 1)
      return;
    final WritableDataPoints dataPoints = tsdb.newDataPoints();
    final Message.Event first = data.getEvents(0);
    dataPoints.setSeries(first.getMetric(), toMap(first.getTagsList()));
    dataPoints.setBatchImport(false); // we need data to be persisted
    final List<Message.Event> eventList = new ArrayList<Message.Event>(data.getEventsList());
    // sort data in increased timestamp order
    Collections.sort(eventList, orderByTimestamp());
    long prevTs = 0;
    Set<String> failures = new HashSet<String>();
    for (Message.Event eventData : eventList) {
      try {
        if (eventData.getTimestamp() == prevTs)
          continue;
        prevTs = eventData.getTimestamp();
        final Deferred<Object> d;
        if (eventData.hasIntValue()) {
          d = dataPoints.addPoint(eventData.getTimestamp(), eventData.getIntValue());
        } else if (eventData.hasFloatValue()) {  // floating point value
          d = dataPoints.addPoint(eventData.getTimestamp(), eventData.getFloatValue());
        } else {
          throw new IllegalArgumentException("Metric doesn't have either int no float value");
        }
        batchedEvents.incrementAndGet();
        d.addBoth(this);
        if (throttle)
          throttle(d);
      } catch (IllegalArgumentException ie) {
        failures.add(ie.getMessage());
      }
    }
    batched.set(true);
    tryComplete();
    if (failures.size() > 0) {
      logger.error("Points imported with " + failures.toString() + " IllegalArgumentExceptions");
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
    if (awaits == 0 && batched.get())
      complete();
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

  static Comparator<Message.Event> orderByTimestamp() {
    return new Comparator<Message.Event>() {
      public int compare(Message.Event o1, Message.Event o2) {
        return (o1.getTimestamp() < o2.getTimestamp()) ? -1 : (o1.getTimestamp() == o2.getTimestamp() ? 0 : 1);
      }
    };
  }

}
