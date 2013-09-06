package popeye.storage.hbase

import java.util.Random
import org.apache.hadoop.hbase.client.{Put, HTableInterface, HTablePool}
import org.scalatest.FlatSpec
import org.scalatest.matchers.MustMatchers
import org.mockito.Matchers._
import org.mockito.Mockito._
import popeye.test.MockitoStubs
import popeye.test.PopeyeTestUtils._
import scala.concurrent.{Promise, Await}
import scala.concurrent.duration._
import java.util.concurrent.atomic.AtomicInteger
import org.apache.hadoop.hbase.util.Bytes
import java.util
import org.apache.hadoop.hbase.HBaseIOException

/**
 * @author Andrey Stepachev
 */
class PointsStorageSpec extends FlatSpec with MustMatchers with MockitoStubs {

  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val random = new Random(1234)
  final val tableName = "my-table"

  behavior of "PointsStorage"

  it should "produce key values" in {
    val state = new State

    val future = state.storage.writePoints(mkEvents())
    val written = Await.result(future, 5 seconds)
    written must be(2)
    verify(state.hTablePool, atLeastOnce()).getTable(any[String]())
    verify(state.hTable, atLeastOnce()).put(anyListOf(classOf[Put]))
    verify(state.hTable, atLeastOnce()).flushCommits()
    verify(state.hTable, atLeastOnce()).close()
  }

  it should "handle failures" in {
    val state = new State

    state.hTable.put(anyListOf(classOf[Put])) throws new HBaseIOException("Some cryptic hbase exception")

    val future = state.storage.writePoints(mkEvents())
    intercept[HBaseIOException] {
      Await.result(future, 5 seconds)
    }
    verify(state.hTablePool, atLeastOnce()).getTable(any[String]())
    verify(state.hTable, atLeastOnce()).close()
    verify(state.hTable, never()).flushCommits()
  }

  class State {
    val id = new AtomicInteger(1)
    val hTablePool = mock[HTablePool]
    val hTable = {
      val m = mock[HTableInterface]
      hTablePool.getTable(tableName) returns m
      m
    }

    val metrics = setup(mock[UniqueId])
    val attrNames = setup(mock[UniqueId])
    val attrValues = setup(mock[UniqueId])
    val storage = new PointsStorage(tableName, hTablePool, metrics, attrNames, attrValues)

    def setup(uniq: UniqueId): UniqueId = {
      val idRow = util.Arrays.copyOf(Bytes.toBytes(id.incrementAndGet()), 3)
      uniq.findIdByName(any()) returns Some(idRow)
      uniq.findNameById(any()) returns Some("Name!")
      uniq.resolveIdByName(any(), any(), any())(any()) returns Promise().success(idRow).future
      uniq.resolveNameById(any())(any()) returns Promise().success("Hello!").future
      uniq.width returns 3
      uniq.kind returns "kind"
      uniq
    }
  }

}
