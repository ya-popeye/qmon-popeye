package popeye.storage.hbase

import akka.testkit.TestProbe
import java.io.IOException
import popeye.Logging
import popeye.storage.hbase.UniqueIdProtocol.{Race, ResolutionFailed, Resolved, FindName}
import popeye.transport.test.AkkaTestKitSpec
import scala.concurrent.Await
import scala.concurrent.duration._

/**
 * @author Andrey Stepachev
 */
class UniqueIdSpec extends AkkaTestKitSpec("uniqueid") with Logging {

  import scala.concurrent.ExecutionContext.Implicits.global

  val metric: String = "test.metric.1"
  val qname: QualifiedName = QualifiedName(HBaseStorage.MetricKind, metric)
  implicit val timeout: FiniteDuration = 500 seconds

  behavior of "Uniqueid"

  it should "handle empty cache" in {
    val (probe, uniq) = mkUniq()
    uniq.findIdByName(metric) must be(None)
    uniq.findNameById(id("\01\02\03")) must be(None)
  }

  it should "resolve" in {
    val (probe, uniq) = mkUniq()
    val future = uniq.resolveIdByName(metric, create = true)
    probe.expectMsg(FindName(qname, create = true))
    probe.lastSender ! Resolved(ResolvedName(qname, id("abc")))
    Await.result(future, timeout) must be(id("abc"))
  }

  it should "concurrent resolve" in {
    val (probe, uniq) = mkUniq()
    val future = uniq.resolveIdByName(metric, create = true)
    val future2 = uniq.resolveIdByName(metric, create = true)
    probe.expectMsg(FindName(qname, create = true))
    probe.lastSender ! Resolved(ResolvedName(qname, id("abc")))
    Await.result(future, timeout) must be(id("abc"))
    Await.result(future2, timeout) must be(id("abc"))
  }

  it should "handle resolve failure" in {
    val (probe, uniq) = mkUniq()
    val future = uniq.resolveIdByName(metric, create = true)
    probe.expectMsg(FindName(qname, create = true))
    probe.lastSender ! ResolutionFailed(new IOException("Some hbase error"))
    intercept[IOException] {
      Await.result(future, timeout)
    }
  }

  it should "handle resolve race" in {
    val (probe, uniq) = mkUniq()
    val future = uniq.resolveIdByName(metric, create = true, retries = 2)
    probe.expectMsg(timeout, FindName(qname, create = true))
    probe.lastSender ! Race(qname)
    probe.expectMsg(timeout, FindName(qname, create = true))
    probe.lastSender ! Race(qname)
    probe.expectMsg(timeout, FindName(qname, create = true))
    probe.lastSender ! Race(qname)
    intercept[UniqueIdRaceException] {
      Await.result(future, timeout)
    }
  }

  def id(hex: String): Array[Byte] = {
    hex.getBytes
  }

  def mkUniq(): (TestProbe, UniqueId) = {
    val resolverProbe = TestProbe()
    val uniq = new UniqueId(3, HBaseStorage.MetricKind, resolverProbe.ref, 1, 10, 1 seconds)
    (resolverProbe, uniq)
  }
}
