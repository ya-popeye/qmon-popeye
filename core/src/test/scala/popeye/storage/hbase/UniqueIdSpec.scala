package popeye.storage.hbase

import akka.testkit.TestProbe
import java.io.IOException
import popeye.Logging
import popeye.storage.hbase.UniqueIdProtocol._
import popeye.transport.test.AkkaTestKitSpec
import scala.concurrent.Await
import scala.concurrent.duration._
import popeye.storage.hbase.UniqueIdProtocol.FindName
import popeye.storage.hbase.UniqueIdProtocol.Race
import popeye.storage.hbase.UniqueIdProtocol.Resolved
import popeye.storage.hbase.UniqueIdProtocol.ResolutionFailed
import popeye.storage.hbase.HBaseStorage.{ResolvedName, QualifiedId, QualifiedName}

/**
 * @author Andrey Stepachev
 */
class UniqueIdSpec extends AkkaTestKitSpec("uniqueid") with Logging {

  import scala.concurrent.ExecutionContext.Implicits.global

  val metric = "test.metric.1"
  val qname = QualifiedName(HBaseStorage.MetricKind, metric)
  val metricId = id("\01\02\03")
  val qId = QualifiedId(HBaseStorage.MetricKind, metricId)
  implicit val timeout: FiniteDuration = 500 seconds

  behavior of "id->name"

  it should "handle empty cache" in {
    val (probe, uniq) = mkUniq()
    uniq.findIdByName(metric) must be(None)
    uniq.findNameById(metricId) must be(None)
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

  behavior of "name->id"

  it should "resolve" in {
    val (probe, uniq) = mkUniq()
    val future = uniq.resolveNameById(metricId)
    probe.expectMsg(FindId(qId))
    probe.lastSender ! Resolved(ResolvedName(qname, metricId))
    Await.result(future, timeout) must be(metric)
  }

  it should "concurrent resolve" in {
    val (probe, uniq) = mkUniq()
    val future = uniq.resolveNameById(metricId)
    val future2 = uniq.resolveNameById(metricId)
    probe.expectMsg(FindId(qId))
    probe.lastSender ! Resolved(ResolvedName(qname, metricId))
    Await.result(future, timeout) must be(metric)
    Await.result(future2, timeout) must be(metric)
  }

  it should "handle resolve failure" in {
    val (probe, uniq) = mkUniq()
    val future = uniq.resolveNameById(metricId)
    probe.expectMsg(FindId(qId))
    probe.lastSender ! ResolutionFailed(new IOException("Some hbase error"))
    intercept[IOException] {
      Await.result(future, timeout)
    }
  }

  def id(hex: String): BytesKey = {
    new BytesKey(hex.getBytes)
  }

  def mkUniq(): (TestProbe, UniqueId) = {
    val resolverProbe = TestProbe()
    val uniq = new UniqueIdImpl(3, HBaseStorage.MetricKind, resolverProbe.ref, 1, 10, 1 seconds)
    (resolverProbe, uniq)
  }
}
