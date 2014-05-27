package popeye.storage.hbase

import akka.testkit.TestProbe
import java.io.IOException
import popeye.Logging
import popeye.storage.hbase.UniqueIdProtocol._
import popeye.pipeline.test.AkkaTestKitSpec
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration._
import popeye.storage.hbase.UniqueIdProtocol.FindName
import popeye.storage.hbase.UniqueIdProtocol.Race
import popeye.storage.hbase.UniqueIdProtocol.Resolved
import popeye.storage.hbase.UniqueIdProtocol.ResolutionFailed
import popeye.storage.hbase.HBaseStorage.{ResolvedName, QualifiedId, QualifiedName}
import java.util.NoSuchElementException

/**
 * @author Andrey Stepachev
 */
class UniqueIdSpec extends AkkaTestKitSpec("uniqueid") with Logging {

  implicit val executionContext = system.dispatcher

  val metric = "test.metric.1"
  val defaultNamespace: BytesKey = new BytesKey(Array[Byte](0, 0))
  val qname = QualifiedName(HBaseStorage.MetricKind, defaultNamespace, metric)
  val metricId = id("\01\02\03")
  val qId = QualifiedId(HBaseStorage.MetricKind, defaultNamespace, metricId)
  implicit val timeout: FiniteDuration = 500 seconds

  behavior of "id->name"

  it should "handle empty cache" in {
    val (probe, uniq) = mkUniq()
    uniq.findIdByName(metric) should be(None)
    uniq.findNameById(metricId) should be(None)
  }

  it should "resolve" in {
    val (probe, uniq) = mkUniq()
    val future = uniq.resolveIdByName(metric, create = true)
    probe.expectMsg(FindName(qname, create = true))
    probe.lastSender ! Resolved(ResolvedName(qname, id("abc")))
    Await.result(future, timeout) should be(id("abc"))
  }

  it should "resolve already created ids" in {
    val (probe, uniq) = mkUniq()
    val future = uniq.resolveIdByName(metric, create = false)
    probe.expectMsg(FindName(qname, create = false))
    probe.lastSender ! Resolved(ResolvedName(qname, id("abc")))
    Await.result(future, timeout) should be(id("abc"))
  }

  it should "concurrent resolve" in {
    val (probe, uniq) = mkUniq()
    val future = uniq.resolveIdByName(metric, create = true)
    val future2 = uniq.resolveIdByName(metric, create = true)
    probe.expectMsg(FindName(qname, create = true))
    probe.lastSender ! Resolved(ResolvedName(qname, id("abc")))
    Await.result(future, timeout) should be(id("abc"))
    Await.result(future2, timeout) should be(id("abc"))
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

  it should "not hang in nonexistent name resolving" in {
    val (probe, uniq) = mkUniq()
    val future = uniq.resolveIdByName(qname.name, create = false)
    probe.expectMsg(FindName(qname, create = false))
    probe.lastSender ! NotFoundName(qname)
    val exception = intercept[NoSuchElementException] {
      Await.result(future, 5 seconds)
    }
    exception.getMessage should (include(qname.name) and include(qname.kind))
  }

  behavior of "name->id"

  it should "resolve" in {
    val (probe, uniq) = mkUniq()
    val future = uniq.resolveNameById(metricId)
    probe.expectMsg(FindId(qId))
    probe.lastSender ! Resolved(ResolvedName(qname, metricId))
    Await.result(future, timeout) should be(metric)
  }

  it should "concurrent resolve" in {
    val (probe, uniq) = mkUniq()
    val future = uniq.resolveNameById(metricId)
    val future2 = uniq.resolveNameById(metricId)
    probe.expectMsg(FindId(qId))
    probe.lastSender ! Resolved(ResolvedName(qname, metricId))
    Await.result(future, timeout) should be(metric)
    Await.result(future2, timeout) should be(metric)
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

  def mkUniq(): (TestProbe, ScopedUniqueId) = {
    val resolverProbe = TestProbe()
    val uniq = new UniqueIdImpl(3, HBaseStorage.MetricKind, resolverProbe.ref, 1, 10, 1 seconds)
    (resolverProbe, new ScopedUniqueId(uniq, defaultNamespace))
  }

  class ScopedUniqueId(uniqueId: UniqueId, namespace: BytesKey) {
    def findIdByName(name: String): Option[BytesKey] = uniqueId.findIdByName(namespace, name)

    def findNameById(id: BytesKey): Option[String] = uniqueId.findNameById(namespace, id)

    def resolveIdByName(name: String, create: Boolean, retries: Int = 3)(implicit timeout: Duration): Future[BytesKey] =
      uniqueId.resolveIdByName(namespace, name, create, retries)

    def resolveNameById(id: BytesKey)(implicit timeout: Duration): Future[String] =
      uniqueId.resolveNameById(namespace, id)
  }

}
