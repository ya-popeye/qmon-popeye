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
  implicit val timeout: FiniteDuration = 5 seconds

  behavior of "id->name"

  it should "handle empty cache" in {
    val (probe, uniq) = mkUniq()
    uniq.findIdByName(qname) should be(None)
    uniq.findNameById(qId) should be(None)
  }

  it should "resolve" in {
    val (probe, uniq) = mkUniq()
    val future = uniq.resolveIdByName(qname, create = true)
    probe.expectMsg(FindName(qname, create = true))
    probe.lastSender ! Resolved(ResolvedName(qname, id("abc")))
    Await.result(future, timeout) should be(id("abc"))
  }

  it should "resolve already created ids" in {
    val (probe, uniq) = mkUniq()
    val future = uniq.resolveIdByName(qname, create = false)
    probe.expectMsg(FindName(qname, create = false))
    probe.lastSender ! Resolved(ResolvedName(qname, id("abc")))
    Await.result(future, timeout) should be(id("abc"))
  }

  it should "concurrent resolve" in {
    val (probe, uniq) = mkUniq()
    val future = uniq.resolveIdByName(qname, create = true)
    val future2 = uniq.resolveIdByName(qname, create = true)
    probe.expectMsg(FindName(qname, create = true))
    probe.lastSender ! Resolved(ResolvedName(qname, id("abc")))
    Await.result(future, timeout) should be(id("abc"))
    Await.result(future2, timeout) should be(id("abc"))
  }

  it should "handle resolve failure" in {
    val (probe, uniq) = mkUniq()
    val future = uniq.resolveIdByName(qname, create = true)
    probe.expectMsg(FindName(qname, create = true))
    probe.lastSender ! ResolutionFailed(new IOException("Some hbase error"))
    intercept[IOException] {
      Await.result(future, timeout)
    }
  }

  it should "handle resolve race" in {
    val (probe, uniq) = mkUniq()
    val future = uniq.resolveIdByName(qname, create = true, retries = 2)
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
    val future = uniq.resolveIdByName(qname, create = false)
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
    val future = uniq.resolveNameById(qId)
    probe.expectMsg(FindId(qId))
    probe.lastSender ! Resolved(ResolvedName(qname, metricId))
    Await.result(future, timeout) should be(metric)
  }

  it should "concurrent resolve" in {
    val (probe, uniq) = mkUniq()
    val future = uniq.resolveNameById(qId)
    val future2 = uniq.resolveNameById(qId)
    probe.expectMsg(FindId(qId))
    probe.lastSender ! Resolved(ResolvedName(qname, metricId))
    Await.result(future, timeout) should be(metric)
    Await.result(future2, timeout) should be(metric)
  }

  it should "handle resolve failure" in {
    val (probe, uniq) = mkUniq()
    val future = uniq.resolveNameById(qId)
    probe.expectMsg(FindId(qId))
    probe.lastSender ! ResolutionFailed(new IOException("Some hbase error"))
    intercept[IOException] {
      Await.result(future, timeout)
    }
  }

  behavior of "relations"

  it should "add relations" in {
    val (probe, uniq) = mkUniq()
    val qId = QualifiedId(HBaseStorage.MetricKind, defaultNamespace, id("a"))
    val values = Seq(
      QualifiedId(HBaseStorage.MetricKind, defaultNamespace, id("a")),
      QualifiedId(HBaseStorage.MetricKind, defaultNamespace, id("b"))
    )
    val future = uniq.addRelations(qId, values)
    probe.expectMsg(AddRelations(qId, values))
    probe.lastSender ! RelationsResult(qId, values)
    Await.result(future, timeout)
  }

  it should "not add relations twice" in {
    val (probe, uniq) = mkUniq()
    val qId = QualifiedId(HBaseStorage.MetricKind, defaultNamespace, id("a"))
    val values = Seq(
      QualifiedId(HBaseStorage.MetricKind, defaultNamespace, id("a")),
      QualifiedId(HBaseStorage.MetricKind, defaultNamespace, id("b"))
    )
    val firstFuture = uniq.addRelations(qId, values)
    val secondFuture = uniq.addRelations(qId, values)
    probe.expectMsg(AddRelations(qId, values))
    probe.lastSender ! RelationsResult(qId, values)
    Await.result(firstFuture, timeout)
    Await.result(secondFuture, timeout)
  }

  it should "get relations" in {
    val (probe, uniq) = mkUniq()
    val qId = QualifiedId(HBaseStorage.MetricKind, defaultNamespace, id("a"))
    val values = Seq(
      QualifiedId(HBaseStorage.MetricKind, defaultNamespace, id("a")),
      QualifiedId(HBaseStorage.MetricKind, defaultNamespace, id("b"))
    )
    val future = uniq.getRelations(qId)
    probe.expectMsg(GetRelations(qId))
    probe.lastSender ! RelationsResult(qId, values)
    Await.result(future, timeout).toSet should equal(values.toSet)
  }

  def id(hex: String): BytesKey = {
    new BytesKey(hex.getBytes)
  }

  def mkUniq(): (TestProbe, UniqueIdImpl) = {
    val resolverProbe = TestProbe()
    val uniq = new UniqueIdImpl(resolverProbe.ref, 1, 10, 1 seconds)
    (resolverProbe, uniq)
  }
}
