package popeye.storage.hbase

import popeye.test.MockitoStubs
import popeye.pipeline.test.AkkaTestKitSpec
import org.mockito.Mockito._
import akka.testkit.TestActorRef
import akka.actor.Props
import popeye.storage.hbase.HBaseStorage._
import akka.pattern.ask
import scala.concurrent.Await
import scala.concurrent.duration._
import akka.util.Timeout
import popeye.test.PopeyeTestUtils._
import popeye.storage.hbase.UniqueIdProtocol.RelationsResult
import popeye.storage.hbase.UniqueIdProtocol.GetRelations
import popeye.storage.hbase.UniqueIdProtocol.FindName
import popeye.storage.hbase.HBaseStorage.QualifiedId
import popeye.storage.hbase.HBaseStorage.QualifiedName
import popeye.storage.hbase.UniqueIdProtocol.AddRelations
import popeye.storage.hbase.UniqueIdProtocol.Resolved


class UniqueIdActorSpec extends AkkaTestKitSpec("uniqueid") with MockitoStubs {
  implicit val timeout = Timeout(5 seconds)
  behavior of "UniqueIdActor"

  it should "create new unique id" in {
    val storage = mock[UniqueIdStorageTrait]
    val actor = TestActorRef(Props.apply(new UniqueIdActor(storage)))
    val namespace = bytesKey(0, 0)
    val qName = QualifiedName("kind", namespace, "name")
    val id: BytesKey = bytesKey(0)
    stub(storage.findByName(Seq(qName))).toReturn(Seq())
    stub(storage.registerName(qName)).toReturn(ResolvedName(qName, id))
    val responseFuture = actor ? FindName(qName, create = true)
    val response = Await.result(responseFuture, 5 seconds)
    verify(storage).findByName(Seq(qName))
    verify(storage).registerName(qName)
    verifyNoMoreInteractions(storage)
    response should equal(Resolved(ResolvedName(qName, id)))
  }

  it should "add relations" in {
    val storage = mock[UniqueIdStorageTrait]
    val actor = TestActorRef(Props.apply(new UniqueIdActor(storage)))
    val namespace = bytesKey(0, 0)
    val key = QualifiedId("name", namespace, bytesKey(0))
    val values = Seq(
      QualifiedId("value", namespace, bytesKey(0)),
      QualifiedId("value", namespace, bytesKey(1))
    )
    val responseFuture = actor ? AddRelations(key, values)
    val response = Await.result(responseFuture, 5 seconds)
    verify(storage).addRelations(key, values)
    verifyNoMoreInteractions(storage)
    response should equal(RelationsResult(key, values))
  }

  it should "get relations" in {
    val storage = mock[UniqueIdStorageTrait]
    val actor = TestActorRef(Props.apply(new UniqueIdActor(storage)))
    val namespace = bytesKey(0, 0)
    val key = QualifiedId("name", namespace, bytesKey(0))
    val values = Seq(
      QualifiedId("value", namespace, bytesKey(0)),
      QualifiedId("value", namespace, bytesKey(1))
    )
    stub(storage.getRelations(key)).toReturn(values)
    val responseFuture = actor ? GetRelations(key)
    val response = Await.result(responseFuture, 5 seconds)
    verify(storage).getRelations(key)
    verifyNoMoreInteractions(storage)
    response should equal(RelationsResult(key, values))
  }
}
