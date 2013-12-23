package popeye.storage.hbase

import popeye.test.MockitoStubs
import popeye.pipeline.test.AkkaTestKitSpec
import org.mockito.Mockito._
import akka.testkit.TestActorRef
import akka.actor.Props
import popeye.storage.hbase.UniqueIdProtocol.{Resolved, FindName}
import popeye.storage.hbase.HBaseStorage.{ResolvedName, QualifiedName}
import akka.pattern.ask
import scala.concurrent.Await
import scala.concurrent.duration._
import akka.util.Timeout


class UniqueIdActorSpec extends AkkaTestKitSpec("uniqueid") with MockitoStubs {
  implicit val timeout = Timeout(5 seconds)
  behavior of "UniqueIdActor"

  it should "create new unique id" in {
    val storage = mock[UniqueIdStorageTrait]
    val actor = TestActorRef(Props.apply(new UniqueIdActor(storage)))
    val qName = QualifiedName("kind", "name")
    val id: BytesKey = new BytesKey(Array[Byte](0))
    stub(storage.findByName(Seq(qName))).toReturn(Seq())
    stub(storage.registerName(qName)).toReturn(ResolvedName(qName, id))
    val responseFuture = actor ? FindName(qName, create = true)
    val response = Await.result(responseFuture, 5 seconds)
    verify(storage).findByName(Seq(qName))
    verify(storage).registerName(qName)
    verifyNoMoreInteractions(storage)
    response should equal(Resolved(ResolvedName(qName, id)))
  }
}
