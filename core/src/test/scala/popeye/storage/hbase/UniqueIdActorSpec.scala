package popeye.storage.hbase

import java.util.concurrent.atomic.AtomicInteger

import popeye.test.MockitoStubs
import popeye.pipeline.test.AkkaTestKitSpec
import org.mockito.Mockito._
import akka.testkit.TestActorRef
import akka.actor.Props
import popeye.storage.hbase.UniqueIdProtocol.{Resolved, FindName}
import popeye.storage.hbase.HBaseStorage.{QualifiedId, ResolvedName, QualifiedName}
import akka.pattern.ask
import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import akka.util.Timeout
import org.mockito.Matchers._


class UniqueIdActorSpec extends AkkaTestKitSpec("uniqueid") with MockitoStubs {
  implicit val timeout = Timeout(1 seconds)
  implicit val executionContext = system.dispatcher
  behavior of "UniqueIdActor"

  it should "create new unique id" in {
    val storage = mock[UniqueIdStorageTrait]
    val actor = TestActorRef(Props.apply(new UniqueIdActor(storage)))
    val generationId = new BytesKey(Array[Byte](0, 0))
    val qName = QualifiedName("kind", generationId, "name")
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

  it should "use batching" in {
    val fetches = new AtomicInteger(0)
    val storage = new UniqueIdStorageStub {
      override def findByName(qnames: Seq[QualifiedName]): Seq[ResolvedName] = {
        Thread.sleep(10)
        fetches.getAndIncrement
        qnames.map(qName => ResolvedName(qName, new BytesKey(Array())))
      }
    }
    val actor = system.actorOf(Props.apply(new UniqueIdActor(storage)))
    val generationId = new BytesKey(Array[Byte](0, 0))
    val numberOfNames = 10
    val qNames = (0 until numberOfNames).map(i => QualifiedName("kind", generationId, i.toString))
    val idsFuture = Future.traverse(qNames) {
      qName => actor ? FindName(qName)
    }
    Await.result(idsFuture, 100 millis)
    fetches.get() should be < (numberOfNames / 3)
  }

  it should "handle failures" in {
    val storage = mock[UniqueIdStorageTrait]
    val actor = TestActorRef(Props.apply(new UniqueIdActor(storage)))
    val generationId = new BytesKey(Array[Byte](0, 0))
    val resolvedName = ResolvedName(QualifiedName("kind", generationId, "first"), new BytesKey(Array[Byte](0)))
    val nExceptions = 10
    val exceptions = (1 to nExceptions).map(_ => new RuntimeException)
    require(nExceptions == exceptions.size)

    exceptions.foldLeft(storage.findByName(any[Seq[QualifiedName]]) throws new RuntimeException) {
      (stor, ex) => stor thenThrows ex
    } thenAnswers (_ => Seq(resolvedName))
    for(_ <- 1 to nExceptions + 1){
      actor ! FindName(resolvedName.toQualifiedName, create = true)
    }
    val future = actor ? FindName(resolvedName.toQualifiedName, create = true)
    val response = Await.result(future, 5 seconds)
    response should equal(Resolved(resolvedName))
  }

  class UniqueIdStorageStub extends UniqueIdStorageTrait {
    override def findByName(qnames: Seq[QualifiedName]): Seq[ResolvedName] = ???

    override def findById(ids: Seq[QualifiedId]): Seq[ResolvedName] = ???

    override def registerName(qname: QualifiedName): ResolvedName = ???
  }

}
