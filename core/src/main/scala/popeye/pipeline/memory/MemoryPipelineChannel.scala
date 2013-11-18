package popeye.pipeline.memory

import akka.actor.{ActorRef, ActorSystem}
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.Config
import popeye.{Logging, IdGenerator}
import popeye.pipeline._
import popeye.proto.PackedPoints
import scala.concurrent.{Future, Promise}

class MemoryPipelineChannel(val config: Config,
                            val actorSystem: ActorSystem,
                            val metrics: MetricRegistry,
                            val idGenerator: IdGenerator)
  extends PipelineChannel with Logging {

  val readers: AtomicList[PointsSink] = new AtomicList[PointsSink]

  def newWriter(): PipelineChannelWriter = new PipelineChannelWriter {
    def write(promise: Option[Promise[Long]], points: PackedPoints): Unit = {
      import actorSystem.dispatcher

      val batchId = idGenerator.nextId()
      debug(s"$batchId: writing points")

      val allReaders: Future[Long] = Future.sequence(readers.map { r=>
        r.send(Seq(batchId), points)
      }).collect{ case x =>
        debug(s"$batchId write complete: ${x.mkString}}")
        batchId
      }
      if (promise.isDefined)
        promise.get.completeWith(allReaders)
    }
  }

  def startReader(group: String, sink: PointsSink): Unit = {
    readers.add(sink)
  }
}
