package popeye.pipeline.memory

import popeye.Logging
import popeye.pipeline._
import popeye.proto.PackedPoints
import scala.concurrent.{Future, Promise}
import com.typesafe.config.Config

class MemoryPipelineChannel(val context: PipelineContext)
  extends PipelineChannel with Logging {

  val readers: AtomicList[PointsSink] = new AtomicList[PointsSink]

  def newWriter(): PipelineChannelWriter = new PipelineChannelWriter {
    def write(promise: Option[Promise[Long]], points: PackedPoints): Unit = {
      import context.actorSystem.dispatcher

      val batchId = context.idGenerator.nextId()
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

  def startReader(group: String, mainSink: PointsSink, dropSink: PointsSink): Unit = {
    readers.add(mainSink)
  }
}

object MemoryPipelineChannel {
  def apply(config: Config, context: PipelineContext): MemoryPipelineChannel = {
    new MemoryPipelineChannel(context)
  }
  def factory(): PipelineChannelFactory = {
    new PipelineChannelFactory {
      override def make(config: Config, context: PipelineContext): PipelineChannel = {
        MemoryPipelineChannel(config, context)
      }
    }
  }

}


