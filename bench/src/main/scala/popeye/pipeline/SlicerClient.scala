package popeye.pipeline

import java.io.{BufferedReader, InputStreamReader, PrintWriter}
import java.net.Socket

import popeye.Logging

case class TsPoint(metric: String, timestamp: Int, value: Either[Long, Float], tags: Map[String, String])

class SlicerClient(socket: Socket) extends Logging {
  val reader = new BufferedReader(new InputStreamReader(socket.getInputStream))
  val writer = new PrintWriter(socket.getOutputStream, false)

  def putPoints(points: Seq[TsPoint]) = {
    for (point <- points) {
      val valueString = point.value.fold(_.toString, _.toString)
      val tagsString = point.tags.toList.map { case (tagKey, tagValue) => s"$tagKey=$tagValue" }.mkString(" ")
      writer.println(s"put ${ point.metric } ${ point.timestamp } $valueString $tagsString")
    }
  }

  def commit(): Boolean = {
    writer.println("commit 1")
    writer.flush()
    val response = reader.readLine()
    info(s"got response: $response")
    response.startsWith("OK 1")
  }

  def close(): Unit = {
    socket.close()
  }
}

object SlicerClient {
  def createClient(host: String, port: Int) = {
    val socket = new Socket(host, port)
    new SlicerClient(socket)
  }
}
