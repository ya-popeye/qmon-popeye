package popeye.pipeline


import _root_.kafka.message.{Message => KafkaMessage}
import _root_.kafka.api.ProducerRequest
import _root_.kafka.common.TopicAndPartition
import _root_.kafka.message.ByteBufferMessageSet
import java.net.{InetAddress, InetSocketAddress}
import akka.actor.{ActorSystem, Props, ActorRef, Actor}
import akka.io.{IO, Tcp}
import com.codahale.metrics.{CsvReporter, ConsoleReporter, MetricRegistry, Timer}
import akka.util.ByteString
import popeye.pipeline.KafkaClientActor._
import java.nio.ByteBuffer
import scala.collection.mutable
import java.util.zip.CRC32
import scala.util.Random
import java.util.concurrent.TimeUnit
import java.io.File

class KafkaClientActor(remote: InetSocketAddress, clientId: Int, nMessages: Int, messageSize: Int, topicName: String) extends Actor {

  import Tcp._
  import context.system

  val random = new Random(0)

  def receive = {
    case Start =>
      IO(Tcp) ! Connect(remote)

    case CommandFailed(_: Connect) =>
      println("initial connection failed")
      context stop self

    case c@Connected(remote, local) =>
      KafkaClientActor.connections.inc()
      val connection = sender
      connection ! Register(self)
      context become kafkaClient(connection)
      self ! SendNewMessages
  }

  def kafkaClient(connection: ActorRef): Actor.Receive = {
    var commitContext: Timer.Context = null
    var unparsedResponse: ByteString = ByteString.empty
    def reconnect() {
      connection ! Close
      KafkaClientActor.connections.dec()
      IO(Tcp) ! Connect(remote)
      context unbecome()
    }
    {
      case Received(data) =>
        unparsedResponse = unparsedResponse ++ data
        if (KafkaResponseProtocol.isComplete(unparsedResponse)) {
          val response = KafkaResponseProtocol.parseResponse(unparsedResponse)
          unparsedResponse = ByteString.empty
          val errorCode = response.topics.find(_.topic == topicName).get.statuses.find(_.partition == 0).get.errorCode
          errorCode match {
            case 0 =>
              KafkaClientActor.successfulCommits.mark()
              commitContext.stop()
            case 7 => // timeout
              KafkaClientActor.failedCommits.mark()
              commitContext.stop()
            case _ =>
              println(f"error code: $errorCode")
          }
          self ! SendNewMessages
        }
      case CommandFailed(w: Write) =>
        System.err.println(f"actorId:$clientId write command failed, reconnecting")
        reconnect()
      case ErrorClosed(cause) =>
        System.err.println(f"actorId:$clientId error:$cause, reconnecting")
        reconnect()
      case SendNewMessages =>
        connection ! Write(randomRequest)
        commitContext = KafkaClientActor.commitTimeMetric.time()
        KafkaClientActor.successfulCommits.mark()
      case x =>
        println(x)
    }
  }

  def randomMessages =
    for (_ <- 0 until nMessages) yield {
      val bytes = Array.ofDim[Byte](messageSize)
      random.nextBytes(bytes)
      new KafkaMessage(bytes)
    }

  def randomRequest = {
    val request = ProducerRequest.apply(
      correlationId = 0,
      clientId = "test",
      requiredAcks = -1.toShort,
      ackTimeoutMs = 30000,
      data = mutable.Map(TopicAndPartition(topicName, 0) -> new ByteBufferMessageSet(randomMessages: _*))
    )
    val buffer = ByteBuffer.allocate(request.sizeInBytes + (if (request.requestId.isDefined) 2 else 0))
    request.requestId.foreach {
      id => buffer.putShort(id)
    }
    request.writeTo(buffer)
    val bytes = buffer.array()
    KafkaRequestProtocol.int32ToByteString(bytes.length) ++ ByteString(bytes)
  }
}

object KafkaClientActor {

  val metrics = new MetricRegistry()
  val commitTimeMetric = metrics.timer("commit-time")
  val successfulCommits = metrics.meter("commits")
  val failedCommits = metrics.meter("failed-commits")
  val connections = metrics.counter("connections")

  case object Start

  case object SendNewMessages

  def props(remote: InetSocketAddress, id: Int, nMessages: Int, messageSize: Int, topic: String) =
    Props(classOf[KafkaClientActor], remote, id, nMessages, messageSize, topic)

  def main(args: Array[String]) {
    val nConnections = args(0).toInt
    val (nMessages, messageSize) = {
      val List(nMessStr, sizeStr) = args(1).split(":").toList
      (nMessStr.toInt, sizeStr.toInt)
    }
    val topic = args(2)
    val address = {
      val List(host, port) = args(3).split(":").toList
      new InetSocketAddress(InetAddress.getByName(host), port.toInt)
    }
    val metricsDir = args(4)
    val system = ActorSystem()
    val consoleReporter = ConsoleReporter
      .forRegistry(KafkaClientActor.metrics)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .convertRatesTo(TimeUnit.SECONDS)
      .build()
    consoleReporter.start(1, TimeUnit.SECONDS)


    val csvReporter = CsvReporter
      .forRegistry(KafkaClientActor.metrics)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .convertRatesTo(TimeUnit.SECONDS)
      .build(new File(metricsDir))

    csvReporter.start(5, TimeUnit.SECONDS)

    for (i <- 0 until nConnections) {
      Thread.sleep(10)
      system.actorOf(KafkaClientActor.props(address, i, nMessages, messageSize, topic)) ! Start
    }
  }
}

object KafkaResponseProtocol {

  //  RequestOrResponse => Size (RequestMessage | ResponseMessage)
  //  Size => int32

  //  Response => CorrelationId ResponseMessage
  //  CorrelationId => int32
  //  ResponseMessage => MetadataResponse | ProduceResponse | FetchResponse | OffsetResponse | OffsetCommitResponse | OffsetFetchResponse

  //  ProduceResponse => [TopicName [Partition ErrorCode Offset]]
  //  TopicName => string
  //  Partition => int32
  //  ErrorCode => int16
  //  Offset => int64

  case class PartitionStatus(partition: Int, errorCode: Short, offset: Long)

  case class TopicAndPartitionStatuses(topic: String, statuses: Seq[PartitionStatus])

  case class KafkaResponse(correlationId: Int, topics: Seq[TopicAndPartitionStatuses])

  def isComplete(bytes: ByteString) = (bytes.length >= 4) && {
    val size = bytes.asByteBuffer.getInt()
    if (bytes.length > size + 4) {
      println("bytes.length > size + 4")
    }
    bytes.length == size + 4
  }

  def parseArray[T](buffer: ByteBuffer, parser: ByteBuffer => T) = {
    val arraySize = buffer.getInt
    for (_ <- 0 until arraySize) yield {
      parser(buffer)
    }
  }

  def parseString(buffer: ByteBuffer) = {
    val stringSize = buffer.getShort.toInt
    val bytes = Array.ofDim[Byte](stringSize)
    buffer.get(bytes)
    new String(bytes)
  }

  def parsePartitionStatus(buffer: ByteBuffer) = {
    val partition = buffer.getInt()
    val errCode = buffer.getShort()
    val offset = buffer.getLong()
    PartitionStatus(partition, errCode, offset)
  }

  def parseTopicAndPartitionStatuses(buffer: ByteBuffer) = {
    val topic = parseString(buffer)
    val statuses = parseArray(buffer, parsePartitionStatus)
    TopicAndPartitionStatuses(topic, statuses)
  }

  def parseResponse(bytes: ByteString): KafkaResponse = {
    val buffer = bytes.asByteBuffer
    val correlationId = buffer.getInt()
    val topics = parseArray(buffer, parseTopicAndPartitionStatuses)
    KafkaResponse(correlationId, topics)
  }

}

object KafkaRequestProtocol {

  def sendMessagesBytesString(clientId: String,
                              messages: Seq[TopicAndMessageSets],
                              corellationId: Int,
                              requiredAcks: Short = -1,
                              timeout: Int = 5000) = {
    val messagesSets = arrayToByteString(messages.map(messageSetsToByteString))
    val produceRequestMessage = produceRequest(requiredAcks, timeout, messagesSets)
    val requestMessage = requestMessageToByteString(
      apiKey = 0, // ProduceRequest
      apiVersion = 0.toShort,
      corellationId,
      clientId = clientId,
      produceRequestMessage
    )
    clientMessage(requestMessage)
  }

  case class PartitionAndMessageSet(partition: Int, messageSet: Seq[(Array[Byte], Array[Byte])])

  case class TopicAndMessageSets(topic: String, messageSets: Seq[PartitionAndMessageSet])

  //  Message => Crc MagicByte Attributes Key Value
  //  Crc => int32
  //  MagicByte => int8   // should be 0
  //  Attributes => int8 // should be 0 (no compression)
  //  Key => bytes
  //  Value => bytes

  def messageToByteString(key: Array[Byte], value: Array[Byte]) = {
    val buffer = ByteBuffer.allocate(
      4 + // Crc
        1 + // MagicByte
        1 + // Attributes
        4 + // key lenght size
        (if (key == null) 0 else key.length) +
        4 + //value length size
        (if (value == null) 0 else value.length)
    )
    buffer.position(4) // skip Crc
    buffer.put(0.toByte)
    buffer.put(0.toByte)
    buffer.put(bytesToByteString(key).toByteBuffer)
    buffer.put(bytesToByteString(value).toByteBuffer)

    val crc = new CRC32()
    val bytes = buffer.array()
    crc.update(bytes, 4 /* MagicOffset */ , bytes.length - 4)
    val crcValue = (crc.getValue() & 0xffffffffL).asInstanceOf[Int]

    buffer.putInt(0, crcValue)
    ByteString(buffer.array())
  }

  //  MessageSet => [Offset MessageSize Message]
  //  Offset => int64
  //  MessageSize => int32

  def partitionAndMessageSetToByteString(partitionAndMessageSet: PartitionAndMessageSet) = {
    val PartitionAndMessageSet(partition, messageSet) = partitionAndMessageSet
    val serializedSet = arrayToByteString(
      messageSet.map {
        case (key, value) =>
          val dummyOffset = 10
          val messageByteString = messageToByteString(key, value)
          int64ToByteString(dummyOffset) ++ int32ToByteString(messageByteString.length) ++ messageByteString
      }
    )
    int32ToByteString(partition) ++ serializedSet
  }

  def messageSetsToByteString(topicAndMessageSets: TopicAndMessageSets) = {
    val TopicAndMessageSets(topic, messageSets) = topicAndMessageSets
    stringToByteString(topic) ++ arrayToByteString(messageSets.map(partitionAndMessageSetToByteString))
  }

  //  ProduceRequest => RequiredAcks Timeout [TopicName [Partition MessageSetSize MessageSet]]
  //  RequiredAcks => int16
  //  Timeout => int32
  //  Partition => int32
  //  MessageSetSize => int32

  def produceRequest(requiredAcks: Short, timeout: Int, messages: ByteString) = {
    int16ToByteString(requiredAcks) ++ int32ToByteString(timeout) ++ messages
  }

  //  RequestMessage => ApiKey ApiVersion CorrelationId ClientId RequestMessage
  //  ApiKey => int16
  //  ApiVersion => int16
  //  CorrelationId => int32
  //  ClientId => string
  //  RequestMessage => MetadataRequest | ProduceRequest | FetchRequest | OffsetRequest | OffsetCommitRequest | OffsetFetchRequest

  def requestMessageToByteString(apiKey: Short,
                                 apiVersion: Short,
                                 correlationId: Int,
                                 clientId: String,
                                 requestMessage: ByteString) = {
    int16ToByteString(apiKey) ++
      int16ToByteString(apiVersion) ++
      int32ToByteString(correlationId) ++
      stringToByteString(clientId) ++
      requestMessage
  }

  //  RequestOrResponse => Size (RequestMessage | ResponseMessage)
  //  Size => int32

  def clientMessage(message: ByteString) = {
    val length: Int = message.length
    val messageSize = int32ToByteString(length)
    messageSize ++ message
  }

  def int64ToByteString(n: Long): ByteString = {
    ByteString(ByteBuffer.allocate(8).putLong(n).array())
  }

  def int32ToByteString(n: Int): ByteString = {
    ByteString(ByteBuffer.allocate(4).putInt(n).array())
  }

  def int16ToByteString(n: Short): ByteString = {
    ByteString(ByteBuffer.allocate(2).putShort(n).array())
  }

  def stringToByteString(string: String): ByteString =
    if (string != null) {
      val bytes: Array[Byte] = string.getBytes(java.nio.charset.Charset.defaultCharset())
      int16ToByteString(bytes.length.toShort) ++ ByteString(bytes)
    } else {
      int16ToByteString(-1)
    }

  def bytesToByteString(bytes: Array[Byte]) =
    if (bytes != null) {
      int32ToByteString(bytes.length) ++ ByteString(bytes)
    } else {
      int32ToByteString(-1)
    }

  def arrayToByteString(array: Seq[ByteString]) = {
    int32ToByteString(array.size) ++ array.foldLeft(ByteString.empty)(_ ++ _)
  }
}


