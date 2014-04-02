package popeye.pipeline

import _root_.kafka.producer.{KeyedMessage, ProducerConfig, Producer}
import java.util.Properties
import java.io.{File, FileReader}
import scala.util.Random
import scala.concurrent.{ExecutionContext, Future}
import com.codahale.metrics.{ConsoleReporter, CsvReporter, MetricRegistry}
import popeye.pipeline.KafkaProducerPerfTest.Instrumented
import java.util.concurrent.{TimeUnit, Executors}
import org.apache.log4j.{Level, Logger}

object KafkaProducerPerfTest {

  trait Instrumented extends nl.grons.metrics.scala.InstrumentedBuilder {
    val metricRegistry = perfTestMetricRegistry
    val sendTime = metrics.timer("send-time")
    val sentMessages = metrics.meter("messages")
    val bytes = metrics.meter("bytes")
  }

  val perfTestMetricRegistry = new MetricRegistry()

  def readProducerConfig(file: String) = {
    val p = new Properties
    val reader = new FileReader(new File(file))
    try {
      p.load(reader)
    } finally {
      reader.close()
    }
    new ProducerConfig(p)
  }

  def main(args: Array[String]) {
    val config = readProducerConfig(args(0))
    val nThreads = args(1).toInt
    val (batchSize, messageSize) = {
      val List(batchSizeStr, messageSizeStr) = args(2).split(":").toList
      (batchSizeStr.toInt, messageSizeStr.toInt * 1000)
    }
    val metricsDir = args(3)
    implicit val exct = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(nThreads))
    for (i <- 0 until nThreads) {
      val producer = new Producer[Array[Byte], Array[Byte]](config)
      new KafkaProducerPerfTest("test", messageSize, batchSize, producer).startTest
    }
    val csvReporter = CsvReporter
      .forRegistry(KafkaProducerPerfTest.perfTestMetricRegistry)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .convertRatesTo(TimeUnit.SECONDS)
      .build(new File(metricsDir))
    csvReporter.start(5, TimeUnit.SECONDS)

    val consoleReporter = ConsoleReporter
      .forRegistry(KafkaProducerPerfTest.perfTestMetricRegistry)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .convertRatesTo(TimeUnit.SECONDS)
      .build()
    consoleReporter.start(1, TimeUnit.SECONDS)

  }
}

class KafkaProducerPerfTest(topic: String,
                            messageSize: Int,
                            batchSize: Int,
                            producer: Producer[Array[Byte], Array[Byte]]) extends Instrumented {

  val random = new Random()

  def randomMessage = {
    val keyBytes = Array.ofDim[Byte](8)
    val valueBytes = Array.ofDim[Byte](messageSize)
    random.nextBytes(keyBytes)
    random.nextBytes(valueBytes)
    new KeyedMessage[Array[Byte], Array[Byte]](topic, keyBytes, valueBytes)
  }

  def startTest(implicit exct: ExecutionContext) = Future {
    while(true) {
      val messages = (0 until batchSize).map(i => randomMessage)
      sendTime.time {
        producer.send(messages: _*)
      }
      bytes.mark(batchSize * messageSize)
      sentMessages.mark(batchSize)
    }
  }.onFailure {
    case x => x.printStackTrace(System.err)
  }

}

