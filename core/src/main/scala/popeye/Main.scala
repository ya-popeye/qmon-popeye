package popeye

import java.io.File
import com.typesafe.config.{ConfigFactory, Config}
import com.codahale.metrics.MetricRegistry
import akka.actor.ActorSystem
import popeye.pipeline.PipelineCommand

case class MainConfig(debug: Boolean = false,
                      configPath: Option[File] = None,
                      command: Option[PopeyeCommand] = None)

trait PopeyeCommand {
  def prepare(parser: scopt.OptionParser[MainConfig]): scopt.OptionParser[MainConfig]
  def run(actorSystem: ActorSystem, metrics: MetricRegistry, config: Config, mainConfig: MainConfig): Unit
}

object Main extends App with MetricsConfiguration with Logging {
  val commands = List[PopeyeCommand](new PipelineCommand)

  val commonParser = new scopt.OptionParser[MainConfig]("popeye") {
    head("popeye", "0.x")

    help("help")
    version("version")

    opt[File]('c', "config") valueName "conf" optional() action {
      (x,c) => c.copy(configPath = Some(x))
    } validate {
      x => if (x.exists() && x.canRead) success else failure(s"File $x not exists or not readable")
    }

    opt[Unit]("debug") optional() action { (_, c) =>
      c.copy(debug = true) } text "this option is hidden in any usage text"
  }

  val parser =  Main.commands.foldLeft(commonParser)({(parser, cmd) => cmd.prepare(parser)})

  val main = parser.parse(args, MainConfig())
  if (!main.isDefined) {
    System.exit(1)
  } else {
    val cmd = main.get.command
    if (!cmd.isDefined) {
      parser.reportError("No command was passed")
      System.exit(2)
    } else {
      val conf = (main.get.configPath match {
        case Some(file) =>
          ConfigFactory.parseFile(file)
        case None =>
          ConfigFactory.load("application")
      }).withFallback(ConfigFactory.load("reference"))
        .withFallback(ConfigFactory.load())
        .resolve()
      val metrics = initMetrics(conf)
      val actorSystem = ActorSystem("popeye", conf)
      Runtime.getRuntime.addShutdownHook(new Thread(new Runnable() {
        def run() {
          actorSystem.shutdown()
        }
      }))
      try {
        cmd.get.run(actorSystem, metrics, conf, main.get)
      } catch {
        case e: Exception=>
          log.error("Failed to start", e)
          actorSystem.shutdown()
      }
    }
  }
}

