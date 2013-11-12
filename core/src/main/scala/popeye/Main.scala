package popeye

import java.io.File
import com.typesafe.config.{ConfigFactory, Config}
import com.codahale.metrics.MetricRegistry
import akka.actor.ActorSystem

case class MainConfig(debug: Boolean = false,
                      configPath: Option[File] = None,
                      command: Option[PopeyeCommand] = None)

trait PopeyeCommand {
  def prepare(parser: scopt.OptionParser[MainConfig]): scopt.OptionParser[MainConfig]
  def run(actorSystem: ActorSystem, metrics: MetricRegistry, config: Config, mainConfig: MainConfig): Unit
}

object Main {
  val commands = List[PopeyeCommand]()
}

class Main extends App with MetricsConfiguration with Logging {
  val initialParser = new scopt.OptionParser[MainConfig]("popeye") {
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

  val parser =  Main.commands.foldLeft(initialParser)({(parser, cmd) => cmd.prepare(parser)})

  for {
    main <- parser.parse(args, MainConfig())
    cmd <- main.command
  } yield {
    val app = ConfigFactory.parseResources("application.conf")
    val conf = (main.configPath match {
      case Some(file) =>
        app.withFallback(ConfigFactory.parseFile(file))
      case None => app
    }).withFallback(ConfigFactory.parseResources("reference.conf"))
     .withFallback(ConfigFactory.load())
    val metrics = initMetrics(conf)
    val actorSystem = ActorSystem("popeye", conf)
    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable() {
      def run() {
        actorSystem.shutdown()
      }
    }))
    try {
      cmd.run(actorSystem, metrics, conf, main)
    } catch {
      case e: Exception=>
        log.error("Failed to start", e)
        actorSystem.shutdown()
    }
  }
}

