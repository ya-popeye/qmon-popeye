package popeye

import com.typesafe.config.{ConfigFactory, ConfigValue, Config}
import java.util.Properties
import java.util.Map.Entry
import scala.collection.JavaConversions._
import scala.concurrent.duration.FiniteDuration
import java.util.concurrent.TimeUnit
import java.io.InputStream

/**
 * @author Andrey Stepachev
 */
object ConfigUtil {

  def mergeProperties(globalConfig: Config, path: String): Properties = {
    val mergedProperties: Properties = globalConfig.getStringList("kafka.producer.config")
      .map{ conf =>
      val props = new Properties
      this.getClass.getClassLoader.getResourceAsStream(conf) match {
        case null => throw new IllegalArgumentException(s"Config $conf not found in classpath")
        case stream =>
          props.load(stream)
          props
      }
    }.reduce{ (l, r) =>  l.putAll(r); l}
    mergedProperties
  }

  def loadSubsysConfig(subsys: String): Config = {
    ConfigFactory.parseResources(s"$subsys.conf")
      .withFallback(ConfigFactory.parseResources("reference.conf"))
      .withFallback(ConfigFactory.load())
  }

  def toFiniteDuration(milliseconds: Long): FiniteDuration = {
    new FiniteDuration(milliseconds, TimeUnit.MILLISECONDS)
  }

  implicit def toProperties(config: Config): Properties = {
    val p = new Properties()
    config.entrySet().foreach({
      e: Entry[String, ConfigValue] => {
        p.setProperty(e.getKey, e.getValue.unwrapped().toString())
      }
    })
    p
  }
}
