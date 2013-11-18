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

  def foreachKeyValue(config: Config, what: String)(body: (String, String) => Unit) = {
    val inner = config.getConfig(what)
    inner.entrySet().foreach { entry =>
      val confName = entry.getKey
      val confType = inner.getString(confName)
      body(confType, confName)
    }
  }

  def mergeDefaults(pc: Config, typeName: String, name: String): Config = {
    val settings = if (pc.hasPath(s"$name"))
      pc.getConfig(s"$name")
    else
      ConfigFactory.empty()
    if (pc.hasPath(s"defaults.$typeName")) {
      settings.withFallback(pc.getConfig(s"defaults.$typeName"))
    } else {
      settings
    }
  }

  def mergeProperties(config: Config, path: String): Properties = {
    val mergedProperties: Properties = config.getStringList(path)
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
