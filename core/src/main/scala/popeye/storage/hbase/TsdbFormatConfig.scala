package popeye.storage.hbase

import com.typesafe.config.{Config, ConfigValueFactory, ConfigFactory}

case class TsdbFormatConfig(startTimeAndPeriods: Seq[StartTimeAndPeriod], shardAttributes: Set[String]) {
  val generationIdMapping = {
    val periodConfigs = PeriodicGenerationId.createPeriodConfigs(startTimeAndPeriods)
    PeriodicGenerationId(periodConfigs)
  }

  val tsdbFormat = new TsdbFormat(generationIdMapping, shardAttributes)
}

object TsdbFormatConfig {

  import scala.collection.JavaConverters._

  val generationsKey = "generations"
  val shardAttributesKey = "shard-attributes"

  def renderConfig(config: TsdbFormatConfig) = {
    val generationsConfig = ConfigValueFactory.fromIterable(
      StartTimeAndPeriod.renderConfigList(config.startTimeAndPeriods).asScala.map {
        config => config.root.unwrapped()
      }.asJava
    )
    ConfigFactory.empty()
      .withValue(generationsKey, generationsConfig)
      .withValue(shardAttributesKey, ConfigValueFactory.fromIterable(config.shardAttributes.asJava))
  }

  def renderString(config: TsdbFormatConfig) = {
    val typesafeConf = renderConfig(config)
    typesafeConf.root().render()
  }

  def parseConfig(config: Config) = {
    val generationsConfigList = config.getConfigList(generationsKey)
    val startTimeAndPeriods = StartTimeAndPeriod.parseConfigList(generationsConfigList)
    val shardAttributes = config.getStringList(shardAttributesKey).asScala.toSet
    TsdbFormatConfig(startTimeAndPeriods, shardAttributes)
  }

  def parseString(string: String) = parseConfig(ConfigFactory.parseString(string))
}
