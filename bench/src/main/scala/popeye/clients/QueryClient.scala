package popeye.clients

import java.io.{InputStreamReader, BufferedReader, PrintWriter}
import java.net.{HttpURLConnection, URL}

import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.map.ObjectMapper
import org.codehaus.jackson.node.{JsonNodeFactory, ObjectNode}
import popeye.query.PointsStorage.NameType
import popeye.query.PointsStorage.NameType._

import scala.collection.JavaConverters._

case class QueryResult(metric: String, tags: Map[String, String], dps: Seq[(Int, Double)])

class QueryClient(host: String, port: Int) {
  val jsonFactory = JsonNodeFactory.instance

  def queryJson(metricName: String,
                aggregator: String,
                tags: Map[String, String],
                downsampleOption: Option[String] = None,
                rateOption: Option[Boolean] = None): ObjectNode = {
    val query = jsonFactory.objectNode()
    query.put("metric", metricName)
    query.put("aggregator", aggregator)
    val tagsNode = jsonFactory.objectNode()
    for ((tagKey, tagValue) <- tags) {
      tagsNode.put(tagKey, tagValue)
    }
    query.put("tags", tagsNode)
    for (downsample <- downsampleOption) {
      query.put("downsample", downsample)
    }
    for (rate <- rateOption) {
      query.put("rate", rate)
    }
    query
  }

  def runQuery(startSeconds: Int, endSecondsOption: Option[Int], queries: Seq[ObjectNode]) = {
    val request = jsonFactory.objectNode()
    request.put("start", startSeconds.toLong * 1000)
    for (endSeconds <- endSecondsOption) {
      request.put("end", endSeconds.toLong * 1000)
    }
    val queriesArray = jsonFactory.arrayNode()
    for (query <- queries) {
      queriesArray.add(query)
    }
    request.put("queries", queriesArray)
    val queryApiUrl = new URL(s"http://$host:$port/api/query")
    val response = doPost(queryApiUrl, request)
    response.asScala.toList.map {
      resultJson =>
        val metric = resultJson.get("metric").getTextValue
        val tags = resultJson.get("tags").getFields.asScala.map {
          jsonTag =>
            val key = jsonTag.getKey
            val value = jsonTag.getValue.getTextValue
            (key, value)
        }.toMap
        val dps = resultJson.get("dps").getFields.asScala.toVector.map {
          jsonPoint =>
            (jsonPoint.getKey.toInt, jsonPoint.getValue.getDoubleValue)
        }
        QueryResult(metric, tags, dps)
    }
  }

  def getSuggestions(prefix: String, suggestionType: NameType.NameType): List[String] = {
    import NameType._
    val typeString = suggestionType match {
      case MetricType => "metrics"
      case AttributeNameType => "tagk"
      case AttributeValueType => "tagv"
    }
    val url = new URL(s"http://$host:$port/api/suggest?type=$typeString&q=$prefix")
    val response = doGet(url)
    response.asScala.map(_.getTextValue).toList
  }

  def doGet(url: URL): JsonNode = {
    val connection = url.openConnection().asInstanceOf[HttpURLConnection]
    try {
      connection.setRequestMethod("GET")
      val objectMapper = new ObjectMapper()
      objectMapper.readTree(connection.getInputStream)
    } finally {
      connection.disconnect()
    }
  }

  def doPost(url: URL, request: ObjectNode): JsonNode = {
    val connection = url.openConnection().asInstanceOf[HttpURLConnection]
    try {
      connection.setDoOutput(true)
      connection.setRequestMethod("POST")
      val httpOut = new PrintWriter(connection.getOutputStream, true)
      val objectMapper = new ObjectMapper()
      objectMapper.writeValue(httpOut, request)
      objectMapper.readTree(connection.getInputStream)
    } finally {
      connection.disconnect()
    }
  }

}

