package popeye.transport.legacy

import org.scalatest.FlatSpec

/**
 * @author Andrey Stepachev
 */
class JsonToEventParserSpec extends FlatSpec {
  var testRequests: Array[String] =
    Array[String](
      "{\"TESTHOST/nobus/test\": [{\"type\": \"numeric\", \"timestamp\": 1364451167, " + "\"value\": 3.14}]}",
      "{\"TESTHOST/nobus/test\": [{\"timestamp\": 1364451167, \"type\": \"numeric\", " + "\"value\": 3.14}]}",
      "{\"TESTHOST/nobus/test\": [{" + "\"value\": 3.14, \"timestamp\": 1364451167, \"type\": \"numeric\" }]}",
      "{\"TESTHOST/nobus/test\": {" + "\"value\": 3.14, \"timestamp\": 1364451167, \"type\": \"numeric\" }}")

  "Parser" should "handle valid json" in {
    val v = for (req <- testRequests;
                 ev <- new JsonToEventParser(req.getBytes)) yield {
      assert(ev.getTimestamp == 1364451167)
      assert(ev.getMetric == "TESTHOST/nobus/test")
    }
    assert(v.length == testRequests.length)
  }
}
