package popeye.transport.server.http

import org.scalatest.FlatSpec

/**
 * @author Andrey Stepachev
 */
class JsonToEventParserSpec extends FlatSpec {
  var testRequests: Array[String] =
    Array[String](
      "[{\"TESTHOST/nobus/test\": {" + "\"value\": 3.14, \"timestamp\": 1364451167, \"type\": \"numeric\" }},"
        + "{\"TESTHOST/nobus/test\": {" + "\"value\": 3.14, \"timestamp\": 1364451167, \"type\": \"numeric\" }}]",
      "{\"TESTHOST/nobus/test\": [{" + "\"value\": 3.14, \"timestamp\": 1364451167, \"type\": \"numeric\" }],"
        + "\"TESTHOST/nobus/test\": [{" + "\"value\": 3.14, \"timestamp\": 1364451167, \"type\": \"numeric\" }]}",
      "{\"TESTHOST/nobus/test\": [{\"type\": \"numeric\", \"timestamp\": 1364451167, " + "\"value\": 3.14}]}",
      "{\"TESTHOST/nobus/test\": [{\"timestamp\": 1364451167, \"type\": \"numeric\", " + "\"value\": 3.14}]}",
      "{\"TESTHOST/nobus/test\": [{" + "\"value\": 3.14, \"timestamp\": 1364451167, \"type\": \"numeric\" }]}",
      "{\"TESTHOST/nobus/test\": {" + "\"value\": 3.14, \"timestamp\": 1364451167, \"type\": \"numeric\" }}"
    )

  "Parser" should "handle valid json" in {
    val v = for (req <- testRequests;
                 pt <- new JsonToPointParser(req.getBytes)) yield {
      assert(pt.getTimestamp == 1364451167)
      assert(pt.getMetric == "l.nobus/test")
    }
    assert(v.length == testRequests.length + 2)
  }
}
