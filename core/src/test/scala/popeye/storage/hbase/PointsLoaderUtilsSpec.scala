package popeye.storage.hbase

import org.scalatest.matchers.{MustMatchers, ShouldMatchers}
import popeye.test.MockitoStubs
import org.scalatest.FlatSpec
import java.util.regex.Pattern
import scala.util.Random
import java.nio.CharBuffer
import scala.Array
import PointsLoaderUtils.ValueIdFilterCondition._

class PointsLoaderUtilsSpec extends FlatSpec with ShouldMatchers with MustMatchers with MockitoStubs {

  behavior of "PointsLoaderUtils"

  it should "handle a simple case" in {
    val attributes = Map(bytesKey(0, 0, 1) -> Single(bytesKey(0, 0, 1)))
    val regexp = PointsLoaderUtils.createRowRegexp(offset = 7, attrNameLength = 3, attrValueLength = 3, attributes)
    val pattern = Pattern.compile(regexp)

    val validRow = bytesToString(bytesKey(0, 0, 2, 76, -45, -71, -128, 0, 0, 1, 0, 0, 1))
    pattern.matcher(validRow).matches() should be(true)

    val invalidRow = bytesToString(bytesKey(0, 0, 2, 76, -45, -71, -128, 0, 0, 1, 0, 0, 3))
    pattern.matcher(invalidRow).matches() should be(false)
  }

  it should "check attribute name length" in {
    val attributes = Map(bytesKey(0) -> Single(bytesKey(0)))
    val exception = intercept[IllegalArgumentException] {
      PointsLoaderUtils.createRowRegexp(offset = 7, attrNameLength = 3, attrValueLength = 1, attributes)
    }
    exception.getMessage should (include("3") and include("1") and include("name"))
  }

  it should "check attribute value length" in {
    val attributes = Map(bytesKey(0) -> Single(bytesKey(0)))
    val exception = intercept[IllegalArgumentException] {
      PointsLoaderUtils.createRowRegexp(offset = 7, attrNameLength = 1, attrValueLength = 3, attributes)
    }
    exception.getMessage should (include("3") and include("1") and include("value"))
  }

  it should "check that attribute name and value length is greater than zero" in {
    intercept[IllegalArgumentException] {
      PointsLoaderUtils.createRowRegexp(
        offset = 7,
        attrNameLength = 0,
        attrValueLength = 1,
        attributes = Map(bytesKey(0) -> Single(bytesKey(0)))
      )
    }.getMessage should (include("0") and include("name"))
    intercept[IllegalArgumentException] {
      PointsLoaderUtils.createRowRegexp(
        offset = 7,
        attrNameLength = 1,
        attrValueLength = 0,
        attributes = Map(bytesKey(0) -> Single(bytesKey(0)))
      )
    }.getMessage should (include("0") and include("value"))
  }

  it should "check that attribute list is not empty" in {
    val exception = intercept[IllegalArgumentException] {
      PointsLoaderUtils.createRowRegexp(offset = 7, attrNameLength = 1, attrValueLength = 2, attributes = Map())
    }
    exception.getMessage should include("empty")
  }

  it should "escape regex escaping sequences symbols" in {
    val badStringBytes = stringToBytes("aaa\\Ebbb")
    val attrName = badStringBytes
    val attrValue = badStringBytes
    val rowRegexp = PointsLoaderUtils.createRowRegexp(offset = 0, attrName.length, attrValue.length, Map(attrName -> Single(attrValue)))
    val rowString = createRowString(attrs = List((attrName, attrValue)))
    rowString should fullyMatch regex rowRegexp
  }

  it should "escape regex escaping sequences symbols (non-trivial case)" in {
    val attrName = stringToBytes("aaa\\")
    val attrValue = stringToBytes("Eaaa")
    val regexp = PointsLoaderUtils.createRowRegexp(offset = 0, attrName.length, attrValue.length, Map(attrName -> Single(attrValue)))
    val rowString = createRowString(attrs = List((attrName, attrValue)))
    rowString should fullyMatch regex regexp
  }

  it should "handle newline characters" in {
    val attrName = stringToBytes("attrName")
    val attrValue = stringToBytes("attrValue")
    val rowRegexp = PointsLoaderUtils.createRowRegexp(offset = 1, attrName.length, attrValue.length, Map(attrName -> Single(attrValue)))
    val row = createRow(prefix = stringToBytes("\n"), List((attrName, attrValue)))
    val rowString = bytesToString(row)
    rowString should fullyMatch regex rowRegexp
  }

  it should "create regexp for multiple value filtering" in {
    val attrName = stringToBytes("attrName")
    val attrValues = List(bytesKey(1), bytesKey(2))
    val rowRegexp = PointsLoaderUtils.createRowRegexp(
      offset = 0,
      attrName.length,
      attrValueLength = 1,
      Map((attrName, Multiple(attrValues)))
    )

    createRowString(attrs = List((attrName, bytesKey(1)))) should fullyMatch regex rowRegexp
    createRowString(attrs = List((attrName, bytesKey(2)))) should fullyMatch regex rowRegexp
    createRowString(attrs = List((attrName, bytesKey(3)))) should not(fullyMatch regex rowRegexp)
  }

  it should "create regexp for any value filtering" in {
    val attrName = stringToBytes("attrName")
    val rowRegexp = PointsLoaderUtils.createRowRegexp(
      offset = 0,
      attrName.length,
      attrValueLength = 1,
      Map(attrName -> All)
    )

    createRowString(attrs = List((attrName, bytesKey(1)))) should fullyMatch regex rowRegexp
    createRowString(attrs = List((attrName, bytesKey(100)))) should fullyMatch regex rowRegexp
    createRowString(attrs = List((stringToBytes("ATTRNAME"), bytesKey(1)))) should not(fullyMatch regex rowRegexp)
  }

  it should "pass randomized test" in {
    implicit val random = deterministicRandom
    for (_ <- 0 to 100) {
      val offset = random.nextInt(10)
      val attrNameLength = random.nextInt(5) + 1
      val attrValueLength = random.nextInt(5) + 1
      val searchAttrs = randomAttributes(attrNameLength, attrValueLength)
      val attrsForRegexp = searchAttrs.map { case (n, v) => (n, Single(v))}.toMap
      val searchAttrNamesSet = searchAttrs.map { case (name, _) => name.bytes.toList}.toSet
      val rowRegexp = PointsLoaderUtils.createRowRegexp(offset, attrNameLength, attrValueLength, attrsForRegexp)
      def createJunkAttrs() = randomAttributes(attrNameLength, attrValueLength).filter {
        case (name, _) => !searchAttrNamesSet(name.bytes.toList)
      }
      for (_ <- 0 to 10) {
        val junkAttrs = createJunkAttrs()
        val rowString = arrayToString(createRow(offset, searchAttrs ++ junkAttrs))
        if (!Pattern.matches(rowRegexp, rowString)) {
          println(attrNameLength)
          println(attrValueLength)
          println(stringToBytes(rowRegexp).bytes.toList)
          println(stringToBytes(rowString).bytes.toList)
        }
        rowString should fullyMatch regex rowRegexp

        val anotherJunkAttrs = createJunkAttrs()
        val anotherRowString = arrayToString(createRow(offset, anotherJunkAttrs ++ junkAttrs))
        anotherRowString should not (fullyMatch regex rowRegexp)
      }
    }
  }

  def bytesKey(bytes: Byte*) = new BytesKey(Array[Byte](bytes: _*))

  def deterministicRandom: Random = {
    new Random(0)
  }

  def randomBytes(nBytes: Int)(implicit random: Random): List[Byte] = {
    val array = new Array[Byte](nBytes)
    random.nextBytes(array)
    array.toList
  }

  def randomAttributes(attrNameLength: Int, attrValueLength: Int)(implicit random: Random) = {
    val randomAttrs =
      for (_ <- 0 to random.nextInt(7))
      yield {
        (randomBytes(attrNameLength), randomBytes(attrValueLength))
      }
    // uniquify attribute names
    randomAttrs.toMap.toList.map {
      case (attrName, attrValue) => (new BytesKey(attrName.toArray), new BytesKey(attrValue.toArray))
    }
  }

  def createRow(prefix: Array[Byte], attrs: List[(BytesKey, BytesKey)]) = {
    val sorted = attrs.sortBy(_._1)
    prefix ++ sorted.map(pair => pair._1.bytes ++ pair._2.bytes).foldLeft(Array[Byte]())(_ ++ _)
  }

  def createRow(offset: Int, attrs: List[(BytesKey, BytesKey)])(implicit random: Random): Array[Byte] =
    createRow(randomBytes(offset).toArray, attrs)

  def createRowString(prefix: Array[Byte] = Array.empty[Byte], attrs: List[(BytesKey, BytesKey)]) =
    bytesToString(createRow(prefix, attrs))

  private def bytesToString(bKey: BytesKey) = arrayToString(bKey.bytes)

  private def arrayToString(array: Array[Byte]) = new String(array, PointsLoaderUtils.ROW_REGEX_FILTER_ENCODING)

  private def stringToBytes(string: String): BytesKey = {
    val charBuffer = CharBuffer.wrap(string)
    new BytesKey(PointsLoaderUtils.ROW_REGEX_FILTER_ENCODING.encode(charBuffer).array())
  }
}
