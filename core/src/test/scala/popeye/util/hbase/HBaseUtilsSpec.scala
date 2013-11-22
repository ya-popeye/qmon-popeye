package popeye.util.hbase

import org.scalatest.matchers.{MustMatchers, ShouldMatchers}
import popeye.test.MockitoStubs
import org.scalatest.FlatSpec
import java.util.regex.Pattern
import scala.util.Random
import java.nio.CharBuffer
import scala.Array

class HBaseUtilsSpec extends FlatSpec with ShouldMatchers with MustMatchers with MockitoStubs {

  behavior of "HBaseUtils"

  it should "handle a simple case" in {
    val orderedAttributes: Seq[(Array[Byte], Array[Byte])] = Seq((Array[Byte](0, 0, 1), Array[Byte](0, 0, 1)))
    val regexp = HBaseUtils.createRowRegexp(offset = 7, attrLength = 6, orderedAttributes)
    val pattern = Pattern.compile(regexp)

    val validRow = bytesToString(Array[Byte](0, 0, 2, 76, -45, -71, -128, 0, 0, 1, 0, 0, 1))
    pattern.matcher(validRow).matches() should be(true)

    val invalidRow = bytesToString(Array[Byte](0, 0, 2, 76, -45, -71, -128, 0, 0, 1, 0, 0, 3))
    pattern.matcher(invalidRow).matches() should be(false)
  }

  it should "check attribute length" in {
    val orderedAttributes: Seq[(Array[Byte], Array[Byte])] = Seq((Array[Byte](0), Array[Byte](0)))
    val exception = intercept[IllegalArgumentException] {
      HBaseUtils.createRowRegexp(offset = 7, attrLength = 3, orderedAttributes)
    }
    exception.getMessage should (include("3") and include("2"))
  }

  it should "check that attribute length is greater than zero" in {
    val wrongAttrLength = 0
    val exception = intercept[IllegalArgumentException] {
      HBaseUtils.createRowRegexp(offset = 7, wrongAttrLength, orderedAttributes = Seq((Array[Byte](0), Array[Byte](0))))
    }
    exception.getMessage should include(wrongAttrLength.toString)
  }

  it should "check that attribute list is not empty" in {
    val exception = intercept[IllegalArgumentException] {
      HBaseUtils.createRowRegexp(offset = 7, attrLength = 2, orderedAttributes = Seq())
    }
    exception.getMessage should include("empty")
  }

  it should "escape regex escaping sequences symbols" in {
    val badString = "aaa\\Ebbb"
    val badAttr = (stringToBytes(badString), stringToBytes(badString))
    val badAttrLength = stringToBytes(badString).length * 2
    val regexp = HBaseUtils.createRowRegexp(offset = 0, badAttrLength, Seq(badAttr))
    val row = createRow(prefix = Array[Byte](), List(badAttr))
    val rowString = bytesToString(row)
    rowString should fullyMatch regex (regexp)
  }

  it should "handle newline characters" in {
    val attr = (stringToBytes("attrName"), Array[Byte]())
    val attrLength = attr._1.length
    val regexp = HBaseUtils.createRowRegexp(offset = 1, attrLength, Seq(attr))
    val row = createRow(prefix = stringToBytes("\n"), List(attr))
    val rowString = bytesToString(row)
    rowString should fullyMatch regex (regexp)
  }

  it should "pass randomized test" in {
    implicit val random = new Random(0)
    for (_ <- 0 to 100) {
      val offset = random.nextInt(10)
      val attrNameLength = random.nextInt(5) + 1
      val attrValueLength = random.nextInt(5) + 1
      val attrLength = attrNameLength + attrValueLength
      val searchAttrs = randomAttributes(attrNameLength, attrValueLength)
      val searchAttrsSet = searchAttrs.map {case (n, v) => (n.toList, v.toList)}.toSet
      val regexp = HBaseUtils.createRowRegexp(offset, attrLength, searchAttrs)
      def createJunkAttrs() = randomAttributes(attrNameLength, attrValueLength).filter {
        case (n, v) => !searchAttrsSet((n.toList, v.toList))
      }
      for (_ <- 0 to 10) {
        val junkAttrs = createJunkAttrs()
        val allAttrs = randomListMerge(searchAttrs, junkAttrs)
        val rowString = bytesToString(createRow(offset, allAttrs))
        if (!Pattern.matches(regexp, rowString)) {
          println(HBaseUtils.CHARSET.encode(regexp).array().toList)
          println(HBaseUtils.CHARSET.encode(rowString).array().toList)
        }
        rowString should fullyMatch regex (regexp)

        val anotherJunkAttrs = createJunkAttrs()
        val anotherAllAattrs = randomListMerge(anotherJunkAttrs, junkAttrs)
        val anotherRowString = bytesToString(createRow(offset, anotherAllAattrs))
        anotherRowString should not fullyMatch regex(regexp)
      }
    }
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
    randomAttrs.toList.distinct.map {
      case (attrName, attrValue) => (attrName.toArray, attrValue.toArray)
    }
  }

  def randomListMerge[T](left: List[T], right: List[T])(implicit random: Random): List[T] = (left, right) match {
    case (Nil, xs) => xs
    case (xs, Nil) => xs
    case (x :: xs, y :: ys) =>
      if (random.nextBoolean())
        x :: randomListMerge(xs, y :: ys)
      else
        y :: randomListMerge(x :: xs, ys)
  }

  def createRow(prefix: Array[Byte], attrs: List[(Array[Byte], Array[Byte])]) =
    prefix ++ attrs.map(pair => pair._1 ++ pair._2).foldLeft(Array[Byte]())(_ ++ _)

  def createRow(offset: Int, attrs: List[(Array[Byte], Array[Byte])])(implicit random: Random): Array[Byte] =
    createRow(randomBytes(offset).toArray, attrs)

  private def bytesToString(array: Array[Byte]) = new String(array, HBaseUtils.CHARSET)

  private def stringToBytes(string: String): Array[Byte] = {
    val charBuffer = CharBuffer.wrap(string)
    HBaseUtils.CHARSET.encode(charBuffer).array()
  }
}
