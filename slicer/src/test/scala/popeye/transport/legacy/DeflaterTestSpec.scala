package popeye.transport.legacy

import org.scalatest.FlatSpec
import akka.util.ByteString
import java.util.zip.DeflaterOutputStream
import java.io.ByteArrayOutputStream
import popeye.transport.Deflate

/**
 * @author Andrey Stepachev
 */
class DeflaterTestSpec extends FlatSpec {

  def encode(s: ByteString): ByteString = {
    val data: ByteArrayOutputStream = new ByteArrayOutputStream()
    val deflater = new DeflaterOutputStream(data)
    deflater.write(s.toArray)
    deflater.close()
    ByteString.fromArray(data.toByteArray)
  }

  "Deflater" should "deflate" in {
    val a = ByteString("a")
    val dataBuf = encode(a)

    new Deflate(100).decode(dataBuf) match {
      case (t, None) =>
        t.foreach {
          buffer =>
            assert(buffer == a)
        }
      case x => fail(x.toString())
    }
  }

  "Deflater" should "deflate partial input" in {
    val a = ByteString("ab")
    val dataBuf = encode(a)

    val p1: ByteString = dataBuf.take(3)
    val p2: ByteString = dataBuf.drop(3)
    val d = new Deflate(100)
    d.decode(p1) match {
      case (t, None) =>
        var unpacked: Option[ByteString] = None
        t.foreach {
          buffer =>
            unpacked = Some(buffer)
        }
        assert(unpacked.isEmpty)
      case x => fail(x.toString())
    }
    d.decode(p2) match {
      case (t, None) =>
        t.foreach {
          buffer =>
            assert(buffer == a)
        }
      case x => fail(x.toString())
    }
  }

  "Deflater" should "respect limit" in {
    val a = ByteString("ab")
    val b = ByteString("unencoded")
    val dataBuf = encode(a)
    val d = new Deflate(dataBuf.length)

    d.decode(dataBuf ++ b) match {
      case (t, Some(r)) =>
        t.foreach {
          buffer =>
            assert(buffer == a)
        }
        assert(r == b)
      case x => fail(x.toString())
    }

  }
}
