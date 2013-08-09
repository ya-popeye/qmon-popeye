package popeye.transport.legacy

import org.scalatest.FlatSpec
import akka.util.ByteString
import java.util.zip.{Deflater, DeflaterOutputStream}
import java.io.{File, ByteArrayOutputStream}
import popeye.transport.Deflate
import com.google.common.io.Files
import java.util.Random

/**
 * @author Andrey Stepachev
 */
class DeflaterTestSpec extends FlatSpec {

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

  val rnd = new Random(1234)

  "Deflater" should "be able to parse by chunks packed by java inflater" in {
    val bytes = new Array[Byte](100000)
    rnd.nextBytes(bytes)
    val original: ByteString = ByteString(bytes)
    val file = encode(original)
    val decoder = new Deflate(file.length)
    val groups = file.grouped(file.length / 2).toSeq
    val result = ByteString.newBuilder

    groups.foreach {
      buf =>
        decoder.decode(buf) match {
          case (t, None) =>
            t.foreach {
              buffer =>
                result.append(buffer)
            }
          case (t, Some(x)) => fail(x.toString() + s"${x.length}")
        }
    }
    assert(decoder.isClosed)
    assert(result.result() == original)
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

  "Deflater" should "be able to parse by chunks" in {
    val file = loadFile("gen.bin")
    val decoder = new Deflate(file.length)
    val result = ByteString.newBuilder

    decoder.decode(file) match {
      case (t, None) =>
        t.foreach {
          buffer =>
            result.append(buffer)
        }
      case (t, Some(x)) => fail(x.toString() + s"${x.length}")
    }
    assert(decoder.isClosed)
    assert(result.result().utf8String.contains("Bart Simpson"))
  }

  def encode(s: ByteString): ByteString = {
    val data: ByteArrayOutputStream = new ByteArrayOutputStream()
    val deflater = new DeflaterOutputStream(data, new Deflater(Deflater.BEST_COMPRESSION, false))
    deflater.write(s.toArray)
    deflater.close()
    ByteString.fromArray(data.toByteArray)
  }


  def loadFile(name: String): ByteString = {
    val file = new File(this.getClass.getResource(name).toURI)
    val builder = ByteString.newBuilder
    Files.copy(file, builder.asOutputStream)
    builder.result()
  }

}
