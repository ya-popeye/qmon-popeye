package popeye.transport.legacy

import org.scalatest.FlatSpec
import akka.util.ByteString
import java.util.zip.{Deflater, DeflaterOutputStream}
import java.io.{File, ByteArrayOutputStream}
import popeye.transport.CompressionDecoder
import com.google.common.io.Files
import java.util.Random

/**
 * @author Andrey Stepachev
 */
class CompressionDecoderTestSpec extends FlatSpec {

  "Deflater" should "deflate" in {
    val a = ByteString("a")
    val dataBuf = encode(a)

    new CompressionDecoder(100).decode(dataBuf){
      buffer =>
        assert(buffer == a)
    } match {
      case None =>
      case x => fail(x.toString)
    }
  }

  "Deflater" should "deflate partial input" in {
    val a = ByteString("ab")
    val dataBuf = encode(a)

    val p1: ByteString = dataBuf.take(3)
    val p2: ByteString = dataBuf.drop(3)
    val d = new CompressionDecoder(100)
    var unpacked: Option[ByteString] = None
    d.decode(p1){
      buffer =>
        unpacked = Some(buffer)
    } match {
      case None => assert(unpacked.isEmpty)
      case x => fail(x.toString)
    }
    d.decode(p2){
      buffer =>
        assert(buffer == a)
    } match {
      case None =>
      case x => fail(x.toString)
    }
  }

  val rnd = new Random(1234)

  "Deflater" should "be able to parse by chunks packed by java inflater" in {
    val bytes = new Array[Byte](100000)
    rnd.nextBytes(bytes)
    val original: ByteString = ByteString(bytes)
    val file = encode(original)
    val decoder = new CompressionDecoder(file.length)
    val groups = file.grouped(file.length / 2).toSeq
    val result = ByteString.newBuilder

    groups.foreach {
      buf =>
        decoder.decode(buf){
          buffer =>
            result.append(buffer)
        } match {
          case None =>
          case Some(x) => fail(x.toString)
        }
    }
    assert(decoder.isClosed)
    assert(result.result() == original)
  }

  "Deflater" should "respect limit" in {
    val a = ByteString("ab")
    val b = ByteString("unencoded")
    val dataBuf = encode(a)
    val d = new CompressionDecoder(dataBuf.length)

    d.decode(dataBuf ++ b){
      buffer =>
        assert(buffer == a)
    } match {
      case Some(r) => assert(r == b, r.utf8String)
      case x => fail(x.toString)
    }
  }

  "Deflater" should "should parse sequence of inputs" in {
    val a = ByteString("a")
    val b = ByteString("b")
    val c = ByteString("unencoded")
    val dataBuf = Seq(encode(a), encode(b))
    val d = new CompressionDecoder(dataBuf.foldLeft(0)((s, b) => s+b.length))

    d.decode(dataBuf :+ c){
      buffer =>
        assert(buffer == a)
    } match {
      case Some(r) => assert(r == c, r.utf8String)
      case x => fail(x.toString)
    }
  }

  "Deflater" should "be able to parse by chunks" in {
    val file = loadFile("gen.bin")
    val decoder = new CompressionDecoder(file.length)
    val result = ByteString.newBuilder

    decoder.decode(file){
      buffer =>
        result.append(buffer)
    } match {
      case None =>
      case Some(x) => fail(x.toString())
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
