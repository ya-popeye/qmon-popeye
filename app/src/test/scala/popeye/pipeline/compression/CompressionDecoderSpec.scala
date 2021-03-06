package popeye.pipeline.compression

import CompressionDecoder.{Codec, Deflate, Snappy}
import akka.util.ByteString
import com.google.common.io.{Resources, Files, ByteStreams}
import java.io.ByteArrayOutputStream
import java.util
import java.util.Random
import org.scalatest.FlatSpec

/**
 * @author Andrey Stepachev
 */
class CompressionDecoderSpec extends FlatSpec {

  for (codec <- Seq(Deflate(), Snappy())) {

    behavior of codec.toString

    it should "deflate" in {
      val a = ByteString("a")
      val dataBuf = encode(a, codec)

      new CompressionDecoder(100, codec).decode(dataBuf) {
        buffer =>
          assert(buffer == a)
      } match {
        case None =>
        case x => fail(x.toString)
      }
    }

    it should "deflate partial input" in {
      val a = ByteString("ab")
      val dataBuf = encode(a, codec)

      val p1: ByteString = dataBuf.take(3)
      val p2: ByteString = dataBuf.drop(3)
      val d = new CompressionDecoder(100, codec)
      var unpacked: Option[ByteString] = None
      d.decode(p1) {
        buffer =>
          unpacked = Some(buffer)
      } match {
        case None => assert(unpacked.isEmpty)
        case x => fail(x.toString)
      }
      d.decode(p2) {
        buffer =>
          assert(buffer == a)
      } match {
        case None =>
        case x => fail(x.toString)
      }
    }

    it should "be able to parse by chunks packed by java version" in {
      val rnd = new Random(1234)
      val bytes = new Array[Byte](100000)
      rnd.nextBytes(bytes)
      util.Arrays.fill(bytes, 25000, 35000, 'a'.toByte)
      val original: ByteString = ByteString(bytes)
      val file = encode(original, codec)
      val decoder = new CompressionDecoder(file.length, codec)
      val groups = file.grouped(file.length / 2).toSeq
      val result = ByteString.newBuilder

      groups.foreach {
        buf =>
          decoder.decode(buf) {
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

    it should "respect limit" in {
      val a = ByteString("ab")
      val b = ByteString("unencoded")
      val dataBuf = encode(a, codec)
      val d = new CompressionDecoder(dataBuf.length, codec)

      d.decode(dataBuf ++ b) {
        buffer =>
          assert(buffer === a)
      } match {
        case Some(r) => assert(r === b, r.utf8String)
        case x => fail(x.toString)
      }
    }

    it should "be able to parse generated by python version" in {
      val (file, decoded) = loadFile(s"${codec.getClass.getSimpleName}.bin", codec)
      val decoder = new CompressionDecoder(file.length, codec)
      val resultBuilder = ByteString.newBuilder

      decoder.decode(file) {
        buffer =>
          resultBuilder.append(buffer)
      } match {
        case None =>
        case Some(x) => fail(x.toString())
      }
      assert(decoder.isClosed)
      val result = resultBuilder.result()
      assert(decoded.length === result.length)
      assert(decoded.utf8String === result.utf8String)
      val resultString: String = result.utf8String
      assert(resultString.contains("Bart Simpson"))
    }

  }


  def encode(s: ByteString, codec: Codec): ByteString = {
    val data: ByteArrayOutputStream = new ByteArrayOutputStream()
    val deflater = codec.makeOutputStream(data)
    deflater.write(s.toArray)
    deflater.close()
    ByteString.fromArray(data.toByteArray)
  }


  def loadFile(name: String, codec: Codec): (ByteString, ByteString) = {
    val fileUrl = Resources.getResource(this.getClass, name)
    val file = Resources.newInputStreamSupplier(fileUrl);

    val decoded = ByteString.fromArray(
      ByteStreams.toByteArray(
        codec.makeInputStream(file.getInput)))
    val fileContent = ByteString.apply(Resources.toByteArray(fileUrl))
    (fileContent, decoded)
  }

}
