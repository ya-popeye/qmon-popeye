package popeye.pipeline.compression

import java.io.{DataInputStream, InputStream}
import akka.util.ByteString

/**
 * @author Andrey Stepachev
 */
class SnappyStreamInputStream(inner: DataInputStream) extends InputStream {

  import SnappyDecoder._

  private[this] val decoder = new SnappyDecoder()
   private[this] val buffer = new Array[Byte](chunkMax + decoder.headerSize)
   private[this] var consumed = 0
   private[this] var cached = ByteString.empty

   def nextBlock(): Int = {
     if (inner.available() == 0)
       return -1
     val headerSize = decoder.headerSize
     inner.readFully(buffer, 0, headerSize)
     val chunkSize = inner.read(buffer, headerSize, buffer.length - headerSize)
     consumed = 0
     decoder.decompress(ByteString.fromArray(buffer, 0, chunkSize + headerSize), {
       buf =>
         cached ++= buf
     })
     cached.length
   }

   def read(): Int = {
     if (consumed >= cached.length && nextBlock() == -1)
       return -1
     val off = consumed
     consumed += 1
     cached(off)
   }

   override def read(b: Array[Byte], off: Int, len: Int): Int = {
     if (consumed >= cached.length && nextBlock() == -1)
       return -1
     val forConsume = Math.min(len, cached.length - consumed)
     cached.copyToArray(b, off, forConsume)
     consumed += forConsume
     forConsume
   }
 }
