package popeye.pipeline.compression

import akka.util.ByteString

/**
 * @author Andrey Stepachev
 */
trait Decoder {
   protected var pushedBack: Option[ByteString] = None

   /**
    * Push back data for reread later.
    * Buffer will be compacted (to avoid reference to buffer)
    * @param what
    */
   def pushBack(what: ByteString) = {
     pushedBack match {
       case Some(b) => pushedBack = Some((b ++ what).compact)
       case None => pushedBack = Some(what.compact)
     }
   }

   def decompress[U](input: ByteString, f: (ByteString) => U): Unit

   def close(): Unit
 }
