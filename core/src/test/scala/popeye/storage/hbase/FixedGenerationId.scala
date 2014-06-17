package popeye.storage.hbase

class FixedGenerationId(id: Short) extends GenerationIdMapping {
  override def getGenerationId(timestampInSeconds: Int, currentTimeInSeconds: Int): Short = id

  override def backwardIterator(timestampInSeconds: Int): Iterator[TimeRangeAndId] = {
    Iterator(TimeRangeAndId(Int.MinValue, Int.MaxValue, id))
  }
}

