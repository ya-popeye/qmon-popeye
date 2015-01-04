package popeye.paking

import java.io.ByteArrayOutputStream

import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.KeyValue
import popeye.paking.RowPacker.{RowPackerException, QualifierAndValueOrdering, QualifierAndValue}
import popeye.util.CollectionsUtils


trait ValueTypeDescriptor {
  def getValueLength(qualifierArray: Array[Byte], qualifierOffset: Int, qualifierLength: Int): Int
}

object RowPacker {

  class RowPackerException(message: String) extends RuntimeException(message)

  object QualifierAndValue {
    def areQualifiersEqual(left: QualifierAndValue, right: QualifierAndValue) = {
      Bytes.equals(
        left.qualifierArray, left.qualifierOffset, left.qualifierLength,
        right.qualifierArray, right.qualifierOffset, right.qualifierLength)
    }
  }

  case class QualifierAndValue(qualifierArray: Array[Byte], qualifierOffset: Int, qualifierLength: Int,
                               valueArray: Array[Byte], valueOffset: Int, valueLength: Int,
                               timestamp: Long)

  object QualifierAndValueOrdering extends Ordering[QualifierAndValue] {
    override def compare(left: QualifierAndValue, right: QualifierAndValue): Int = {
      val qualComparison = Bytes.compareTo(
        left.qualifierArray, left.qualifierOffset, left.qualifierLength,
        right.qualifierArray, right.qualifierOffset, right.qualifierLength
      )
      if (qualComparison != 0) {
        qualComparison
      } else {
        // latest timestamp should be ordered first
        val timestampComparison = java.lang.Long.compare(left.timestamp, right.timestamp)
        -timestampComparison
      }
    }
  }

}

class RowPacker(qualifierLength: Int, valueTypeDescriptor: ValueTypeDescriptor) {
  def packRow(keyValues: Seq[KeyValue]): KeyValue = {
    if (keyValues.size == 1) {
      return keyValues.head
    }
    val qualBuffer = new ByteArrayOutputStream(countQualSizes(keyValues))
    val valueBuffer = new ByteArrayOutputStream(countValueSizes(keyValues))

    val qualifierAndValues = unpackRow(keyValues)

    for (qualAndValue <- qualifierAndValues) {
      qualBuffer.write(qualAndValue.qualifierArray, qualAndValue.qualifierOffset, qualAndValue.qualifierLength)
      valueBuffer.write(qualAndValue.valueArray, qualAndValue.valueOffset, qualAndValue.valueLength)
    }
    val qualArray = qualBuffer.toByteArray
    val valArray = valueBuffer.toByteArray

    val firstKeyValue = keyValues.head
    new KeyValue(
      firstKeyValue.getRowArray, firstKeyValue.getRowOffset, firstKeyValue.getRowLength,
      firstKeyValue.getFamilyArray, firstKeyValue.getFamilyOffset, firstKeyValue.getFamilyLength,
      qualArray, 0, qualArray.length,
      getMaxTimestamp(keyValues),
      KeyValue.Type.Put,
      valArray, 0, valArray.length)
  }

  def unpackRow(keyValues: Seq[KeyValue]): Iterable[QualifierAndValue] = {
    if (keyValues.size == 1) {
      return unpackKeyValue(keyValues.head)
    }
    checkEqualRowsAndFamilies(keyValues)
    val qualifierAndValues = keyValues.view.flatMap(unpackKeyValue).sorted(QualifierAndValueOrdering)
    CollectionsUtils.uniqIterable(
      qualifierAndValues,
      QualifierAndValue.areQualifiersEqual
    )
  }

  private def checkEqualRowsAndFamilies(keyValues: Seq[KeyValue]) = {
    val firstKeyValue = keyValues.head
    for (keyValue <- keyValues) {
      val rowsAreEqual = Bytes.equals(
        firstKeyValue.getRowArray, firstKeyValue.getRowOffset, firstKeyValue.getRowLength,
        keyValue.getRowArray, keyValue.getRowOffset, keyValue.getRowLength
      )
      if (!rowsAreEqual) {
        val firstRow = Bytes.toStringBinary(
          firstKeyValue.getRowArray,
          firstKeyValue.getRowOffset,
          firstKeyValue.getRowLength
        )
        val row = Bytes.toStringBinary(
          keyValue.getRowArray,
          keyValue.getRowOffset,
          keyValue.getRowLength
        )
        throw new RowPackerException(f"rows are not the same: $firstRow != $row")
      } else {
        val familiesAreEqual = Bytes.equals(
          firstKeyValue.getFamilyArray, firstKeyValue.getFamilyOffset, firstKeyValue.getFamilyLength,
          keyValue.getFamilyArray, keyValue.getFamilyOffset, keyValue.getFamilyLength
        )
        if (!familiesAreEqual) {
          val firstFamily = Bytes.toStringBinary(
            firstKeyValue.getFamilyArray,
            firstKeyValue.getFamilyOffset,
            firstKeyValue.getFamilyLength
          )
          val family = Bytes.toStringBinary(
            keyValue.getFamilyArray,
            keyValue.getFamilyOffset,
            keyValue.getFamilyLength
          )
          throw new RowPackerException(f"families are not the same: $firstFamily != $family")
        }
      }
    }
  }

  private def unpackKeyValue(keyValue: KeyValue): Iterable[QualifierAndValue] = {
    if (keyValue.getQualifierLength % qualifierLength != 0) {
      val qual = Bytes.toStringBinary(
        keyValue.getQualifierArray,
        keyValue.getQualifierOffset,
        keyValue.getQualifierLength
      )
      throw new RowPackerException(f"bad qualifier size: $qual")
    }
    val qualCount = keyValue.getQualifierLength / qualifierLength
    val qualOffsets = (0 until qualCount).map {
      i =>
        keyValue.getQualifierOffset + i * qualifierLength
    }
    val maxValueByteIndex = keyValue.getValueOffset + keyValue.getValueLength
    var nextValueOffset = keyValue.getValueOffset
    for (qualOffset <- qualOffsets) yield {
      val valueLength = valueTypeDescriptor.getValueLength(keyValue.getQualifierArray, qualOffset, qualifierLength)
      val valueOffset = nextValueOffset
      nextValueOffset += valueLength
      if (valueOffset + valueLength > maxValueByteIndex) {
        throw meaningfulException("bad value size", keyValue)
      }
      QualifierAndValue(
        keyValue.getQualifierArray, qualOffset, qualifierLength,
        keyValue.getValueArray, valueOffset, valueLength,
        keyValue.getTimestamp
      )
    }
  }

  private def meaningfulException(message: String, keyValue: KeyValue) = {
    val qualCount = keyValue.getQualifierLength / qualifierLength
    val qualOffsets = (0 until qualCount).map {
      i =>
        keyValue.getQualifierOffset + i * qualifierLength
    }
    val qualifiers = qualOffsets.map {
      off => Bytes.toStringBinary(keyValue.getQualifierArray, off, qualifierLength)
    }
    val value = Bytes.toStringBinary(keyValue.getValueArray, keyValue.getValueOffset, keyValue.getValueLength)
    val valueSizes = qualOffsets.map {
      off => valueTypeDescriptor.getValueLength(keyValue.getQualifierArray, off, qualifierLength)
    }
    val fullMessage = f"$message; qualifiers: $qualifiers, value: $value, value sizes: $valueSizes"
    new RowPackerException(fullMessage)
  }

  private def countQualSizes(keyValues: Seq[KeyValue]): Int = {
    var sum = 0
    for (keyValue <- keyValues) {
      sum += keyValue.getQualifierLength
    }
    sum
  }

  private def countValueSizes(keyValues: Seq[KeyValue]): Int = {
    var sum = 0
    for (keyValue <- keyValues) {
      sum += keyValue.getValueLength
    }
    sum
  }

  private def getMaxTimestamp(keyValues: Seq[KeyValue]): Long = {
    var max = 0l
    for (keyValue <- keyValues) {
      max = math.max(max, keyValue.getTimestamp)
    }
    max
  }
}
