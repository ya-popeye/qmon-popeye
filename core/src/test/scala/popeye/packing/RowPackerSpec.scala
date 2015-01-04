package popeye.packing

import org.apache.hadoop.hbase.{CellUtil, KeyValue}
import org.apache.hadoop.hbase.util.Bytes
import org.scalatest.{Matchers, FlatSpec}
import popeye.paking.RowPacker.RowPackerException
import popeye.paking.{ValueTypeDescriptor, RowPacker}

class RowPackerSpec extends FlatSpec with Matchers {

  class FixedValueLength(valueLength: Int) extends ValueTypeDescriptor {
    override def getValueLength(qualifierArray: Array[Byte],
                                qualifierOffset: Int,
                                qualifierLength: Int): Int = valueLength
  }

  object QualShortValueLength extends ValueTypeDescriptor {
    override def getValueLength(qualifierArray: Array[Byte],
                                qualifierOffset: Int,
                                qualifierLength: Int): Int = {
      Bytes.toShort(qualifierArray, qualifierOffset, qualifierLength)
    }
  }

  behavior of "RowPacker"

  it should "handle a trivial case" in {
    val row = Array[Byte](0, 0, 0, 0)
    val family = Array[Byte](0, 0, 0, 0)
    val keyValues = Seq(
      new KeyValue(row, family, Array[Byte](0, 0), Array[Byte](1, 1)),
      new KeyValue(row, family, Array[Byte](1, 1), Array[Byte](2, 2)),
      new KeyValue(row, family, Array[Byte](2, 2), Array[Byte](3, 3))
    )
    val rowPacker = new RowPacker(qualifierLength = 2, valueTypeDescriptor = new FixedValueLength(2))
    val packedRow = rowPacker.packRow(keyValues)
    val qual = CellUtil.cloneQualifier(packedRow)
    val value = CellUtil.cloneValue(packedRow)
    qual should equal(Array[Byte](0, 0, 1, 1, 2, 2))
    value should equal(Array[Byte](1, 1, 2, 2, 3, 3))
  }

  it should "handle a non trivial case" in {
    val row = Array[Byte](0, 0, 0, 0)
    val family = Array[Byte](0, 0, 0, 0)
    val keyValues = Seq(
      new KeyValue(row, family, Array[Byte](2, 2), Array[Byte](3, 3)),
      new KeyValue(row, family, Array[Byte](0, 0, 1, 1), Array[Byte](1, 1, 2, 2))
    )
    val rowPacker = new RowPacker(qualifierLength = 2, valueTypeDescriptor = new FixedValueLength(2))
    val packedRow = rowPacker.packRow(keyValues)
    val qual = CellUtil.cloneQualifier(packedRow)
    val value = CellUtil.cloneValue(packedRow)
    qual should equal(Array[Byte](0, 0, 1, 1, 2, 2))
    value should equal(Array[Byte](1, 1, 2, 2, 3, 3))
  }

  it should "handle a non trivial case variable value sizes" in {
    val row = Array[Byte](0, 0, 0, 0)
    val family = Array[Byte](0, 0, 0, 0)
    val keyValues = Seq(
      new KeyValue(row, family, Array[Byte](0, 1), Array[Byte](1)),
      new KeyValue(row, family, Array[Byte](0, 2, 0, 3), Array[Byte](2, 2, 3, 3, 3))
    )
    val rowPacker = new RowPacker(qualifierLength = 2, valueTypeDescriptor = QualShortValueLength)
    val packedRow = rowPacker.packRow(keyValues)
    val qual = CellUtil.cloneQualifier(packedRow)
    val value = CellUtil.cloneValue(packedRow)
    qual should equal(Array[Byte](0, 1, 0, 2, 0, 3))
    value should equal(Array[Byte](1, 2, 2, 3, 3, 3))
  }

  it should "handle a non trivial case with duplicate qualifiers" in {
    val row = Array[Byte](0, 0, 0, 0)
    val family = Array[Byte](0, 0, 0, 0)
    val oldTimestamp = 1000
    val latestTimestamp = 1001
    val keyValues = Seq(
      new KeyValue(row, family, Array[Byte](0, 0, 1, 1), oldTimestamp, Array[Byte](1, 1, 2, 2)),
      new KeyValue(row, family, Array[Byte](1, 1), latestTimestamp, Array[Byte](3, 3))
    )
    val rowPacker = new RowPacker(qualifierLength = 2, valueTypeDescriptor = new FixedValueLength(2))
    val packedRow = rowPacker.packRow(keyValues)
    val qual = CellUtil.cloneQualifier(packedRow)
    val value = CellUtil.cloneValue(packedRow)
    qual should equal(Array[Byte](0, 0, 1, 1))
    value should equal(Array[Byte](1, 1, 3, 3))
  }

  it should "throw an exception on bad qualifier size" in {
    val row = Array[Byte](0, 0, 0, 0)
    val family = Array[Byte](0, 0, 0, 0)
    val keyValues = Seq(
      new KeyValue(row, family, Array[Byte](2, 2), Array[Byte](3, 3)),
      new KeyValue(row, family, Array[Byte](0, 0, 1), Array[Byte](1, 1, 2, 2))
    )
    val rowPacker = new RowPacker(qualifierLength = 2, valueTypeDescriptor = new FixedValueLength(2))
    intercept[RowPackerException] {
      rowPacker.packRow(keyValues)
    }
  }

  it should "throw an exception on bad value size" in {
    val row = Array[Byte](0, 0, 0, 0)
    val family = Array[Byte](0, 0, 0, 0)
    val keyValues = Seq(
      new KeyValue(row, family, Array[Byte](2, 2), Array[Byte](3, 3)),
      new KeyValue(row, family, Array[Byte](0, 0, 1, 1), Array[Byte](1, 1, 2))
    )
    val rowPacker = new RowPacker(qualifierLength = 2, valueTypeDescriptor = new FixedValueLength(2))
    intercept[RowPackerException] {
      rowPacker.packRow(keyValues)
    }
  }

  it should "throw an exception if rows are not the same" in {
    val row = Array[Byte](0, 0, 0, 0)
    val family = Array[Byte](0, 0, 0, 0)
    val keyValues = Seq(
      new KeyValue(row, family, Array[Byte](2, 2), Array[Byte](3, 3)),
      new KeyValue(row ++ Array[Byte](0), family, Array[Byte](0, 0), Array[Byte](1, 1))
    )
    val rowPacker = new RowPacker(qualifierLength = 2, valueTypeDescriptor = new FixedValueLength(2))
    intercept[RowPackerException] {
      rowPacker.packRow(keyValues)
    }
  }

  it should "throw an exception if families are not the same" in {
    val row = Array[Byte](0, 0, 0, 0)
    val family = Array[Byte](0, 0, 0, 0)
    val keyValues = Seq(
      new KeyValue(row, family, Array[Byte](2, 2), Array[Byte](3, 3)),
      new KeyValue(row, family ++ Array[Byte](0), Array[Byte](0, 0), Array[Byte](1, 1))
    )
    val rowPacker = new RowPacker(qualifierLength = 2, valueTypeDescriptor = new FixedValueLength(2))
    intercept[RowPackerException] {
      rowPacker.packRow(keyValues)
    }
  }
}
