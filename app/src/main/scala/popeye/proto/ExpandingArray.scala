package popeye.proto

class ExpandingArray[Elem](extentSize: Int)
                          (implicit evidence$1: scala.reflect.ClassTag[Elem]) {
  private var elems = new Array[Elem](extentSize)
  private var consumed: Int = 0
  private var filled: Int = 0

  def consume(bytes: Int): Unit = {
    if (bytes < 0 || bytes > filled - consumed)
      throw new IndexOutOfBoundsException
    if (filled == bytes)
      init()
    else
      consumed += bytes
  }

  private def init() {
    consumed = 0
    filled = 0
    elems = new Array[Elem](extentSize)
  }

  @inline
  def add(another: ExpandingArray[Elem]): Unit = add(another, another.consumed, another.length)

  def add(another: ExpandingArray[Elem], start: Int, amount: Int): Unit = {
    if (amount < 0 || amount > another.filled - another.consumed || start > another.filled)
      throw new IndexOutOfBoundsException
    add(another.elems, start, amount)
  }

  def add(elem: Elem): Unit = {
    ensureSize(length + 1)
    elems(filled) = elem
    filled += 1
  }

  def add(elemArr: Array[Elem], offset: Int, len: Int): Unit = {
    ensureSize(length + len)
    System.arraycopy(elemArr, offset, elems, filled, len)
    filled += len
  }

  def apply(idx: Int) = {
    elems(consumed + idx)
  }

  def offset = consumed

  def length = {
    filled - consumed
  }

  def buffer = {
    elems
  }

  protected def ensureSize(newSize: Int) {
    if (newSize > elems.length - consumed) {
      val sz = (newSize / extentSize + 1) * extentSize
      val oldElems = elems
      val oldLen = length
      elems = new Array[Elem](sz)
      if (oldLen > 0)
        System.arraycopy(oldElems, consumed, elems, 0, oldLen)
      consumed = 0
      filled = oldLen
    }
  }
}
