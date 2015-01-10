package popeye.storage

import popeye.storage.hbase.BytesKey

sealed trait ValueIdFilterCondition {
  def isGroupByAttribute: Boolean
}

object ValueIdFilterCondition {

  case class SingleValueId(id: BytesKey) extends ValueIdFilterCondition {
    def isGroupByAttribute: Boolean = false
  }

  case class MultipleValueIds(ids: Seq[BytesKey]) extends ValueIdFilterCondition {
    require(ids.size > 1, "must be more than one value id")

    def isGroupByAttribute: Boolean = true
  }

  case object AllValueIds extends ValueIdFilterCondition {
    def isGroupByAttribute: Boolean = true
  }

}

sealed trait ValueNameFilterCondition {
  def isGroupByAttribute: Boolean
}

object ValueNameFilterCondition {

  case class SingleValueName(name: String) extends ValueNameFilterCondition {
    def isGroupByAttribute: Boolean = false
  }

  case class MultipleValueNames(names: Seq[String]) extends ValueNameFilterCondition {
    require(names.size > 1, "must be more than one value name")

    def isGroupByAttribute: Boolean = true
  }

  case object AllValueNames extends ValueNameFilterCondition {
    def isGroupByAttribute: Boolean = true
  }

}
