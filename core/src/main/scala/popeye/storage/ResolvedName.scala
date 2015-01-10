package popeye.storage

import popeye.storage.hbase.BytesKey

object ResolvedName {
  def apply(qname: QualifiedName, id: BytesKey) = new ResolvedName(qname.kind, qname.generationId, qname.name, id)

  def apply(qid: QualifiedId, name: String) = new ResolvedName(qid.kind, qid.generationId, name, qid.id)
}

sealed case class ResolvedName(kind: String, generationId: BytesKey, name: String, id: BytesKey) {
  def this(qname: QualifiedName, id: BytesKey) = this(qname.kind, qname.generationId, qname.name, id)

  def this(qid: QualifiedId, name: String) = this(qid.kind, qid.generationId, name, qid.id)

  def toQualifiedName = QualifiedName(kind, generationId, name)

  def toQualifiedId = QualifiedId(kind, generationId, id)
}

sealed case class QualifiedName(kind: String, generationId: BytesKey, name: String)

sealed case class QualifiedId(kind: String, generationId: BytesKey, id: BytesKey)