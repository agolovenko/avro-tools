package io.github.agolovenko.avro

sealed trait PathEntry {
  def isField: Boolean
  def field: String = throw new IllegalArgumentException(s"Not a field: ${getClass.getName}")
}

object PathEntry {
  case class FieldEntry(override val field: String) extends PathEntry {
    override def isField: Boolean = true
    override def toString: String = field
  }

  case class ArrayEntry(idx: Int) extends PathEntry {
    override def isField: Boolean = false
    override def toString: String = s"[$idx]"
  }

  case class MapEntry(key: String) extends PathEntry {
    override def isField: Boolean = false
    override def toString: String = s"[$key]"
  }
}
