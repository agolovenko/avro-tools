package io.github.agolovenko.avro

import io.github.agolovenko.avro.PathEntry.FieldEntry

case class RenameRule(path: Path, avroName: String)

class RenameRules(rules: RenameRule*) {
  private val renameMap = rules.map { rule =>
    val srcName = rule.path.pop().field
    rule.path.push(FieldEntry(rule.avroName))
    rule.path.mkString(fieldsOnly = true) -> srcName
  }.toMap

  def apply(avroName: String)(implicit path: Path): String =
    if (rules.nonEmpty) {
      path.push(FieldEntry(avroName))
      val pathKey = path.mkString(fieldsOnly = true)
      path.pop()

      val result = renameMap.getOrElse(pathKey, avroName)

      result
    } else avroName

  override def equals(other: Any): Boolean = other match {
    case that: RenameRules => renameMap == that.renameMap
    case _                 => false
  }

  override def hashCode(): Int = renameMap.hashCode()
}

object RenameRules {
  val empty: RenameRules = new RenameRules()
}
