package io.github.agolovenko.avro

case class RenameRule(path: Path, avroName: String)

class FieldRenamings(rules: RenameRule*) {
  private val renamingsMap = rules.map { rule =>
    val srcName = rule.path.pop()
    rule.path.push(rule.avroName)
    rule.path.mkString(withArrayIdx = false) -> srcName
  }.toMap

  def apply(avroName: String)(implicit path: Path): String =
    if (rules.nonEmpty) {
      path.push(avroName)
      val pathKey = path.mkString(withArrayIdx = false)
      path.pop()

      val result = renamingsMap.getOrElse(pathKey, avroName)

      result
    } else avroName
}
