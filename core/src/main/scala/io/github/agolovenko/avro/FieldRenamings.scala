package io.github.agolovenko.avro

class FieldRenamings(renamings: Map[Path, String]) {
  private val renamingsMap = renamings.map {
    case (path, avroName) =>
      val srcName = path.pop()
      path.push(avroName)
      path.mkString(withArrayIdx = false) -> srcName
  }

  def apply(fieldName: String)(implicit path: Path): String =
    if (renamings.nonEmpty) {
      path.push(fieldName)
      val pathKey = path.mkString(withArrayIdx = false)
      path.pop()

      val result = renamingsMap.getOrElse(pathKey, fieldName)

      result
    } else fieldName
}
