package io.github.agolovenko.avro.csv

trait CsvData {
  private[csv] def headers: Map[String, Int]
  def isEmpty: Boolean = headers.isEmpty

  def get(key: String): Option[String]

  def apply(key: String): String = get(key).getOrElse(throw new CsvException(s"No key '$key' found: $this"))

  override def toString: String =
    headers.toSeq
      .sortBy(_._2)
      .map { case (key, _) => s"$key -> ${apply(key)}" }
      .mkString("(", ", ", ")")
}

class CsvRow(headerMap: Map[String, Int], values: Array[String]) extends CsvData {
  if (headers.size != values.length)
    throw new CsvException(s"Headers size (${headers.size}) not equal to values size (${values.length})")

  override private[csv] def headers: Map[String, Int] = headerMap
  override def get(key: String): Option[String]       = headers.get(key).map(values)
}

private[csv] class PrefixFilteringCsvData(data: CsvData, prefix: String) extends CsvData {
  override private[csv] def headers: Map[String, Int] = data.headers.collect {
    case (key, idx) if key.startsWith(prefix) => key.stripPrefix(prefix) -> idx
  }
  override def get(key: String): Option[String] = data.get(s"$prefix$key")
}
