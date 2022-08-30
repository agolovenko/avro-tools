package io.github.agolovenko.avro.csv

trait CsvData {
  private[csv] def headers: Map[String, Int]
  def isEmpty: Boolean = headers.isEmpty

  def get(key: String): Option[String]

  def apply(key: String): String = get(key).getOrElse(throw new CsvException(s"No key '$key' found: $this"))

  private def values: Seq[(String, String)] = headers.toSeq.sortBy(_._2).map { case (key, _) => key -> apply(key) }

  override def toString: String = values.map { case (key, value) => s"$key -> $value" }.mkString("(", ", ", ")")

  override def hashCode: Int = values.hashCode()

  override def equals(obj: Any): Boolean = obj match {
    case other: CsvData => headers == other.headers && headers.forall { case (key, _) => this(key) == other(key) }
    case _              => false
  }
}

class CsvRow private[csv] (headerMap: Map[String, Int], values: Array[String]) extends CsvData {
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

object CsvRow {
  def apply(values: (String, String)*): CsvRow = {
    val headerMap  = values.map(_._1).zipWithIndex.toMap
    val valueArray = values.map(_._2).toArray

    new CsvRow(headerMap, valueArray)
  }
}
