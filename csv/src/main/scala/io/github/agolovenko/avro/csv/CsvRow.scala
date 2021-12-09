package io.github.agolovenko.avro.csv

class CsvRow(val headers: Array[String], headerMap: Map[String, Int], values: Array[String]) {
  if (headers.length != values.length)
    throw new CsvException(s"Headers size (${headers.length}) not equal to values size (${values.length})")

  def get(key: String): Option[String] = headerMap.get(key).map(values)
  def apply(key: String): String       = get(key).getOrElse(throw new CsvException(s"No key '$key' found among ${headerMap.keys}"))

  override def toString: String =
    headers
      .map { key => s"$key -> ${values(headerMap(key))}" }
      .mkString("(", ", ", ")")
}
