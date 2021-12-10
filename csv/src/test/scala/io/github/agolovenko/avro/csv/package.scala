package io.github.agolovenko.avro

import com.univocity.parsers.csv.CsvParserSettings

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets.UTF_8

package object csv {
  def csvRow(str: String): CsvRow = {
    val settings = new CsvParserSettings()
    settings.setReadInputOnSeparateThread(false)

    CsvIterator(settings, UTF_8)(new ByteArrayInputStream(str.getBytes(UTF_8))).next()
  }
}
