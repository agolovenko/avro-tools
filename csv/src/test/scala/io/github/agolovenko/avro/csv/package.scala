package io.github.agolovenko.avro

import com.univocity.parsers.csv.CsvParserSettings

import java.io.StringReader

package object csv {
  def csvRow(str: String): CsvRow = {
    val settings = new CsvParserSettings()
    settings.setReadInputOnSeparateThread(false)

    CsvIterator(settings, customHeaders = None)(new StringReader(str)).next()
  }
}
