package io.github.agolovenko.avro.csv

import com.univocity.parsers.csv.CsvParserSettings
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets.UTF_8

class CsvIteratorSpec extends AnyWordSpec with Matchers {
  "handles empty input" in {
    val settings = new CsvParserSettings()
    settings.setKeepQuotes(false)
    settings.setDelimiterDetectionEnabled(true, ',', ';')
    settings.setReadInputOnSeparateThread(false)
    settings.setNumberOfRowsToSkip(1L)

    CsvIterator(settings, UTF_8)(new ByteArrayInputStream(Array.empty[Byte])) shouldBe empty
  }
}
