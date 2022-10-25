package io.github.agolovenko.avro.csv

import com.univocity.parsers.csv.CsvParserSettings
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.StringReader

class CsvIteratorSpec extends AnyWordSpec with Matchers {
  "handles empty input" in {
    val settings = new CsvParserSettings()
    settings.setReadInputOnSeparateThread(false)

    CsvIterator(settings, customHeaders = None)(new StringReader("")) shouldBe empty
  }

  "parses all the rows" in {
    val input = """
        |rstring,rint,rlong,rfloat,rdouble
        |qwerty,123,123456789012345667,123.45,12345.12345
        |ytrewq,321,333,,92345.1
        |""".stripMargin

    val settings = new CsvParserSettings()
    settings.setReadInputOnSeparateThread(false)

    val expected = Seq(
      CsvRow("rstring" -> "qwerty", "rint" -> "123", "rlong" -> "123456789012345667", "rfloat" -> "123.45", "rdouble" -> "12345.12345"),
      CsvRow("rstring" -> "ytrewq", "rint" -> "321", "rlong" -> "333", "rfloat"                -> null, "rdouble"     -> "92345.1")
    )

    CsvIterator(settings, customHeaders = None)(new StringReader(input)).toSeq should contain theSameElementsAs expected
  }
}
