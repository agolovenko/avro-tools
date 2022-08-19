package io.github.agolovenko.avro.csv

import io.github.agolovenko.avro.StringParsers
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.GenericData
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class RecordSpec extends AnyWordSpec with Matchers {
  "parses record with various types" in {
    val csv = csvRow("""
        |rstring,rint,rlong,rfloat,rdouble,rboolean,rdate
        |qwerty,123,123456789012345667,123.45,12345.12345,true,2021-11-23
        |""".stripMargin)

    val schema = new Parser().parse("""
        |{
        |  "type": "record",
        |  "name": "r1",
        |  "fields": [
        |    {
        |      "name": "rstring",
        |      "type": "string"
        |    },
        |    {
        |      "name": "rint",
        |      "type": "int"
        |    },
        |    {
        |      "name": "rlong",
        |      "type": "long"
        |    },
        |    {
        |      "name": "rfloat",
        |      "type": "float"
        |    },
        |    {
        |      "name": "rdouble",
        |      "type": "double"
        |    },
        |    {
        |      "name": "rboolean",
        |      "type": "boolean"
        |    },
        |    {
        |      "name": "rdate",
        |      "type": {
        |        "type": "int",
        |        "logicalType": "date"
        |      }
        |    }
        |  ]
        |}
        |""".stripMargin)

    val expected = new GenericData.Record(schema)
    expected.put("rstring", "qwerty")
    expected.put("rint", 123)
    expected.put("rlong", 123456789012345667L)
    expected.put("rfloat", 123.45f)
    expected.put("rdouble", 12345.12345d)
    expected.put("rboolean", true)
    expected.put("rdate", LocalDate.of(2021, 11, 23).toEpochDay.toInt)

    val parser = new CsvParser(
      schema,
      arrayDelimiter = None,
      recordDelimiter = None,
      StringParsers.dateParser(DateTimeFormatter.ISO_DATE) orElse StringParsers.primitiveParsers
    )

    parser(csv) shouldBe expected
  }
}
