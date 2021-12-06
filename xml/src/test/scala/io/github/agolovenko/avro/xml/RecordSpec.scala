package io.github.agolovenko.avro.xml

import io.github.agolovenko.avro.StringParsers
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.GenericData
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.xml.XML

class RecordSpec extends AnyWordSpec with Matchers {
  "parses record with various types" in {
    val xml = XML.loadString("""
        |<r1>
        |    <rstring>qwerty</rstring>
        |    <rint>123</rint>
        |    <rlong>123456789012345667</rlong>
        |    <rfloat>123.45</rfloat>
        |    <rdouble>12345.12345</rdouble>
        |    <rboolean>true</rboolean>
        |    <rdate>2021-11-23</rdate>
        |</r1>
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

    val parser = new XmlParser(schema, StringParsers.primitiveParsers ++ StringParsers.dateParser(DateTimeFormatter.ISO_DATE))

    parser(xml) shouldBe expected
  }
}
