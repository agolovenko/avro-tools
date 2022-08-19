package io.github.agolovenko.avro.csv

import io.github.agolovenko.avro.StringParsers
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.jdk.CollectionConverters._

class ArraySpec extends AnyWordSpec with Matchers {
  import Schema._

  "parses primitive arrays" in {
    val schema = new Parser().parse("""
        |{
        |  "type": "record",
        |  "name": "sch_rec1",
        |  "fields": [
        |    {
        |     "name": "iar",
        |     "type": {
        |       "type": "array",
        |       "items": "int"
        |     }
        |    },
        |    {
        |     "name": "dar",
        |     "type": {
        |        "type": "array",
        |        "items": {
        |          "type": "int",
        |          "logicalType": "date"
        |        }
        |      }
        |    }
        |  ]
        |}""".stripMargin)

    val csv = csvRow("""
        |iar,dar
        |111|234,2021-11-23|2020-10-23
        |""".stripMargin)

    val iar = new GenericData.Array(schema.getField("iar").schema(), Seq(111, 234).asJava)
    val dar = new GenericData.Array(
      schema.getField("dar").schema(),
      Seq(
        LocalDate.of(2021, 11, 23),
        LocalDate.of(2020, 10, 23)
      ).map(_.toEpochDay.toInt).asJava
    )
    val expected = new GenericData.Record(schema)
    expected.put("iar", iar)
    expected.put("dar", dar)

    val parser = new CsvParser(
      schema,
      arrayDelimiter = Some('|'),
      recordDelimiter = None,
      StringParsers.dateParser(DateTimeFormatter.ISO_DATE) orElse StringParsers.primitiveParsers
    )

    parser(csv) shouldBe expected
  }
}
