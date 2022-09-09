package io.github.agolovenko.avro.csv

import io.github.agolovenko.avro.StringParsers
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class NestedRecordSpec extends AnyWordSpec with Matchers {
  import Schema._

  "reads nested records" in {
    val csv = csvRow("""
                       |r1.r1_s,r1.r1_r2.r1_r2_s,r1.r1_r2.r1_r2_i
                       |aaa,bbb,123
                       |""".stripMargin)

    val schema = new Parser().parse("""
        |{
        |  "type": "record",
        |  "name": "sch_rec1",
        |  "fields": [
        |    {
        |      "name": "r1",
        |      "type": {
        |        "name": "sch_r1",
        |        "type": "record",
        |        "fields": [
        |          {
        |            "name": "r1_s",
        |            "type": "string"
        |          },
        |          {
        |            "name": "r1_r2",
        |            "type": {
        |              "name": "sch_r1_r2",
        |              "type": "record",
        |              "fields": [
        |                {
        |                  "name": "r1_r2_s",
        |                  "type": "string"
        |                },
        |                {
        |                  "name": "r1_r2_i",
        |                  "type": "int"
        |                }
        |              ]
        |            }
        |          }
        |        ]
        |      }
        |    }
        |  ]
        |}""".stripMargin)

    val r1_r2 = new GenericData.Record(schema.getField("r1").schema().getField("r1_r2").schema())
    r1_r2.put("r1_r2_s", "bbb")
    r1_r2.put("r1_r2_i", 123)

    val r1 = new GenericData.Record(schema.getField("r1").schema())
    r1.put("r1_s", "aaa")
    r1.put("r1_r2", r1_r2)

    val expected = new GenericData.Record(schema)
    expected.put("r1", r1)

    val parser = new CsvParser(
      schema,
      arrayDelimiter = None,
      recordDelimiter = Some('.'),
      StringParsers.primitiveParsers
    )

    parser(csv) shouldBe expected
  }

  "applies default value" in {
    val schema = new Parser().parse("""
        |{
        |  "type": "record",
        |  "name": "sch_rec1",
        |  "fields": [
        |    {
        |      "name": "r1",
        |      "type": {
        |          "name": "sch_n_rec2",
        |          "type": "record",
        |          "fields": [
        |            {
        |              "name": "n_field1",
        |              "type": "string"
        |            },
        |            {
        |              "name": "n_field2",
        |              "type": "int"
        |            }
        |          ]
        |        },
        |      "default": {"n_field1": "bbb", "n_field2": 33}
        |    }
        |  ]
        |}""".stripMargin)

    val csv = csvRow("""
                       |r2
                       |aaa
                       |""".stripMargin)

    val parser = new CsvParser(
      schema,
      arrayDelimiter = None,
      recordDelimiter = Some('.'),
      StringParsers.primitiveParsers
    )

    val expected = new GenericData.Record(schema.getField("r1").schema())
    expected.put("n_field1", "bbb")
    expected.put("n_field2", 33)

    parser(csv).get("r1") shouldBe expected
  }

  "applies default null value" in {
    val schema = new Parser().parse("""
        |{
        |  "type": "record",
        |  "name": "sch_rec1",
        |  "fields": [
        |    {
        |      "name": "r1",
        |      "type": [
        |        "null",
        |        {
        |          "name": "sch_n_rec2",
        |          "type": "record",
        |          "fields": [
        |            {
        |              "name": "n_field1",
        |              "type": "string"
        |            },
        |            {
        |              "name": "n_field2",
        |              "type": "int"
        |            }
        |          ]
        |        }
        |      ],
        |      "default": null
        |    }
        |  ]
        |}""".stripMargin)

    val csv = csvRow("""
                       |r2
                       |aaa
                       |""".stripMargin)

    val parser = new CsvParser(
      schema,
      arrayDelimiter = None,
      recordDelimiter = Some('.'),
      StringParsers.primitiveParsers
    )

    parser(csv).get("r1") shouldBe null
  }
}
