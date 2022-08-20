package io.github.agolovenko.avro

import org.apache.avro.Schema.Parser
import org.apache.avro.generic.GenericData
import org.scalatest.Inspectors.forAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.LocalDate

class RandomDataSpec extends AnyWordSpec with Matchers {
  import RandomData._

  private val schema = new Parser().parse("""
      |{
      |  "type": "record",
      |  "name": "sch_rec",
      |  "fields": [
      |    {
      |      "name": "f_record",
      |      "type": {
      |        "name": "sch_f_record",
      |        "type": "record",
      |        "fields": [
      |          {
      |            "name": "nf_string",
      |            "type": "string"
      |          },
      |          {
      |            "name": "nf_int",
      |            "type": "int"
      |          },
      |          {
      |            "name": "f_date_spec",
      |            "type": {
      |               "type": "int",
      |               "logicalType": "date"
      |             }
      |           }
      |        ]
      |      }
      |    },
      |    {
      |      "name": "f_array",
      |      "type": {
      |        "type": "array",
      |        "items": "int"
      |      }
      |    },
      |    {
      |      "name": "f_map",
      |      "type": {
      |        "type": "map",
      |        "values": "int"
      |      }
      |    },
      |    {
      |      "name": "f_enum",
      |      "type": {
      |        "type": "enum",
      |        "name": "Suit",
      |        "symbols": [
      |          "SPADES",
      |          "HEARTS",
      |          "DIAMONDS",
      |          "CLUBS"
      |        ]
      |      }
      |    },
      |    {
      |      "name": "f_union",
      |      "type": [
      |        "int",
      |        "boolean"
      |      ]
      |    },
      |    {
      |      "name": "f_long",
      |      "type": "long"
      |    },
      |    {
      |      "name": "f_float",
      |      "type": "float"
      |    },
      |    {
      |      "name": "f_double",
      |      "type": "double"
      |    },
      |    {
      |      "name": "f_boolean",
      |      "type": "boolean"
      |    },
      |    {
      |      "name": "f_bytes",
      |      "type": "bytes"
      |    },
      |    {
      |      "name": "f_fixed",
      |      "type": {
      |        "name": "md5",
      |        "type": "fixed",
      |        "size": 16
      |      }
      |    },
      |    {
      |      "name": "f_date",
      |      "type": {
      |        "type": "int",
      |        "logicalType": "date"
      |      }
      |    },
      |    {
      |      "name": "f_time",
      |      "type": {
      |        "type": "int",
      |        "logicalType": "time-millis"
      |      }
      |    }
      |  ]
      |}""".stripMargin)

  "encodes json for RandomData and parses it back to original" in {
    val fromDate = LocalDate.of(2020, 1, 1)

    val specificPath = Path("f_record", "f_date_spec")
    val specificDateGenerator: PartialFunction[GeneratorContext, Int] = {
      case ctx if ctx.path =~= specificPath => randomDay(LocalDate.of(2021, 1, 1), 10)(ctx.random)
    }

    val randomData = new RandomData(
      schema,
      total = 16,
      generators = specificDateGenerator orElse dateGenerator(fromDate, maxDays = 16) orElse timeGenerators
    )

    forAll(randomData.toSeq) {
      case record: GenericData.Record =>
        GenericData.get().validate(schema, record) shouldBe true

        val epochDay = record.get("f_record").asInstanceOf[GenericData.Record].get("f_date_spec").asInstanceOf[Int]

        LocalDate.ofEpochDay(epochDay.toLong).getYear shouldBe 2021
    }
  }
}
