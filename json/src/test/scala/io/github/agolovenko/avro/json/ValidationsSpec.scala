package io.github.agolovenko.avro.json

import io.github.agolovenko.avro.{InvalidValueException, Path, StringParsers, ValidationContext}
import org.apache.avro.{LogicalTypes, Schema}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import play.api.libs.json.Json

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class ValidationsSpec extends AnyWordSpec with Matchers {
  import StringParsers._
  import DateTimeFormatter._

  private val schema = new Schema.Parser().parse("""
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
      |          }
      |        ]
      |      }
      |    },
      |    {
      |      "name": "f_string",
      |      "type": "string"
      |    },
      |    {
      |      "name": "f_long",
      |      "type": "long"
      |    },
      |    {
      |      "name": "f_date",
      |      "type": {
      |        "type": "int",
      |        "logicalType": "date"
      |      }
      |    }
      |  ]
      |}""".stripMargin)

  private val validations: PartialFunction[ValidationContext, Unit] = {
    val nestedStringPath = Path("f_record", "nf_string")

    {
      case ctx if ctx.path =~= nestedStringPath && ctx.value.asInstanceOf[String].isEmpty =>
        throw new IllegalArgumentException("empty string")
      case ctx if ctx.schema.getType == Schema.Type.LONG && ctx.value.asInstanceOf[Long] < 0L =>
        throw new IllegalArgumentException("negative value")
      case ctx if ctx.schema.getLogicalType == LogicalTypes.date() =>
        val year = LocalDate.ofEpochDay(ctx.value.asInstanceOf[Int].toLong).getYear
        if (year != 2022) throw new IllegalArgumentException("invalid year")
    }
  }

  private val parser = new JsonParser(schema, dateParser(ISO_DATE), validations)

  "validates correct input" in {
    val input =
      """
        |{ 
        |  "f_record": {
        |     "nf_string": "non-empty",
        |     "nf_int": 1
        |  },
        |  "f_string": "",
        |  "f_long": 42,
        |  "f_date": "2022-01-01"
        |}
        |""".stripMargin

    noException should be thrownBy parser(Json.parse(input))
  }

  "fails on negative number" in {
    val input =
      """
        |{
        |  "f_record": {
        |     "nf_string": "non-empty",
        |     "nf_int": 1
        |  },
        |  "f_string": "",
        |  "f_long": -42,
        |  "f_date": "2022-01-01"
        |}
        |""".stripMargin

    an[InvalidValueException] should be thrownBy parser(Json.parse(input))
  }

  "fails on empty nested string" in {
    val input =
      """
        |{
        |  "f_record": {
        |     "nf_string": "",
        |     "nf_int": 1
        |  },
        |  "f_string": "",
        |  "f_long": 42,
        |  "f_date": "2022-01-01"
        |}
        |""".stripMargin

    an[InvalidValueException] should be thrownBy parser(Json.parse(input))
  }

  "fails on wrong year" in {
    val input =
      """
        |{
        |  "f_record": {
        |     "nf_string": "non-empty",
        |     "nf_int": 1
        |  },
        |  "f_string": "",
        |  "f_long": 42,
        |  "f_date": "2021-01-01"
        |}
        |""".stripMargin

    an[InvalidValueException] should be thrownBy parser(Json.parse(input))
  }
}
