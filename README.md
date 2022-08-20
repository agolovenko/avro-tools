# avro-tools

Set of tools for conversions between various text formats and Avro written in Scala. Available for scala `2.11`, `2.12`
and `2.13`

Some notable features:

* Supported input formats: `json`, `csv`, `xml`
* Pluggable `StringParsers`
* Pluggable validations
* Descriptive errors that include path of origin

## Sub-projects (look inside for more documentation)

* [Core](core/README.md) - Base common utilities
* [JSON](json/README.md) - Set of tools for JSON to Avro conversions
* [XML](xml/README.md)   - Set of tools for XML to Avro conversions
* [CSV](csv/README.md)   - Set of tools for CSV to Avro conversions

## Some sample code

```scala
import io.github.agolovenko.avro._
import io.github.agolovenko.avro.StringParsers._
import io.github.agolovenko.avro.json.JsonParser
import org.apache.avro.{LogicalTypes, Schema}
import org.apache.avro.generic.GenericData
import play.api.libs.json.Json

val schema = new Schema.Parser().parse("""
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

val validations: PartialFunction[ValidationContext, Unit] = {
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

val parser = new JsonParser(schema, dateParser(ISO_DATE), validations)

val input = Json.parse("""
    |{ 
    |  "f_record": {
    |     "nf_string": "non-empty",
    |     "nf_int": 1
    |  },
    |  "f_string": "",
    |  "f_long": 42,
    |  "f_date": "2022-01-01"
    |}
    |""".stripMargin)

val record: GenericData.Record = parser(input) //OK

val input2 = Json.parse("""
    |{ 
    |  "f_record": {
    |     "nf_string": "",
    |     "nf_int": 1
    |  },
    |  "f_string": "",
    |  "f_long": 42,
    |  "f_date": "2022-01-01"
    |}
    |""".stripMargin)

parser(input2) //io.github.agolovenko.avro.InvalidValueException: Invalid value '': empty string @ /f_record/nf_string
```