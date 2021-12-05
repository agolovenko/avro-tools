# avro-json-tools

Set of tools for JSON <=> Avro conversions written in Scala using Play-Json. Available for scala `2.11`, `2.12` and `2.13`

## Features

### [JsonParser](src/main/scala/io/github/agolovenko/avro/JsonParser.scala) - parses Json object into avro's `GenericData.Record`

* unions as you would expect them to be in plain JSON: without avro wrapping
* schema's default values are used if data is missing
* data not present in schema is ignored
* extendable [StringParsers](src/main/scala/io/github/agolovenko/avro/StringParsers.scala). Some predefined ones:
    * numeric and boolean types
    * `BYTES` and `FIXED` as base64-strings
    * `Logical Types`:
        * `date`
        * `time-millis`
        * `time-micros`
        * `timestamp-millis`
        * `timestamp-micros`
* Comprehensive Exceptions: General/Missing Value/Wrong Type. All containing JSON path and description

### [JsonEncoder](src/main/scala/io/github/agolovenko/avro/JsonEncoder.scala) - encodes avro's `GenericData.Record` into Json object

* unions as you would expect them to be in plain JSON: without avro wrapping
* extendable [StringEncoders](src/main/scala/io/github/agolovenko/avro/StringEncoders.scala). Some predefined ones:
    * `BYTES` and `FIXED` as base64-strings
    * `Logical Types`:
        * `date`
        * `time-millis`
        * `time-micros`
        * `timestamp-millis`
        * `timestamp-micros`

### [RandomData](src/main/scala/io/github/agolovenko/avro/RandomData.scala) - given the schema generates a random avro record

* inspired by `org.apache.avro.util.RandomData`
* allows custom generators by field name or by type
* predefined generators for some `Logical Types`:
    * `date`
    * `time-millis`
    * `time-micros`
    * `timestamp-millis`
    * `timestamp-micros`

## Usage

### build.sbt:

```sbt
libraryDependencies ++= "io.github.agolovenko" %% "avro-tools-json" % "0.2.0"
```

### code:

```scala
import io.github.agolovenko.avro._
import io.github.agolovenko.avro.json.{JsonEncoder, JsonParser, RandomData}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import play.api.libs.json.{JsObject, Json}

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.util.Random

val schema = new Schema.Parser().parse(
  """
    |{
    |  "type": "record",
    |  "name": "sch_rec1",
    |  "fields": [
    |    {
    |     "name": "field1", 
    |     "type": {
    |       "type": "array",
    |       "items": "int"
    |     },
    |     "default": [1, 2, 3]
    |    }
    |  ]
    |}""".stripMargin)

// JsonParser example

import StringParsers._

val data = Json.parse("""{"field1": [12, 14]}""")
val parser = new JsonParser(primitiveParsers ++ base64Parsers)
val record: GenericData.Record = parser(data, schema)
val bytes: Array[Byte] = toBytes(record)



// JsonEncoder example

import StringEncoders._

val encoder = new JsonEncoder(base64Encoders ++ dateEncoder(DateTimeFormatter.ISO_DATE))
val json: JsObject = encoder(record)

// RandomData example

import RandomData._

val namedGenerators: Map[Path, Random => Any] = Map(Path("a", "past") -> (implicit random => randomDay(LocalDate.now().minusDays(30), maxDays = 30)))
val typedGenerators = timeGenerators ++ dateGenerator(fromDate = LocalDate.now(), maxDays = 10)
val records: Iterator[GenericData.Record] = new RandomData(schema, total = 1 << 10, typedGenerators, namedGenerators).map(_.asInstanceOf[GenericData.Record])
```

For more examples check out the [tests](src/test/scala/io/github/agolovenko/avro)!
