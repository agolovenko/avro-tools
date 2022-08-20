# avro-tools-json

Set of tools for JSON to Avro conversions written in Scala using Play-Json. Available for scala `2.11`, `2.12`
and `2.13`

## Features

### [JsonParser](src/main/scala/io/github/agolovenko/avro/json/JsonParser.scala) - parses Json object into avro's `GenericData.Record`

* unions as you would expect them to be in plain JSON: without avro wrapping
* schema's default values are used if data is missing
* data not present in schema is ignored
* extendable [StringParsers](../core/src/main/scala/io/github/agolovenko/avro/StringParsers.scala). Some predefined
  ones:
    * numeric and boolean types
    * `BYTES` and `FIXED` as base64-strings
    * `Logical Types`:
        * `date`
        * `time-millis`
        * `time-micros`
        * `timestamp-millis`
        * `timestamp-micros`
* Extendable validations
* Comprehensive Exceptions: General/Missing Value/Wrong Type. All containing path and description

### [JsonEncoder](src/main/scala/io/github/agolovenko/avro/json/JsonEncoder.scala) - encodes avro's `GenericData.Record` into Json object

* unions as you would expect them to be in plain JSON: without avro wrapping
* extendable [StringEncoders](../core/src/main/scala/io/github/agolovenko/avro/StringEncoders.scala). Some predefined
  ones:
    * `BYTES` and `FIXED` as base64-strings
    * `Logical Types`:
        * `date`
        * `time-millis`
        * `time-micros`
        * `timestamp-millis`
        * `timestamp-micros`

## Usage

### build.sbt:

```sbt
libraryDependencies += "io.github.agolovenko" %% "avro-tools-json" % "0.5.0"
```

### code:

```scala
import io.github.agolovenko.avro._
import io.github.agolovenko.avro.json.{JsonEncoder, JsonParser}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import play.api.libs.json.{JsObject, Json}

import java.time.format.DateTimeFormatter

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

import io.github.agolovenko.avro.StringParsers._

val data = Json.parse("""{"field1": [12, 14]}""")
val parser = new JsonParser(schema, primitiveParsers orElse base64Parsers)
val record: GenericData.Record = parser(data)
val bytes: Array[Byte] = toBytes(record)

// JsonEncoder example

import io.github.agolovenko.avro.StringEncoders._

val encoder = new JsonEncoder(base64Encoders orElse dateEncoder(DateTimeFormatter.ISO_DATE))
val json: JsObject = encoder(record)
```

For more examples check out the [tests](src/test/scala/io/github/agolovenko/avro/json)!
