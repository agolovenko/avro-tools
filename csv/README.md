# avro-tools-csv

Set of tools for CSV to Avro conversions written in Scala using Univocity CSV Parser. Available for scala `2.11`, `2.12`
and `2.13`

## Features

### [CsvIterator](src/main/scala/io/github/agolovenko/avro/csv/CsvIterator.scala) - wrapper around Univocity CSV Parser to adapt to Scala Collections Framework. Produces `CsvRow`s.

### [CsvRow](src/main/scala/io/github/agolovenko/avro/csv/CsvRow.scala) - simple map-like structure for rapid access to parsed CSV row.

### [CsvParser](src/main/scala/io/github/agolovenko/avro/csv/CsvParser.scala) - parses CsvRow into avro's `GenericData.Record`

* Primitive Arrays are supported when a corresponding delimiter is provided to cut the String value
* Nested Records are supported when a corresponding delimiter is provided to construct complex field names
* Data not present in schema is ignored
* Extendable [StringParsers](../core/src/main/scala/io/github/agolovenko/avro/StringParsers.scala). Some predefined
  ones:
    * numeric and boolean types
    * `BYTES` and `FIXED` as base64-strings
    * `Logical Types`:
        * `uuid`
        * `date`
        * `time-millis`
        * `time-micros`
        * `timestamp-millis`
        * `timestamp-micros`
* Comprehensive Exceptions: General/Missing Value/Wrong Type. All containing path and description

## Usage

### build.sbt:

```sbt
libraryDependencies ++= "io.github.agolovenko" %% "avro-tools-csv" % "0.3.0"
```

### code:

```scala
import com.univocity.parsers.csv.CsvParserSettings
import io.github.agolovenko.avro._
import io.github.agolovenko.avro.csv.{CsvIterator, CsvParser, CsvRow}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets.UTF_8

val csv =
  """
    |rstring,rint,rlong,rfloat,rdouble,rboolean,rdate
    |qwerty,123,123456789012345667,123.45,12345.12345,true,2021-11-23
    |""".stripMargin

val schema = new Schema.Parser().parse(
  """
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

val settings = new CsvParserSettings()
settings.setReadInputOnSeparateThread(false)

val csvRows: Iterator[CsvRow] = CsvIterator(settings, UTF_8)(new ByteArrayInputStream(csv.getBytes(UTF_8)))

val parser = new CsvParser(schema, arrayDelimiter = None, recordDelimiter = None, StringParsers.primitiveParsers)

val records: Iterator[GenericData.Record] = csvRows.map(parser.apply)
```

For more examples check out the [tests](src/test/scala/io/github/agolovenko/avro/csv)!
