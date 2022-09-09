# avro-tools-xml

Set of tools for XML to Avro conversions written in Scala using scala-xml. Available for scala `2.11`, `2.12` and `2.13`

## Features

### [XMLParser](src/main/scala/io/github/agolovenko/avro/xml/XmlParser.scala) - parses XML object into avro's `GenericData.Record`

* XML arrays are supported with or without container elements
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
* Comprehensive Exceptions: General/Missing Value/Wrong Type. All containing path and description

## Usage

### build.sbt:

```sbt
libraryDependencies += "io.github.agolovenko" %% "avro-tools-xml" % "0.6.2"
```

### code:

```scala
import io.github.agolovenko.avro._
import io.github.agolovenko.avro.xml.XmlParser
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

import scala.xml.XML

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
    |     }
    |    }
    |  ]
    |}""".stripMargin)

val data = XML.loadString(
  """
    |<sch_rec1>
    | <field1>1</field1>
    | <field1>2</field1>
    |</sch_rec1>
    |""".stripMargin)

val parser = new XmlParser(schema, StringParsers.primitiveParsers)
val record: GenericData.Record = parser(data)
val bytes: Array[Byte] = toBytes(record)
```

For more examples check out the [tests](src/test/scala/io/github/agolovenko/avro/xml)!
