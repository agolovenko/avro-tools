# avro-tools-json

Base classes and utilities for Avro conversion projects.

## Features

### [RandomData](src/main/scala/io/github/agolovenko/avro/RandomData.scala) - given the schema generates a random avro record

* inspired by `org.apache.avro.util.RandomData`
* allows custom generators by field path or by type
* predefined generators for some `Logical Types`:
    * `uuid`
    * `date`
    * `time-millis`
    * `time-micros`
    * `timestamp-millis`
    * `timestamp-micros`

## Usage

### build.sbt:

```sbt
libraryDependencies += "io.github.agolovenko" %% "avro-tools-core" % "0.6.1"

```

### code:

```scala
import io.github.agolovenko.avro._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

import java.time.LocalDate
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

import io.github.agolovenko.avro.RandomData._

val specificPath = Path("record", "nested_past_date")
val specificDateGenerator: PartialFunction[GeneratorContext, Int] = {
  case ctx if ctx.path =~= specificPath => randomDay(LocalDate.of(2021, 1, 1), 10)(ctx.random)
}

val typedGenerators = timeGenerators orElse dateGenerator(fromDate = LocalDate.now(), maxDays = 10)

val records: Iterator[GenericData.Record] = new RandomData(schema, total = 1 << 10, specificDateGenerator orElse typedGenerators).map(_.asInstanceOf[GenericData.Record])
```