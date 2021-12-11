# avro-tools-json

Base utilities for Avro conversion projects.

## Features

### [RandomData](src/main/scala/io/github/agolovenko/avro/RandomData.scala) - given the schema generates a random avro record

* inspired by `org.apache.avro.util.RandomData`
* allows custom generators by field name or by type
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
libraryDependencies ++= "io.github.agolovenko" %% "avro-tools-core" % "0.3.0"

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

val namedGenerators: Map[Path, Random => Any] = Map(Path("a", "past") -> (implicit random => randomDay(LocalDate.now().minusDays(30), maxDays = 30)))

val typedGenerators = timeGenerators ++ dateGenerator(fromDate = LocalDate.now(), maxDays = 10)

val records: Iterator[GenericData.Record] = new RandomData(schema, total = 1 << 10, typedGenerators, namedGenerators).map(_.asInstanceOf[GenericData.Record])
```