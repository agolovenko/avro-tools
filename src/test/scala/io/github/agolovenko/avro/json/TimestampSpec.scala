package io.github.agolovenko.avro.json

import io.github.agolovenko.avro.StringParsers
import org.apache.avro.generic.GenericData
import org.apache.avro.{LogicalTypes, Schema}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import play.api.libs.json.Json

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId}
import scala.jdk.CollectionConverters._

class TimestampSpec extends AnyWordSpec with Matchers {
  import Schema._
  import StringParsers.localDateTimeParsers

  private val doc               = "no-doc"
  private val ns                = "parser.test"
  private val timestampType     = LogicalTypes.timestampMillis().addToSchema(create(Type.LONG))
  private val schema            = createRecord("sch_rec1", doc, ns, false, Seq(new Field("field1", timestampType)).asJava)
  private val schemaWithDefault = createRecord("sch_rec2", doc, ns, false, Seq(new Field("field2", timestampType, doc, 92147483647L)).asJava)

  private val zoneId    = ZoneId.of("CET")
  private val formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME

  "parses correctly from long" in {
    val data   = Json.parse("""{"field1": 92147483647}""")
    val record = new JsonParser()(data, schema)

    GenericData.get().validate(schema, record) should ===(true)
    record.get("field1") should ===(92147483647L)
  }

  "parses correctly from string" in {
    val ins      = Instant.ofEpochMilli(1613334344141L)
    val dateTime = LocalDateTime.ofInstant(ins, zoneId)
    val data     = Json.parse(s"""{"field1": "${dateTime.format(formatter)}"}""")
    val record   = new JsonParser(localDateTimeParsers(formatter, zoneId))(data, schema)

    GenericData.get().validate(schema, record) should ===(true)
    record.get("field1") should ===(ins.toEpochMilli)
  }

  "fails on invalid type" in {
    val data = Json.parse(s"""{"field1": []}""")
    a[WrongTypeException] should be thrownBy new JsonParser(localDateTimeParsers(formatter, zoneId))(data, schema)
  }

  "applies default value" in {
    val data   = Json.parse("{}")
    val record = new JsonParser()(data, schemaWithDefault)

    GenericData.get().validate(schema, record) should ===(true)
    record.get("field2") should ===(92147483647L)
  }
}
