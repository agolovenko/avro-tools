package io.github.agolovenko.avro.json

import io.github.agolovenko.avro.{MissingValueException, StringParsers, WrongTypeException}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import play.api.libs.json.Json

import scala.jdk.CollectionConverters._

class BooleanSpec extends AnyWordSpec with Matchers {
  import Schema._

  private val doc               = "no-doc"
  private val ns                = "parser.test"
  private val field             = new Field("field1", create(Type.BOOLEAN))
  private val schema            = createRecord("sch_rec1", doc, ns, false, Seq(field).asJava)
  private val fieldWithDefault  = new Field("field2", create(Type.BOOLEAN), doc, true)
  private val schemaWithDefault = createRecord("sch_rec2", doc, ns, false, Seq(fieldWithDefault).asJava)

  "parses correctly" in {
    val data   = Json.parse("""{"field1": true}""")
    val record = new JsonParser(schema)(data)

    GenericData.get().validate(schema, record) should ===(true)
    record.get("field1") should ===(true)
  }

  "fails on missing value" in {
    val data = Json.parse("{}")
    a[MissingValueException] should be thrownBy new JsonParser(schema)(data)
  }

  "fails on wrong type" in {
    val data = Json.parse("""{"field1": "12"}""")
    a[WrongTypeException] should be thrownBy new JsonParser(schema)(data)
  }

  "parses from string" in {
    val data   = Json.parse("""{"field1": "true"}""")
    val record = new JsonParser(schema, StringParsers.primitiveParsers)(data)

    GenericData.get().validate(schema, record) should ===(true)
    record.get("field1") should ===(true)
  }

  "applies default value" in {
    val data   = Json.parse("{}")
    val record = new JsonParser(schemaWithDefault)(data)

    GenericData.get().validate(schema, record) should ===(true)
    record.get("field2") should ===(true)
  }
}
