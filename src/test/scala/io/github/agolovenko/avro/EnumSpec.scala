package io.github.agolovenko.avro

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import play.api.libs.json.Json

import scala.jdk.CollectionConverters._

class EnumSpec extends AnyWordSpec with Matchers {
  import Schema._

  private val doc               = "no-doc"
  private val ns                = "parser.test"
  private val field             = new Field("field1", createEnum("sch_field1", doc, ns, Seq("ev1", "ev2").asJava))
  private val schema            = createRecord("sch_rec1", doc, ns, false, Seq(field).asJava)
  private val fieldWithDefault  = new Field("field2", createEnum("sch_field2", doc, ns, Seq("ev1", "ev2").asJava), doc, "ev2")
  private val schemaWithDefault = createRecord("sch_rec2", doc, ns, false, Seq(fieldWithDefault).asJava)

  "parses correctly" in {
    val data   = Json.parse("""{"field1": "ev1"}""")
    val record = new JsonParser()(data, schema)

    GenericData.get().validate(schema, record) should ===(true)
    record.get("field1") should ===(new GenericData.EnumSymbol(fieldWithDefault.schema(), "ev1"))
  }

  "fails on missing value" in {
    val data = Json.parse("{}")
    a[MissingValueException] should be thrownBy new JsonParser()(data, schema)
  }

  "fails on invalid value" in {
    val data = Json.parse("""{"field1": "ev33"}""")
    a[JsonParserException] should be thrownBy new JsonParser()(data, schema)
  }

  "applies default value" in {
    val data   = Json.parse("{}")
    val record = new JsonParser()(data, schemaWithDefault)

    GenericData.get().validate(schema, record) should ===(true)
    record.get("field2") should ===(new GenericData.EnumSymbol(fieldWithDefault.schema(), "ev2"))
  }
}
