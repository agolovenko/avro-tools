package io.github.agolovenko.avro.json

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import play.api.libs.json.Json

import scala.jdk.CollectionConverters._

class ArraySpec extends AnyWordSpec with Matchers {
  import Schema._

  private val field             = new Field("field1", createArray(create(Type.INT)))
  private val schema            = createRecord("sch_rec1", "no-doc", "parser.test", false, Seq(field).asJava)
  private val schemaWithDefault = new Parser().parse("""
      |{
      |  "type": "record",
      |  "name": "sch_rec2",
      |  "fields": [
      |    {
      |     "name": "field2", 
      |     "type": {
      |       "type": "array",
      |       "items": "int"
      |     },
      |     "default": [1, 2, 3]
      |    }
      |  ]
      |}""".stripMargin)

  "parses correctly" in {
    val data   = Json.parse("""{"field1": [12, 14]}""")
    val record = new JsonParser()(data, schema)

    GenericData.get().validate(schema, record) should ===(true)
    record.get("field1") should ===(new GenericData.Array(field.schema(), Seq(12, 14).asJava))
  }

  "fails on missing value" in {
    val data = Json.parse("{}")
    a[MissingValueException] should be thrownBy new JsonParser()(data, schema)
  }

  "fails on wrong type" in {
    val data = Json.parse("""{"field1": [1, "12"]}""")
    a[WrongTypeException] should be thrownBy new JsonParser()(data, schema)
  }

  "applies default value" in {
    val data   = Json.parse("{}")
    val record = new JsonParser()(data, schemaWithDefault)

    GenericData.get().validate(schemaWithDefault, record) should ===(true)
    record.get("field2") should ===(new GenericData.Array(createArray(create(Type.INT)), Seq(1, 2, 3).asJava))
  }
}
