package io.github.agolovenko.avro

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import play.api.libs.json.Json

import java.util.Collections

class NestedRecordMapSpec extends AnyWordSpec with Matchers {
  import Schema._

  private val schema = new Parser().parse("""
      |{
      |  "type": "record",
      |  "name": "sch_rec1",
      |  "fields": [
      |    {
      |     "name": "field1",
      |     "type": {
      |       "name": "sch_n_arr2",
      |       "type": "map",
      |       "values": {
      |         "name": "sch_n_rec2",
      |         "type": "record",
      |         "fields": [
      |           {
      |             "name": "n_field1",
      |             "type": "string"
      |           },
      |           {
      |             "name": "n_field2",
      |             "type": "int"
      |           }
      |          ]
      |        }
      |      },
      |      "default": {"key1": {"n_field1": "bbb", "n_field2": 33}}
      |    }
      |  ]
      |}""".stripMargin)

  "parses nested record map correctly" in {
    val data   = Json.parse("""{"field1": {"key1": {"n_field1": "aaa", "n_field2": 23}}}""")
    val record = new JsonParser()(data, schema)

    GenericData.get().validate(schema, record) should ===(true)

    val rec1 = new GenericData.Record(schema.getField("field1").schema().getValueType)
    rec1.put("n_field1", "aaa")
    rec1.put("n_field2", 23)
    record.get("field1") should ===(Collections.singletonMap("key1", rec1))
  }

  "applies default value to nested record map" in {
    val data   = Json.parse("{}")
    val record = new JsonParser()(data, schema)

    GenericData.get().validate(schema, record) should ===(true)

    val rec1 = new GenericData.Record(schema.getField("field1").schema().getValueType)
    rec1.put("n_field1", "bbb")
    rec1.put("n_field2", 33)
    record.get("field1") should ===(Collections.singletonMap("key1", rec1))
  }
}
