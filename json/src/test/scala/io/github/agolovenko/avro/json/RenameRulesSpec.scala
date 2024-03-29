package io.github.agolovenko.avro.json

import io.github.agolovenko.avro.{Path, RenameRule, RenameRules}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import play.api.libs.json.Json

import scala.jdk.CollectionConverters._

class RenameRulesSpec extends AnyWordSpec with Matchers {
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
      |       "type": "array",
      |       "items": {
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
      |      }
      |    }
      |  ]
      |}""".stripMargin)

  "renames fields correctly" in {
    val data        = Json.parse("""{"field-1": [{"n-field1": "aaa", "n_field2": 23}]}""")
    val renameRules = new RenameRules(RenameRule(Path("field-1"), "field1"), RenameRule(Path("field-1", "n-field1"), "n_field1"))
    val record      = new JsonParser(schema, renameRules = renameRules)(data)

    GenericData.get().validate(schema, record) shouldBe true

    val rec1 = new GenericData.Record(schema.getField("field1").schema().getElementType)
    rec1.put("n_field1", "aaa")
    rec1.put("n_field2", 23)
    val expected = new GenericData.Array(schema.getField("field1").schema(), Seq(rec1).asJava)

    record.get("field1") shouldBe expected
  }
}
