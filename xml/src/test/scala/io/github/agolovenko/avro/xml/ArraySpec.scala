package io.github.agolovenko.avro.xml

import io.github.agolovenko.avro.StringParsers
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters._
import scala.xml.XML

class ArraySpec extends AnyWordSpec with Matchers {
  import Schema._

  "parses array from container element" in {
    val schema = new Parser().parse("""
        |{
        |  "type": "record",
        |  "name": "r1",
        |  "fields": [
        |    {
        |      "name": "a1",
        |      "type": {
        |        "type": "array",
        |        "items": {
        |          "type": "record",
        |          "name": "ar1",
        |          "fields": [
        |            {
        |              "name": "f1",
        |              "type": "string"
        |            },
        |            {
        |              "name": "f2",
        |              "type": "int"
        |            }
        |          ]
        |        }
        |      }
        |    }
        |  ]
        |}
        |""".stripMargin)

    val xml = XML.loadString("""
        |<r1>
        |    <a1>
        |        <ar1>
        |            <f1>aaa</f1>
        |            <f2>234</f2>
        |        </ar1>
        |        <ar1>
        |            <f1>bbb</f1>
        |            <f2>123</f2>
        |        </ar1>
        |    </a1>
        |</r1>
        |""".stripMargin)

    val ar1_1 = new GenericData.Record(schema.getField("a1").schema().getElementType)
    ar1_1.put("f1", "aaa")
    ar1_1.put("f2", 234)

    val ar1_2 = new GenericData.Record(schema.getField("a1").schema().getElementType)
    ar1_2.put("f1", "bbb")
    ar1_2.put("f2", 123)

    val a1       = new GenericData.Array(schema.getField("a1").schema(), Seq(ar1_1, ar1_2).asJava)
    val expected = new GenericData.Record(schema)
    expected.put("a1", a1)

    val parser = new XmlParser(schema, StringParsers.primitiveParsers)

    parser(xml) shouldBe expected
  }

  "parses array without container element" in {
    val schema = new Parser().parse("""
        |{
        |  "type": "record",
        |  "name": "r1",
        |  "fields": [
        |    {
        |       "name": "rf1",
        |       "type": "string"
        |    },
        |    {
        |      "name": "ar1",
        |      "type": {
        |        "type": "array",
        |        "items": {
        |          "type": "record",
        |          "name": "ar1",
        |          "fields": [
        |            {
        |              "name": "f1",
        |              "type": "string"
        |            },
        |            {
        |              "name": "f2",
        |              "type": "int"
        |            }
        |          ]
        |        }
        |      }
        |    }
        |  ]
        |}
        |""".stripMargin)

    val xml = XML.loadString("""
        |<r1>
        |    <rf1>qwerty</rf1>
        |    <ar1>
        |        <f1>aaa</f1>
        |        <f2>234</f2>
        |    </ar1>
        |    <ar1>
        |        <f1>bbb</f1>
        |        <f2>123</f2>
        |    </ar1>
        |</r1>
        |""".stripMargin)

    val ar1_1 = new GenericData.Record(schema.getField("ar1").schema().getElementType)
    ar1_1.put("f1", "aaa")
    ar1_1.put("f2", 234)

    val ar1_2 = new GenericData.Record(schema.getField("ar1").schema().getElementType)
    ar1_2.put("f1", "bbb")
    ar1_2.put("f2", 123)

    val a1       = new GenericData.Array(schema.getField("ar1").schema(), Seq(ar1_1, ar1_2).asJava)
    val expected = new GenericData.Record(schema)
    expected.put("ar1", a1)
    expected.put("rf1", "qwerty")

    val parser = new XmlParser(schema, StringParsers.primitiveParsers)

    parser(xml) shouldBe expected
  }
}
