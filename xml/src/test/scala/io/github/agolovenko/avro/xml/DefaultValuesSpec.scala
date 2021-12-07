package io.github.agolovenko.avro.xml

import io.github.agolovenko.avro.StringParsers
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.GenericData
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.xml.XML

class DefaultValuesSpec extends AnyWordSpec with Matchers {
  "reads empty elements" in {
    val xml = XML.loadString("""
        |<r1>
        |    <rstring/>
        |    <rint></rint>
        |    <res></res>
        |</r1>
        |""".stripMargin)

    val schema = new Parser().parse("""
        |{
        |  "type": "record",
        |  "name": "r1",
        |  "fields": [
        |    {
        |      "name": "rstring",
        |      "type": ["null", "string"],
        |      "default": null
        |    },
        |    {
        |      "name": "rint",
        |      "type": ["null", "int"],
        |      "default": null
        |    },
        |    {
        |      "name": "rabsent",
        |      "type": "float",
        |      "default": 1.2
        |    },
        |    {
        |      "name": "rabsent2",
        |      "type": ["null", "string"],
        |      "default": null
        |    },
        |    {
        |      "name": "res",
        |      "type": "string"
        |    }
        |  ]
        |}
        |""".stripMargin)

    val expected = new GenericData.Record(schema)
    expected.put("rstring", null)
    expected.put("rint", null)
    expected.put("rabsent", 1.2f)
    expected.put("rabsent2", null)
    expected.put("res", "")

    val parser = new XmlParser(schema, StringParsers.primitiveParsers)

    parser(xml) shouldBe expected
  }
}
