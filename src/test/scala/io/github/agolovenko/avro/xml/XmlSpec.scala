package io.github.agolovenko.avro.xml

import io.github.agolovenko.avro.StringParsers
import org.apache.avro.Schema
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.xml.XML

class XmlSpec extends AnyWordSpec with Matchers {
  "parses xml" in {
    val schema   = new Schema.Parser().parse(getClass.getResourceAsStream("/xml.avsc"))
    val rootElem = XML.load(getClass.getResourceAsStream("/correct.xml"))
    val parser   = new XmlParser(StringParsers.primitiveParsers)

    val result = parser(rootElem, schema)

    println("success")
  }
}
