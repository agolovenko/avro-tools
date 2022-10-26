package io.github.agolovenko.avro.xml

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.StringReader

class XmlIteratorSpec extends AnyWordSpec with Matchers {
  "parses XML" in {
    val xml = """
        |<r1>
        |    <rstring>qwerty</rstring>
        |    <rint>123</rint>
        |    <rlong>123456789012345667</rlong>
        |    <rfloat>123.45</rfloat>
        |    <rdouble>12345.12345</rdouble>
        |    <rboolean>true</rboolean>
        |    <rdate>2021-11-23</rdate>
        |    <rstring><rdate>2021-11-23</rdate><rboolean>true</rboolean></rstring>
        |</r1>
        |""".stripMargin

    new XmlIterator(elementName = None)(new StringReader(xml)).toVector should have size 1
    new XmlIterator(elementName = Some("rstring"))(new StringReader(xml)).toVector should have size 2
  }
}
