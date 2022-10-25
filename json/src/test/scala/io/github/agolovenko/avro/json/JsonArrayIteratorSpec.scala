package io.github.agolovenko.avro.json

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.StringReader

class JsonArrayIteratorSpec extends AnyWordSpec with Matchers {
  "parses json array" in {
    val json =
      """
        |[
        |  {
        |    "lastUpdated": "2020-04-17T19:38:47.04+00:00",
        |    "id": "16289",
        |    "clientId": 123,
        |    "description": {
        |      "type": "normal",
        |      "text": "good one"
        |    }
        |  },
        |  {
        |    "lastUpdated": "2020-04-17T19:38:40.04+00:00",
        |    "id": "16288",
        |    "clientId": 124,
        |    "description": {
        |      "type": "normal",
        |      "text": "very good one"
        |    }
        |  }
        |]
        |""".stripMargin

    val it        = new JsonArrayIterator(new StringReader(json))
    val jsObjects = it.toList

    jsObjects should have size 2
    (jsObjects(1) \ "clientId").as[Int] shouldBe 124
  }

  "expects array of objects as input" in {
    an[IllegalStateException] should be thrownBy new JsonArrayIterator(new StringReader(" {}"))
    an[IllegalStateException] should be thrownBy new JsonArrayIterator(new StringReader(" [{}, 1234]")).foreach(_ => ())
    noException should be thrownBy new JsonArrayIterator(new StringReader(""" [{}, {"number": 1234}  ]""")).foreach(_ => ())
  }
}
