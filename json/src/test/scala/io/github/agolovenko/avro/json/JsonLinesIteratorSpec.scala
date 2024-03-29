package io.github.agolovenko.avro.json

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.StringReader

class JsonLinesIteratorSpec extends AnyWordSpec with Matchers {
  "parses jsonL" in {
    val json =
      """
        |{"lastUpdated": "2020-04-17T19:38:47.04+00:00", "id": "16289", "clientId": 123, "description": {"type": "normal", "text": "good one"}}
        |
        |
        |{"lastUpdated": "2020-04-17T19:38:40.04+00:00", "id": "16288", "clientId": 124, "description": {"type": "normal", "text": "very good one"}}
        |
        |""".stripMargin

    val it        = new JsonLinesIterator(new StringReader(json))
    val jsObjects = it.toVector

    jsObjects should have size 2
    (jsObjects(1) \ "clientId").as[Int] shouldBe 124
  }

  "accepts empty input" in {
    noException should be thrownBy new JsonLinesIterator(new StringReader("   ")).foreach(_ => ())
  }

  "expects jsonL as input" in {
    a[JsonException] should be thrownBy new JsonLinesIterator(new StringReader("[{}]")).foreach(_ => ())
    noException should be thrownBy new JsonLinesIterator(new StringReader("{}\n\n{}")).foreach(_ => ())
  }
}
