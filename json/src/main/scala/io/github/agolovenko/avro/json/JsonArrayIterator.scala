package io.github.agolovenko.avro.json

import com.fasterxml.jackson.core.{JsonParser, JsonToken}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import play.api.libs.json.jackson.PlayJsonModule
import play.api.libs.json.{JsObject, JsonParserSettings}

import java.io.Reader

class JsonArrayIterator(reader: => Reader) extends Iterator[JsObject] {
  private val mapper = new ObjectMapper()
    .registerModule(new PlayJsonModule(JsonParserSettings()))
    .enable(JsonParser.Feature.AUTO_CLOSE_SOURCE)

  private val parser         = mapper.createParser(reader)
  private val jsObjectReader = mapper.readerFor(classOf[JsObject])

  private var nextToken = {
    val next = parser.nextToken
    if (next != JsonToken.START_ARRAY) throw new IllegalStateException(s"Expected start of array, got instead: $next")

    parser.nextToken
  }

  override def hasNext: Boolean =
    if (nextToken == JsonToken.START_OBJECT) true
    else if (nextToken == JsonToken.END_ARRAY) {
      parser.close()
      false
    } else throw new IllegalStateException(s"Expected end of array, got instead: $nextToken")

  override def next(): JsObject = {
    val objectNode = mapper.readTree[ObjectNode](parser)
    val result     = jsObjectReader.readValue[JsObject](objectNode)

    nextToken = parser.nextToken()

    result
  }
}
