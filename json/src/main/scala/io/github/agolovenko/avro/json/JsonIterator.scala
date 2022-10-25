package io.github.agolovenko.avro.json

import com.fasterxml.jackson.core.{JsonToken, JsonParser => JJsonParser}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import play.api.libs.json.jackson.PlayJsonModule
import play.api.libs.json.{JsObject, JsonParserSettings}

import java.io.Reader

class JsonIterator(reader: => Reader) extends Iterator[JsObject] {
  private val mapper = new ObjectMapper()
    .registerModule(new PlayJsonModule(JsonParserSettings()))
    .enable(JJsonParser.Feature.AUTO_CLOSE_SOURCE)

  private val parser         = mapper.createParser(reader)
  private val jsObjectReader = mapper.readerFor(classOf[JsObject])

  private var nextToken = parser.nextToken match {
    case JsonToken.START_ARRAY  => parser.nextToken
    case JsonToken.START_OBJECT => JsonToken.START_OBJECT
    case next                   => throw new JsonException(s"Expected start of an array or an object, got instead: $next")
  }

  override def hasNext: Boolean = nextToken match {
    case JsonToken.START_OBJECT => true
    case JsonToken.END_ARRAY | null =>
      parser.close()
      false
    case next => throw new JsonException(s"Expected end of an array or an object, got instead: $next")
  }

  override def next(): JsObject = {
    val objectNode = mapper.readTree[ObjectNode](parser)
    val result     = jsObjectReader.readValue[JsObject](objectNode)

    nextToken = parser.nextToken()

    result
  }
}
