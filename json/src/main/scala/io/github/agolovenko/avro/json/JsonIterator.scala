package io.github.agolovenko.avro.json

import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import io.github.agolovenko.avro.closeOnError
import play.api.libs.json.jackson.PlayJsonModule
import play.api.libs.json.{JsObject, JsonParserSettings}

import java.io.Reader
import scala.util.control.NonFatal

class JsonIterator(reader: Reader) extends Iterator[JsObject] with AutoCloseable {
  private val mapper         = new ObjectMapper().registerModule(new PlayJsonModule(JsonParserSettings()))
  private val parser         = mapper.createParser(reader)
  private val jsObjectReader = mapper.readerFor(classOf[JsObject])

  private var nextToken = closeOnError(this) {
    parser.nextToken match {
      case JsonToken.START_ARRAY  => parser.nextToken
      case JsonToken.START_OBJECT => JsonToken.START_OBJECT
      case next                   => throw new JsonException(s"Expected start of an array or an object, got instead: $next")
    }
  }

  override def hasNext: Boolean = closeOnError(this) {
    nextToken match {
      case JsonToken.START_OBJECT => true
      case JsonToken.END_ARRAY | null =>
        close()
        false
      case next => throw new JsonException(s"Expected end of an array or an object, got instead: $next")
    }
  }

  override def next(): JsObject = closeOnError(this) {
    try {
      val objectNode = mapper.readTree[ObjectNode](parser)
      val result     = jsObjectReader.readValue[JsObject](objectNode)

      nextToken = parser.nextToken()

      result
    } catch { case NonFatal(ex) => throw new JsonException("Failed to read next object", ex) }
  }

  override def close(): Unit = {
    parser.close()
    reader.close()
  }
}
