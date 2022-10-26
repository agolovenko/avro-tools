package io.github.agolovenko.avro.json

import io.github.agolovenko.avro.closeOnError
import play.api.libs.json.{JsObject, Json}

import java.io.{BufferedReader, Reader}
import scala.util.control.NonFatal

class JsonLinesIterator(reader: Reader) extends Iterator[JsObject] with AutoCloseable {
  private val bufferedReader = reader match {
    case buffered: BufferedReader => buffered
    case unBuffered               => new BufferedReader(unBuffered)
  }

  private var nextLine = getNextLine()

  private def getNextLine(): String = closeOnError(this) {
    try {
      var line = bufferedReader.readLine()

      while (line != null && line.trim.isEmpty) {
        line = bufferedReader.readLine()
      }

      line
    } catch { case NonFatal(ex) => throw new JsonException("Failed to read next line", ex) }
  }

  override def hasNext: Boolean = closeOnError(this) {
    if (nextLine != null) true
    else {
      close()
      false
    }
  }

  override def next(): JsObject = closeOnError(this) {
    try {
      val line = nextLine
      nextLine = getNextLine()

      Json.parse(line).as[JsObject]
    } catch { case NonFatal(ex) => throw new JsonException("Failed to parse next line", ex) }
  }

  override def close(): Unit = bufferedReader.close()
}
