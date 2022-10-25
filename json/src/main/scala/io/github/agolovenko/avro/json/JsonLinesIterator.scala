package io.github.agolovenko.avro.json

import play.api.libs.json.{JsObject, Json}

import java.io.{BufferedReader, Reader}

class JsonLinesIterator(reader: => Reader) extends Iterator[JsObject] {
  private val bufferedReader = reader match {
    case buffered: BufferedReader => buffered
    case unBuffered               => new BufferedReader(unBuffered)
  }

  private var nextLine = getNextLine()

  private def getNextLine(): String = {
    var line = bufferedReader.readLine()

    while (line != null && line.trim.isEmpty) {
      line = bufferedReader.readLine()
    }

    line
  }

  override def hasNext: Boolean =
    if (nextLine != null) true
    else {
      bufferedReader.close()
      false
    }

  override def next(): JsObject = {
    val line = nextLine

    nextLine = getNextLine()

    Json.parse(line).as[JsObject]
  }
}