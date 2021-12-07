package io.github.agolovenko.avro.json

import io.github.agolovenko.avro.{Path, typeName}
import org.apache.avro.Schema
import play.api.libs.json.JsValue

import scala.util.control.NoStackTrace

class JsonParserException(message: String)(implicit path: Path) extends RuntimeException(s"$message @ $path") with NoStackTrace

class WrongTypeException(schema: Schema, value: JsValue, expl: Option[String] = None)(implicit path: Path)
    extends JsonParserException(s"Failed to extract ${typeName(schema)} from $value${expl.fold("")(": " + _)}")

class MissingValueException(schema: Schema)(implicit path: Path) extends JsonParserException(s"Missing ${typeName(schema)} node")
