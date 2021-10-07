package io.github.agolovenko.avro.json

import io.github.agolovenko.avro.StackType.Stack
import io.github.agolovenko.avro.{pathOf, typeName}
import org.apache.avro.Schema
import play.api.libs.json.JsValue

class JsonParserException(message: String)(implicit path: Stack[String]) extends RuntimeException(s"$message @ ${pathOf(path)}")

class WrongTypeException(schema: Schema, value: JsValue, expl: Option[String] = None)(implicit path: Stack[String])
    extends JsonParserException(s"Failed to extract ${typeName(schema)} from $value${expl.fold("")(": " + _)}")

class MissingValueException(schema: Schema)(implicit path: Stack[String]) extends JsonParserException(s"Missing ${typeName(schema)} node")
