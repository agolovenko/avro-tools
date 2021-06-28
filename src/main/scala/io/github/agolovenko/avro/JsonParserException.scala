package io.github.agolovenko.avro

import org.apache.avro.Schema
import play.api.libs.json.JsValue
import io.github.agolovenko.avro.StackType.Stack

class JsonParserException(message: String)(implicit path: Stack[String]) extends RuntimeException(s"$message @ ${pathOf(path)}")

class WrongTypeException(schema: Schema, value: JsValue, expl: Option[String] = None)(implicit path: Stack[String])
    extends JsonParserException(s"Failed to extract ${typeName(schema)} from $value${expl.fold("")(": " + _)}")

class MissingValueException(schema: Schema)(implicit path: Stack[String]) extends JsonParserException(s"Missing ${typeName(schema)} node")
