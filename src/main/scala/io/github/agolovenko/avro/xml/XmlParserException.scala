package io.github.agolovenko.avro.xml

import io.github.agolovenko.avro.StackType.Stack
import io.github.agolovenko.avro.{pathOf, typeName}
import org.apache.avro.Schema

import scala.xml.NodeSeq

class XmlParserException(message: String)(implicit path: Stack[String]) extends RuntimeException(s"$message @ ${pathOf(path)}")

class WrongTypeException(schema: Schema, value: NodeSeq, expl: Option[String] = None)(implicit path: Stack[String])
    extends XmlParserException(s"Failed to extract ${typeName(schema)} from $value${expl.fold("")(": " + _)}")

class MissingValueException(schema: Schema)(implicit path: Stack[String]) extends XmlParserException(s"Missing ${typeName(schema)} node")
