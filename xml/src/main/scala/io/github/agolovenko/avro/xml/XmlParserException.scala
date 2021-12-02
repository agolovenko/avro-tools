package io.github.agolovenko.avro.xml

import io.github.agolovenko.avro.{Path, typeName}
import org.apache.avro.Schema

import scala.xml.NodeSeq

class XmlParserException(message: String)(implicit path: Path) extends RuntimeException(s"$message @ $path")

class WrongTypeException(schema: Schema, value: NodeSeq, expl: Option[String] = None)(implicit path: Path)
    extends XmlParserException(s"Failed to extract ${typeName(schema)} from $value${expl.fold("")(": " + _)}")

class MissingValueException(schema: Schema)(implicit path: Path) extends XmlParserException(s"Missing ${typeName(schema)} node")
