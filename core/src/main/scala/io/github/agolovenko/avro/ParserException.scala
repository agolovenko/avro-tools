package io.github.agolovenko.avro

import org.apache.avro.Schema

import scala.util.control.NoStackTrace

class ParserException(message: String)(implicit path: Path) extends RuntimeException(s"$message @ $path") with NoStackTrace

class WrongTypeException(schema: Schema, value: String, explanations: Seq[String] = Seq.empty)(implicit path: Path)
    extends ParserException(
      s"Failed to extract ${typeName(schema)} from $value${if (explanations.isEmpty) "" else explanations.mkString("\nCaused by:\n", "\nand\n", "")}"
    )

class MissingValueException(schema: Schema)(implicit path: Path) extends ParserException(s"Missing ${typeName(schema)} node")
