package io.github.agolovenko.avro

import org.apache.avro.Schema

import scala.util.control.NoStackTrace

class ParserException(message: String)(implicit path: Path) extends RuntimeException(s"$message @ $path") with NoStackTrace

class WrongTypeException(schema: Schema, value: String, reasons: Seq[String] = Seq.empty)(implicit path: Path)
    extends ParserException(
      s"Failed to extract ${typeName(schema)} from '$value'${if (reasons.isEmpty) "" else reasons.mkString(". Caused by: ", "and", "")}"
    )

class MissingValueException(schema: Schema)(implicit path: Path) extends ParserException(s"Missing ${typeName(schema)} node")

class InvalidValueException(value: Any, reason: String)(implicit path: Path)
    extends ParserException(s"Invalid value '$value'${Option(reason).fold("") { r => s": $r" }}")
