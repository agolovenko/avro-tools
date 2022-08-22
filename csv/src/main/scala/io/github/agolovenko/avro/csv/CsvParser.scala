package io.github.agolovenko.avro.csv

import io.github.agolovenko.avro._
import org.apache.avro.Schema
import org.apache.avro.Schema.Type._
import org.apache.avro.generic.GenericData

import scala.jdk.CollectionConverters._
import scala.util.Try

class CsvParser(
    schema: Schema,
    arrayDelimiter: Option[Char],
    recordDelimiter: Option[Char],
    stringParsers: PartialFunction[ParserContext, Any] = PartialFunction.empty,
    validations: PartialFunction[ValidationContext, Unit] = PartialFunction.empty,
    fieldRenamings: FieldRenamings = FieldRenamings.empty
) extends AbstractParser(stringParsers, validations) {
  def apply(data: CsvRow): GenericData.Record = {
    implicit val path: Path = Path.empty
    if (schema.getType == RECORD)
      readRecord(data, schema, defaultValue = None)
    else throw new ParserException(s"Unsupported root schema of type ${schema.getType}")
  }

  private def readRecord(data: CsvData, schema: Schema, defaultValue: Option[Any])(implicit path: Path): GenericData.Record = {
    if (data.isEmpty) fallbackToDefault(defaultValue, schema).asInstanceOf[GenericData.Record]
    else {
      val result = new GenericData.Record(schema)
      schema.getFields.asScala.foreach { field =>
        val fieldName = fieldRenamings(field.name())
        path.push(fieldName)

        try {
          val value = if (field.schema().getType == RECORD) {
            val delimiter = recordDelimiter
              .getOrElse(throw new ParserException("Nested 'RECORD' type is only supported when 'recordDelimiter' is provided"))

            readRecord(new PrefixFilteringCsvData(data, s"$fieldName$delimiter"), field.schema(), Option(field.defaultVal()))
          } else readAny(data.get(fieldName), field.schema(), Option(field.defaultVal()))

          result.put(field.name(), value)
        } finally {
          path.pop()
          ()
        }
      }

      validate(result, schema)

      result
    }
  }

  private def readAny(data: Option[String], schema: Schema, defaultValue: Option[Any])(implicit path: Path): Any = {
    val result = schema.getType match {
      case RECORD  => throw new ParserException("'RECORD' type is not allowed while parsing flat data")
      case ENUM    => readEnum(data, schema, defaultValue)
      case MAP     => throw new ParserException("'MAP' type is not supported for CSV format")
      case ARRAY   => readArray(data, schema, defaultValue)
      case UNION   => readUnion(data, schema, defaultValue)
      case BYTES   => readBytes(data, schema, defaultValue)
      case FIXED   => readFixed(data, schema, defaultValue)
      case STRING  => read(data, schema, defaultValue)
      case INT     => read(data, schema, defaultValue)
      case LONG    => read(data, schema, defaultValue)
      case FLOAT   => read(data, schema, defaultValue)
      case DOUBLE  => read(data, schema, defaultValue)
      case BOOLEAN => read(data, schema, defaultValue)

      case NULL => readNull(data, schema, defaultValue)
    }

    validate(result, schema)

    result
  }

  private def readEnum(data: Option[String], schema: Schema, defaultValue: Option[Any])(
      implicit path: Path
  ): GenericData.EnumSymbol = {
    val symbol = read(data, schema, defaultValue)

    if (schema.getEnumSymbols.contains(symbol)) new GenericData.EnumSymbol(schema, symbol)
    else throw new WrongTypeException(schema, data.toString)
  }

  private def readArray(data: Option[String], schema: Schema, defaultValue: Option[Any])(implicit path: Path): GenericData.Array[Any] =
    data match {
      case None | Some(null) => fallbackToDefault(defaultValue, schema).asInstanceOf[GenericData.Array[Any]]
      case Some(s)           => parseArray(s, schema)
    }

  private def parseArray(str: String, schema: Schema)(implicit path: Path): GenericData.Array[Any] = {
    val elems = str.split(arrayDelimiter.getOrElse(throw new ParserException("'ARRAY' type is only supported when 'arrayDelimiter' is provided")))

    val result = new GenericData.Array[Any](elems.length, schema)
    elems.zipWithIndex.foreach {
      case (elem, idx) =>
        path.push(s"[$idx]")
        try {
          val value = readAny(Some(elem), schema.getElementType, None)
          result.add(idx, value)
        } finally {
          path.pop()
          ()
        }
    }
    result
  }

  private def readUnion(data: Option[String], schema: Schema, defaultValue: Option[Any])(implicit path: Path): Any = {
    def unionIt = schema.getTypes.asScala.iterator.zipWithIndex.map {
      case (subSchema, idx) => Try(readAny(data, subSchema, defaultValue.filter(_ => idx == 0)))
    }

    val it = unionIt.flatMap(_.toOption)

    if (it.hasNext) it.next()
    else if (data.isEmpty) throw new MissingValueException(schema)
    else {
      val explanations = unionIt.flatMap(_.failed.map(_.getMessage).toOption).toSeq
      throw new WrongTypeException(schema, data.toString, explanations)
    }
  }

  private def readBytes(data: Option[String], schema: Schema, defaultValue: Option[Any])(implicit path: Path): Any =
    read(data, schema, defaultValue)

  private def readFixed(data: Option[String], schema: Schema, defaultValue: Option[Any])(
      implicit path: Path
  ): GenericData.Fixed = {
    val bytes = readBytes(data, schema, defaultValue).asInstanceOf[Array[Byte]]

    if (bytes.length == schema.getFixedSize) new GenericData.Fixed(schema, bytes)
    else throw new WrongTypeException(schema, data.toString, Seq(s"incorrect size: ${bytes.length} instead of ${schema.getFixedSize}"))
  }

  private def read(data: Option[String], schema: Schema, defaultValue: Option[Any])(implicit path: Path): Any =
    data match {
      case None | Some(null) => fallbackToDefault(defaultValue, schema)
      case Some(str)         => parseString(str, schema)
    }

  private def readNull(data: Option[String], schema: Schema, defaultValue: Option[Any])(implicit path: Path): Null =
    data match {
      case None                  => fallbackToDefault(defaultValue, schema).asInstanceOf[Null]
      case Some(null) | Some("") => null
      case _                     => throw new WrongTypeException(schema, data.toString)
    }
}
