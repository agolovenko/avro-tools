package io.github.agolovenko.avro.csv

import com.univocity.parsers.common.{AbstractParser, CommonParserSettings}
import com.univocity.parsers.csv.{CsvParserSettings, CsvParser => UnivocityCsvParser}
import com.univocity.parsers.tsv.{TsvParserSettings, TsvParser => UnivocityTsvParser}
import io.github.agolovenko.avro.closeOnError

import java.io.Reader

class CsvIterator private (parser: AbstractParser[_], customHeaders: Option[Array[String]]) extends Iterator[CsvRow] with AutoCloseable {
  private val headerMap = {
    val headers = customHeaders.getOrElse(parser.parseNext())
    if (headers == null) null else headers.zipWithIndex.toMap
  }

  private var nextTokens = closeOnError(this) { parser.parseNext() }

  override def hasNext: Boolean =
    if (nextTokens != null) true
    else {
      close()
      false
    }

  override def next(): CsvRow = closeOnError(this) {
    val tokens = nextTokens

    nextTokens = parser.parseNext()

    new CsvRow(headerMap, tokens)
  }

  override def close(): Unit = parser.stopParsing()
}

object CsvIterator {
  def apply[S <: CommonParserSettings[_]](settings: S, customHeaders: Option[Array[String]])(
      reader: Reader
  ): CsvIterator = {
    val parser = newParser(settings)

    parser.beginParsing(reader)

    new CsvIterator(parser, customHeaders)
  }

  private def newParser[S <: CommonParserSettings[_]](settings: S) = settings match {
    case csvSettings: CsvParserSettings => new UnivocityCsvParser(csvSettings)
    case tsvSettings: TsvParserSettings => new UnivocityTsvParser(tsvSettings)
    case unsupported                    => throw new CsvException(s"Unsupported settings type: ${unsupported.getClass.getName}")
  }
}
