package io.github.agolovenko.avro.csv

import com.univocity.parsers.common.{AbstractParser, CommonParserSettings}
import com.univocity.parsers.csv.{CsvParserSettings, CsvParser => UnivocityCsvParser}
import com.univocity.parsers.tsv.{TsvParserSettings, TsvParser => UnivocityTsvParser}

import java.io.Reader

class CsvIterator private (parser: AbstractParser[_], customHeaders: Option[Array[String]]) extends Iterator[CsvRow] {
  private val headerMap = {
    val headers = customHeaders.getOrElse(parser.parseNext())
    if (headers == null) null else headers.zipWithIndex.toMap
  }

  private var nextTokens = parser.parseNext()

  override def hasNext: Boolean =
    if (nextTokens != null) true
    else {
      parser.stopParsing()
      false
    }

  override def next(): CsvRow = {
    val tokens = nextTokens

    nextTokens = parser.parseNext()

    new CsvRow(headerMap, tokens)
  }
}

object CsvIterator {
  def apply[S <: CommonParserSettings[_]](settings: S, customHeaders: Option[Array[String]])(
      reader: => Reader
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
