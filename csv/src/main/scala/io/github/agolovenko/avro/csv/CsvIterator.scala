package io.github.agolovenko.avro.csv

import com.univocity.parsers.common.{AbstractParser, CommonParserSettings}
import com.univocity.parsers.csv.{CsvParser, CsvParserSettings}
import com.univocity.parsers.tsv.{TsvParser, TsvParserSettings}

import java.io.InputStream
import java.nio.charset.{Charset, StandardCharsets}

class CsvIterator private (parser: AbstractParser[_], customHeaders: Option[Array[String]]) extends Iterator[CsvRow] {
  private val headers   = customHeaders.getOrElse(parser.parseNext())
  private val headerMap = if (headers == null) null else headers.zipWithIndex.toMap

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

    new CsvRow(headers, headerMap, tokens)
  }
}

object CsvIterator {
  def apply[S <: CommonParserSettings[_]](settings: S, encoding: Charset = StandardCharsets.UTF_8, customHeaders: Option[Array[String]] = None)(
      is: => InputStream
  ): CsvIterator = {
    val parser = settings match {
      case csvSettings: CsvParserSettings => new CsvParser(csvSettings)
      case tsvSettings: TsvParserSettings => new TsvParser(tsvSettings)
      case unsupported                    => throw new CsvException(s"Unsupported settings type: ${unsupported.getClass.getName}")
    }

    parser.beginParsing(is, encoding)

    new CsvIterator(parser, customHeaders)
  }
}
