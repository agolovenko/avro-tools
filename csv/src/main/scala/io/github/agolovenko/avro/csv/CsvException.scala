package io.github.agolovenko.avro.csv

import scala.util.control.NoStackTrace

class CsvException(message: String, cause: Throwable = null) extends RuntimeException(message, cause) with NoStackTrace
