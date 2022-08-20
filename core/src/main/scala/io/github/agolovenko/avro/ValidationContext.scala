package io.github.agolovenko.avro

import org.apache.avro.Schema

case class ValidationContext(value: Any, schema: Schema, path: Path)
