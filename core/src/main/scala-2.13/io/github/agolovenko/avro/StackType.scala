package io.github.agolovenko.avro

import scala.collection.mutable

object StackType {
  type Stack[T] = mutable.Stack[T]
}
