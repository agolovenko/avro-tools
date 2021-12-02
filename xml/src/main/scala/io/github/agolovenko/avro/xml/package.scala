package io.github.agolovenko.avro

import scala.xml.{Elem, Node, NodeSeq, Text}

package object xml {
  object Single {
    def unapply(data: NodeSeq): Option[Node] = if (data.lengthCompare(1) == 0) Some(data.head) else None
  }

  object TextNode {
    def unapply(data: NodeSeq): Option[String] = {
      val childNodes = data match {
        case Single(elem: Elem) => Some(elem.child)
        case _                  => None
      }

      toText(childNodes)
    }

    def toText(nodes: Option[Seq[Node]]): Option[String] = nodes.collect {
      case (textNode: Text) :: Nil => textNode.text
      case textNode: Text          => textNode.text
    }
  }
}
