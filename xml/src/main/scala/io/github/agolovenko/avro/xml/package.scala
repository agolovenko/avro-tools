package io.github.agolovenko.avro

import scala.xml.{Node, NodeSeq, Text}

package object xml {
  object SingleNode {
    def unapply(data: NodeSeq): Option[Node] = if (data.lengthCompare(1) == 0) Some(data.head) else None
  }

  object EmptyNode {
    def unapply(data: NodeSeq): Option[Node] = SingleNode.unapply(data).filter(_.child.isEmpty)
  }

  object NoNode {
    def unapply(data: NodeSeq): Option[NodeSeq] = if (data.isEmpty) Some(data) else None
  }

  object TextNode {
    def unapply(data: NodeSeq): Option[String] = SingleNode.unapply(data).flatMap { node => toText(Some(node.child)) }

    def toText(nodes: Option[Seq[Node]]): Option[String] = nodes.collect {
      case (textNode: Text) :: Nil => textNode.text
      case textNode: Text          => textNode.text
    }
  }
}
