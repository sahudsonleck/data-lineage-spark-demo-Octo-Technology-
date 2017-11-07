package org.apache.spark.util

import org.apache.spark.ShuffleDependency
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer

/**
  * Created by Y. Benabderrahmane on 09/10/2017.
  */
object RDDLineageExtractor {
  def lineageTree[T](rdd: RDD[T]): TreeNode[String] = {

    def firstDebugNode(rdd: RDD[_]): TreeNode[String] = {
      val partitionStr = "(" + rdd.partitions.length + ")"
      TreeNode.get[String](s"$partitionStr ${debugSelf(rdd)}", None, debugChildren(rdd))
    }

    def debugSelf(rdd: RDD[_]): String = {
      val persistence = if (rdd.getStorageLevel != StorageLevel.NONE) rdd.getStorageLevel.description else ""
      s"$rdd [$persistence]"
    }

    def debugChildren(rdd: RDD[_]): ListBuffer[TreeNode[String]] = {
      val len = rdd.dependencies.length
      len match {
        case 0 => ListBuffer[TreeNode[String]]()
        case 1 =>
          val d = rdd.dependencies.head
          debugString(d.rdd, d.isInstanceOf[ShuffleDependency[_, _, _]], isLastChild = true)
        case _ =>
          val frontDeps = rdd.dependencies.take(len - 1)
          val frontDepStringsOne = frontDeps.map(
            d => debugString(d.rdd, d.isInstanceOf[ShuffleDependency[_, _, _]])
          )
          val frontDepStrings: ListBuffer[TreeNode[String]] = frontDepStringsOne.foldLeft(ListBuffer[TreeNode[String]]())((p, n) => p ++ n)

          val lastDep = rdd.dependencies.last
          val lastDepStrings =
            debugString(lastDep.rdd, lastDep.isInstanceOf[ShuffleDependency[_, _, _]], isLastChild = true)

          frontDepStrings ++ lastDepStrings
      }
    }

    def shuffleDebugString(rdd: RDD[_], isLastChild: Boolean): ListBuffer[TreeNode[String]] = {
      ListBuffer(TreeNode.get[String](s"${debugSelf(rdd)}", None, debugChildren(rdd)))
    }

    def debugString(
                     rdd: RDD[_],
                     isShuffle: Boolean = true,
                     isLastChild: Boolean = false): ListBuffer[TreeNode[String]] = {
      if (isShuffle) {
        shuffleDebugString(rdd, isLastChild)
      } else {
        ListBuffer(TreeNode.get[String](s"${debugSelf(rdd)}", None, debugChildren(rdd)))
      }
    }

    firstDebugNode(rdd)

  }

}
