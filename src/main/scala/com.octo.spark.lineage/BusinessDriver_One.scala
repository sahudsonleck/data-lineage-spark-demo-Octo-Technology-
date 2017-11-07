package com.octo.spark.lineage

import java.io.File

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.util.{RDDLineageExtractor, RDDToStringExtraction, TreeNode}

/**
  * Created by Y. Benabderrahmane on 04/10/2017.
  */
object BusinessDriver_One extends AppCore with LazyLogging {
  def main(args: Array[String]): Unit = {
    val runId = args(0).toInt
    val clientsFileName = args(1)
    val productsFileName = args(2)

    init(getClass, runId)

    val clients = readJson(s"src/main/resources/examples/raw/$clientsFileName", referenceDataset = true)
    val products = readJson(s"src/main/resources/examples/raw/$productsFileName")

    val namesAndProductsPairRdd = combineClientsAndProducts(clients, products)

    namesAndProductsPairRdd.foreach(value => println(s"===========> value: $value"))

    val outputFilePath = "src/main/resources/examples/generated2/namesAndProducts.json"
//    delete(new File(outputFilePath))
    writeRddToJson(namesAndProductsPairRdd, outputFilePath)
    println(namesAndProductsPairRdd.toDebugString)
    println("//// RDDtoStringExtraction------------------------------------------------")
    println(RDDToStringExtraction.toDebugString(namesAndProductsPairRdd))
    println("//// RDDLineageExtractor.lineageTree------------------------------------------------")
    val lineageTree = RDDLineageExtractor.lineageTree(namesAndProductsPairRdd)
    println(TreeNode.describe(lineageTree))
  }

  /**
    * Merge keeping the version of the client
    * @param clients (considered as the reference dataset for dataset lineage)
    * @param products
    * @return
    */
  private def combineClientsAndProducts(clients: RDD[Row], products: RDD[Row]): RDD[Row] = {
    val clientsPairRdd = clients.keyBy(_.getAs[String]("product-id"))
    val productsPairRdd = products.keyBy(_.getAs[String]("id"))
    val join = clientsPairRdd.join(productsPairRdd)
    val schema = StructType(List(StructField("name", StringType), StructField("surname", StringType), StructField("quantity", StringType), StructField("version", StringType), StructField("product-name", StringType)))
    join.map({case (_, (cl, pr)) => new GenericRowWithSchema(
      Array(
        cl.getAs[String]("name"),
        cl.getAs[String]("surname"),
        cl.getAs[String]("quantity"),
        cl.getAs[String]("version"),
        pr.getAs("name")),
      schema
    )})
  }

}
