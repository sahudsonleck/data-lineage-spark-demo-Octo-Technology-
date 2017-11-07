package com.octo.spark.lineage

import java.io.File

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by Y. Benabderrahmane on 04/10/2017.
  */
object BusinessDriver_Two extends AppCore with LazyLogging {
  def main(args: Array[String]): Unit = {
    init(getClass, runId = 47)

    val namesAndProductsVersion = args(0)
    val inputFilePath = s"src/main/resources/examples/generated2/namesAndProducts.json"
    val productPurchaseOutputFilePath = "src/main/resources/examples/generated2/productSummary.json"

    val namesAndProductsRdd = readPrunedJson(inputFilePath, version = namesAndProductsVersion, referenceDataset = true)
    namesAndProductsRdd.foreach(println)
    case class Data(name: String, surname: String, quantity: String, version: String, productName: String)
    val dataRdd = namesAndProductsRdd.map[Data](row => {
      Data(row.getAs[String]("name"), row.getAs[String]("surname"), row.getAs[String]("quantity"), row.getAs("version").toString, row.getAs[String]("product-name"))
    })
    dataRdd.foreach(println)
    val schema = StructType(Array(StructField("product-name", StringType), StructField("quantity", StringType), StructField("version", StringType)))
    val productPurchaseSummary = dataRdd.map(data => new GenericRowWithSchema(Array(data.productName, data.quantity, data.version), schema))
//    delete(new File(productPurchaseOutputFilePath))
    writeRddToJson(productPurchaseSummary, productPurchaseOutputFilePath)
  }
}
