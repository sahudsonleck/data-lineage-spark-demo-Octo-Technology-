package com.octo.spark.lineage

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.DataFrame

/**
  * Created by Y. Benabderrahmane on 04/10/2017.
  */
object BusinessDriver_WithDF extends AppCore with LazyLogging {
  def main(args: Array[String]): Unit = {
    init(getClass, runId = 56)

    val clientsDF = readJsonDF("data-lineage-spark-demo/src/main/resources/examples/raw/clients-v15.json")
    val productsDF = readJsonDF("data-lineage-spark-demo/src/main/resources/examples/raw/products-v3.json")
    val namesAndProductsDF: DataFrame = combineClientsAndProducts(clientsDF, productsDF)

    writeJson(namesAndProductsDF, "data-lineage-spark-demo/src/main/resources/examples/generated/namesAndProducts.json")
    println("//// Show-------------------------------------------------------------")
    namesAndProductsDF.show(false)
  }

  /**
    * Merge keeping the version of the client
    * @param clientsDF
    * @param productsDF
    * @return
    */
  private def combineClientsAndProducts(clientsDF: DataFrame, productsDF: DataFrame): DataFrame = {
    clientsDF.join(
      productsDF.drop("version").withColumnRenamed("name", "product-name"), clientsDF.col("product-id") === productsDF.col("id")
    ).drop("product-id", "type", "availability")
  }

}
