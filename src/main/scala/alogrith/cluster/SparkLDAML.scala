package alogrith.cluster

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.mllib.linalg.{VectorUDT, Vectors}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StructField, StructType}
/**
  * Created by jack on 2017/11/21.
  */
object SparkLDAML {
  def main(args: Array[String]) {
    var sparkConf = new SparkConf().setAppName("ml local").setMaster("local")
    var sc = new SparkContext(sparkConf)
    var sqlContext = new SQLContext(sc)
    // Loads data
    val rowRDD = sc.textFile(input).filter(_.nonEmpty)
      .map(_.split(" ").map(_.toDouble)).map(Vectors.dense).map(Row(_))
    val schema = StructType(Array(StructField(FEATURES_COL, new VectorUDT, false)))
    val dataset = sqlContext.createDataFrame(rowRDD, schema)

    // Trains a LDA model
    val lda = new LDA()
      .setK(10)
      .setMaxIter(10)
      .setFeaturesCol(FEATURES_COL)
    val model = lda.fit(dataset)
    val transformed = model.transform(dataset)

    val ll = model.logLikelihood(dataset)
    val lp = model.logPerplexity(dataset)

    // describeTopics
    val topics = model.describeTopics(3)

    // Shows the result
    topics.show(false)
    transformed.show(false)
  }
}

 lcc198612
