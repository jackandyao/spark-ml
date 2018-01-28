package feture.word2vec
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jack on 2017/11/15.
  * 单词转换成对应的向量
  */
object Word2Vector {
  case class DataRecord(text: String)

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("local").setAppName("word2vector")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)
    import sqlContext.implicits._

    val textDF = sqlContext.createDataFrame(Seq(
      "苹果 官网 苹果 宣布".split(" "),
      "苹果 梨 香蕉".split(" ")
    ).map(Tuple1.apply)).toDF("text")

//    val filePath = "/soft/idea/spark-ml/src/main/resources/tf.txt"
//    var textRDD = sparkContext.textFile(filePath).map(x =>x.split(",")).map(x =>x(1))
//    var flatRDD = textRDD.flatMap(x =>x.split(" "))
//

    //创建词向量
    val w2v = new Word2Vec().setInputCol("text").setOutputCol("w2c").setVectorSize(3).setMinCount(1)
    val w2vModel = w2v.fit(textDF)
    val result = w2vModel.transform(textDF)
    result.collect().foreach(println)

  }
}
