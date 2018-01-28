package feture.countvectorizer
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
/**
  * Created by jack on 2017/11/15.
  * 该方法用于将所有的文本词语进行编号，每个词语对应一个编号，并统计该词语在文档中的词频作为特征向量。
  */
object CountVector {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("lxw1234.com")
    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val df = sqlContext.createDataFrame(Seq(
      (0, Array("苹果","官网","苹果","宣布")),
      (1, Array("苹果","梨","香蕉"))
    )).toDF("id", "words")

    var cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setVocabSize(5)  //设置词语的总个数，词语编号后的数值均小于该值
      .setMinDF(1) //设置包含词语的最少的文档数
      .fit(df)

    println("output1:")
    cvModel.transform(df).select("id","words","features").collect().foreach(println)

  }
}
