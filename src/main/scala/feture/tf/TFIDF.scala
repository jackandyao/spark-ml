package feture.tf

import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jack on 2017/11/10.
  * 一定要注意spark的版本和Scala的版本要一致 否则会出现类似 java.lang.NoSuchMethodError: scala.reflect.api.JavaUniverse.runtimeMirror
  */
object TFIDF {

  case class RowDataRecord(category: String, text: String)

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("tfidf")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val filePath = "/soft/idea/spark-ml/src/main/resources/tf.txt"
    val textDF = sc.textFile(filePath).map(x =>x.split(",")).map(x => RowDataRecord(x(0),x(1))).toDF()

    textDF.select("category", "text").foreach(println)

    //分好的词转换为数组
    var tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    var worddata = tokenizer.transform(textDF)

    worddata.select($"category",$"text",$"words").foreach(println)

    //每个词转换为int行,并计算词的词频 setNumFeatures 代表hashing分桶的数量
    var hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawfeture").setNumFeatures(100)
    var feturedata = hashingTF.transform(worddata)

    feturedata.select($"category",$"words",$"rawfeture").foreach(println)

    //计算tf-idf值
    var tFIDF = new IDF().setInputCol("rawfeture").setOutputCol("features")
    var idfmodel = tFIDF.fit(feturedata)
    var idfdata = idfmodel.transform(feturedata)
    idfdata.select($"category", $"words", $"features").foreach(println)
    idfdata.foreach(println)

  }
}
