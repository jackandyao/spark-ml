//package alogrith.app.news
//
//import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
//import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
//import org.apache.spark.mllib.linalg.{Vector, Vectors}
//import org.apache.spark.mllib.regression.LabeledPoint
//import org.apache.spark.sql.{Row, SQLContext}
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.rdd.RDD
//
///**
//  * Created by jack on 2017/11/17.
//  * navies 新闻分类
//  * spark mllib
//  */
//object SparkNaviesNewsUseMLLIB {
//
//  case class NewCategory (category: String, text: String)
//
//  //调用MLLIB算法
//  def naviesMLLib(training:RDD[LabeledPoint]):NaiveBayesModel ={
//    val lamba =1.0
//    val modelType ="multinomial"
//    var navieModel =NaiveBayes.train(training,lambda = lamba,modelType=modelType)
//    return navieModel
//  }
//
//  //保存模型
//  def saveNaviesModel(naiveBayesModel: NaiveBayesModel,sparkContext: SparkContext,path:String): Unit ={
//    naiveBayesModel.save(sparkContext,path)
//  }
//
//  //加载模型
//  def loadNaviesModel(sparkContext: SparkContext,naiveBayesModel: NaiveBayesModel,path:String): Unit ={
//  }
//
//
//  def main(args: Array[String]) {
//    val sparkConf = new SparkConf().setMaster("local").setAppName("spark navies news")
//    val sparkContext = new SparkContext(sparkConf)
//    val sqlContext = new SQLContext(sparkContext)
//    import sqlContext.implicits._
//    val newFilePath ="/soft/idea/spark-ml/src/main/resources/sougou-train.tar.gz"
//    var newRDD = sparkContext.textFile(newFilePath).map{
//      x =>
//        val data = x.split(",")
//        NewCategory(data(0),data(1))
//    }
//
//    var splitData = newRDD.randomSplit(Array(0.7,0.3))
//
//    var trainingDF = splitData(0).toDF()
//    var testDF =splitData(1).toDF()
//
//    //词语转换为数组
//    var tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
//    var wordData = tokenizer.transform(trainingDF)
//    print("worddata:")
//    wordData.select($"category",$"text",$"words").take(1)
//
//    //计算每个词的词频
//    var hashingTF = new HashingTF().setInputCol("words").setOutputCol("features").setNumFeatures(500000)
//    var fetureData = hashingTF.transform(wordData)
//
//    //计算tf-idf
//    var tfIDF = new IDF().setInputCol("features").setOutputCol("tfidf")
//    var tfidfModel = tfIDF.fit(fetureData)
//    var tfidf = tfidfModel.transform(fetureData)
//
//    //数据转换成navies输入的格式
//    var trainingRDD = tfidf.select($"category",$"features").map{
//      case Row(label : String ,features : Vector) =>
//        LabeledPoint(label.toDouble,Vectors.dense(features.toArray))
//    }
//
//    //测试数据集，做同样的特征表示及格式转换
//    var testwordsData = tokenizer.transform(testDF)
//    var testfeaturizedData = hashingTF.transform(testwordsData)
//    var testrescaledData = tfidfModel.transform(testfeaturizedData)
//    var testDataRdd = testrescaledData.select($"category",$"features").map {
//      case Row(label: String, features: Vector) =>
//        LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
//    }
//
//    //调用训练模型
//    var navieModel = naviesMLLib(trainingRDD)
//    //对测试数据集使用训练模型进行分类预测
//    val testpredictionAndLabel = testDataRdd.map(p => (navieModel.predict(p.features), p.label))
//    //统计分类的准确度
//    var testaccuracy = 1.0 * testpredictionAndLabel.filter(x => x._1 == x._2).count() / testDataRdd.count()
//    print("testaccuracy",testaccuracy)
//  }
//}
