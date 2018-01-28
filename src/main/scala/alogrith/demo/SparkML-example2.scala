//
//import org.apache.spark.ml.feature.{Tokenizer,HashingTF}
//import org.apache.spark.ml.{Pipeline,PipelineModel}
//import org.apache.spark.ml.classification.LogisticRegression
//import org.apache.spark.mllib.linalg.Vector
//import org.apache.spark.sql.Row
//
//val training = sqlContext.createDataFrame(Seq(
//  (0L, "a b c d e spark", 1.0),
//  (1L, "b d", 0.0),
//  (2L, "spark f g h", 1.0),
//  (3L, "hadoop mapreduce", 0.0)
//)).toDF("id","text","label")
//
//val tokenizer = new Tokenizer().
//setInputCol("text").
//setOutputCol("words")
//val hashingTF = new HashingTF().
//setNumFeatures(1000).
//setInputCol(tokenizer.getOutputCol).
//setOutputCol("features")
//val lr = new LogisticRegression().
//setMaxIter(10).setRegParam(0.01)
//
//val pipeline = new Pipeline().
//setStages(Array(tokenizer,hashingTF,lr))
//
//val model = pipeline.fit(training)
//pipeline.save("/tmp/sparkML-LRpipeline")
//model.save("/tmp/sparkML-LRmodel")
//
//val model2 = PipelineModel.load("/tmp/sparkML-LRmodel")
//
//val test = sqlContext.createDataFrame(Seq(
//  (4L, "spark h d e"),
//  (5L, "a f c"),
//  (6L, "mapreduce spark"),
//  (7L, "apache hadoop")
//)).toDF("id","text")
//
//model.transform(test).select("id","text","probability","prediction").collect().foreach{case Row(id: Long, text: String, probability: Vector, prediction: Double) => println(s"($id, $text) -> probability=$probability, prediction=$prediction")}
//
