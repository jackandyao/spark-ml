//
//import org.apache.spark.ml.feature.{Tokenizer,HashingTF}
//import org.apache.spark.ml.{Pipeline,PipelineModel}
//import org.apache.spark.ml.classification.LogisticRegression
//import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
//import org.apache.spark.ml.tuning.{CrossValidator,ParamGridBuilder}
//import org.apache.spark.mllib.linalg.Vector
//import org.apache.spark.sql.Row
//
//val training = sqlContext.createDataFrame(Seq(
//    (0L, "a b c d e spark", 1.0),
//    (1L, "b d", 0.0),
//    (2L, "spark f g h", 1.0),
//    (3L, "hadoop mapreduce", 0.0),
//    (4L, "b spark who", 1.0),
//    (5L, "g d a y", 0.0),
//    (6L, "spark fly", 1.0),
//    (7L, "was mapreduce", 0.0),
//    (8L, "e spark program", 1.0),
//    (9L, "a e c l", 0.0),
//    (10L, "spark compile", 1.0),
//    (11L, "hadoop software", 0.0)
//)).toDF("id","text","label")
//
//val tokenizer = new Tokenizer().
//setInputCol("text").
//setOutputCol("words")
//val hashingTF = new HashingTF().
//setInputCol(tokenizer.getOutputCol).
//setOutputCol("features")
//val lr = new LogisticRegression().
//setMaxIter(10)
//val pipeline = new Pipeline().
//setStages(Array(tokenizer,hashingTF,lr))
//
//val paramGrid = new ParamGridBuilder().
//addGrid(hashingTF.numFeatures, Array(10,100,1000)).
//addGrid(lr.regParam, Array(0.1,0.01)).
//build()
//
//val cv = new CrossValidator().
//setEstimator(pipeline).
//setEstimatorParamMaps(paramGrid).
//setEvaluator(new BinaryClassificationEvaluator()).
//setNumFolds(2)
//
//val cvModel = cv.fit(training)
//
//val test = sqlContext.createDataFrame(Seq(
//  (12L, "spark h d e"),
//  (13L, "a f c"),
//  (14L, "mapreduce spark"),
//  (15L, "apache hadoop")
//)).toDF("id","text")
//
//cvModel.transform(test).select("id","text","probability","prediction").collect().foreach{case Row(id: Long, text: String, probability: Vector, prediction: Double) => println(s"($id, $text) -> probability=$probability, prediction=$prediction")}
//
