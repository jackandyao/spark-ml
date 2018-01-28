//package alogrith.cf
//
//import org.apache.log4j.{ Level, Logger }
//import org.apache.spark.{ SparkConf, SparkContext }
//import org.apache.spark.rdd.RDD
//
//object ItemCF {
//  def main(args: Array[String]) {
//
//
//    val conf = new SparkConf().setAppName("ItemCF")
//    val sc = new SparkContext(conf)
//    Logger.getRootLogger.setLevel(Level.WARN)
//
//
//    val data_path = "/home/jb-huangmeiling/sample_itemcf2.txt"
//    val data = sc.textFile(data_path)
//    val userdata = data.map(_.split(",")).map(f => (ItemPref(f(0), f(1), f(2).toDouble))).cache()
//
//
//    val mysimil = new ItemSimilarity()
//    val simil_rdd1 = mysimil.Similarity(userdata, "cooccurrence")
//    val recommd = new RecommendedItem
//    val recommd_rdd1 = recommd.Recommend(simil_rdd1, userdata, 30)
//
//    simil_rdd1.collect().foreach { ItemSimi =>
//      println(ItemSimi.itemid1 + ", " + ItemSimi.itemid2 + ", " + ItemSimi.similar)
//    }
//    recommd_rdd1.collect().foreach { UserRecomm =>
//      println(UserRecomm.userid + ", " + UserRecomm.itemid + ", " + UserRecomm.pref)
//    }
//
//  }
//}
//
