package alogrith.graph
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by jack on 2017/12/16.
  */
object SparkGraphJoin {
  def main(args: Array[String]) {
    //设置运行环境
    val conf = new SparkConf().setAppName("SNSAnalysisGraphX").setMaster("local[4]")
    val sc = new SparkContext(conf)

    //创建点RDD
    val usersVertices: RDD[(VertexId, (String, String))] = sc.parallelize(Array(
      (1L, ("Spark", "scala")), (2L, ("Hadoop", "java")),
      (3L, ("Kafka", "scala")), (4L, ("Zookeeper", "Java "))))
    //创建边RDD
    val usersEdges: RDD[Edge[String]] = sc.parallelize(Array(
      Edge(2L, 1L, "study"), Edge(3L, 2L, "train"),
      Edge(1L, 2L, "exercise"), Edge(4L, 1L, "None")))

    val salaryVertices :RDD[(VertexId,(String,Long))] =sc.parallelize(Array(
      (1L,("Spark",30L)),(2L, ("Hadoop", 15L)),
      (3L, ("Kafka", 10L)), (5L, ("parameter server", 40L))
    ))
    val salaryEdges: RDD[Edge[String]] = sc.parallelize(Array(
      Edge(2L, 1L, "study"), Edge(3L, 2L, "train"),
      Edge(1L, 2L, "exercise"), Edge(5L, 1L, "None")))

    //构造Graph
    val graph = Graph(usersVertices, usersEdges)
    val graph1 = Graph(salaryVertices, salaryEdges)
    //outerJoinVertices操作,
    val joinGraph = graph.outerJoinVertices(graph1.vertices) { (id, attr, deps) =>
      deps match {
        case Some(deps) => deps
        case None => 0
      }
    }

//    val joinGraph = graph.outerJoinVertices(salaryVertices) { (id, attr, deps) =>
//      deps match {
//        case Some(deps) => deps
//        case None => 0
//      }
//    }

    joinGraph.vertices.collect.foreach(println)
    sc.stop()
  }
}
