package alogrith.graph

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
  * Created by jack on 2017/12/15.
  */
object SparkGraphTwo {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("spark graphx two demo").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //创造用户
    val userArray = Array(
      (3L, ("jhp", "架构师")),
      (7L, ("bruce", "功夫演员")),
      (5L, ("shuzhu", "高级工程师")),
      (2L, ("yaojie", "产品经理")),
      (4L, ("wangping", "高级工程师")))

    //创造用户之间的关系
    val relationArray = Array(
      Edge(3L, 7L, "粉丝"),
      Edge(5L, 3L, "下属"),
      Edge(2L, 5L, "同事"),
      Edge(5L, 7L, "偶像"),
      Edge(4L, 5L, "同事"))

    //创建用户订单信息
    val userOrderArray = Array(
      ("jhp",String.valueOf(Random.nextInt(100000))),
      ("bruce",String.valueOf(Random.nextInt(100000))),
      ("shuzhu",String.valueOf(Random.nextInt(100000))),
      ("yaojie",String.valueOf(Random.nextInt(100000))),
      ("wangping",String.valueOf(Random.nextInt(100000)))
    )

    val userRDD: RDD[(VertexId, (String, String))] = sparkContext.parallelize(userArray)

    val relationRDD:RDD[Edge[String]] = sparkContext.parallelize(relationArray)

    val orderRDD:RDD[(String,String)] = sparkContext.parallelize(userOrderArray)

    val graph = Graph(userRDD,relationRDD)

    println("======找出图中所有的顶点=============")
    graph.vertices.collect.foreach{
      case (id,(name,profession))=>println(s"$name is $profession")
    }
    println


    println("======找出图中所有包含属性的边的图形=============")
    graph.edges.collect.foreach(e=>println(s"${e.srcId} to ${e.dstId} attr ${e.attr}"))
    println

    println("======遍历所有的triples=============")
    val facts: RDD[String] = graph.triplets.map(triplet => triplet.srcAttr._1 + " and " + triplet.dstAttr._1 +" "+ "is"+ " "+ triplet.attr)
    facts.collect.foreach(println(_))
    println


    println("============按照triples条件进行遍历====================")
    graph.triplets.filter(trip=>trip.attr.equals("同事")).collect.foreach(trip=>println(s"${trip.srcAttr._1} and ${trip.dstAttr._1} is ${trip.attr}"))
    println()

    println("=============过滤子图subgraph=========================")
    val validGraph = graph.subgraph(vpred = (id, attr) => attr._2.equals("高级工程师"))
    validGraph.vertices.collect.foreach(println(_))
    println()
    println("================遍历所有子图的triplets=======================")
    validGraph.triplets.map(triplet => triplet.srcAttr._1 + " and " + triplet.dstAttr._1 +" "+ "is"+ " "+ triplet.attr).collect.foreach(println(_))
    println()

    println("====================用户连接订单操作组成新的图形=====================================")
    //创建用户订单
    case class Order(user:String,orderId: String)
    //创建新的图
    val userOrderGraph = graph.mapVertices{
      case (id,(name,profession))=>Order(name,String.valueOf(Random.nextInt(100000)))
    }
    userOrderGraph.vertices.collect.foreach{
      case (id,Order(name,oid))=>println(name,oid)
    }

    sparkContext.stop()
  }
}
