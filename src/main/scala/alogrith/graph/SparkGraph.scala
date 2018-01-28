package alogrith.graph

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{GraphLoader, PartitionStrategy}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jack on 2017/12/15.
  */
object SparkGraph {

  /**
    * 测试pagerank算法
    * @param sparkContext
    * @param commonPath
    */
  def testPageRank(sparkContext:SparkContext,commonPath:String):Unit={
        val graph = GraphLoader.edgeListFile(sparkContext, commonPath+"followers.txt")
        val ranks = graph.pageRank(0.0001).vertices
        val users = sparkContext.textFile(commonPath+"users.txt").map { line =>
          val fields = line.split(",")
          (fields(0).toLong, fields(1))
        }
        val ranksByUsername = users.join(ranks).map {
          case (id, (username, rank)) => (username, rank)
        }
        println(ranksByUsername.collect().mkString("\n"))
  }


  /**
    * 测试直连通算法
    * @param sc
    * @param commonPath
    */
  def testConnectComponent(sc:SparkContext,commonPath:String):Unit={

        val graph = GraphLoader.edgeListFile(sc, commonPath + "followers.txt")
        val cc = graph.connectedComponents().vertices
        val users = sc.textFile(commonPath + "users.txt").map { line =>
          val fields = line.split(",")
          (fields(0).toLong, fields(1))
        }
        val ccByUsername = users.join(cc).map {
          case (id, (username, cc)) => (username, cc)
        }
        println(ccByUsername.collect().mkString("\n"))
  }

  /**
    * 测试三角形计算法
    * @param sc
    * @param commonPath
    */
  def testTriangleCount(sc: SparkContext,commonPath:String):Unit={

     val graph = GraphLoader.edgeListFile(sc, commonPath + "followers.txt", true).partitionBy(PartitionStrategy.RandomVertexCut)
     val triCounts = graph.triangleCount().vertices
     val users = sc.textFile(commonPath + "users.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    val triCountByUsername = users.join(triCounts).map { case (id, (username, tc)) =>
      (username, tc)
    }
     println(triCountByUsername.collect().mkString("\n"))

  }


  /**
    * 测试子图远行
    * @param sc
    * @param commonPath
    */
  def testSubGraphPageRank(sc: SparkContext,commonPath:String):Unit={

        val users = (sc.textFile(commonPath + "users.txt")
          .map(line => line.split(",")).map( parts => (parts.head.toLong, parts.tail) ))

        val followerGraph = GraphLoader.edgeListFile(sc, commonPath + "followers.txt")

        val graph = followerGraph.outerJoinVertices(users) {
          case (uid, deg, Some(attrList)) => attrList
          case (uid, deg, None) => Array.empty[String]
        }

        val subgraph = graph.subgraph(vpred = (vid, attr) => attr.size == 2)

        val pagerankGraph = subgraph.pageRank(0.001)

        val userInfoWithPageRank = subgraph.outerJoinVertices(pagerankGraph.vertices) {
          case (uid, attrList, Some(pr)) => (pr, attrList.toList)
          case (uid, attrList, None) => (0.0, attrList.toList)
        }

        println(userInfoWithPageRank.vertices.top(5)(Ordering.by(_._2._1)).mkString("\n"))
  }

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("spark graph").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val commonPath ="/soft/idea/spark-ml/src/main/resources/"

    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    println("===========测试pagerank算法========================")
    testPageRank(sc,commonPath)
    println

    println("=============测试connectcomponent算法===============")
    testConnectComponent(sc,commonPath)
    println

    println("=============testTriangleCount===============")
    testTriangleCount(sc,commonPath)

    println
    println("=============testTriangleCount===============")
    testSubGraphPageRank(sc,commonPath)

  }
}
