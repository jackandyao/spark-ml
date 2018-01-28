package alogrith.graph

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jack on 2017/12/16.
  * 高级api 不太好理解的
  */
object SparkGraphAdvancedApi {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("spark graph").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //顶点
    val vertextArray = Array(
      (1L, ("Alice", 28)),
      (2L, ("Bob", 27)),
      (3L, ("Charlie", 65)),
      (4L, ("David", 42)),
      (5L, ("Ed", 55)),
      (6L, ("Fran", 50))
    )

//    val loveArray = Array(
//      (1L, ("gongfu", 2888)),
//      (2L, ("code", 2789)),
//      (3L, ("desgin", 6500)),
//      (4L, ("love", 4278)),
//      (5L, ("manage", 5589)),
//      (6L, ("eat", 5007))
//    )


    val loveArray = Array(
      (1L, "gongfu"),
      (2L, "code"),
      (3L, "desgin"),
      (4L, "love"),
      (5L, "manage"),
      (6L, "eat")
    )

    //边
    val edgeArray = Array(
      Edge(2L, 1L, 7),
      Edge(2L, 4L, 2),
      Edge(3L, 2L, 4),
      Edge(3L, 6L, 3),
      Edge(4L, 1L, 1),
      Edge(5L, 2L, 2),
      Edge(5L, 3L, 8),
      Edge(5L, 6L, 3)
    )

    //构造vertextRDD 和 edgeRDD
    val vertextRDD = sparkContext.parallelize(vertextArray)
    val edgeRDD = sparkContext.parallelize(edgeArray)

    //构建graph
    val graph = Graph(vertextRDD,edgeRDD)

    //构建用户爱好rdd
    val loveRDD = sparkContext.parallelize(loveArray)

    //连接操作 相当于leftjoin 如果能匹配到顶点那就取一个,如果匹配不到就保留原有的顶点
    //这个方法返回的是额外新增的属性
    val joinGraph =graph.outerJoinVertices(loveRDD){ (id,attr, options) =>
      options match {
        case Some(options) => options
          //println("opt:"+options)
        case None => null
      }
    }
    joinGraph.vertices.collect.foreach(println)


    println("=====直接把原有图的顶点和新图的顶点进行匹配,并返回对应的新图========")
    val ja = graph.joinVertices(loveRDD){(id,attr,options)=>attr}

    ja.vertices.collect.foreach(println)
    ja.edges.collect.foreach(println)




    println("===先把原有图的顶点设置为空,然后增加新的属性到图上==")
    val ja1 = graph.mapVertices((id, attr) => "").joinVertices(loveRDD){
      (id,attr,options)=>options
    }
    ja1.vertices.collect.foreach(println)




    sparkContext.stop()

  }
}
