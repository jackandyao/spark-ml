//package alogrith.graph
//
//import org.apache.spark.graphx.{EdgeDirection, EdgeTriplet, Pregel}
//
///**
//  * Created by jack on 2017/12/20.
//  */
//class SparkGrpahDenifine {
//
//  //二楼邻居算法
//  def sendMsgFunc(edge:EdgeTriplet[Int, Int]) = {
//    if(edge.srcAttr <= 0){
//      if(edge.dstAttr <= 0){
//        // 如果双方都小于0，则不发送信息
//        Iterator.empty
//      }else{
//        // srcAttr小于0，dstAttr大于零，则将dstAttr-1后发送
//        Iterator((edge.srcId, edge.dstAttr - 1))
//      }
//    }else{
//      if(edge.dstAttr <= 0){
//        // srcAttr大于0，dstAttr<0,则将srcAttr-1后发送
//        Iterator((edge.dstId, edge.srcAttr - 1))
//      }else{
//        // 双方都大于零，则将属性-1后发送
//        val toSrc = Iterator((edge.srcId, edge.dstAttr - 1))
//        val toDst = Iterator((edge.dstId, edge.srcAttr - 1))
//        toDst ++ toSrc
//      }
//    }
//  }
//
//  val friends = Pregel(
//    graph.mapVertices((vid, value)=> if(vid == 1) 2 else -1),
//
//    // 发送初始值
//    -1,
//    // 指定阶数
//    2,
//    // 双方向发送
//    EdgeDirection.Either
//  )(
//    // 将值设为大的一方
//    vprog = (vid, attr, msg) => math.max(attr, msg),
//    //
//    sendMsgFunc,
//    //
//    (a, b) => math.max(a, b)
//  ).subgraph(vpred = (vid, v) => v >= 0)
//
//  println("\n\n~~~~~~~~~ Confirm Vertices of friends ")
//  friends.vertices.collect.foreach(println(_))
//  // (4,1)
//  // (8,0)
//  // (2,1)
//  // (1,2)
//  // (3,0)
//  // (5,0)
//
//  sc.stop
//
//
//  // 多图进行合并
//
//  def mergeGraphs(g1:Graph[String,String], g2:Graph[String,String]) = {
//    val v = g1.vertices.map(_._2).union(g2.vertices.map(_._2)).distinct.zipWithIndex
//    def edgesWithNewVertexIds(g:Graph[String,String]) =
//      g.triplets.map(et => (et.srcAttr, (et.attr,et.dstAttr))).join(v).map(x => (x._2._1._2, (x._2._2,x._2._1._1))).join(v).map(x => new Edge(x._2._1._1,x._2._2,x._2._1._2))
//    Graph(v.map(_.swap),edgesWithNewVertexIds(g1).union(edgesWithNewVertexIds(g2)))
//  }
//
//  val philosophers = Graph(
//    sc.makeRDD(Seq(
//      (1L, "Aristotle"),(2L,"Plato"),(3L,"Socrates"),(4L,"male"))),
//    sc.makeRDD(Seq(
//      Edge(2L,1L,"Influences"),
//      Edge(3L,2L,"Influences"),
//      Edge(3L,4L,"hasGender"))))
//
//  val rdfGraph = Graph(
//    sc.makeRDD(Seq(
//      (1L,"wordnet_philosophers"),(2L,"Aristotle"),
//      (3L,"Plato"),(4L,"Socrates"))),
//    sc.makeRDD(Seq(
//      Edge(2L,1L,"rdf:type"),
//      Edge(3L,1L,"rdf:type"),
//      Edge(4L,1L,"rdf:type"))))
//
//  val combined = mergeGraphs(philosophers, rdfGraph)
//
//  combined.triplets.foreach(
//    t => println(s"${t.srcAttr} --- ${t.attr} ---> ${t.dstAttr}"))
//
//
//}

