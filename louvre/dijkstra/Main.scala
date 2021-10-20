package src

import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD

import java.io._


object Main {
  //Write results in txt
  def writeFile(filename: String, lines: Seq[String]): Unit = {
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(s"srcId\tshortest\tpath\n")
    for (line <- lines) {
      if(!line.contains(s"${Double.MaxValue}"))
        bw.write(line)
    }
    bw.close()
  }
  // Find the shortest path by dijkstra algorithm
  def dijkstra[VD](g:Graph[VD,Double], origin:VertexId) = {
    //Initialize the table
    var g2 = g.mapVertices(
      (vid, _)=>(false, if(vid == origin) 0 else Double.MaxValue,List[VertexId]())
    )
    // g.vertices.collect.foreach(println)

    for(i <- 1L to g.vertices.count-1){
      val currentVertexId = 
        g2.vertices.filter(!_._2._1)
          .fold((0L,(false, Double.MaxValue, List[VertexId]())))(
            (a,b) => if(a._2._2 < b._2._2) a else b)
          ._1
        
      val newDistances = 
        g2.aggregateMessages[(Double,List[VertexId])](
          ctx => if(ctx.srcId == currentVertexId)
            ctx.sendToDst((ctx.srcAttr._2 + ctx.attr, 
              ctx.srcAttr._3 :+ ctx.srcId)),
          (a,b) => if (a._1 < b._1) a else b
        )

      g2 = g2.outerJoinVertices(newDistances)((vid, vd, newSum)=> {
        val newSumVal =
          newSum.getOrElse((Double.MaxValue,List[VertexId]()))
        (vd._1 || vid == currentVertexId,
        math.min(vd._2, newSumVal._1),
        if (vd._2 < newSumVal._1) vd._3 else newSumVal._2)}
      )
    }
    // g2.vertices.collect.foreach(i => println("G2: "+i+"\n"))

    g.outerJoinVertices(g2.vertices)((vid, vd, dist) =>
      (vd, dist.getOrElse((false,Double.MaxValue,List[VertexId]()))
        .productIterator.toList.tail))
  }
  // remove paralyzed node from the graph
  def removeSingleNode[VD](g:Graph[VD,Double], miss:VertexId) = {
    // Remove missing vertices as well as the edges to connected to them
    val validGraph = g.subgraph(vpred = (id, attr) => id != miss)
    var results:Seq[String] = List[String]()
    
    dijkstra(validGraph, 1L).vertices.map(_._2).collect.foreach(
      r => {
        val arr = r._2.toArray
        val path = arr(1).asInstanceOf[List[String]].mkString("->")
        results = results :+ s"${r._1}\t\t${arr(0)}\t\t${path}\n"
      }
    )
    writeFile("d_paralyzed.txt",results)
  }

  def main(args: Array[String]){
    val conf = new SparkConf().setMaster("local[2]")
    .setAppName("Louvre")
    .setSparkHome(System.getenv("SPARK_HOME"))
    .setJars(SparkContext.jarOfClass(this.getClass).toList)

    val sc = new SparkContext(conf)

    // Create an RDD for vertices
    val vertices = 
      sc.makeRDD(Seq(
          (1L, "N1"), 
          (2L, "N2"),
          (3L, "N3"), 
          (4L, "N4"),
          (5L, "N5"),
          (6L, "N6"),
          (7L, "N7"),
          (8L, "N8"),
          (9L, "N9"),
          (10L, "N10"),
          (11L, "N11"),
          (12L, "N12"),
          (13L, "N13"),
          (14L, "N14"),
          (15L, "N15"),
          (16L, "N16"),
          (17L, "N17"),
          (18L, "N18"),
          (19L, "N19"),
          (20L, "N20"),
          (21L, "N21"),
          (22L, "N22"),
          (23L, "N23"),
          (24L, "N24"),
          (25L, "N25"),
          (26L, "N26"),
          (27L, "N27"),
          (28L, "N28")
        ))

    // Create an RDD for edges
    val edges: RDD[Edge[Double]] =
        sc.parallelize(Seq(
            Edge(1L, 2L, 150.0), 
            Edge(3L, 2L, 70.0), 
            Edge(8L, 2L, 90.0), 
            Edge(1L, 3L, 120.0), 
            Edge(3L, 4L, 75.0), 
            Edge(9L, 4L, 100.0), 
            Edge(10L, 4L, 70.0), //kanske bort?
            Edge(1L, 5L, 180.0), 
            Edge(12L, 5L, 80.0), 
            Edge(1L, 6L, 200.0), 
            Edge(3L, 6L, 130.0), 
            Edge(4L, 6L, 110.0), 
            Edge(5L, 6L, 60.0), 
            Edge(12L, 6L, 90.0), 
            Edge(14L, 7L, 125.0),
            Edge(9L, 8L, 60.0),
            Edge(13L, 9L, 100.0),
            Edge(4L, 10L, 70.0),
            Edge(9L, 10L, 75.0),
            Edge(11L, 10L, 150.0),
            Edge(12L, 11L, 325.0),//kanske bort?
            Edge(13L, 11L, 325.0),
            Edge(15L, 11L, 100.0),
            Edge(11L, 12L, 75.0),
            Edge(15L, 12L, 75.0),
            Edge(16L, 15L, 75.0),
            Edge(7L, 17L, 200.0),
            Edge(17L, 18L, 60.0),
            Edge(9L, 19L, 160.0),
            Edge(18L, 19L, 75.0),
            Edge(10L, 20L, 95.0),
            Edge(19L, 20L, 150.0),
            Edge(21L, 20L, 100.0),
            Edge(11L, 21L, 140.0),
            Edge(11L, 22L, 175.0),
            Edge(23L, 22L, 75.0),
            Edge(12L, 23L, 210.0),
            Edge(22L, 23L, 65.0),// kanske bort?
            Edge(24L, 23L, 125.0),
            Edge(15L, 24L, 120.0),
            Edge(17L, 25L, 130.0),
            Edge(26L, 25L, 70.0),
            Edge(26L, 17L, 135.0),
            Edge(27L, 26L, 80.0),
            Edge(19L, 27L, 130.0),
            Edge(28L, 27L, 150.0),
            Edge(21L, 28L, 80.0)
        ))

    // Build the initial Graph
    val myGraph = Graph(vertices, edges)

    // Test: Dijkstra 
    var results:Seq[String] = List[String]()
    
    // Exit 1
    dijkstra(myGraph, 1L).vertices.map(_._2).collect.foreach(
      r => {
        val arr = r._2.toArray
        val path = arr(1).asInstanceOf[List[String]].mkString("->")
        results = results :+ s"${r._1}\t\t${arr(0)}\t\t${path}\n"
      }
    )
    writeFile("d_1.txt",results)

    results= List[String]()
    // Exit 13
    dijkstra(myGraph, 13L).vertices.map(_._2).collect.foreach(
      r => {
        val arr = r._2.toArray
        val path = arr(1).asInstanceOf[List[String]].mkString("->")
        results = results :+ s"${r._1}\t\t${arr(0)}\t\t${path}\n"
      }
    )
    writeFile("d_13.txt",results)

    results= List[String]()
    // Exit 14
    dijkstra(myGraph, 14L).vertices.map(_._2).collect.foreach(
      r => {
        val arr = r._2.toArray
        val path = arr(1).asInstanceOf[List[String]].mkString("->")
        results = results :+ s"${r._1}\t\t${arr(0)}\t\t${path}\n"
      }
    )
    writeFile("d_14.txt",results)
    // Test: Paralyzed Nodes
    removeSingleNode(myGraph, 5L)
  }
}