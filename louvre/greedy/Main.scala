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
    for (line <- lines) {
        bw.write(line)
    }
    bw.close()
  }
  // Find the shortest path by dijkstra algorithm
  def dijkstra[VD](g:Graph[VD,Double], origin:VertexId) = {
    //Initialize the table
    var g2 = g.mapVertices(
      (vid, _)=>(false, if(vid == origin) 0 else Double.MaxValue)
    )
    // g.vertices.collect.foreach(println)

    for(i <- 1L to g.vertices.count-1){
      val currentVertexId = 
        g2.vertices.filter(!_._2._1)
          .fold((0L,(false, Double.MaxValue)))(
            (a,b) => if(a._2._2 < b._2._2) a else b)
          ._1
        
      val newDistances = 
        g2.aggregateMessages[Double](
          ctx => if(ctx.srcId == currentVertexId){
            ctx.sendToDst(ctx.srcAttr._2 + ctx.attr)
          },
          (a,b)=>math.min(a,b)
        )

      g2 = g2.outerJoinVertices(newDistances)((vid, vd, newSum)=> 
        (vd._1 || vid == currentVertexId,
          math.min(vd._2, newSum.getOrElse(Double.MaxValue))
        )
      )
    }
    // g2.vertices.collect.foreach(i => println("G2: "+i+"\n"))

    g.outerJoinVertices(g2.vertices)((vid, vd, dist) => 
      (vd, dist.getOrElse((false, Double.MaxValue))._2)
    )
  }
  // remove paralyzed node from the graph
  def removeSingleNode[VD](g:Graph[VD,Double], miss:VertexId) = {
    // Remove missing vertices as well as the edges to connected to them
    val validGraph = g.subgraph(vpred = (id, attr) => id != miss)
    var results:Seq[String] = List[String]()
    dijkstra(g, 1L).vertices.map(_._2).collect.foreach(
      r => results = results :+ s"${r._1}\t${r._2}\n"
    )
    writeFile("paralyzed.txt",results)
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
            Edge(2L, 1L, 150.0), 
            Edge(2L, 3L, 70.0), 
            Edge(2L, 8L, 90.0), 
        
            Edge(3L, 1L, 120.0), 
            
            Edge(4L, 3L, 75.0), 
            Edge(4L, 9L, 100.0), 
            Edge(4L, 10L, 70.0), 
            
            Edge(5L, 1L, 180.0), 
            Edge(5L, 12L, 80.0), 
            
            Edge(6L, 1L, 200.0), 
            Edge(6L, 3L, 130.0), 
            Edge(6L, 4L, 110.0), 
            Edge(6L, 5L, 60.0), 
            Edge(6L, 12L, 90.0), 
            
            Edge(7L, 14L, 125.0),
            
            Edge(8L, 9L, 60.0),
            
            Edge(9L, 13L, 100.0),
            
            Edge(10L, 4L, 70.0),
            Edge(10L, 9L, 75.0),
            Edge(10L, 11L, 150.0),
        
            Edge(11L, 12L, 325.0),
            Edge(11L, 13L, 325.0),
            Edge(11L, 15L, 100.0),
            
            Edge(12L, 11L, 75.0),
            Edge(12L, 15L, 75.0),
            
            Edge(15L, 16L, 75.0),
            
            Edge(17L, 7L, 200.0),
            
            Edge(18L, 17L, 60.0),
            
            Edge(19L, 9L, 160.0),
            Edge(19L, 18L, 75.0),
            
            Edge(20L, 10L, 95.0),
            Edge(20L, 19L, 150.0),
            Edge(20L, 21L, 100.0),
            
            Edge(21L, 11L, 140.0),
            
            Edge(22L, 11L, 175.0),
            Edge(22L, 23L, 75.0),
            
            Edge(23L, 12L, 210.0),
            Edge(23L, 22L, 65.0),
            Edge(23L, 24L, 125.0),
            
            Edge(24L, 15L, 120.0),
            
            Edge(25L, 17L, 130.0),
            Edge(25L, 26L, 70.0),
            
            Edge(26L, 17L, 135.0),
            Edge(26L, 27L, 80.0),
        
            Edge(27L, 19L, 130.0),
            Edge(27L, 28L, 150.0),
            
            Edge(28L, 21L, 80.0)
        ))

    // Build the initial Graph
    val myGraph = Graph(vertices, edges)

    // Test: Dijkstra 
    var results:Seq[String] = List[String]()
    
    dijkstra(myGraph, 1L).vertices.map(_._2).collect.foreach(
      r => results = results :+ s"${r._1}\t${r._2}\n"
    )
    writeFile("init.txt",results)
    // Test: Paralyzed Nodes
    removeSingleNode(myGraph, 5L)
  }
}
