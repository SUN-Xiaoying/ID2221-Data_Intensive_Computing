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
          (7L, "N7")
        ))

    // Create an RDD for edges
    val edges: RDD[Edge[Double]] =
        sc.parallelize(Seq(
            Edge(1L, 2L, 7.0),    
            Edge(1L, 4L, 5.0),
            Edge(2L, 3L, 8.0), 
            Edge(2L, 4L, 9.0),
            Edge(2L, 5L, 7.0), 
            Edge(3L, 5L, 5.0), 
            Edge(4L, 5L, 15.0), 
            Edge(5L, 6L, 8.0), 
            Edge(5L, 7L, 9.0),
            Edge(6L, 7L, 11.0)  
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