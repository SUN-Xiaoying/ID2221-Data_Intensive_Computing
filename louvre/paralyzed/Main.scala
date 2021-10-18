package src

import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD

object Main {

  // remove paralyzed node from the graph
  def removeSingleNode(g:Graph[VD, Double], miss:VertexId) = {
    val validGraph = g.subgraph(vpred = (id, attr) => id != miss)
    validGraph.vertices.collect.foreach(println)
  }

  // Find the shortest path by dijkstra algorithm
  def dijkstra[VD](g:Graph[VD, Double], origin:VertexId) = {
    //Initialize the table
    var g2 = g.mapVertices(
      (vid, _)=>(false, if(vid == origin) 0 else Double.MaxValue)
    )
    // g.vertices.collect.foreach(println)
    // (4,(false,1.7976931348623157E308,List()))
    // (6,(false,1.7976931348623157E308,List()))
    // (2,(false,1.7976931348623157E308,List()))
    // (1,(false,0.0,List()))
    // (3,(false,1.7976931348623157E308,List()))
    // (7,(false,1.7976931348623157E308,List()))
    // (5,(false,1.7976931348623157E308,List()))
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
    g2.vertices.collect.foreach(i => println("G2: "+i+"\n"))


    g.outerJoinVertices(g2.vertices)((vid, vd, dist) => 
      (vd, dist.getOrElse((false, Double.MaxValue))._2)
    )
    
  }

  def main(args: Array[String]){
    val conf = new SparkConf().setMaster("local[2]")
    .setAppName("Louvre")
    .setSparkHome(System.getenv("SPARK_HOME"))
    .setJars(SparkContext.jarOfClass(this.getClass).toList)

    val sc = new SparkContext(conf)

    // (1L, 0)...(7L, 0)
    // val vertices: VertexRDD[Int] = 
    //   VertexRDD(sc.parallelize(1L until 7L).map(id => (id, 0)))
    val vertices = 
      sc.makeRDD(Array(
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
        sc.parallelize(Array(
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
    // val graph: Graph[(String, Int), Int] = Graph(vertices, edges)
    val myGraph:Graph[VD, Double] = Graph(vertices, edges)
    removeSingleNode(myGraph, 1L)
    // dijkstra(myGraph, 1L).vertices.map(_._2).collect.foreach(r => println("RESULT: "+r+"\n"))
  }
}