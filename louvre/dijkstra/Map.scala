package src

import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD

import java.io._


object Map {
    val conf = new SparkConf().setMaster("local[2]")
    .setAppName("Louvre")
    .setSparkHome(System.getenv("SPARK_HOME"))
    .setJars(SparkContext.jarOfClass(this.getClass).toList)

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

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

    val exits:List[VertexId] = List(1L, 13L, 14L, 16L)
    
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

}
