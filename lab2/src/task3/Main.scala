package task3

import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD

// class Graph[VD, ED]{
//   val vertices: VertexRDD[VD]
//   val edges: EdgeRDD[ED]
// }


object Main {
  def main(args: Array[String]){
    val conf = new SparkConf().setMaster("local[2]")
      .setAppName("Task 3")
      .setSparkHome(System.getenv("SPARK_HOME"))
      .setJars(SparkContext.jarOfClass(this.getClass).toList)

    val sc = new SparkContext(conf)

    // Create an RDD for the vertices
    val vertices: RDD[(VertexId, (String, Int))] =
      sc.parallelize(Seq(
        (1L, ("Alice", 28)), 
        (2L, ("Bob", 27)),
        (3L, ("Charlie", 65)), 
        (4L, ("David", 42)),
        (5L, ("Ed", 55)),
        (6L, ("Fran", 50)),
        (7L, ("Alex", 55))
      ))

    // Create an RDD for edges
    val edges: RDD[Edge[Int]] =
      sc.parallelize(Seq(
        Edge(2L, 1L, 7),    
        Edge(2L, 4L, 2),
        Edge(3L, 2L, 4), 
        Edge(3L, 6L, 3),
        Edge(4L, 1L, 1), 
        Edge(5L, 2L, 2), 
        Edge(5L, 3L, 8), 
        Edge(5L, 6L, 3), 
        Edge(7L, 5L, 3),
        Edge(7L, 6L, 4)  
      ))

    // Build the initial Graph
    val graph:Graph[(String, Int), Int] = Graph(vertices, edges)

    // // 1. Display the names of the users that are at least 30 years old.
    graph.vertices.filter(vertex => vertex._2._2 > 29).collect.foreach(v => println(v._2._1 + " is " + v._2._2))

    // // 2. Display who likes who
    // graph.triplets.map(
    //   triplet => triplet.srcAttr._1 + " likes " + triplet.dstAttr._1
    // ).collect.foreach(println(_))

    // 3. someone likes someone else more than 5 times
    // graph.triplets.filter(triplet => triplet.attr > 5).collect.foreach( 
    // triplet => println(triplet.srcAttr._1 + " loves " + triplet.dstAttr._1 + " " + triplet.attr + " times."))

    // Define a class to more clearly model the user property
    case class User(name: String, age: Int, inDeg: Int, outDeg: Int)

    val initUserGraph: Graph[User, Int] = graph.mapVertices{
      case (id, (name, age)) => User(name, age, 0, 0)
    }

    // Joins the vertices with the input RDD and returns a new graph with the vertex properties
    val userGraph = initUserGraph.outerJoinVertices(initUserGraph.inDegrees){
      case (id, u, inDegOpt) => User(u.name, u.age, inDegOpt.getOrElse(0), 0)
    }.outerJoinVertices(initUserGraph.outDegrees){
      case (id, u, outDegOpt) => User(u.name, u.age, u.inDeg, outDegOpt.getOrElse(0))
    }
    // // 4. Print the number of people who like each user
    // for ((id, property) <- userGraph.vertices.collect) {
    //   println(s"${property.name} is liked by ${property.inDeg} people.")
    // }

    // 5. Print the names of the users who are liked by the same number of people they like
    userGraph.vertices.filter{ 
        case (id, u) => u.inDeg == u.outDeg
    }.collect.foreach{
        case (id, property) => println(property.name +" likes and is liked by " + property.inDeg + " people.")
    }

    // userGraph.vertices.filter(user => user._2.inDeg == user._2.outDeg).collect.foreach{
    //     case (id, property) => println(property.name +" likes and is liked by " + property.inDeg + " people.")
    // }
    // 6. Find the oldest follower of each user
    val oldestFollower: VertexRDD[(String, Int)] = userGraph.aggregateMessages[(String, Int)](
      // sendMsg
      triplet => triplet.sendToDst((triplet.srcAttr.name, triplet.srcAttr.age)),
      // mergeMsg
      (a, b) => if (a._2 > b._2) a else b
    )

    userGraph.vertices.leftJoin(oldestFollower) { (id, user, optOldestFollower) =>
      optOldestFollower match {
        case None => s"${user.name} does not have any followers."
        case Some((name, age)) => s"${name} is the oldest follower of ${user.name}."
      }
    }.collect.foreach { case (id, str) => println(str) }
  }
}