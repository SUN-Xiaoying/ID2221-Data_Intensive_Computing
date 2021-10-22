package src

import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD

import java.io._
import src.Dijkstra.dijkstra
import src.Map.{sc, myGraph, exits}

object Main {

  def compare(a:(Double, String), b:(Double, String)) = {
    if(a._1<b._1) a else b
  }

  //Write results in txt
  def writeFile(filename: String, lines: Seq[(String,(Double, String))]): Unit = {
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    var str = ""
    bw.write(s"srcId\tshortest\tpath\n")
    for (line <- lines) {
      str = s"${line._1}\t\t${line._2._1}\t\t${line._2._2}\n"
      if(!str.contains(s"${Double.MaxValue}"))
        bw.write(str)
    }
    bw.close()
  }
  
  // remove paralyzed node from the graph
  def removeSingleNode[VD](g:Graph[VD,Double], miss:VertexId) = {
    // Remove missing vertices as well as the edges to connected to them
    val validGraph = g.subgraph(vpred = (id, attr) => id != miss)
    var data: Seq[(String,(Double, String))] = Seq[(String,(Double,String))]()
    var finalResults:Seq[(String,(Double, String))] = Seq[(String,(Double,String))]()
    dijkstra(validGraph, 1L).vertices.map(_._2).collect.foreach(
      r => {
        val arr = r._2.toArray
        val shortest = arr(0).toString.toDouble
        val path = arr(1).asInstanceOf[List[String]].mkString("->")
        data = data :+ (r._1.toString, (shortest,path))
      }
    )
    val theRDD = sc.parallelize(data)
    val rddReduced = theRDD.reduceByKey(compare(_,_))
    rddReduced.collect().foreach(y => finalResults = finalResults :+ (y._1,(y._2._1,y._2._2)))
    
    writeFile("d_paralyzed_results.txt",finalResults)
  }



  def main(args: Array[String]){
     
    // Test: Dijkstra 
    var data:Seq[(String,(Double, String))] = Seq[(String,(Double,String))]()
    var reducedResults:Seq[(String,(Double, String))] = Seq[(String,(Double,String))]()
    def leaveFromExit(exit:VertexId) = {
      var cur:Seq[(String,(Double, String))] = Seq[(String,(Double,String))]()
      dijkstra(myGraph, exit).vertices.map(_._2).collect.foreach(
        r => {
          val key = r._1.toString
          val arr = r._2.toArray
          val value = arr(0).toString.toDouble
          val path = arr(1).asInstanceOf[List[String]].mkString("->")
          
          data = data :+ (key, (value,path))
          cur = cur :+ (key, (value,path))
        }
      )
      writeFile(s"d_${exit}.txt",cur)
    }
    
    exits.foreach(exit => leaveFromExit(exit))    
    writeFile("d_total.txt",data)

    // Reduce the total results by srcId
    val theRDD = sc.parallelize(data)
    val rddReduced = theRDD.reduceByKey(compare(_,_))
    rddReduced.collect().foreach(y => reducedResults = reducedResults :+ y)
    
    writeFile("d_reduced_total.txt",reducedResults)

    // // Test: Paralyzed Nodes
    // removeSingleNode(myGraph, 5L)
  }
}
