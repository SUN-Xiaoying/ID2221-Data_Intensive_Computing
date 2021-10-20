package src

import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD

import java.io._
import src.Dijkstra.dijkstra
import src.Map.{myGraph, exits}

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
     
    // Test: Dijkstra 
    var results:Seq[String] = Seq[String]()

    def leaveFromExit(exit:VertexId) = {
      var cur = Seq[String]()
      dijkstra(myGraph, exit).vertices.map(_._2).collect.foreach(
        r => {
          val arr = r._2.toArray
          val path = arr(1).asInstanceOf[List[String]].mkString("->")
          cur = cur :+ s"${r._1}\t\t${arr(0)}\t\t${path}\n"
          results = results :+ s"${r._1}\t\t${arr(0)}\t\t${path}\n"
        }
      )
      writeFile(s"d_${exit}.txt",cur)
    }

    exits.foreach(exit => leaveFromExit(exit))

    writeFile("d_total.txt",results)
    // Test: Paralyzed Nodes
    removeSingleNode(myGraph, 5L)
  }
}