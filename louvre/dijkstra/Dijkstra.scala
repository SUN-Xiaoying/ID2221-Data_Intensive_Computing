package src

import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD

import java.io._


object Dijkstra {
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
}