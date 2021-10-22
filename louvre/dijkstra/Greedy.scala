package src

import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD

import java.io._


object Greedy {
 // Find the shortest path by greedy algorithm
  def greedy[VD](g:Graph[VD,Double], origin:VertexId) = {

    // indicate whether the vertex has been incorporated
    var g2 = g.mapVertices((vid,vd) => vid == origin)
      .mapTriplets(et => (et.attr,false))

    var nextVertexId = origin
    // loopd control
    var edgesAreAvailable = true

    do {
      // [Whether the edge has been incorporated, 
      // [Edge property, whether the vertex has been incorporated]]
      type tripletType = EdgeTriplet[Boolean,Tuple2[Double,Boolean]]

      val availableEdges =
        g2.triplets
          .filter(et => !et.attr._2
                        && (et.srcId == nextVertexId && !et.dstAttr
                            || et.dstId == nextVertexId && !et.srcAttr))

      edgesAreAvailable = availableEdges.count > 0

      if (edgesAreAvailable) {
        val smallestEdge = availableEdges
            .min()(new Ordering[tripletType]() {
              override def compare(a:tripletType, b:tripletType) = {
                Ordering[Double].compare(a.attr._1,b.attr._1)
              }
            })

        nextVertexId = Seq(smallestEdge.srcId, smallestEdge.dstId)
                      .filter(_ != nextVertexId)(0)

        g2 = g2.mapVertices((vid,vd) => vd || vid == nextVertexId)
              .mapTriplets(et => (et.attr._1,
                                  et.attr._2 ||
                                    (et.srcId == smallestEdge.srcId
                                      && et.dstId == smallestEdge.dstId)))
      }
    } while(edgesAreAvailable)

    g2
  }
}