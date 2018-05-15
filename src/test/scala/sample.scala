import org.apache.spark.SparkConf
import org.apache.spark.graphx.{Edge, GraphLoader, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object sample {

  def main (args: Array[String]): Unit = {
    print("Hello World" +"\n")

    val conf = new SparkConf().setMaster("local[2]")
    val spark = SparkSession
      .builder
      .appName("SampleApp").config(conf)
      .getOrCreate()
    val sc = spark.sparkContext


    //$example on$
    // Load the edges as a graph
    val graph = GraphLoader.edgeListFile(sc, "twitter_combined.txt")

    // Run PageRank
    val ranks = graph.pageRank(0.0001).vertices

    val vertices = graph.vertices

    def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
      if (a._2 > b._2) a else b
    }
    val maxInDegree: (VertexId, Int)  = graph.inDegrees.reduce(max)
    val maxOutDegree: (VertexId, Int) = graph.outDegrees.reduce(max)
    val maxDegrees: (VertexId, Int)   = graph.degrees.reduce(max)

    print("Result")

    // Print the result
  //  println(ranksByUsername.collect().mkString("\n"))
    print(ranks.collect().mkString("\n"))
     //$example off$

  print("\n")

    print("Indegree :"+maxInDegree + "\n")
    print("OutDegree :"+maxOutDegree + "\n")
    print("Degrees :"+maxDegrees + "\n")

    ranks.foreach(println)

    /**
      * As ranks is in the (vertex ID, pagerank) format,
      * swap it to make it in the (pagerank,vertex ID) format:
      */

    val swappedRanks = ranks.map(_.swap)


    swappedRanks.foreach(println)


    /**
      * Sort to get the highest ranked pages first:
      */

    val sortedRanks = swappedRanks.sortByKey(false)


    sortedRanks.foreach(println)

    /**
      * Get the highest ranked page:
      */

    val highest = sortedRanks.first

    highest.toString().mkString(" ").foreach(println)

    /**
      * The preceding command gives the vertex id,
      * which you still have to look up to see the actual title with rank. Let's do a join:
      */

    val allJoin = ranks.join(vertices)


    /**
      * Sort the joined RDD again after converting from the
      * (vertex ID, (page rank, title))format to the (page rank, (vertex ID, title)) format:
      */

    val finalAll = allJoin.map(v => (v._2._1, (v._1, v._2._2))).
      sortByKey(false)

    /**
      * Print the top five ranked pages
      */

    print("Top five pageRank and vertices number")
    finalAll.collect().take(5).foreach(println)

    print("\n")

    print("Max Indegree :"+maxInDegree + "\n")
    print("Max OutDegree :"+maxOutDegree + "\n")
    print("Max Degrees :"+maxDegrees + "\n")



    val inDeg: VertexRDD[Int]= graph.inDegrees

    spark.stop()
  }


}
