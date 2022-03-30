package rs

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager

object ReplicatedJoin {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nrep.RepRMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("ReplicatedJoin")
    val sc = new SparkContext(conf)
    val accum = sc.longAccumulator;
    val textFile = sc.textFile(args(0))
    
    // read the text file and form a rdd and emit user_X and user_Y
    val XtoY =
      textFile.map(line => {
        line.split(",")
      }).filter(users => users(0).toInt < 40000 && users(1).toInt < 40000)
        .map(users => (users(0).toInt, users(1).toInt))

    // Create a hashmap such that all the uniques values related to a key gets emitted
    val userMap = XtoY.map(rdd => (rdd._1, Set(rdd._2)))
                            .reduceByKey(_ ++ _)


    // Create a hashmap to broadcast
    val broadcastmap = sc.broadcast(userMap.collect.toMap)

    // Join First on XY = YZ such that X!=Z and Y =YtoZ
    // Second join is such that XY = YZ where X == Z
    val Triangle = XtoY.map {
     case (userX, userY) => broadcastmap.value.getOrElse(userY, Set[Int]()).foreach {
       userZ => if(userZ != userX && broadcastmap.value.getOrElse(userZ, Set[Int]()).contains(userX)) {
         accum.add(1)
       }
     }
   }

    Triangle.collect()
    println("Triangle Count " + accum.value/3)
  }
}