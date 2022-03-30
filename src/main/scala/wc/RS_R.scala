package rs

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
//import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.types.IntegerType



object ReduceSide_rdd {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.ReduceSide_rdd <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("ReduceSide_rdd")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

		// Delete output directory, only to ease local development; will not work on AWS. ===========
//    val hadoopConf = new org.apache.hadoop.conf.Configuration
//    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
//    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
		// ================
    
    
    val linesrdd = sc.textFile(args(0))
    val accum = sc.longAccumulator("SumAccumulator")
 
    // Reads the input file and puts a MAX threshold and emits X and Y 
    val XtoY =linesrdd.map(line => {
        line.split(",")
      }).filter(users => users(0).toInt < 10000 && users(1).toInt < 10000)
        .map(users => (users(0), users(1)))
    

    // Reverses the mapped RDD from X->Y to Y->X for join 
    val YtoZ = XtoY.map{case (user0,user1) => (user1,user0)}
   
    //println("print!"+XtoY.count()+"print!"+YtoZ.count())

    //Join XY and YZ to get Pathlength such that Y = Y and X!=Z
    //Map the output such that (XZ),Y
    val pathLength2 = XtoY.join(YtoZ).filter(collected => {
        val YZ = collected._2
        YZ._1 != YZ._2
        }).map{
            case(userY,(userZ,userX)) => ((userZ,userX),userY)
            }
     
    // Join the path lenth to X to Y such that X and Z are Equal
    val ZtoX = XtoY.map{case(user0,user1) => ((user0,user1),"")}
    val triangle1 = pathLength2.join(ZtoX)
    pathLength2.join(ZtoX).foreach(f=> {accum.add(1)})
   
   //val followercount = linesrdd.map(line => (line.split(",")(1))).filter(userid => userid.toInt % 100 == 0).map(result => (result,1))reduceByKey(_ + _)
   

    //triangle1.saveAsTextFile(args(1))
    //println("SocialTriangleCount: "+ triangle1.count()/3)
    println("Accumulator Value : " + accum.value/3)

  }
}
