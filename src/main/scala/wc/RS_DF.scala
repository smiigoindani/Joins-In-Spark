package rs

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
//import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.types.IntegerType

object ReduceSide_df {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nrs.ReduceSide_df <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("ReduceSide_df")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

		// Delete output directory, only to ease local development; will not work on AWS. ===========
//    val hadoopConf = new org.apache.hadoop.conf.Configuration
//    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
//    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
		// ================
    
   

    val linesrdd = sc.textFile(args(0))
    val Counter = sc.longAccumulator("Counter")

    //Convert Rdd to DataFrame 
 
    // Reads the input file and puts a threshold and emits X and Y 
    val XtoY_rdd =linesrdd.map(line => {
        line.split(",")
      }).filter(users => users(0).toInt < 10000 && users(1).toInt < 10000)
        .map(users => (users(0), users(1)))

    // Create alias for every column such that it can be used after merging
    val df = XtoY_rdd.toDF("user","follower")
    val XY_df = df.select(col("user").as("user_X"),col("follower").as("user_Y1")).as("xtoy")
    val YZ_df = df.select(col("user").as("user_Y2"),col("follower").as("user_Z1")).as("ytoz")
    val ZX_df = df.select(col("user").as("user_Z2"),col("follower").as("user_Z")).as("ztox")


    //Check for the conditions such that for the First Join XY = YZ where (X!=Z)and for the second join 
    //YZ = XY where Z == X
    val pathLength2 = XY_df.join(YZ_df).where($"xtoy.user_Y1" === $"ytoz.user_Y2" && !($"xtoy.user_X" === $"ytoz.user_Z1"))

    val triangle = pathLength2.as("path").join(ZX_df).where($"path.user_Z1"===$"ztox.user_Z2" && $"path.user_X" === $"ztox.user_Z")
    // Counts number of rows and adds to the counter
    triangle.foreach(f=> {Counter.add(1)})

    
    //triangle.printSchema()
    
  
    //triangle.tordd.saveAsTextFile(args(1))
    //println("SocialTriangleCount: "+ triangle.count()/3)
    println("Accumulator Value : " + Counter.value/3)
    //traingle.rdd.map(_.toString())saveAsTextFile(args(1))
 
    //val followercount = linesrdd.map(line => (line.split(",")(1))).filter(userid => userid.toInt % 100 == 0).map(result => (result,1))reduceByKey(_ + _)
   
    
  }
}
