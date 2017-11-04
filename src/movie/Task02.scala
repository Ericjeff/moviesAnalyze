package movie

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashSet

object Task02 {
  def main(args: Array[String]): Unit = {

    //参数初始化
    val conf = new SparkConf().setAppName("task02").setMaster("local")

    val sc = new SparkContext(conf)

    //数据加载
    val userRDDs = sc.textFile("file:///H:/movie/users.dat")
    val ratingRDDs = sc.textFile("file:///H:/movie/ratings.dat")
    val moviesRDDs = sc.textFile("file:///H:/movie/movies.dat")

    //数据提取
    //users:RDD (userID,age)
    val users = userRDDs.map(_.split("::")).filter(x=>x(1).equals("M")&&(x(2).toInt>=18||x(2).toInt<=24))

    //Array[String]
    val  userlist = users.map(x=>x(0)).collect()

    //broadcast
    val userSet = HashSet()++userlist
    val broadcastUserSet = sc.broadcast(userSet)

    //join RDD
    val topKmovies = ratingRDDs.map(_.split("::")).map{
      x=>(x(0),x(1))
    }.filter(
      x=>broadcastUserSet.value.contains(x._1)
    ).map(x=>(x._2,1))
      .reduceByKey(_+_).map(x=>(x._2,x._1))
      .sortByKey(false).map(x=>(x._2,x._1)).take(10)


    //print result
    val movieID2name = moviesRDDs.map(_.split("::")).map(x=>(x(0),x(1))).collect().toMap

    topKmovies.map(x=>(movieID2name.getOrElse(x._1,null),x._2)).foreach(println)

    sc.stop()


    /*//ratings :RDD (userID,movieID)
    val ratings = ratingRDDs.map(_.split("::")).map(x=>(x(0),x(1)))

    //userratings:RDD (userId,(occupation,MovieID))
    val userratings = users.join(ratings).map(x=>x._2)

    //movies:RDD (movieID,title)
    val movies = moviesRDDs.map(_.split("::")).map(x=>(x(0),x(1)))

    //userratingsmoves:RDD (title)
    val userratingsmoves = movies.join(userratings).map(x=>((x._2)._1,1)).reduceByKey(_+_)

    //sorted
    val sorted = userratingsmoves.map(x=>(x._2,x._1)).sortBy()
    userratingsmoves.foreach(println)

    */


  }

}
