package movie

import org.apache.spark.{SparkConf,SparkContext}

object Tak01 {
  def main(args: Array[String]): Unit = {

    //初始化参初始化参数
    val conf = new SparkConf().setAppName("movie").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //数据加载
    val userRdd = sc.textFile("file:///H:/movie/users.dat")
    val ratingsRdd = sc.textFile("file:///H:/movie/ratings.dat")

    //数据抽取

    //users:Rdd(userID,(gender,age))
    val users = userRdd.map(_.split("::")).map{x=>(x(0),(x(1),x(2)))}

    //ratings:Rdd(userID,movieID,rating,timestamp)
    val ratings = ratingsRdd.map(_.split("::"))
    //ratings.foreach(println)

    //usermovie:RDD(userID,movieID)
    val MOVIE_ID = "2116"
    val usermovies = ratings.map(x=>(x(0),x(1))).filter(_._2.equals(MOVIE_ID))
    //usermovies.foreach(println)

    //join RDDs
    //userRating:RDD(userID,(movieID,(gender,age)))
    val userRatings = usermovies.join(users)
    //userRatings.collect().foreach(println)

    //movieuser:RDD(movieID,(movieTitle,(gender,age)))
    val userDistribution = userRatings.map(x=>
      ((x._2)._2,1)
    ).reduceByKey(_+_)

    //show
    userDistribution.collect().foreach(println)
    userDistribution.saveAsTextFile("file:///H:/result.txt")
    sc.stop()
  }
}
