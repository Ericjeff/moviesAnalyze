package movie

import org.apache.spark.{SparkConf, SparkContext}

object Task03 {
  def main(args: Array[String]): Unit = {

    //参数初始化
    val conf = new SparkConf().setAppName("task02").setMaster("local")

    val sc = new SparkContext(conf)

    //数据加载
    val userRDDs = sc.textFile("file:///H:/movie/users.dat")
    val ratingRDDs = sc.textFile("file:///H:/movie/ratings.dat")
    val moviesRDDs = sc.textFile("file:///H:/movie/movies.dat")

    //数据提取
    val ratings = ratingRDDs.map(_.split("::")).map(x=>
      (x(0),x(1),x(2))
    ).cache()

    val movies = moviesRDDs.map(_.split("::")).map(x=>
      (x(0),x(1))
    ).cache()


    val users = userRDDs.map(_.split("::")).map(x=>
      (x(0),x(1))
    ).cache()

    //print result
    val topKmovieMostMovie  = ratings.map(x=>
      (x._2,((x._3).toInt,1))
    ).reduceByKey((v1,v2)=>
      (v1._1+v2._1,v1._2+v2._2)
    ).map(x=>
      (x._2._1.toFloat/x._2._2,x._1)
    ).sortByKey(false).take(10)

    topKmovieMostMovie.foreach(println)

    val topPeopleViewMovie = ratings.map(x=>
      (x._1,1)
    ).reduceByKey(_+_).map(x=>
      (x._2,x._1)
    ).sortByKey(false).take(10)

    println("-----------------\n看过电影最多的前10个人\n-----------------")
    topPeopleViewMovie.foreach(println)



    val simpleM = ratings.map(x=>(x._1,x._2))
    val famale = users.filter(x=>(x._2).equals("F")).join(simpleM).map{x=>
      (x._2._2,1)
    }.reduceByKey(_+_).map(x=>
      (x._2,x._1)
    ).sortByKey(false).take(10)

    println("-----------------\n女性看最多的10部电影\n-----------------")
    famale.foreach(println)


    val male = users.filter(x=>(x._2).equals("M")).join(simpleM).map{x=>
      (x._2._2,1)
    }.reduceByKey(_+_).map(x=>
      (x._2,x._1)
    ).sortByKey(false).take(10)

    println("-----------------\n男性看过最多的10电影\n-----------------")
    male.foreach(println)
  }


}
