package test

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by thucnt on 1/4/17.
  */
object TestMySQL extends App{

  val conf = new SparkConf().setAppName("MY_APP_NAME").setMaster("local[*]")

  val sc = new SparkContext(conf)

  val sqlContext = new SQLContext(sc)

  val dataframe_mysql = sqlContext.read
    .format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/mas")
    .option("driver", "com.mysql.jdbc.Driver")
    .option("dbtable", "(select idPaper, title, abstract from paper LIMIT 100) as paper_content")
    .option("user", "root")
    .option("password", "thuc1980")
    .load()
  dataframe_mysql.show();
}
