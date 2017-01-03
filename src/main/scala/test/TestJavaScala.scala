package test
import db.Corpus
import org.apache.spark.api.java.function.VoidFunction

/**
  * Created by thucnt on 1/3/17.
  */
object TestJavaScala extends App{
  db.Corpus.getPapers.foreach(new VoidFunction[(String, String)] {
    override def call(t: (String, String)) = {
      println(s"id: ${t._1}")
      println(s"content: ${t._2}")
    }
  })
}
