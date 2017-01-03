package lda;

import scala.Tuple2;

import org.apache.spark.api.java.*;

import org.apache.spark.api.java.function.Function;

import org.apache.spark.mllib.clustering.DistributedLDAModel;

import org.apache.spark.mllib.clustering.LDA;

import org.apache.spark.mllib.linalg.Vector;

import org.apache.spark.mllib.linalg.Vectors;

import org.apache.spark.SparkConf;

public class LDAJava {

    public static void main(String[] args) {
        /*// Spark configuration details

        SparkConf conf = new SparkConf().setAppName("LDA");

        JavaSparkContext sc = new JavaSparkContext(conf);



        // Load and parse the data (sample_lda_data.txt is available with Spark installation)

        // word count vectors (columns: terms [vocabulary], rows [documents])

        String path = "data/mllib/sample_lda_data.txt";



        // Read data

        // creates a RDD with each line as an element

        // E.g., 1 2 6 0 2 3 1 1 0 0 3

        JavaRDD data = sc.textFile(path);



        // Map is a transformation that passes each data set element through a function

        // It returns a new RDD representing the results

        // Prepares input as numerical representation

        JavaRDD parsedData = data.map(new Function() {
                        public Vector call(String s) {

                        String[] sarray = s.trim().split(" ");

                        double[] values = new double[sarray.length];

                        for (int i = 0; i < sarray.length; i++)

                            values[i] = Double.parseDouble(sarray[i]);

                        return Vectors.dense(values);

                    }

                }

        );



        // Index documents with unique IDs

        // The transformation 'zipWithIndex' provides a stable indexing, numbering each element in its original order.

        JavaPairRDD corpus = JavaPairRDD.fromJavaRDD(parsedData.zipWithIndex().map(

                new Function, Tuple2>() {
            public Tuple2 call(Tuple2 doc_id) {

                return doc_id.swap();

            }

        }

    ));

        corpus.cache();



        // Cluster the documents into three topics using LDA

        // number of topics = 3

        DistributedLDAModel ldaModel = (DistributedLDAModel) new LDA().setK(3).run(corpus);

        // Topic and its term distribution

        // columns = 3 topics/ rows = terms (vocabulary)

        System.out.println("Topic-Term distribution: \n" + ldaModel.topicsMatrix());

        // document and its topic distribution

        // [(doc ID: [topic 1, topic 2, topic3]), (doc ID: ...]

        JavaRDD topicDist = ldaModel.topicDistributions().toJavaRDD();

        System.out.println("Document-Topic distribution: \n" + topicDist.collect());

        sc.close();*/

    }

}