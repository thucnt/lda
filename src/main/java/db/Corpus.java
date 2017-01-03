package db;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.sql.Blob;
import java.util.List;
import java.util.Properties;

/**
 * Created by thucnt on 1/3/17.
 */
public class Corpus {
    private static final org.apache.log4j.Logger LOGGER = org.apache.log4j.Logger.getLogger(Corpus.class);

    private static final String MYSQL_CONNECTION_URL = "jdbc:mysql://localhost:3306/mas";
    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PWD = "thuc1980";

    private static final SparkSession sparkSession =
            SparkSession.builder().master("local[*]").appName("Spark2JdbcDs").getOrCreate();

    public static void main(String[] args) {
        //JDBC connection properties
        final Properties connectionProperties = new Properties();
        connectionProperties.put("user", MYSQL_USERNAME);
        connectionProperties.put("password", MYSQL_PWD);

        final String dbTable =
                "(select idPaper, title, abstract from paper) as paper_content";

        //Load MySQL query result as Dataset
        Dataset<Row> jdbcDF =
                sparkSession.read().jdbc(MYSQL_CONNECTION_URL, dbTable, connectionProperties);

        List<Row> l = jdbcDF.collectAsList();
        for (Row r : l) {
            long id = r.getLong(0);
            String title = r.getString(1);
            byte[] ab = (byte[]) r.get(2);
            if (ab != null){
                System.out.println(new String(ab,0));
            }
        }

    }

    public static JavaRDD<Tuple2<String, String>> getPapers(){
        //JDBC connection properties
        final Properties connectionProperties = new Properties();
        connectionProperties.put("user", MYSQL_USERNAME);
        connectionProperties.put("password", MYSQL_PWD);

        final String dbTable =
                "(select idPaper, title, abstract from paper) as paper_content";

        //Load MySQL query result as Dataset
        Dataset<Row> jdbcDF =
                sparkSession.read().jdbc(MYSQL_CONNECTION_URL, dbTable, connectionProperties);
        JavaRDD<Tuple2<String,String>> result = jdbcDF.javaRDD().map(new Function<Row, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> call(Row v1) throws Exception {
                String id = "" + v1.getLong(0);
                StringBuilder content = new StringBuilder();
                String title = v1.getString(1);
                if (title != null)
                    content.append(title);
                byte[] abs =  (byte[]) v1.get(2);
                if (abs != null)
                    content.append("\t" + new String(abs,0));
                Tuple2<String,String> tuple = new Tuple2<String,String>(id,content.toString());
                return tuple;
            }
        });
        return result;
    }
}
