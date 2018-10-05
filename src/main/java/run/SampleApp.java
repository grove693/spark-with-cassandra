package run;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.HashMap;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import app.CassandraApp;

public class SampleApp {

	public static String INPUT_LOCATION_MOVIE_FILE = "movies.csv";
	public static String INPUT_LOCATION_MOVIE_RATINGS_FILE = "ratings.csv";

	public static void main(String[] args) throws Exception {
		
		CassandraApp.readFromDB(createCassandraClusterConfig());
	

	}
	
	private static SparkConf createCassandraClusterConfig() {
		SparkConf conf =  new SparkConf();
		conf.setAppName("Cassandra App");
		conf.setMaster("local");
		conf.set("spark.cassandra.connection.host", "127.0.0.1,10.0.75.2");
	    conf.set("spark.cassandra.connection.port", "9042");
	    conf.set("spark.sql.warehouse.dir", "D:/sparkSample/warehouse");

		return conf;
	}

}
