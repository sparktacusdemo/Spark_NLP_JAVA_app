package sparktacus.kafka;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
//---------------------------------
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.EmptyRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.*;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;
import java.util.Iterator; 

//--

import scala.Tuple2;
import scala.collection.Seq;
import scala.collection.JavaConverters;

import org.apache.spark.ml.Model;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;

import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline;
import com.johnsnowlabs.nlp.pretrained.ResourceDownloader;
import com.johnsnowlabs.nlp.SparkNLP;
import com.johnsnowlabs.nlp.annotators.Tokenizer;
import com.johnsnowlabs.nlp.annotators.TokenizerModel;
import com.johnsnowlabs.nlp.annotators.pos.perceptron.PerceptronModel;
import com.johnsnowlabs.nlp.annotators.sbd.pragmatic.SentenceDetector;
import com.johnsnowlabs.nlp.annotators.sda.pragmatic.SentimentDetector;
import com.johnsnowlabs.nlp.annotators.sda.vivekn.ViveknSentimentApproach;
import com.johnsnowlabs.nlp.annotators.sda.vivekn.ViveknSentimentModel;
import com.johnsnowlabs.nlp.AnnotatorApproach;
import com.johnsnowlabs.nlp.DocumentAssembler;
import com.johnsnowlabs.nlp.Finisher;
import com.johnsnowlabs.nlp.HasInputAnnotationCols;
import com.johnsnowlabs.nlp.HasOutputAnnotationCol;
import com.johnsnowlabs.nlp.HasOutputAnnotatorType;
import com.johnsnowlabs.nlp.LightPipeline;
import  com.johnsnowlabs.nlp.util.regex.RuleFactory;
import com.johnsnowlabs.nlp.base.*;
import com.johnsnowlabs.nlp.annotator.*;


//-- for hdfs
import org.apache.spark.sql.SaveMode;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;


import java.nio.file.FileSystems;

//----------------------------

public class kafkaConsumerTool {

  public static void main(String[] args) {
    
		System.out.println("+++++++++++++++++++++++++++++++++++++++++++");
		System.out.println("+        Spark  Configuration             +");
		System.out.println("+++++++++++++++++++++++++++++++++++++++++++");
		
		SparkConf conf = new SparkConf()
				.setAppName("Java Spark streaming For Kafka")
				.setMaster("spark://pclocalhost:7077");
        
		
		conf.setJars(new String[] {
				"hdfs://localhost:9000/jars/spark_kafka/spark.nlp.app-0.0.1-SNAPSHOT_kafka05.jar",
				"hdfs://localhost:9000/jars/kafka-clients-0.10.2.2.jar",
				"hdfs://localhost:9000/jars/spark-sql-kafka-0-10_2.11-2.4.4.jar",
                "hdfs://localhost:9000/jars/spark_nlp/scala/spark-nlp_2.11-2.4.5.jar",
                "hdfs://localhost:9000/jars/spark_nlp/scala/spark-nlp-gpu_2.11-2.4.5.jar",
                "hdfs://localhost:9000/jars/jsr166e-1.1.0.jar",
                "hdfs://localhost:9000/jars/spark-cassandra-connector_2.11-2.4.3.jar",
                "hdfs://localhost:9000/jars/hadoop-hdfs-3.1.3.jar"
		});
		
	   
		
		//StreamingContext ssc = new StreamingContext(conf); // <-- for Spark Streaming RDD
		
		SparkSession spark = SparkSession.builder()
	            .config(conf)
	            .config("spark.sql.streaming.checkpointLocation","hdfs://localhost:9000/sparkStreamingStructKafka/sparkappcheckpoints02")
	            .config("spark.streaming.stopGracefullyOnShutdown","true")
	            .getOrCreate(); 
	        

	
	   /* conf.setJars(new String[] {
	            "hdfs://localhost:9000/jars/spark_nlp/java/gmailproject/spark.nlp.app-0.0.1-SNAPSHOT01.jar",
	            "hdfs://localhost:9000/jars/spark_nlp/scala/spark-nlp_2.11-2.4.5.jar",
	            "hdfs://localhost:9000/jars/spark-cassandra-connector_2.11-2.4.3.jar",
	            "hdfs://localhost:9000/jars/jsr166e-1.1.0.jar"
	            });
	    */
	    
		conf.set("spark.driver.memory", "1g");
	    conf.set("spark.executor.memory", "2g");


		System.out.println("SparkConf configuration: \n" + conf.toDebugString());
		
		/*@SuppressWarnings("resource")
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		SparkSession spark = SparkSession.builder()
				  .config(sc.getConf())
				  .getOrCreate();
	 */
		
		
		
		// Subscribe to 1 topic
		Dataset<Row> dfKafka = spark
	      .readStream()
		  .format("kafka")
		  .option("kafka.bootstrap.servers", "localhost:9092")
		  .option("subscribe", "sparkKafkaTopic2")
		  .load();
		
		//dfKafka.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");  convert Binary from binary to String
		
		dfKafka.createOrReplaceTempView("dftemp983");
		//Dataset<Row> sqlDF = spark.sql("SELECT CAST(key AS STRING) as key, CAST(value AS STRING) AS value FROM dftemp983");
		Dataset<Row> sqlDF = spark.sql("SELECT FLOOR(RAND() * 64899) as id, CAST(value AS STRING) AS text FROM dftemp983");
		// Start running the query that prints the running counts to the console
		
		/*StreamingQuery query = sqlDF.writeStream()
		  .outputMode("update")
		  .format("console")
		  .start();
	    */
		SparkNLP.version();
        PipelineModel sentiment_eng = PipelineModel.load("hdfs://localhost:9000/sparkNlpPreTrainedModels/analyze_sentiment_en_2.4.0_2.4_1580483464667");
        Dataset<Row> sentiment_eng_output = sentiment_eng.transform(sqlDF);
		
		StreamingQuery query00 = sentiment_eng_output.select(
				"text",
				"sentiment.result"
				).writeStream()
		  .outputMode("update")
	      .format("console")                                                                                                                                                                                                                                                                           
		  .start();
	
		
		
		try {
			query00.awaitTermination();
		} catch (StreamingQueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
    
    

  }
  
  
}



