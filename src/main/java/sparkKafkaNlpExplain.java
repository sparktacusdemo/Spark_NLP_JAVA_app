
//package sparktacus.kafka;

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

public class sparkKafkaNlpExplain {

  public static void main(String[] args) {
    
		System.out.println("+++++++++++++++++++++++++++++++++++++++++++");
		System.out.println("+        Spark  Configuration             +");
		System.out.println("+++++++++++++++++++++++++++++++++++++++++++");
		
		SparkConf sparkConf = new SparkConf()
				.setAppName("Java Spark streaming For Kafka")
				.setMaster("spark://pclocalhost:7077");
        
		
		sparkConf.setJars(new String[] {
				"hdfs://localhost:9000/jars/spark_kafka/spark.nlp.app-0.0.1-SNAPSHOT_NLPexplaindoc_00.jar",
				"hdfs://localhost:9000/jars/kafka-clients-0.10.2.2.jar",
				"hdfs://localhost:9000/jars/spark-sql-kafka-0-10_2.11-2.4.4.jar",
                "hdfs://localhost:9000/jars/spark_nlp/scala/spark-nlp_2.11-2.4.5.jar",
                //"hdfs://localhost:9000/jars/spark_nlp/scala/spark-nlp-gpu_2.11-2.4.4.jar",
                "hdfs://localhost:9000/jars/jsr166e-1.1.0.jar",
                "hdfs://localhost:9000/jars/spark-cassandra-connector_2.11-2.4.3.jar",
                "hdfs://localhost:9000/jars/hadoop-hdfs-3.1.3.jar"
		});
		
		sparkConf.set("spark.driver.memory", "1g");
		sparkConf.set("spark.executor.memory", "2g");
		sparkConf.set( "spark.hadoop.fs.defaultFS","hdfs://localhost:9000");
		sparkConf.set( "spark.executorEnv.hadoop.fs.defaultFS","hdfs://localhost:9000");
	
	    //FileSystem.get(sparkConf);
		
		/*
		sparkConf.set("spark.hadoop.yarn.resourcemanager.hostname","XXX");
		sparkConf.set("spark.hadoop.yarn.resourcemanager.address","XXX:8032");
		sparkConf.set("spark.yarn.access.namenodes", "hdfs://XXXX:8020,hdfs://XXXX:8020");
		sparkConf.set("spark.yarn.stagingDir", "hdfs://XXXX:8020/user/hduser/");
		*/
		
		//StreamingContext ssc = new StreamingContext(conf); // <-- for Spark Streaming RDD
		
		SparkSession spark = SparkSession.builder()
	            .config(sparkConf)
	            .config("spark.sql.streaming.checkpointLocation","hdfs://localhost:9000/sparkStreamingStructKafka/sparkappcheckpoints_nlpexplaindoc_03")
	            .config("spark.streaming.stopGracefullyOnShutdown","true")
	            .getOrCreate(); 
	        


	   /* conf.setJars(new String[] {
	            "hdfs://localhost:9000/jars/spark_nlp/java/gmailproject/spark.nlp.app-0.0.1-SNAPSHOT01.jar",
	            "hdfs://localhost:9000/jars/spark_nlp/scala/spark-nlp_2.11-2.4.5.jar",
	            "hdfs://localhost:9000/jars/spark-cassandra-connector_2.11-2.4.3.jar",
	            "hdfs://localhost:9000/jars/jsr166e-1.1.0.jar"
	            });
	    */
		//spark.sparkContext();



		System.out.println("SparkConf configuration: \n" + sparkConf.toDebugString());
		
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
		
		System.out.println("\n+++++++++++++++++++++++++++++++++++++++++++");
		System.out.println("+        Data Processing - Spark NLP      +");
		System.out.println("+++++++++++++++++++++++++++++++++++++++++++\n");
		
		SparkNLP.version();
		
		//PipelineModel mypipemodelloaded = PipelineModel.load("hdfs://localhost:9000/sparkNlpPreTrainedModels/explain_document_dl_en_2.4.3_2.4_1584626657780");    
		PipelineModel mypipemodelloaded = PipelineModel.load("/sparkNlpPreTrainedModels/recognize_entities_dl_en_2.4.3_2.4_1584626752821");
		Dataset<Row> pipmodtransfo_output = mypipemodelloaded.transform(sqlDF);
		
		StreamingQuery queryok01 = pipmodtransfo_output.select(
				"text",
				"entities",
				"entities.result"
				)
				  .writeStream()
				  .outputMode("update")
			      .format("console")                                                                                                                                                                                                                                                                           
				  .start();
			
				
				//query.stop();
				try {
					queryok01.awaitTermination();
					
					System.out.println("\n+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
					System.out.println("--------//java app run end at last thoooooooooooooooooooooooo//-----");
					System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
					
				} catch (StreamingQueryException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		
				
				
				//------------------------------------------------------------------------------------------------------------------------
		 // pickup config files off classpath
		
		 // explicitely add other config files
		 // PASS A PATH NOT A STRING!
		 
		 //hadoopConf.addResource(new Path("/home/hadoop/conf/core-site.xml"));
		/*
		 try {
		    SparkContext sc = spark.sparkContext();
		    Configuration hadoopConf = new Configuration(sc.hadoopConfiguration());
		    hadoopConf.addResource(new Path("/usr/lib/hadoop/etc/hadoop/core-site.xml"));
		    hadoopConf.addResource(new Path("/usr/lib/hadoop/etc/hadoop/hdfs-site.xml"));
				//FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"),hadoopConf);
		    FileSystem fs = FileSystem.get(hadoopConf);
			//PipelineModel pretrainedpipmodel = PipelineModel.load("hdfs://localhost:9000/sparkNlpPreTrainedModels/onto_recognize_entities_sm_en_2.4.0_2.4_1579730599257");

		    
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		*/
		
		//hdfs://localhost:9000/sparkNlpPreTrainedModels/onto_recognize_entities_sm_en_2.4.0_2.4_1579730599257/stages/3_WORD_EMBEDDINGS_MODEL_48cffc8b9a76/storage/EMBEDDINGS_glove_100d
		

		
		
       // PipelineModel sentiment_eng = PipelineModel.load("hdfs://localhost:9000/sparkNlpPreTrainedModels/analyze_sentiment_en_2.4.0_2.4_1580483464667");
        //Dataset<Row> sentiment_eng_output = sentiment_eng.transform(sqlDF);
		/*
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
		*/
		
	    
		//PretrainedPipeline pipeline00000 = new PretrainedPipeline("explain_document_dl","en");
		
        //hadoop config
	    
       /*
        try {
            SparkContext sc = spark.sparkContext();
            Configuration hadoopConf = new Configuration(sc.hadoopConfiguration());
            hadoopConf.addResource("/usr/lib/hadoop/etc/hadoop/core-site.xml");
            hadoopConf.addResource("/usr/lib/hadoop/etc/hadoop/hdfs-site.xml");
			FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"),hadoopConf);
			Path p0000 = new Path("hdfs://localhost:9000/sparkNlpPreTrainedModels/onto_recognize_entities_sm_en_2.4.0_2.4_1579730599257");
			PipelineModel pipmod = PipelineModel.load(p0000.toString());	
			} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		} catch (URISyntaxException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
        --- */
        
        //try {
			//FileSystem fs = FileSystem.get(hadoopConf);
			
			//FSDataInputStream fsDataInputStream = fs.getHdfsFileSystem().open(new Path("/user/saurav/input.txt"));
			//System.out.println(fs.getUri());
			//PipelineModel explain_document_en = PipelineModel.load("hdfs://localhost:9000/sparkNlpPreTrainedModels/explain_document_dl_en_2.4.3_2.4_1584626657780");
			
			/*FileSystem.setDefaultUri(hadoopConf, "hdfs://localhost:9000");
			
			Path p1111 = new Path("hdfs://localhost:9000/sparkNlpPreTrainedModels/explain_document_dl_en_2.4.3_2.4_1584626657780");
	        p1111.toUri();
	        URI uri = p1111.toUri();
	        String thatScheme = uri.getScheme();
	        System.out.println(uri);
	        System.out.println(thatScheme);
	       
			PipelineModel explain_document_en = PipelineModel.load(p1111.toString());*/
	        
			//PretrainedPipeline pipeline00000 = new PretrainedPipeline("explain_document_dl","en");
			
			//PretrainedPipeline.fromDisk("hdfs://localhost:9000/sparkNlpPreTrainedModels/explain_document_dl_en_2.4.3_2.4_1584626657780");
			//PretrainedPipeline.fromDisk("hdfs://localhost:9000/cache_pretrained/explain_document_dl_en_2.4.3_2.4_1584626657780");
			
			/*Path fshomepath = fs.getHomeDirectory();
			System.out.println(fshomepath);
			FileStatus[] files = fs.globStatus(new Path("hdfs://localhost:9000/sparkNlpPreTrainedModels/recognize_*"));
		    for (FileStatus f: files) {
	    		System.out.println("hdfs file --> " + f.getPath().toString());
	    		PipelineModel explain_document_en = PipelineModel.load(f.getPath().toString());
		    }*/
					//initialize(new Path("hdfs://localhost:9000/sparkNlpPreTrainedModels/recognize_entities_dl_en"));
			//Path path00 = new Path("hdfs://localhost:9000/sparkNlpPreTrainedModels/recognize_entities_dl_en");
			
			/*
			PipelineModel explain_document_en = PipelineModel.load(path00.toString());
			Dataset<Row> explain_document_en_output = explain_document_en.transform(sqlDF); 
			StreamingQuery query01 = explain_document_en_output
					  .writeStream()
					  .outputMode("update")
				      .format("console")                                                                                                                                                                                                                                                                           
					  .start();
			
			try {
				query01.awaitTermination();
			} catch (StreamingQueryException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}*/
	
			
		    /*FileStatus[] files = fs.listStatus(new Path("hdfs://localhost:9000/tempdir/dir1/test"));
		    Map<String,String> fsmap=new HashMap<String,String>();  
		    //s = files.map(FileStatus f => f.getPath());
		    for (FileStatus f: files) {
		    		//System.out.println("hdfs file --> " + f.toString());
		    	fsmap.put("FileFD -> " + f.getPath().toUri(), f.getPath().getName().toString());
		    	//System.out.println((f.getPath().getName().toString()));
		    }
		    
		    fsmap.entrySet()  
		      //Returns a sequential Stream with this collection as its source  
		      .stream()  
		      //Sorted according to the provided Comparator  
		      //.sorted(Map.Entry.comparingByValue())  
		      //Performs an action for each element of this stream  
		      .forEach(System.out::println);*/
		    
		//} catch (IOException e1) {
			// TODO Auto-generated catch block 
		//	e1.printStackTrace();
		//}
        
        
        		/*

        val model_load = fs.listStatus(new Path("hdfs://localhost:9000/devsparkstreaming/datasets/studentsadmission/Admission_PredictLIBSVM.csv"))
        
        */
        //---------
        
     
		
		//PipelineModel explain_document_en = PipelineModel.load("file:///home/hadoop/Downloads/recognize_entities_dl_en_2.4.3_2.4_1584626752821");
	    //PipelineModel pipmod_recognize_entities_bert_en = PipelineModel.load("hdfs://localhost:9000/sparkNlpPreTrainedModels/recognize_entities_bert_en_2.4.3_2.4_1584626853422");
        
        //PretrainedPipeline pipmod = new PretrainedPipeline("analyze_sentiment","en");
		/*
		Dataset<Row> pipmod_output = pipmod.transform(sqlDF);
		
		StreamingQuery query01 = pipmod_output
				  .writeStream()
				  .outputMode("update")
			      .format("console")                                                                                                                                                                                                                                                                           
				  .start();
			
				
				//query.stop();
				try {
					query01.awaitTermination();
					System.out.println("--------//java app run end//-----");
					
				} catch (StreamingQueryException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		
			
				*/
				
				
		/*
		  val query = sqlCsvDF.writeStream
				    .queryName("SPARKSQL") 
				    .outputMode("append")
				    .format("console")
				    .trigger(Trigger.ProcessingTime("5 seconds"))
				    //.start()
				  
				  //query.awaitTermination()  */
		
		
		
		/*
	Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("group.id", "group-1");
    props.put("enable.auto.commit", "true");
    props.put("auto.commit.interval.ms", "1000");
    props.put("auto.offset.reset", "earliest");
    props.put("session.timeout.ms", "30000");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    
    
    StructField[] structFields = new StructField[]{
            new StructField("text", DataTypes.StringType, true, Metadata.empty()),
    };

    StructType schema = new StructType(structFields);
	List<Row> rows = new ArrayList<>();
	List<String> dataRows = new ArrayList<String>();
	
    @SuppressWarnings("resource")
	KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
    
    kafkaConsumer.subscribe(Arrays.asList("test"));
    
    while (true) {
      ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
      for (ConsumerRecord<String, String> record : records) {
        System.out.println("Partition: " + record.partition() + " Offset: " + record.offset()
            + " Value: " + record.value() + " ThreadID: " + Thread.currentThread().getId());
        
        dataRows.add(record.value());
        JavaRDD<String> rddKafka = sc.parallelize(dataRows);
        rddKafka.foreach(f -> System.out.println(f));
      }
    }
    */
    
    

  }
  
  
}