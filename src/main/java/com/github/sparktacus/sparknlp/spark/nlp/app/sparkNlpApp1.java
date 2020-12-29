package com.github.sparktacus.sparknlp.spark.nlp.app;


//-- import packages


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;

import org.apache.spark.api.java.JavaSparkContext;


import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import scala.Tuple2;
import scala.collection.Seq;
import scala.collection.JavaConverters;

import org.apache.spark.ml.Model;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;

import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline;
import com.johnsnowlabs.nlp.pretrained.ResourceDownloader;
import com.johnsnowlabs.nlp.SparkNLP;
import com.johnsnowlabs.nlp.annotators.pos.perceptron.PerceptronModel;
import com.johnsnowlabs.nlp.annotators.sda.vivekn.ViveknSentimentModel;
import com.johnsnowlabs.nlp.LightPipeline;
import org.apache.spark.ml.PipelineModel;

import org.apache.spark.sql.*;
//-------------------------------

public class sparkNlpApp1 {
	
	public static void main(String args[]) throws Exception {
		
		System.out.println("//-- spark NLP 04-28-2020");
		/*
		List<String> jars_path = new ArrayList<>();
	    jars_path.add("hdfs://localhost:9000/jars/spark_nlp/spark.nlp.app-0.0.1-SNAPSHOT00.jar");
	    Seq<String> jarsSeq = scala.collection.JavaConverters.asScalaBuffer(jars_path).toSeq();
	    //Seq<java.lang.String> jars = seq(
				*/
				
		
		SparkConf conf = new SparkConf()
				.setAppName("Java Spark NLP")
				.setJars(new String[] {"hdfs://localhost:9000/jars/spark_nlp/spark.nlp.app-0.0.1-SNAPSHOT02.jar"})
				//.set("spark.eventLog.enabled", "true")
				.setMaster("spark://pclocalhost:7077");
	    
		conf.set("spark.driver.memory", "1g");
	    conf.set("spark.executor.memory", "1g");

		System.out.println("+++++++++++++++++++++++++++++++++++++++++++");
		System.out.println("+        Spark  Configuration             +");
		System.out.println("+++++++++++++++++++++++++++++++++++++++++++");
		System.out.println("SparkConf configuration: \n" + conf.toDebugString());
		
		@SuppressWarnings("resource")
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		
		SparkSession spark = SparkSession.builder()
				  .config(sc.getConf())
				  .getOrCreate();
		
		//read csv data ad dataframe
		Dataset<Row> df = spark.read().option("header","true").csv("hdfs://localhost:9000/devsparkstreaming/datasets/vgames/vgames.csv");
		
		//df.show();
		
		
		//read txt file as RDD
		/*String inputFile = "hdfs://localhost:9000/devsparkstreaming/datasets/textfile/fiction_novel";

		JavaRDD<String> textFile = sc.textFile(inputFile);
		
		JavaPairRDD<String,Integer> wc = textFile
				 .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
				 .mapToPair(w -> new Tuple2 <> (w,1))
				 .reduceByKey((a,b) -> a + b);
				 */
	
		JavaRDD<String> inputFile = sc.textFile("hdfs://localhost:9000/devsparkstreaming/datasets/textfile/fiction_novel.txt");
		
		JavaRDD<String> rdd00 = inputFile.distinct();
		
		//System.out.println(rdd00.collect());
		
		
		JavaRDD<Integer> lineLengths = inputFile.map(s -> s.length());
		//int totalLength = lineLengths.reduce((a, b) -> a + b);
		//System.out.println(totalLength);
	
		//JavaPairRDD<String, Integer> wcounts = inputFile
		    //.flatMap(s -> Arrays.asList(s.split(" ")).iterator())
		    //.mapToPair(word -> new Tuple2<>(word, 1));
		    //.reduceByKey((a, b) -> a + b);
		/*
		JavaRDD<String> x = inputFile.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
		System.out.println(x.collect());
		
		JavaPairRDD<String, Integer> y = x.mapToPair(word -> new Tuple2<>(word, 1));
		System.out.println(y.collect());
		
		JavaPairRDD<String, Integer> z = y.reduceByKey((a, b) -> a + b);
		System.out.println(z.collect());
		
		
		System.out.println("-------------------------");
		*/
		JavaPairRDD<String, Integer> wcounts = inputFile
				.flatMap(s -> Arrays.asList(s.split(" ")).iterator())
				.mapToPair(word -> new Tuple2<>(word, 1))
	    		.reduceByKey((a, b) -> a + b);
		
		//wcounts.take(20).foreach(<String,Integer> r -> System.out.println(r));
		/*
		for(Tuple2<String,Integer> e : wcounts.collect()) {
			System.out.println(e);
		}*/
		//wcounts.saveAsTextFile("hdfs://localhost:9000/devsparkstreaming/eclipsedataoutput/wc_output0428-6");
		
		
		//-----------------------------
		//--- spark NLP
		//--------------------------------------------
		
		System.out.println("+++++++++++++++++++++++++++++++++++++++++++");
		System.out.println("+      Spark NLP Pipeline                 +");
		System.out.println("+++++++++++++++++++++++++++++++++++++++++++");
		//scala: val explainDocumentPipeline = PretrainedPipeline("explain_document_ml")
		//val annotations = explainDocumentPipeline.annotate("We are very happy about SparkNLP")
		//		println(annotations)
		//SparkNLP.version();
		//PretrainedPipeline explainDocumentPipeline = new PretrainedPipeline("explain_document_ml","en");
		//scala.collection.immutable.Map<String, Seq<String>> annotations = explainDocumentPipeline.annotate("We are very happy about SparkNLP");
		//System.out.println(annotations);
		
		
		//pre trained pipeline stored in hdfs (spark NLP Pipeline Name--> 'explain_document_ml_en')
		//hdfs://localhost:9000/sparkNlpPreTrainedModels/explain_document_ml_en_2.4.0_2.4_1580252705962
		
		PipelineModel nlp_pipeline = PipelineModel.load("hdfs://localhost:9000/sparkNlpPreTrainedModels/explain_document_ml_en_2.4.0_2.4_1580252705962");
		
		/*
		List<Integer, String> data = new ArrayList<>();
	    data.add(1, "Google has announced the release of a beta version of the popular TensorFlow machine learning library");
	    data.add(2, "The Paris metro will soon enter the 21st century, ditching single-use paper tickets for rechargeable electronic cards.");
	    */
		
		/*
	    Map<Integer, String> map = new HashMap<Integer, String>();
	    map.put(1, "Spark is a efficient framework to process data in high volume, and build statistics models");
	    map.put(2, "cronavirus has killed 1255455 people in Sandiego last night, more than cars accidents during the same period in 2019");
	    */
	    //Seq<Integer,String> dataSeq = scala.collection.JavaConverters.asScalaBuffer(data).toSeq();
		//hdfs://localhost:9000/devsparkstreaming/datasets/nlptest.csv
		//Dataset<Row> testData = spark.createDataFrame(map).toDF("id", "text");
		//*****Dataset<Row> dfnlp = spark.read().option("header","true").csv("hdfs://localhost:9000/devsparkstreaming/datasets/nlptest.csv");
		//********dfnlp.show();
		
		//apply model
		//****Dataset<Row>  annotations = nlp_pipeline.transform(dfnlp);
		//results
		//****annotations.show();
		
		Dataset<Row> dfnlp2 = spark.read().option("header", "true").csv("hdfs://localhost:9000/devsparkstreaming/datasets/textfile/trumptweets.csv");
		dfnlp2.createOrReplaceTempView("dftemp00");
		Dataset<Row> dfnlpinput = spark.sql("SELECT Tweet_Id as id,Tweet_text AS text FROM dftemp00");
		//dfnlpinput.show();
		
		Dataset<Row>  annotations = nlp_pipeline.transform(dfnlpinput);
		annotations.show();
		//-----------------------------------------------------
		
		//spark NL JSnow - Sentiment -eng
		//pipeline pretrained: hdfs://localhost:9000/sparkNlpPreTrainedModels/analyze_sentiment_en_2.4.0_2.4_1580483464667
		//text file : hdfs://localhost:9000/devsparkstreaming/datasets/textfile/trumptweets.csv
		System.out.println("+++++++++++++++++++++++++++++++++++++++++++");
		System.out.println("+      Spark NLP Sentiment Analysis       +");
		System.out.println("+          Trump tweets                   +");
		System.out.println("+++++++++++++++++++++++++++++++++++++++++++");
		
		//PipelineModel nlp_sent_pipeline = PipelineModel.load("");
		//String[] a = new String[] {"document", "token"};
		PipelineModel eng_sentiment = PipelineModel.load("hdfs://localhost:9000/sparkNlpPreTrainedModels/analyze_sentiment_en_2.4.0_2.4_1580483464667");
		//eng_sentiment.setOutputCol("sentiment");
		Dataset<Row>  sentiment_output = eng_sentiment.transform(dfnlpinput);
		sentiment_output.show();
		sentiment_output.select(
				"text",
				"sentiment.result"
				).show();
		
		//sentiment_output.printSchema();
		
		//sentiment_output.write()
			//.withColumn("ArrayOfString", col("ArrayOfString").cast("string"))
			//.write
			//.csv("hdfs://localhost:9000/devsparkstreaming/eclipsedataoutput/wc_output0429-010");
		/*
		Dataset<Row> dfnlp2 = spark.read().option("header", "true").csv("hdfs://localhost:9000/devsparkstreaming/datasets/textfile/trumptweets.csv");
		dfnlp2.createOrReplaceTempView("dftemp00");
		Dataset<Row> dfnlpinput = spark.sql("SELECT Tweet_Id,Tweet_text FROM dftemp00");
		dfnlpinput.show();
		
		Dataset<Row> nlp_sentiment_output = nlp_sent_pipeline.transform(dfnlpinput);
		nlp_sentiment_output.show();
		*/
		//stop session

			      
		
		spark.stop();
		sc.stop();
		
	}

}
