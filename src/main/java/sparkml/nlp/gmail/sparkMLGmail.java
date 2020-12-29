package sparkml.nlp.gmail;


//--------------import packages------------------------------------------------------------

import com.google.api.services.gmail.model.ListMessagesResponse;
import com.google.api.services.gmail.model.Message;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;

import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.gmail.Gmail;
import com.google.api.services.gmail.GmailScopes;
//import com.google.api.services.gmail.model.Label;
//import com.google.api.services.gmail.model.ListLabelsResponse;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
//import java.security.GeneralSecurityException;
import java.util.*;
/*import java.util.Collections;
import java.util.List;
import java.util.Vector;
import java.util.ArrayList;
*/

import org.apache.spark.sql.types.StructType;

import com.google.api.client.http.HttpTransport;


import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.StringType;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.EmptyRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.SparkConf;


import com.google.gson.Gson;


/*spark NLP J Snow Labs                                                                                                                                   */
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



/*cassandra*/
import com.datastax.spark.connector.*;
import org.apache.spark.sql.cassandra.*;
import com.datastax.spark.connector.cql.CassandraConnectorConf;
import com.datastax.spark.connector.rdd.ReadConf;




//----------------------------------


public class sparkMLGmail {
	
	
    private static final String APPLICATION_NAME = "Gmail API Java - Spark ML - NLP";
    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
    private static final String TOKENS_DIRECTORY_PATH = "tokens";

	@SuppressWarnings("deprecation")
	public static void main(String args[]) throws Exception {
		
		System.out.println("\n+++++++++++++++++++++++++++++++++++");
		System.out.println("+                                 +");
		System.out.println("+  Spark ML - Java - NLP - Gmail  +");
		System.out.println("+           2020                  +");
		System.out.println("+++++++++++++++++++++++++++++++++++\n");
		
		//build Gmail_service
        final NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
        Credential cred = getCredentials(HTTP_TRANSPORT);
        //cred.refreshToken();
        
        /*
        Gmail Gmail_service = new Gmail.Builder(HTTP_TRANSPORT, JSON_FACTORY, getCredentials(HTTP_TRANSPORT))
                .setApplicationName(APPLICATION_NAME)
                .build();
       */
        
        Gmail Gmail_service = new Gmail.Builder(HTTP_TRANSPORT, JSON_FACTORY, cred)
                .setApplicationName(APPLICATION_NAME)
                .build();
		//run method to list messages
		//List<Message> l = llistMessages(Gmail_service, "awsdevdata441@gmail.com", "is:unread");
        List<Message>  lm = listMessages(Gmail_service, "awsdevdata441@gmail.com","from:klebron75@gmail.com");
		
		//--System.out.println(lm.get(1).get("id"));
        //String mess_id = String.valueOf(lm.get(0).get("id")); 
		//--Message message1 = getMessage(Gmail_service, "awsdevdata441@gmail.com", String.valueOf(lm.get(0).get("id")));//String.valueOf(lm.get(2).get("id")));
		//--Message message2 = getMessage(Gmail_service, "awsdevdata441@gmail.com", String.valueOf(lm.get(1).get("id")));
		

		//System.out.println(message1.getRaw());
		
		
		//SPARK
	    //Define custom schema
		
		//spark session conf
        
        /*Seq<String> jars = Seq(
                "hdfs://localhost:9000/jars/spark_nlp/scala/scala-nlp-0.0.1-SNAPSHOT05.jar",
                "hdfs://localhost:9000/jars/spark_nlp/scala/spark-nlp_2.11-2.4.5.jar",
                "hdfs://localhost:9000/jars/spark-cassandra-connector_2.11-2.4.3.jar",
                "hdfs://localhost:9000/jars/jsr166e-1.1.0.jar"
                );*/
        
		SparkConf conf = new SparkConf()
				.setAppName("Java Spark Gmail - Spams Predict Model")
				.setMaster("spark://pclocalhost:7077");
				//.setJars(new String[] {"hdfs://localhost:9000/jars/spark_nlp/spark.nlp.app-0.0.1-SNAPSHOT02.jar"})
				//.set("spark.eventLog.enabled", "true")
				
	    conf.setJars(new String[] {
                "hdfs://localhost:9000/jars/spark_nlp/java/gmailproject/spark.nlp.app-0.0.1-SNAPSHOT00.jar",
                "hdfs://localhost:9000/jars/spark_nlp/scala/spark-nlp_2.11-2.4.5.jar",
                "hdfs://localhost:9000/jars/spark-cassandra-connector_2.11-2.4.3.jar",
                "hdfs://localhost:9000/jars/jsr166e-1.1.0.jar"
                });
	    
	    
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
	
	   
	    //Dataset<Row> df_with_schema = spark.read().schema(schema).json(new Gson().toJson(message1));
		//List<String> jsonData = Arrays.asList(message1.toString());
		//--List<String> jsonData = Arrays.asList(message1.toString());
		        //"{\"name\":\"Yin\",\"address\":{\"city\":\"Columbus\",\"state\":\"Ohio\"}}");
		//JavaRDD<String> rddtemp = sc.parallelize(jsonData);
	
		//List<String> jsonData2 = Arrays.asList(message2.toString());
		//JavaRDD<String> rddtemp2 = sc.parallelize(jsonData2);
		
		//@SuppressWarnings("deprecation")
		//Dataset<Row> dset1 = spark.read().json(rddtemp);
		//@SuppressWarnings("deprecation")
		//Dataset<Row> dset2 = spark.read().json(rddtemp2);
		//dset2.show();
		/*
		dset1.show();
		dset2.show(); 
		
		// concatenate datasets
        Dataset<Row> dset3 = dset1.union(dset2); 
		
        dset3.show();
        */
		
		
		//Dataset<Row> dsetFinal = spark.emptyDataFrame();
		
		
		
	    StructField[] structFields = new StructField[]{
	            new StructField("historyId", DataTypes.StringType, true, Metadata.empty()),
	            new StructField("id", DataTypes.StringType, true, Metadata.empty()),
	            new StructField("internalDate", DataTypes.StringType, true, Metadata.empty()),
	            new StructField("labelIds", DataTypes.createArrayType(DataTypes.StringType), true, Metadata.empty()),
	            new StructField("raw", DataTypes.StringType, true, Metadata.empty()),
	            new StructField("sizeEstimate", DataTypes.IntegerType, true, Metadata.empty()),
	            new StructField("snippet", DataTypes.StringType, true, Metadata.empty()),
	            new StructField("threadId", DataTypes.StringType, true, Metadata.empty()),
	    };

	    StructType schema = new StructType(structFields);
	    /*
		   /*StructType schema = new StructType()
				      .add("historyId","String",true)
				      .add("id","String",true)
				      .add("internalDate","String",true)
				      .add("labelIds","ArrayType",true)
				      .add("raw","String",true)
				      .add("sizeEstimate","Integer",true)
				      .add("snippet","String",true)
				      .add("threadId","String",true);*/

		List<Row> rows = new ArrayList<>();
		Dataset<Row> dsetEmpty = spark.createDataFrame(rows, schema);
		//dsetEmpty.printSchema();
		//dsetEmpty.show();
        //Dataset<Row> dset4 = dsetFinal.union(dset2).union(dset1); 
		//dset4.union(dset1);
		
        //dset4.show();
		//dftemp0.show();
		
		
		
        lm.forEach(m -> {
        	//System.out.println("\nMessage id " + m.get("id"));
        	//System.out.println(m.get("id")); 
        	try {
        		
        		Message x = getMessage(Gmail_service, "awsdevdata441@gmail.com", String.valueOf(m.get("id")));
        		//System.out.println(x);
	        	List<String> jsonDatax = Arrays.asList(x.toString());
	        	JavaRDD<String> rddtempx = sc.parallelize(jsonDatax);
	    		
	    		Dataset<Row> dsetx = spark.read().json(rddtempx);
	    		//dsetx.show();
	    		Dataset<Row> dsetFinal = dsetEmpty.union(dsetx);
	    		//dsetFinal.show();
	    		//dsetFinal.cache();
	    		dsetFinal.coalesce(1).write().option("path", "hdfs://localhost:9000/spark_save_table2").mode(SaveMode.Append).saveAsTable("dsetFinal");
	    		//dsetx.show();
	    		//dsetx.write().mode(SaveMode.Overwrite).saveAsTable("appendTest");
	    		//Dataset<Row> dsetFinal = dsetEmpty.union(dsetx);
    		    
	    		//appendTest.createOrReplaceTempView("dftempx");
	    		//spark.sql("INSERT INTO TABLE dftemp0 SELECT * FROM appendTest");
	    		
	    		
	        }catch (IOException e) {
	        		// TODO Auto-generated catch block
	        		e.printStackTrace();
	        		}
	        	}
        );
        
        Dataset<Row> parquetFileDF = spark.read().parquet("hdfs://localhost:9000/spark_save_table2/part-00000-*");
        parquetFileDF.show();
        
        
        
        
        
        
		System.out.println("\n+++++++++++++++++++++++++++++++++++");
		System.out.println("+           SPARK ML              +");
		System.out.println("+++++++++++++++++++++++++++++++++++\n");
        //prepare dataset for spark NLP
        
        parquetFileDF.createOrReplaceTempView("dftemp3");
        Dataset<Row> nlpDF = spark.sql("SELECT id, snippet AS text FROM dftemp3");        
		
		nlpDF.show();
		
		//---- Pretrained Pipeline
		/*
        PipelineModel sentiment_eng = PipelineModel.load("hdfs://localhost:9000//sparkNlpPreTrainedModels/analyze_sentiment_en_2.4.0_2.4_1580483464667");
		SparkNLP.version();
		Dataset<Row> sentiment_eng_output = sentiment_eng.transform(nlpDF);
		

		sentiment_eng_output.show();
		sentiment_eng_output.createOrReplaceTempView("dftemp22");
		
		Dataset<Row> sqlNlpDF = spark.sql("SELECT id, text AS tweet, sentiment.result as sentiment FROM dftemp22");
	    sqlNlpDF.printSchema();
        sqlNlpDF.show();
		
		
		*/
        
        //---------------------------------------------------------------------------------------------------
		System.out.println("\n+++++++++++++++++++++++++++++++++++");
		System.out.println("+     SPARK NLP PIPELINE - OWN    +");
		System.out.println("+++++++++++++++++++++++++++++++++++\n");
        /*
		PipelineStage documentAssembler = (PipelineStage) new DocumentAssembler()
				.setInputCol("text")
				.setOutputCol("document");
		
		
		
		String[] array1 = {"document"};
		String[] array10 = {"sentence"};
		PipelineStage sentenceDetector = (PipelineStage) new SentenceDetector().setInputCol("document");
		
		sentenceDetector.setOutputCol(array10);
				*/
		
		
		DocumentAssembler documentAssembler = (DocumentAssembler) new DocumentAssembler()
												.setInputCol("text")
												.setOutputCol("document");
		
		String[] array1 = {"document"};
		SentenceDetector sentenceDetector = (SentenceDetector) new SentenceDetector()
												.setInputCols(array1);
		sentenceDetector.setOutputCol("sentence");
		
		
		String[] array2 = {"sentence"};
		Tokenizer regexTokenizer = (Tokenizer) new Tokenizer()
										.setInputCols(array2);
		regexTokenizer.setOutputCol("token");
		

      
		String[] array3 = {"token"};
		PipelineStage finisher = new Finisher()
				.setInputCols(array3) 
				.setIncludeMetadata(true);
	    
		 
		
		Pipeline pipeline = new Pipeline()
			    .setStages(new PipelineStage[] {
			        documentAssembler,
			        sentenceDetector,
			        regexTokenizer,
			        finisher
			    });
	    
		
		
		/*Dataset <Row> datainput = data.toDF();*/
		
	    StructField[] structFields2 = new StructField[]{
	            new StructField("text", DataTypes.StringType, true, Metadata.empty()),
	    };

	    StructType schema2 = new StructType(structFields2);
		List<Row> rows2 = new ArrayList<>();
		//Dataset<Row> dsetEmpty24646 = spark.createDataFrame(rows2, schema2);
		List<String> dataRows = new ArrayList<String>();
		
		String dataxxx = "test spark NLP own pipeline OK OK input data hello, this is an example sentence";
				dataRows.add("hello, this is an example sentence hello, this is an example sentence");
				dataRows.add("spark apache sparkNLP ml datset java analysis and models Pipelines.");
				dataRows.add("Including org.spire-math:spire_2.11:jar:0.13.0 in the shaded jar.");
				dataRows.add(dataxxx);
		
		JavaRDD<String> rddtempxxx = sc.parallelize(dataRows);
		Dataset<Row> dsetTemp66646= spark.read().json(rddtempxxx);
		Dataset<Row> dsetFinal2 = spark.createDataFrame(rows2, schema2).union(dsetTemp66646);
		
		
		dsetFinal2.show();
		
		/*
		Dataset<Row> annotations = pipeline.
			    fit(dsetFinal2).
			    transform(dsetFinal2).toDF();
		*/
		
		Dataset<Row> annotations = pipeline.
			    fit(nlpDF).
			    transform(nlpDF).toDF();
		
		annotations.show();
		
		/*
        List<String> trainingData = Arrays.asList(
                data
        );
        
        JavaRDD<String> rddtemp6464 = sc.parallelize(trainingData);
        Dataset<Row> df116546 = spark.read().json(rddtemp6464);
        df116546.show();
        
        */
		
		
		//List<Row> rows2 = new ArrayList<>();
		//rows2.add("hello, this is an example sentence");
		
		
		
		//Dataset<Row> dsetEmpty2 = spark.createDataFrame(rows2, schema2);
		
		//setInputCols(Array("document")).
/*
	    HasOutputAnnotationCol regexTokenizer = new Tokenizer().
			    setInputCols(Array("sentence")).
			    setOutputCol("token");
        
	    HasOutputAnnotationCol finisher = new Finisher()
			  .setInputCols("token")
			  .setIncludeMetadata(false);
        */
		
		
		
		

		
		
		sc.stop();
		spark.stop();
        //--------------------
		System.out.println("\n+++++++++++++++++++++++++++++++++++");
		System.out.println("+     APP Run Successfully !      +");
		System.out.println("+++++++++++++++++++++++++++++++++++\n");
	}
	
//*********************************************************************************************************************
	
	
	
	
	//method to implement for several lines (several messages, 1message per line in RDD)
	//1 message => convert to string the, Json =>x then put data in RDD
	// p messages => p RDDs
	// then concatenate all of them =>  finalmessagesrdd = rdd1 + ... + rddp
	/*
	 RDD example corresponding,to 1 line i.e 1 message
+---------+----------------+-------------+--------------------+--------------------+------------+--------------------+----------------+
|historyId|              id| internalDate|            labelIds|                 raw|sizeEstimate|             snippet|        threadId|
+---------+----------------+-------------+--------------------+--------------------+------------+--------------------+----------------+
|    17681|171d74dc6bb08907|1588455119000|[UNREAD, IMPORTAN...|RGVsaXZlcmVkLVRvO...|        6998|It would have bee...|171d74dc6bb08907|
+---------+----------------+-------------+--------------------+--------------------+------------+--------------------+----------------+
	 */
	
    private static Credential getCredentials(final NetHttpTransport HTTP_TRANSPORT) throws IOException {
    	
	    /**
	     * Global instance of the scopes required by this quickstart.
	     * If modifying these scopes, delete your previously saved tokens/ folder.
	     */
	    // creating object of Vector<String> 
	    Vector<String> v = new Vector<String>();
	    v.add(GmailScopes.GMAIL_READONLY); 
	    v.add(GmailScopes.GMAIL_MODIFY); 
	    //v.add(GmailScopes.GMAIL_METADATA); 
	    
	    System.out.println(v);
	    
	    Enumeration<String> e = v.elements();
	   
	    //private static 
	    final List<String> SCOPES = Collections.list(e);
	    //private static 
	    final String CREDENTIALS_FILE_PATH = "/credentials.json";
	    
    	
        // Load client secrets. 
        InputStream in = sparkMLGmail.class.getResourceAsStream(CREDENTIALS_FILE_PATH);
        
        if (in == null) {
            throw new FileNotFoundException("Resource not found: " + CREDENTIALS_FILE_PATH);
        }
        GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(JSON_FACTORY, new InputStreamReader(in));

        // Build flow and trigger user authorization request.
        GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(
                HTTP_TRANSPORT, JSON_FACTORY, clientSecrets, SCOPES)
                .setDataStoreFactory(new FileDataStoreFactory(new java.io.File(TOKENS_DIRECTORY_PATH)))
                .setAccessType("offline")
                .build();
        LocalServerReceiver receiver = new LocalServerReceiver.Builder().setPort(8888).build();
        return new AuthorizationCodeInstalledApp(flow, receiver).authorize("user");
    }

	
    public static List<Message> listMessages(Gmail service, String userId, String query) throws IOException {
			    ListMessagesResponse response = service.users().messages().list(userId).setQ(query).execute();

			    List<Message> messages = new ArrayList<Message>();
			    while (response.getMessages() != null) {
			      messages.addAll(response.getMessages());
			      if (response.getNextPageToken() != null) {
			        String pageToken = response.getNextPageToken();
			        response = service.users().messages().list(userId).setQ(query)
			            .setPageToken(pageToken).execute();
			      } else {
			        break;
			      }
			    }

			    for (Message message : messages) {
			      System.out.println(message.toPrettyString());
			    }

			    return messages;
	}

		
    /*
     * Get Message with given ID.
     *
     * @param service Authorized Gmail API instance.
     * @param userId User's email address. The special value "me"
     * can be used to indicate the authenticated user.
     * @param messageId ID of Message to retrieve.
     * @return Message Retrieved Message.
     * @throws IOException
     */
    public static Message getMessage(Gmail service, String userId, String messageId)
        throws IOException {
      Message message = service.users().messages().get(userId, messageId).setFormat("raw").execute();

      System.out.println("Message snippet: " + message.getSnippet());
    //System.out.println(message);
      return message;
    }	
		

}
