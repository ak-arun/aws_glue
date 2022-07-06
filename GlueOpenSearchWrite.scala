import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.GlueArgParser
import com.amazonaws.services.glue.util.Job
import java.util.Calendar
import org.apache.spark.SparkContext
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.Trigger
import scala.collection.JavaConverters._
import org.apache.spark.SparkConf
import org.elasticsearch.spark._  

//TODO remove un-used imports

object GlueApp {
  def main(sysArgs: Array[String]) {
    
    val conf = new SparkConf()
    
    conf.set("es.nodes.wan.only", "true")
    conf.set("es.nodes", "https://BLAH.example.com")
    conf.set("es.port", "443")
    conf.set("es.net.http.auth.user","SOME USERNAME")
    conf.set("es.net.http.auth.pass","SOME PASSWORD")
    conf.set("es.index.auto.create","true")
    
    val spark: SparkContext = new SparkContext(conf)
    
    val glueContext: GlueContext = new GlueContext(spark)
    val sparkSession: SparkSession = glueContext.getSparkSession
    import sparkSession.implicits._
    // @params: [JOB_NAME]
    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME").toArray)
    Job.init(args("JOB_NAME"), glueContext, args.asJava)

    
    val numbers = Map("key1" -> 1, "key2" -> 2, "key3" -> 3)
    val airports = Map("SRC" -> "ORD", "DEST" -> "EWR")

    spark.makeRDD(Seq(numbers, airports)).saveToEs("test_path1/doc") 
    
    
    Job.commit()
  
}}
