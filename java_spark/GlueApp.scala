import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.MappingSpec
import com.amazonaws.services.glue.errors.CallSite
import com.amazonaws.services.glue.util.GlueArgParser
import com.amazonaws.services.glue.util.Job
import com.amazonaws.services.glue.util.JsonOptions
import org.apache.spark.SparkContext
import scala.collection.JavaConverters._
import org.apache.spark.sql.SparkSession

object GlueApp {
  def main(sysArgs: Array[String]) {
    val spark: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(spark)
    // @params: [JOB_NAME]
    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME").toArray)
    val sparkSession: SparkSession = glueContext.getSparkSession
    
    Job.init(args("JOB_NAME"), glueContext, args.asJava)
    
    val javaArgs = Array(
    args("JOB_NAME"),
    args.getOrElse("sample_param", "default_value")
    )
    
    com.aws.glue.example.SimpleSparkJob.processData(sparkSession, javaArgs)
    
    

    Job.commit()
  }
}
