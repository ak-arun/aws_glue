
//Glue Spark Streaming proxy for Apache Hudi deltastreamer

import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.MappingSpec
import com.amazonaws.services.glue.errors.CallSite
import com.amazonaws.services.glue.util.GlueArgParser
import com.amazonaws.services.glue.util.Job
import com.amazonaws.services.glue.util.JsonOptions
import org.apache.spark.SparkContext
import scala.collection.JavaConverters._
import org.apache.spark.sql.SparkSession
import org.apache.spark.api.java.JavaSparkContext
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer
import org.apache.hudi.utilities.deltastreamer.SchedulerConfGenerator
import org.apache.hudi.utilities.UtilHelpers
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.GetObjectRequest
import scala.io.Source

object GlueApp {
    
    def getConfigFileItr(fileName:String) : Iterator[String] = {
        val bucketName = fileName.replace("s3://","").split("/")(0)
        val key = fileName.replace("s3://"+bucketName+"/","")
        val s3Client = new AmazonS3Client()
		var s3Object = s3Client.getObject(new GetObjectRequest(bucketName, key))
		return Source.fromInputStream(s3Object.getObjectContent()).getLines()
    }

	def getHudiConfigs(fileName:String): Array[String] = {
		var returnConfig = Array[String]()
		var inputConfigs = getConfigFileItr(fileName)
		while (inputConfigs.hasNext) {
			var configuration = inputConfigs.next
			if (configuration.trim().startsWith("hoodie")) {
				returnConfig = returnConfig :+ "--hoodie-conf"
				returnConfig = returnConfig :+ configuration.trim()
			}
		}
		return returnConfig
	}

	def getDeltaStreamerConfigs(fileName:String): Array[String] = {
		var returnConfig = Array[String]()
		var inputConfigs = getConfigFileItr(fileName)
		while (inputConfigs.hasNext) {
			var deltaconfig = inputConfigs.next
			if (deltaconfig.trim().startsWith("--")) {
				if (deltaconfig.contains("=")) {
					returnConfig = returnConfig :+ deltaconfig.trim().split("=")(0)
					returnConfig = returnConfig :+ deltaconfig.trim().split("=")(1)
				} else {
					returnConfig = returnConfig :+ deltaconfig.trim()
				}
			}
		}
		return returnConfig
	}

	def main(sysArgs: Array[String]) {
		val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME","HUDI_PROPERTIES","DELTASTREAMER_PROPERTIES").toArray)
		var config = getHudiConfigs(args("HUDI_PROPERTIES")) ++ getDeltaStreamerConfigs(args("DELTASTREAMER_PROPERTIES"))
		val cfg = HoodieDeltaStreamer.getConfig(config)
		val additionalSparkConfigs = SchedulerConfGenerator.getSparkSchedulingConfigs(cfg)
		val jssc = UtilHelpers.buildSparkContext("delta-streamer-test", "jes", additionalSparkConfigs)
		val spark = jssc.sc
		val glueContext: GlueContext = new GlueContext(spark)
		Job.init(args("JOB_NAME"), glueContext, args.asJava)
		try {
			new HoodieDeltaStreamer(cfg, jssc).sync();
		} finally {
			jssc.stop();
		}
		Job.commit()
	}
}
