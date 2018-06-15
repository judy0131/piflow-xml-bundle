package cn.cnic.bigdata

import cn.piflow.Shadow
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import cn.piflow._
import cn.piflow.util.PropertyUtil

class XmlParser(xmlpath:String, dataframeOut:String) extends Process {



  override def shadow(pec: ProcessExecutionContext) = {

    val spark = pec.get[SparkSession]()

    val xmlDF = spark.read.format("com.databricks.spark.xml").option("rowTag","student").load(xmlpath)
    xmlDF.show()


    new Shadow {
      override def discard(pec: ProcessExecutionContext): Unit = {
        //tmpfile.delete();
      }

      override def perform(pec: ProcessExecutionContext): Unit = {
        //TODO: delete the path
        val dataframeOutPath : String = PropertyUtil.getPropertyValue("dataframe_hdfs_path") + dataframeOut + ".parquet"
        val path = new Path(dataframeOutPath)
        val hdfs = FileSystem.get(new java.net.URI(PropertyUtil.getPropertyValue("hdfsURI")),spark.sparkContext.hadoopConfiguration)
        if (hdfs.exists(path)){
          System.out.println("Deleting " + dataframeOutPath + "!!!!!!!!!!!!!!")
          hdfs.delete(path,true)
        }
        xmlDF.write.parquet(dataframeOutPath)
        //spark.close();
      }

      override def commit(pec: ProcessExecutionContext): Unit = {
        //tmpfile.renameTo(new File("./out/wordcount"));
      }
    };

  }

  /**
    * Backup is used to perform undo()
    *
    * @param pec
    * @return
    */
  override def backup(pec: ProcessExecutionContext): Backup = ???
}
