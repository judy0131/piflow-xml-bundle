package cn.cnic.bigdata

import cn.piflow.{Flow, FlowImpl, Runner}
import org.apache.spark.sql.SparkSession
import org.junit.Test

class XmlTest {

  @Test
  def testNodeXML(): Unit = {

    val flow: Flow = new FlowImpl();
    val xmlpath = "hdfs://10.0.86.89:9000/xjzhu/student.xml"
    val rowTag = "student"

    flow.addProcess("XmlParser", new XmlParser( xmlpath,"student"));
    //flow.addProcess("PutHiveStreaming", new PutHiveStreaming("student","sparktest","studenthivestreaming"));
    //flow.addTrigger("PutHiveStreaming", new DependencyTrigger("XmlParser"));


    val spark = SparkSession.builder()
      .master("spark://10.0.86.89:7077")
      .appName("FlowTestHive")
      .config("spark.driver.memory", "1g")
      .config("spark.executor.memory", "2g")
      .config("spark.cores.max", "2")
      .config("spark.jars","/opt/project/piflow-xml-bundle/out/artifacts/piflow_xml_bundle/piflow-xml-bundle.jar")
      .enableHiveSupport()
      .getOrCreate()

    val exe = Runner.bind("localBackupDir", "/tmp/")
      .bind(classOf[SparkSession].getName, spark)
      .run(flow);

    exe.start("XmlParser");
    Thread.sleep(30000);
    exe.stop();

  }

}
