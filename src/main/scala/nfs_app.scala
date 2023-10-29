import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object nfs_app extends App {
  //Создаем Spark-сессию
  val spark = SparkSession
    .builder()
    .enableHiveSupport()
    .master("yarn")
    .appName("nfs_app")
    .config("hive.exec.dynamic.partition", "true")
    .config("hive.exec.dynamic.partition.mode", "nonstrict")
    .config("spark.kerberos.useTicketCache", "false")
    .config("spark.kerberos.renewTGT", "false")
    .config("spark.executor.extraJavaOptions","-Djava.security.auth.login.config=jaas.conf")
    .config("spark.driver.extraJavaOptions","-Djava.security.auth.login.config=jaas.conf")
    .getOrCreate()


  val fnd_user = spark
    .table("ssc_rpa.nfs_contract_report_main")
    .select(col("po_number").as("po_number"), col("inn").as("inn"), col("kpp").as("kpp"))
    .where((col("inn") isNotNull) and (col("kpp") isNotNull))
    .limit(20)

  fnd_user
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("path", s"hdfs://ns-etl/warehouse/tablespace/external/hive/ssc_rpa_stg.db/nfs_app_test")
    .saveAsTable("ssc_rpa_stg.nfs_app_test")

  spark.stop()
}