import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

object DBLoader extends App {
  //Создаем Spark-сессию
  val spark = SparkSession
    .builder()
    .enableHiveSupport()
    .master("yarn")
    .appName("DBLoader")
    .config("hive.exec.dynamic.partition", "true")
    .config("hive.exec.dynamic.partition.mode", "nonstrict")
    .config("spark.kerberos.useTicketCache", "false")
    .config("spark.kerberos.renewTGT", "false")
    .config("spark.executor.extraJavaOptions","-Djava.security.auth.login.config=jaas.conf")
    .config("spark.driver.extraJavaOptions","-Djava.security.auth.login.config=jaas.conf")
    .getOrCreate()

  val vaultOpts: Map[String, String] = args.map(i => i.split("=")(0) -> i.split("=")(1)).toMap


  //Читаем таблицу bookings.seats в postgres DB
  val bookings_seats = spark
    .read
    .format("jdbc")
    .option("driver","org.postgresql.Driver")
    .option("url","jdbc:postgresql://ingress-1.prod.dmp.vimpelcom.ru:5448/demo")
    .option("dbtable","demo.bookings.seats")
    .options(vaultOpts)
    .load()
  //Записываем seats_svakulin в hdfs
  bookings_seats
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("path", s"hdfs://ns-etl/warehouse/tablespace/external/hive/school_de.db/seats_svakulin")
    .saveAsTable("school_de.seats_svakulin")

  //Читаем вьюху bookings.flights_v в postgres DB
  val bookings_flights_v = spark
    .read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://ingress-1.prod.dmp.vimpelcom.ru:5448/demo")
    .option("dbtable", "demo.bookings.flights_v")
    .options(vaultOpts)
    .load()
  //Записываем bookings_flights_v в hdfs с разбивкой на партиции по дате
  bookings_flights_v
    .withColumn("date", expr("DATE(actual_departure)"))
    .write
    .mode("overwrite")
    .option("path", s"hdfs://ns-etl/warehouse/tablespace/external/hive/school_de.db/flights_v_svakulin")
    .partitionBy("date")
    .saveAsTable("school_de.flights_v_svakulin")

  spark.stop()
}