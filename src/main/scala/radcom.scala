import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object radcom extends App{

  val spark = SparkSession
    .builder()
    .enableHiveSupport()
    .master("yarn")
    .appName("radcom")
    .config("hive.exec.dynamic.partition", "true")
    .config("hive.exec.dynamic.partition.mode", "nonstrict")
    .config("spark.driver.memory", "16G")
    .config("spark.executor.memory", "32G")
    .config("spark.default.parallelism", "1000")
    .config("spark.dynamicAllocation.minExecutors", 10)
    .config("spark.dynamicAllocation.maxExecutors", 100)
    .config("spark.dynamicAllocation.enabled", "true")
    .config("spark.kerberos.useTicketCache", "false")
    .config("spark.kerberos.renewTGT", "false")
    .config("spark.executor.extraJavaOptions", "-Djava.security.auth.login.config=jaas.conf")
    .config("spark.driver.extraJavaOptions", "-Djava.security.auth.login.config=jaas.conf")
    .getOrCreate()

  /*Среднее время ответа на DNS-запросы для филиала в LTE*/
  /*
    val radcom_dns_load = spark
    .table("radcom_dds.radcom_cdr_gtpu_p")
    .groupBy("radcom_cdr_gtpu_p.event_date")
    .agg(avg("radcom_cdr_gtpu_p.procedure_duration").as("dns_time"))
    .where((col("radcom_cdr_gtpu_p.probe") === "yd") and (col("radcom_cdr_gtpu_p.subscriber_activity") === "DNS queries") and (col("radcom_cdr_gtpu_p.rat_type_name") === "LTE") and (col("radcom_cdr_gtpu_p.event_type") === "1") and (col("radcom_cdr_gtpu_p.cell_region") === "tambov") and (col("radcom_cdr_gtpu_p.event_date") between("2023-06-26","2023-06-26")) and (col("radcom_cdr_gtpu_p.procedure_duration") isNotNull))
    .select(col("radcom_cdr_gtpu_p.event_date").as("event_date"), col("dns_time").as("dns_time"))
  */

 /*Все пары S1AP ID за день для выборки TAI Новосибирска*/
 /*
     val radcom_s1ap_load = spark
    .table("radcom_dds.radcom_cdr_s1ap")
    .where(((col("probe") === "ns") and (col("tai_tac") === "154") and (col("event_date") === "2023-09-05"))
      or ((col("probe") === "ns") and (col("tai_tac") === "254") and (col("event_date") === "2023-09-05"))
      or ((col("probe") === "ns") and (col("tai_tac") === "354") and (col("event_date") === "2023-09-05"))
      or ((col("probe") === "ns") and (col("tai_tac") === "454") and (col("event_date") === "2023-09-05"))
      or ((col("probe") === "ns") and (col("tai_tac") === "554") and (col("event_date") === "2023-09-05"))
      or ((col("probe") === "ns") and (col("tai_tac") === "654") and (col("event_date") === "2023-09-05"))
      or ((col("probe") === "ns") and (col("tai_tac") === "754") and (col("event_date") === "2023-09-05"))
      or ((col("probe") === "ns") and (col("tai_tac") === "854") and (col("event_date") === "2023-09-05"))
      or ((col("probe") === "ns") and (col("tai_tac") === "954") and (col("event_date") === "2023-09-05"))
      or ((col("probe") === "ns") and (col("tai_tac") === "1054") and (col("event_date") === "2023-09-05"))
      or ((col("probe") === "ns") and (col("tai_tac") === "1154") and (col("event_date") === "2023-09-05"))
      or ((col("probe") === "ns") and (col("tai_tac") === "1354") and (col("event_date") === "2023-09-05"))
      or ((col("probe") === "ns") and (col("tai_tac") === "1454") and (col("event_date") === "2023-09-05"))
      or ((col("probe") === "ns") and (col("tai_tac") === "1554") and (col("event_date") === "2023-09-05"))
      or ((col("probe") === "ns") and (col("tai_tac") === "1654") and (col("event_date") === "2023-09-05"))
      or ((col("probe") === "ns") and (col("tai_tac") === "1754") and (col("event_date") === "2023-09-05"))
      or ((col("probe") === "ns") and (col("tai_tac") === "1854") and (col("event_date") === "2023-09-05")))
    .select(col("enb_ue_s1ap_id").as("enb_ue_s1ap_id"), col("mme_ue_s1ap_id").as("mme_ue_s1ap_id"))
    .distinct()
*/

  /*Выгрузка сервисных IP Вконтакте с данными об объеме трафика и задержках*/
  /*Нижний Новгород*/
 /*
     val radcom_vk_load_nn = spark
    .table("radcom_dds.radcom_cdr_gtpu_p")
    .where((col("probe") === "nn") and (col("radcom_cdr_gtpu_p.subscriber_activity") === "vkontakte") and (col("radcom_cdr_gtpu_p.rat_type_name") === "LTE") and (col("radcom_cdr_gtpu_p.event_date") between("2023-05-01","2023-09-25")))
    .select(
      col("probe").as("probe"),
      col("radcom_cdr_gtpu_p.event_date").as("event_date"),
      col("service_dst_ip").as("service_dst_ip"),
      col("total_dl_raw_bytes").as("total_dl_raw_bytes"),
      col("ran_latency").as("ran_latency"),
      col("internet_latency").as("internet_latency"),
      col("rtt").as("rtt")
    )
    .groupBy("probe", "event_date", "service_dst_ip")
    .agg(sum("total_dl_raw_bytes").as("total_dl_raw_bytes"), avg("ran_latency").as("ran_latency"), avg("internet_latency").as("internet_latency"), avg("rtt").as("rtt"))
*/


  /*Выгрузка сервисных IP Вконтакте с данными об объеме трафика и задержках*/
  /*Воронеж*/
/*
  val radcom_vk_load_vr = spark
    .table("radcom_dds.radcom_cdr_gtpu_p")
    .where((col("probe") === "vr") and (col("radcom_cdr_gtpu_p.subscriber_activity") === "vkontakte") and (col("radcom_cdr_gtpu_p.rat_type_name") === "LTE") and (col("radcom_cdr_gtpu_p.event_date") between("2023-05-01", "2023-09-25")))
    .select(
      col("probe").as("probe"),
      col("radcom_cdr_gtpu_p.event_date").as("event_date"),
      col("service_dst_ip").as("service_dst_ip"),
      col("total_dl_raw_bytes").as("total_dl_raw_bytes"),
      col("ran_latency").as("ran_latency"),
      col("internet_latency").as("internet_latency"),
      col("rtt").as("rtt")
    )
    .groupBy("probe", "event_date", "service_dst_ip")
    .agg(sum("total_dl_raw_bytes").as("total_dl_raw_bytes"), avg("ran_latency").as("ran_latency"), avg("internet_latency").as("internet_latency"), avg("rtt").as("rtt"))
*/


  /*Выгрузка сервисных IP Вконтакте с данными об объеме трафика и задержках*/
  /*Иркутск*/
 /*
 val radcom_vk_load_ir = spark
      .table("radcom_dds.radcom_cdr_gtpu_p")
      .where((col("probe") === "ir") and (col("radcom_cdr_gtpu_p.subscriber_activity") === "vkontakte") and (col("radcom_cdr_gtpu_p.rat_type_name") === "LTE") and (col("radcom_cdr_gtpu_p.event_date") between("2023-09-26", "2023-10-02")))
      .select(
        col("probe").as("probe"),
        col("radcom_cdr_gtpu_p.event_date").as("event_date"),
        col("service_dst_ip").as("service_dst_ip"),
        col("total_dl_raw_bytes").as("total_dl_raw_bytes"),
        col("ran_latency").as("ran_latency"),
        col("internet_latency").as("internet_latency"),
        col("rtt").as("rtt")
      )
      .groupBy("probe", "event_date", "service_dst_ip")
      .agg(sum("total_dl_raw_bytes").as("total_dl_raw_bytes"), avg("ran_latency").as("ran_latency"), avg("internet_latency").as("internet_latency"), avg("rtt").as("rtt"))
*/

  /*Выгрузка сервисных IP Вконтакте с данными об объеме трафика и задержках*/
  /*Хабаровск*/
  /*
      val radcom_vk_load_kh = spark
        .table("radcom_dds.radcom_cdr_gtpu_p")
        .where((col("probe") === "kh") and (col("radcom_cdr_gtpu_p.subscriber_activity") === "vkontakte") and (col("radcom_cdr_gtpu_p.rat_type_name") === "LTE") and (col("radcom_cdr_gtpu_p.event_date") between("2023-05-01", "2023-09-25")))
        .select(
          col("probe").as("probe"),
          col("radcom_cdr_gtpu_p.event_date").as("event_date"),
          col("service_dst_ip").as("service_dst_ip"),
          col("total_dl_raw_bytes").as("total_dl_raw_bytes"),
          col("ran_latency").as("ran_latency"),
          col("internet_latency").as("internet_latency"),
          col("rtt").as("rtt")
        )
        .groupBy("probe", "event_date", "service_dst_ip")
        .agg(sum("total_dl_raw_bytes").as("total_dl_raw_bytes"), avg("ran_latency").as("ran_latency"), avg("internet_latency").as("internet_latency"), avg("rtt").as("rtt"))
 */

  /*Выгрузка сервисных IP Вконтакте с данными об объеме трафика и задержках*/
  /*Челябинск*/
  /*
       val radcom_vk_load_cl = spark
         .table("radcom_dds.radcom_cdr_gtpu_p")
         .where((col("probe") === "cl") and (col("radcom_cdr_gtpu_p.subscriber_activity") === "vkontakte") and (col("radcom_cdr_gtpu_p.rat_type_name") === "LTE") and (col("radcom_cdr_gtpu_p.event_date") between("2023-05-01", "2023-09-25")))
         .select(
           col("probe").as("probe"),
           col("radcom_cdr_gtpu_p.event_date").as("event_date"),
           col("service_dst_ip").as("service_dst_ip"),
           col("total_dl_raw_bytes").as("total_dl_raw_bytes"),
           col("ran_latency").as("ran_latency"),
           col("internet_latency").as("internet_latency"),
           col("rtt").as("rtt")
         )
         .groupBy("probe", "event_date", "service_dst_ip")
         .agg(sum("total_dl_raw_bytes").as("total_dl_raw_bytes"), avg("ran_latency").as("ran_latency"), avg("internet_latency").as("internet_latency"), avg("rtt").as("rtt"))
 */


  /*Выгрузка сервисных IP Вконтакте с данными об объеме трафика и задержках*/
  /*Екатеринбург*/
  /*
           val radcom_vk_load_ek = spark
           .table("radcom_dds.radcom_cdr_gtpu_p")
           .where((col("probe") === "ek") and (col("radcom_cdr_gtpu_p.subscriber_activity") === "vkontakte") and (col("radcom_cdr_gtpu_p.rat_type_name") === "LTE") and (col("radcom_cdr_gtpu_p.event_date") between("2023-05-01", "2023-09-25")))
           .select(
             col("probe").as("probe"),
             col("radcom_cdr_gtpu_p.event_date").as("event_date"),
             col("service_dst_ip").as("service_dst_ip"),
             col("total_dl_raw_bytes").as("total_dl_raw_bytes"),
             col("ran_latency").as("ran_latency"),
             col("internet_latency").as("internet_latency"),
             col("rtt").as("rtt")
           )
           .groupBy("probe", "event_date", "service_dst_ip")
           .agg(sum("total_dl_raw_bytes").as("total_dl_raw_bytes"), avg("ran_latency").as("ran_latency"), avg("internet_latency").as("internet_latency"), avg("rtt").as("rtt"))
 */

  /*Выгрузка сервисных IP Вконтакте с данными об объеме трафика и задержках*/
  /*Новосибирск*/
  /*
            val radcom_vk_load_ns = spark
            .table("radcom_dds.radcom_cdr_gtpu_p")
            .where((col("probe") === "ns") and (col("radcom_cdr_gtpu_p.subscriber_activity") === "vkontakte") and (col("radcom_cdr_gtpu_p.rat_type_name") === "LTE") and (col("radcom_cdr_gtpu_p.event_date") between("2023-05-01", "2023-09-25")))
            .select(
              col("probe").as("probe"),
              col("radcom_cdr_gtpu_p.event_date").as("event_date"),
              col("service_dst_ip").as("service_dst_ip"),
              col("total_dl_raw_bytes").as("total_dl_raw_bytes"),
              col("ran_latency").as("ran_latency"),
              col("internet_latency").as("internet_latency"),
              col("rtt").as("rtt")
            )
            .groupBy("probe", "event_date", "service_dst_ip")
            .agg(sum("total_dl_raw_bytes").as("total_dl_raw_bytes"), avg("ran_latency").as("ran_latency"), avg("internet_latency").as("internet_latency"), avg("rtt").as("rtt"))
*/

  /*Выгрузка сервисных IP Вконтакте с данными об объеме трафика и задержках*/
  /*Ставрополь*/
  /*
            val radcom_vk_load_st = spark
              .table("radcom_dds.radcom_cdr_gtpu_p")
              .where((col("probe") === "st") and (col("radcom_cdr_gtpu_p.subscriber_activity") === "vkontakte") and (col("radcom_cdr_gtpu_p.rat_type_name") === "LTE") and (col("radcom_cdr_gtpu_p.event_date") between("2023-05-01", "2023-09-25")))
              .select(
                col("probe").as("probe"),
                col("radcom_cdr_gtpu_p.event_date").as("event_date"),
                col("service_dst_ip").as("service_dst_ip"),
                col("total_dl_raw_bytes").as("total_dl_raw_bytes"),
                col("ran_latency").as("ran_latency"),
                col("internet_latency").as("internet_latency"),
                col("rtt").as("rtt")
              )
              .groupBy("probe", "event_date", "service_dst_ip")
              .agg(sum("total_dl_raw_bytes").as("total_dl_raw_bytes"), avg("ran_latency").as("ran_latency"), avg("internet_latency").as("internet_latency"), avg("rtt").as("rtt"))
*/

  /*Выгрузка сервисных IP Вконтакте с данными об объеме трафика и задержках*/
  /*Ростов-на-Дону*/
  /*
                val radcom_vk_load_rd = spark
                .table("radcom_dds.radcom_cdr_gtpu_p")
                .where((col("probe") === "rd") and (col("radcom_cdr_gtpu_p.subscriber_activity") === "vkontakte") and (col("radcom_cdr_gtpu_p.rat_type_name") === "LTE") and (col("radcom_cdr_gtpu_p.event_date") between("2023-05-01", "2023-09-25")))
                .select(
                  col("probe").as("probe"),
                  col("radcom_cdr_gtpu_p.event_date").as("event_date"),
                  col("service_dst_ip").as("service_dst_ip"),
                  col("total_dl_raw_bytes").as("total_dl_raw_bytes"),
                  col("ran_latency").as("ran_latency"),
                  col("internet_latency").as("internet_latency"),
                  col("rtt").as("rtt")
                )
                .groupBy("probe", "event_date", "service_dst_ip")
                .agg(sum("total_dl_raw_bytes").as("total_dl_raw_bytes"), avg("ran_latency").as("ran_latency"), avg("internet_latency").as("internet_latency"), avg("rtt").as("rtt"))
*/

  /*Выгрузка сервисных IP Вконтакте с данными об объеме трафика и задержках*/
  /*Саратов*/
  /*
               val radcom_vk_load_sr = spark
                 .table("radcom_dds.radcom_cdr_gtpu_p")
                 .where((col("probe") === "sr") and (col("radcom_cdr_gtpu_p.subscriber_activity") === "vkontakte") and (col("radcom_cdr_gtpu_p.rat_type_name") === "LTE") and (col("radcom_cdr_gtpu_p.event_date") between("2023-05-01", "2023-09-25")))
                 .select(
                   col("probe").as("probe"),
                   col("radcom_cdr_gtpu_p.event_date").as("event_date"),
                   col("service_dst_ip").as("service_dst_ip"),
                   col("total_dl_raw_bytes").as("total_dl_raw_bytes"),
                   col("ran_latency").as("ran_latency"),
                   col("internet_latency").as("internet_latency"),
                   col("rtt").as("rtt")
                 )
                 .groupBy("probe", "event_date", "service_dst_ip")
                 .agg(sum("total_dl_raw_bytes").as("total_dl_raw_bytes"), avg("ran_latency").as("ran_latency"), avg("internet_latency").as("internet_latency"), avg("rtt").as("rtt"))
 */

/*Выгрузка сервисных IP Вконтакте с данными об объеме трафика и задержках*/
  /*Калининград*/
  /*
               val radcom_vk_load_kg = spark
                 .table("radcom_dds.radcom_cdr_gtpu_p")
                 .where((col("probe") === "kg") and (col("radcom_cdr_gtpu_p.subscriber_activity") === "vkontakte") and (col("radcom_cdr_gtpu_p.rat_type_name") === "LTE") and (col("radcom_cdr_gtpu_p.event_date") between("2023-05-01", "2023-09-25")))
                 .select(
                   col("probe").as("probe"),
                   col("radcom_cdr_gtpu_p.event_date").as("event_date"),
                   col("service_dst_ip").as("service_dst_ip"),
                   col("total_dl_raw_bytes").as("total_dl_raw_bytes"),
                   col("ran_latency").as("ran_latency"),
                   col("internet_latency").as("internet_latency"),
                   col("rtt").as("rtt")
                 )
                 .groupBy("probe", "event_date", "service_dst_ip")
                 .agg(sum("total_dl_raw_bytes").as("total_dl_raw_bytes"), avg("ran_latency").as("ran_latency"), avg("internet_latency").as("internet_latency"), avg("rtt").as("rtt"))
*/

  /*Выгрузка сервисных IP Вконтакте с данными об объеме трафика и задержках*/
  /*Ярославль*/
  /*
                 val radcom_vk_load_yd = spark
                   .table("radcom_dds.radcom_cdr_gtpu_p")
                   .where((col("probe") === "yd") and (col("radcom_cdr_gtpu_p.subscriber_activity") === "vkontakte") and (col("radcom_cdr_gtpu_p.rat_type_name") === "LTE") and (col("radcom_cdr_gtpu_p.event_date") between("2023-09-01", "2023-09-28")))
                   .select(
                     col("probe").as("probe"),
                     col("radcom_cdr_gtpu_p.event_date").as("event_date"),
                     col("service_dst_ip").as("service_dst_ip"),
                     col("total_dl_raw_bytes").as("total_dl_raw_bytes"),
                     col("ran_latency").as("ran_latency"),
                     col("internet_latency").as("internet_latency"),
                     col("rtt").as("rtt")
                   )
                   .groupBy("probe", "event_date", "service_dst_ip")
                   .agg(sum("total_dl_raw_bytes").as("total_dl_raw_bytes"), avg("ran_latency").as("ran_latency"), avg("internet_latency").as("internet_latency"), avg("rtt").as("rtt"))
*/


  /*Статистика для сервисов Google по основным KQI в дневной агрегации*/
  /*
                     val radcom_google_load_mn_3 = spark
                     .table("radcom_dds.radcom_cdr_gtpu_p")
                     .where((col("probe") === "mn") and (col("radcom_cdr_gtpu_p.subscriber_activity") === "google") and (col("radcom_cdr_gtpu_p.rat_type_name") === "LTE") and (col("radcom_cdr_gtpu_p.event_date") between("2023-09-01", "2023-10-03")))
                     .select(
                       col("probe").as("probe"),
                       col("radcom_cdr_gtpu_p.event_date").as("event_date"),
                       col("total_dl_raw_bytes").as("total_dl_raw_bytes"),
                       col("ran_latency").as("ran_latency"),
                       col("internet_latency").as("internet_latency"),
                       col("rtt").as("rtt"),
                       col("procedure_duration").as("procedure_duration"),
                       col("effective_throughput_dl").as("effective_throughput_dl")
                     )
                     .groupBy("probe", "event_date")
                     .agg(avg("total_dl_raw_bytes").as("total_dl_raw_bytes"), avg("ran_latency").as("ran_latency"), avg("internet_latency").as("internet_latency"), avg("rtt").as("rtt"), avg("procedure_duration").as("procedure_duration"), avg("effective_throughput_dl").as("effective_throughput_dl"))
*/


/*Статистика для анализа перемещения абонентов, появившихся на БС, запущенных по программе Устранения цифрового неравенства
Цель - понять откуда "пришли" абоненты в новое покрытие LTE */
  val who_ucn = spark
    .table("radcom_dds.radcom_cdr_gtpu_p")
    .where((col("probe") isin ("rd","st")) and (col("radcom_cdr_gtpu_p.event_date") between("2023-10-09", "2023-10-22")) and (col("imsi") isin
      ( "250993272904533",
        "250993273694940",
        "250997261328403",
        "250990288787394",
        "310170841427562",
        "250993273160663",
        "250993272161034",
        "250996688103231")))
    .select(
      col("radcom_cdr_gtpu_p.event_date").as("event_date"),
      col("imsi").as("imsi"),
      col("total_ul_raw_bytes").as("total_ul_raw_bytes"),
      col("total_dl_raw_bytes").as("total_dl_raw_bytes"),
      col("cgi").as("cgi"),
      col("lac").as("lac"),
      col("tai_tac").as("tai_tac"),
      col("rat").as("rat"),
      col("rtt").as("rtt"),
      col("internet_latency").as("internet_latency"),
      col("ran_latency").as("ran_latency"),
      col("effective_throughput_dl").as("effective_throughput_dl"),
      col("effective_throughput_ul").as("effective_throughput_ul"),
      col("mean_throughput_dl").as("mean_throughput_dl"),
      col("mean_throughput_ul").as("mean_throughput_ul"),
      col("peak_throughput_dl").as("peak_throughput_dl"),
      col("peak_throughput_ul").as("peak_throughput_ul")
    )
    .groupBy("event_date","imsi","rat","cgi","lac","tai_tac")
    .agg(
      sum("total_dl_raw_bytes").as("total_dl_raw_bytes"),
      sum("total_ul_raw_bytes").as("total_ul_raw_bytes"),
      avg("rtt").as("rtt"),
      avg("internet_latency").as("internet_latency"),
      avg("ran_latency").as("ran_latency"),
      avg("effective_throughput_dl").as("effective_throughput_dl"),
      avg("effective_throughput_ul").as("effective_throughput_ul"),
      avg("mean_throughput_dl").as("mean_throughput_dl"),
      avg("mean_throughput_ul").as("mean_throughput_ul"),
      avg("peak_throughput_dl").as("peak_throughput_dl"),
      avg("peak_throughput_ul").as("peak_throughput_ul")
    )
    .orderBy(desc("total_dl_raw_bytes"),desc("event_date"))



  val all_results = who_ucn


  all_results
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("path", "hdfs://ns-etl/warehouse/tablespace/external/hive/school_de_stg.db/who_ucn_top_imsi")
    .saveAsTable("school_de_stg.who_ucn_top_imsi")

  spark.stop()
}
