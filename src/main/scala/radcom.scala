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

  /*val radcom_dns_load = spark
    .table("radcom_dds.radcom_cdr_gtpu_p")
    .groupBy("radcom_cdr_gtpu_p.event_date")
    .agg(avg("radcom_cdr_gtpu_p.procedure_duration").as("dns_time"))
    .where((col("radcom_cdr_gtpu_p.probe") === "yd") and (col("radcom_cdr_gtpu_p.subscriber_activity") === "DNS queries") and (col("radcom_cdr_gtpu_p.rat_type_name") === "LTE") and (col("radcom_cdr_gtpu_p.event_type") === "1") and (col("radcom_cdr_gtpu_p.cell_region") === "tambov") and (col("radcom_cdr_gtpu_p.event_date") between("2023-06-26","2023-06-26")) and (col("radcom_cdr_gtpu_p.procedure_duration") isNotNull))
    .select(col("radcom_cdr_gtpu_p.event_date").as("event_date"), col("dns_time").as("dns_time"))*/

 /* val radcom_dns_load = spark
    .table("radcom_dds.radcom_cdr_gtpu_p")
    .limit(10)
    .where((col("probe") === "yd") and (col("subscriber_activity") === "DNS queries") and (col("rat_type_name") === "LTE") and (col("event_type") === "1") and (col("cell_region") === "tambov") and (col("event_date") between("2023-06-26", "2023-06-26")) and (col("procedure_duration") isNotNull))
    .select(col("imsi").as("imsi"), col("cgi").as("cgi"))*/

 /* val radcom_s1ap_load = spark
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


 /* val radcom_vk_load_nn = spark
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

 val imsi = spark
   .table("radcom_dds.radcom_cdr_iu_sccp_p")
   .where((col("probe") === "ir") and (col("radcom_cdr_iu_sccp_p.event_date") between("2023-09-28", "2023-10-05")) and (col("imsi") === "250993244671232"))
   .select(
     col("event_date"),
     col("procedure_list"),
     col("release_cause"),
     col("procedure_type"),
     col("event_type")
   )
*/

  val who_ucn = spark
    .table("radcom_dds.radcom_cdr_gtpu_p")
    .where((col("probe") isin ("rd","st")) and (col("radcom_cdr_gtpu_p.event_date") between("2023-10-09", "2023-10-22")) and (col("imsi") isin
      ("250994224086916",
        "250994233525425",
        "250994233431030",
        "250994232419432",
        "250994302046647",
        "250993179485586",
        "250994234387806",
        "250994095401396",
        "250992227927655",
        "250996573720023",
        "250994234510981",
        "250994225860537",
        "250994232415946",
        "250992241989933",
        "250994218409772",
        "250993272268202",
        "250998184968272",
        "250993272453408",
        "250994225706708",
        "250994228999219",
        "250999231789203",
        "250993272902931",
        "250994232111392",
        "250993270194840",
        "250992129764413",
        "250994225481125",
        "250994225875424",
        "250994227240212",
        "250992129339889",
        "250994225804580",
        "250993206588380",
        "250994233299318",
        "250994234548386",
        "250993128961748",
        "250994233555620",
        "250997312206763",
        "250994232147823",
        "250993271393821",
        "250994233640944",
        "250994214395777",
        "250992232965674",
        "250994232969549",
        "250997346454287",
        "250992225884142",
        "250993273196230",
        "250996586976261",
        "250994215023560",
        "250993273473681",
        "250994234217864",
        "250994233502997",
        "250993271704210",
        "250994232743361",
        "250994233498285",
        "250993272934090",
        "250994229124468",
        "250994232355924",
        "250994225516323",
        "250993271651043",
        "250994233876005",
        "250994303182960",
        "250994224875346",
        "250994226668960",
        "250994301824768",
        "250994234077052",
        "250994227955311",
        "250992220048641",
        "250993271529190",
        "250994225393410",
        "250993273594678",
        "250994224752001",
        "250994232194486",
        "250993273911539",
        "250993270124249",
        "250994234545768",
        "250998136359092",
        "250993267927887",
        "250994234536117",
        "250996593632959",
        "250994232964434",
        "250994234462891",
        "250994227501571",
        "250993272509544",
        "250992126220205",
        "250993127374226",
        "250993272851717",
        "250993206631611",
        "250994234535067",
        "250993272821684",
        "250994225671334",
        "250993272174392",
        "250992227990679",
        "250993127868970",
        "250994225635298",
        "250994232185616",
        "250994226401290",
        "250993271482587",
        "250993273869808",
        "250994227880288",
        "250993128968664",
        "250994234276507",
        "250994228967659",
        "250993271801528",
        "250994225215589",
        "250994224413707",
        "250993138477166",
        "250993272570280",
        "250994232233047",
        "250997294901745",
        "250994233502999",
        "250994234440906",
        "250994232078498",
        "250993271181126",
        "250993270950868",
        "250993271943403",
        "250994232448233",
        "250993273017526",
        "250993127646790",
        "250993240185280",
        "250994232964434",
        "250994097636579",
        "250993274040623",
        "250993129863086",
        "250994227541421",
        "250994224548415",
        "250994232616024",
        "250993271344048",
        "250994226545014",
        "250993272988301",
        "250994226974221",
        "250994225668966",
        "250997337207595",
        "250993271768061",
        "250993127446984",
        "250993270585782",
        "250995174968478",
        "250993274002661",
        "250994224558557",
        "250993274050149",
        "250994224560035",
        "250994227777617",
        "250993273318952",
        "250993138291566",
        "250993272821296",
        "250993272757994",
        "250993118782447",
        "250993272988301",
        "250994228967659",
        "250993273218855",
        "250994226401290",
        "250997312158056",
        "250993271354882",
        "250994233209373",
        "250993273338915",
        "250993273712556",
        "250993272188146",
        "250994233546754",
        "250993273286490",
        "250993273375394",
        "250993273536204",
        "250993271529199",
        "250993273178028",
        "250993273205300",
        "250993128006720",
        "250993273201241",
        "250993270585782",
        "250993273981872",
        "250994234244743",
        "250993273140871",
        "250993138757615",
        "250993271184278",
        "250993272828129",
        "250993272242309",
        "250993270108731",
        "250993271675600",
        "250994227822936",
        "250993271645083",
        "250993272298520",
        "250993270439798",
        "250993273970162",
        "250998091464935",
        "250993273966744",
        "250993272442548",
        "250993270611387",
        "250993271118826",
        "250993273140863",
        "250997330978757",
        "250993138530121",
        "250993273466971",
        "250993270436001",
        "250993127280841",
        "250994168620579",
        "250993128856433",
        "250997218144004",
        "250993272197268",
        "250993272264499",
        "250993128642444",
        "250994093841322",
        "250993118175829",
        "250993136446978",
        "250993272933763",
        "250993272814125",
        "250993271136146",
        "250994214058460",
        "250993271863307",
        "250994261520663",
        "250993267938114",
        "250993081480886",
        "250993138312015",
        "250993272988301",
        "250993270097831",
        "250991423346084",
        "250993272749828",
        "250993271762931",
        "250993136372623",
        "250993273166891",
        "250993272105293",
        "250993273708332",
        "250993270730193",
        "250993271713864",
        "250997216347587",
        "250993270196194",
        "250993273202019",
        "250993272627137",
        "250993127842347",
        "250993272339181",
        "250993080272685",
        "250993116720082",
        "250993080946530",
        "250993271634153",
        "250993127369545",
        "250993273936380",
        "250993137447522",
        "250993271688932",
        "250994225528455",
        "250993273922770",
        "250993138309262",
        "250993270589204",
        "250993127860907",
        "250993273932158",
        "250993273978109",
        "250996639533353",
        "250993272252279",
        "250993270424410",
        "250993273982238",
        "250993272608888",
        "250993271810597",
        "250993273017526",
        "250993272423968",
        "250993270587777",
        "250993082084294",
        "250997278735988",
        "250993118040236",
        "250993271198114",
        "250993272497296",
        "250991427109912",
        "250993271891189",
        "250996686009780",
        "250994189623156",
        "250993271690488",
        "250993270660811",
        "250993128750189",
        "250993271891189",
        "250997276405280",
        "250993270658361",
        "250993081240788",
        "250993129221083",
        "250993127849621",
        "250993273418726",
        "250996567584116",
        "250993127210831",
        "250993116925868",
        "250993216974994",
        "250993274015109",
        "250996685047760",
        "250993270976924",
        "250993270157005",
        "250993273080732",
        "250994234329631",
        "250993271851986",
        "250994167261126",
        "250996688472139",
        "250993272169111",
        "250993129762327",
        "250993127844969",
        "250993271127305",
        "250993070255048",
        "250996687227080",
        "250996687599083",
        "250993252249427",
        "250993271757990",
        "250996527899456",
        "250993272560348",
        "250997348522541",
        "250993272325072",
        "250996684453971",
        "250993272531335",
        "250993271884097",
        "250993127460012",
        "250993128525752",
        "250994096175496",
        "250993271370858",
        "250993081942022",
        "250993272635344",
        "250993271773015",
        "250993272904533",
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
