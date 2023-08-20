import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object DBCalc extends App {

  val spark = SparkSession
    .builder()
    .enableHiveSupport()
    .master("yarn")
    .appName("DBLoader")
    .config("hive.exec.dynamic.partition", "true")
    .config("hive.exec.dynamic.partition.mode", "nonstrict")
    .config("spark.kerberos.useTicketCache", "false")
    .config("spark.kerberos.renewTGT", "false")
    .config("spark.executor.extraJavaOptions", "-Djava.security.auth.login.config=jaas.conf")
    .config("spark.driver.extraJavaOptions", "-Djava.security.auth.login.config=jaas.conf")
    .getOrCreate()

  /*1.	Вывести максимальное количество человек в одном бронировании*/
  val test_1_result = spark
    .table("school_de.bookings_tickets")
    .groupBy("book_ref")
    .agg(count("*").as("chislo"))
    .select(lit(1).as("test_number"), max(col("chislo")).as("quest1_response"))

  /*2.	Вывести количество бронирований с количеством людей больше среднего значения людей на одно бронирование*/
  val avg_book_ref = spark
    .table("school_de.bookings_tickets")
    .groupBy("book_ref")
    .agg(count("*").as("count"))
    .select(avg(col("count")))
    .head()
    .getDouble(0)

  val test_2_result = spark
    .table("school_de.bookings_tickets")
    .groupBy("book_ref")
    .agg(count("*").as("count"))
    .where(col("count") > avg_book_ref)
    .agg(count("*").as("count_res"))
    .select(lit(2).as("id"), col("count_res").as("quest2_response"))

  /*3.	Вывести количество бронирований, у которых состав пассажиров повторялся два и более раза, среди бронирований с максимальным количеством людей (п.1)?*/
  val board_tickets = spark
    .table("school_de.bookings_tickets")

  val pass_count = board_tickets
    .groupBy("passenger_id")
    .agg(count("passenger_id").as("cnt"))
    .filter(col("cnt") > 1)

  val max_pass = board_tickets
    .groupBy("book_ref")
    .agg(count("passenger_id").as("cnt"))
    .agg(max("cnt"))
    .first()
    .getLong(0)

  val test_3_result = board_tickets
    .join(broadcast(pass_count), usingColumn = "passenger_id")
    .groupBy("book_ref")
    .agg(count("passenger_id").as("cnt"))
    .filter(col("cnt") === max_pass)
    .agg(count("book_ref").as("response"))
    .select(lit(3).as("id"), col("response"))

  /*4.	Вывести номера брони и контактную информацию по пассажирам в брони (passenger_id, passenger_name, contact_data) с количеством людей в брони = 3*/
  val book_ref_list = spark
    .table("school_de.bookings_tickets")
    .groupBy("book_ref")
    .agg(count("*").as("count"))
    .where(col("count") === 3)
    .select("book_ref")


  val test_4_result = spark
    .table("school_de.bookings_tickets")
    .join(right = book_ref_list, usingColumn = "book_ref")
    .withColumn("response", concat_ws("|", col("book_ref"), col("passenger_id"), col("passenger_name"), col("contact_data")))
    .select(lit(4).as("id"), col("response"))
    .orderBy("response")

  /*5.	Вывести максимальное количество перелётов на бронь*/
  val bookings_ticket_flights = spark.table("school_de.bookings_ticket_flights")

  val join_tickets_tflights = spark
    .table("school_de.bookings_tickets")
    .join(bookings_ticket_flights, usingColumn = "ticket_no")

  val test_5_result = join_tickets_tflights
    .groupBy(col("book_ref"))
    .agg(count("flight_id").as("count"))
    .select(lit(5).as("id"), max(col("count")).as("quest5_response"))

  /*6.	Вывести максимальное количество перелётов на пассажира в одной брони*/
  val test_6_result = join_tickets_tflights
    .groupBy(col("book_ref"), col("passenger_id"))
    .agg(count("*").as("count"))
    .select(lit(6).as("id"), max(col("count")).as("quest6_response"))


  /*7.	Вывести максимальное количество перелётов на пассажира*/
  val test_7_result = join_tickets_tflights
    .groupBy(col("passenger_id"))
    .agg(count("*").as("count"))
    .select(lit(7).as("id"), max(col("count")).as("quest6_response"))


  /*8.	Вывести контактную информацию по пассажиру(ам) (passenger_id, passenger_name, contact_data) и общие траты на билеты, для пассажира потратившему минимальное количество денег на перелеты*/
  val bookings_flights = spark.table("school_de.bookings_flights")

  val min_amount_join = join_tickets_tflights
    .join(bookings_flights, usingColumn = "flight_id")
    .where("status != 'Cancelled'")

  val min_amount = min_amount_join
    .groupBy(col("passenger_id"))
    .agg(sum("amount").as("sum_amount"))
    .agg(min("sum_amount").as("min_amount"))
    .head()
    .getDecimal(0)

  val test_8_result = min_amount_join
    .groupBy(col("passenger_name"), col("passenger_id"), col("contact_data"))
    .agg(sum("amount").as("sum_amount"))
    .where(col("sum_amount") === min_amount)
    .withColumn("response", concat_ws("|", col("passenger_id"), col("passenger_name"), col("contact_data"), col("sum_amount")))
    .select(lit(8).as("id"), col("response"))
    .orderBy("response")



  /*9.	Вывести контактную информацию по пассажиру(ам) (passenger_id, passenger_name, contact_data) и общее время в полётах, для пассажира, который провёл максимальное время в полётах*/
  val join_tickets_tflights_bflights = join_tickets_tflights
    .join(bookings_flights, usingColumn = "flight_id")
    .where("status == 'Arrived'")
    .withColumn("time", col("actual_arrival").cast("Long") - col("actual_departure").cast("Long"))
    .groupBy(col("passenger_id"), col("passenger_name"), col("contact_data"))
    .agg(sum(col("time")).as("time"))

  val having_val = join_tickets_tflights_bflights
    .agg(max(col("time")).as("time"))

  val test_9_result = join_tickets_tflights_bflights
    .join(having_val, usingColumn = "time")
    .withColumn("Hours", floor(col("time") / 3600))
    .withColumn("Mins", floor((col("time") - (col("Hours") * 3600)) / 60))
    .withColumn("Secs", (col("time") - (col("Hours") * 3600)) - (col("Mins") * 60))
    .withColumn("time_format", format_string("%02d:%02d:%02d", col("Hours"), col("Mins"), col("Secs")))
    .withColumn("response", concat_ws("|", col("passenger_id"), col("passenger_name"), col("contact_data"), col("time_format")))
    .select(lit(9).as("id"), col("response"))
    .orderBy("response")

  /*10.	Вывести город(а) с количеством аэропортов больше одного*/
  val test_10_result = spark
    .table("school_de.bookings_airports")
    .groupBy("city")
    .agg(count("airport_code").as("airport_code"))
    .where(col("airport_code") > 1)
    .select(lit(10).as("id"), col("city").as("response"))
    .orderBy("response")

  /*11.	Вывести город(а), у которого самое меньшее количество городов прямого сообщения*/
  val distinct_lines = spark
    .table("school_de.bookings_flights_v")
    .select("departure_city", "arrival_city").distinct()
    .groupBy("departure_city")
    .agg(count("departure_city").as("num"))

  val distinct_lines_min = distinct_lines
    .agg(min("num").as("num"))

  val test_11_result = distinct_lines
    .join(distinct_lines_min, usingColumn = "num")
    .select(lit(11).as("id"), col("departure_city").as("response"))
    .orderBy("response")

  /*12.	Вывести пары городов, у которых нет прямых сообщений исключив реверсные дубликаты*/
  val test_12_result = spark
    .table("school_de.bookings_airports")
    .as("left")
    .crossJoin(spark.table("school_de.bookings_airports").as("right"))
    .where((col("left.city") =!= col("right.city")) and (col("left.city") < col("right.city")))
    .select("left.city", "right.city")
    .except(spark
      .table("school_de.bookings_routes")
      .select("departure_city", "arrival_city"))
    .withColumn("response", concat_ws("|", col("left.city"), col("right.city")))
    .select(lit(12).as("id"), col("response"))
    .orderBy("response")


  /*13.	Вывести города, до которых нельзя добраться без пересадок из Москвы?*/
  val test_13_result = spark
    .table("school_de.bookings_airports")
    .as("left")
    .crossJoin(spark.table("school_de.bookings_airports").as("right"))
    .where((col("left.city") =!= col("right.city")) and (col("left.city") === "Москва"))
    .select("right.city")
    .except(spark
      .table("school_de.bookings_routes")
      .select("arrival_city")
      .where(col("departure_city") === "Москва"))
    .withColumn("response", concat_ws("|", col("right.city")))
    .select(lit(13).as("id"), col("response"))
    .orderBy("response")

  /*14.	Вывести модель самолета, который выполнил больше всего рейсов*/
  val arrived = spark
    .table("school_de.bookings_flights_v")
    .join(spark.table("school_de.bookings_aircrafts"), usingColumn = "aircraft_code")
    .where(col("status") === "Arrived")

  val test_14_result = arrived
    .groupBy("model")
    .agg(count("model").as("num"))
    .join(arrived
      .groupBy(col("model"))
      .agg(count("model").as("num"))
      .agg(max("num").as("num")), usingColumn = "num")
    .select(lit(14).as("id"), col("model").as("response"))
    .orderBy("response")


  /*15.	Вывести модель самолета, который перевез больше всего пассажиров*/
  val max_boarding_no_join = spark
    .table("school_de.bookings_boarding_passes")
    .groupBy("flight_id")
    .agg(max("boarding_no").as("boarding_num"))
    .select("flight_id", "boarding_num")

  val joined_tables = spark
    .table("school_de.bookings_flights_v")
    .join(spark.table("school_de.bookings_aircrafts"), usingColumn = "aircraft_code")
    .join(max_boarding_no_join, usingColumn = "flight_id")
    .where(col("status") === "Arrived")
    .groupBy("model")
    .agg(sum("boarding_num").as("boarding_num"))
    .select("model", "boarding_num")

  val test_15_result = joined_tables
    .agg(max("boarding_num").as("boarding_num"))
    .select("boarding_num")
    .join(joined_tables, usingColumn = "boarding_num")
    .select(lit(15).as("id"), col("model").as("response"))
    .orderBy("response")


  /*16.	Вывести отклонение в минутах суммы запланированного времени перелета от фактического по всем перелётам*/
  val test_16_result = spark
    .table("school_de.bookings_flights_v")
    .where(col("status") === "Arrived")
    .withColumn("div", to_timestamp(col("scheduled_duration")).cast("Long") - to_timestamp(col("actual_duration")).cast("Long"))
    .select(lit(16).as("id"), floor(sum("div") / 60).as("response"))

  /*17.	Вывести города, в которые осуществлялся перелёт из Санкт-Петербурга 2016-09-13*/
    val test_17_result = spark
    .table("school_de.bookings_flights_v")
    .withColumn("date", expr("DATE(actual_departure)"))
    .where(col("date") === to_date(lit("2016-09-13")) and col("status") === "Arrived" and col("departure_city") === "Санкт-Петербург")
    .select("arrival_city").distinct()
    .select(lit(17).as("id"), col("arrival_city").as("response"))
    .orderBy("response")


  /*18.	Вывести перелёт(ы) с максимальной стоимостью всех билетов*/
  val join_bflights_btflights = spark
    .table("school_de.bookings_flights")
    .join(spark.table("school_de.bookings_ticket_flights"), usingColumn = "flight_id")
    .where(col("status") =!= "Cancelled")
    .groupBy("flight_id")
    .agg(sum("amount").as("amount"))

  val test_18_result = join_bflights_btflights
    .agg(max("amount").as("amount"))
    .join(join_bflights_btflights, usingColumn = "amount")
    .select(lit(18).as("id"), col("flight_id").as("response"))
    .orderBy("response")

  /*19.	Выбрать дни в которых было осуществлено минимальное количество перелётов*/
  val select_from_bookings_flights_v = spark
    .table("school_de.bookings_flights_v")
    .where(col("status") =!= "Cancelled" and col("actual_departure").isNotNull)
    .withColumn("date", expr("DATE(actual_departure)"))
    .groupBy("date")
    .agg(count("*").as("num"))

  val test_19_result = select_from_bookings_flights_v
    .agg(min("num").as("num"))
    .join(select_from_bookings_flights_v, usingColumn = "num")
    .select(lit(19).as("id"), col("date").as("response"))
    .orderBy("response")

  /*20.	Вывести среднее количество вылетов в день из Москвы за 09 месяц 2016 года*/
  val test_20_result = spark
    .table("school_de.bookings_flights_v")
    .where((col("status") === "Departed" or col("status") === "Arrived") and col("departure_city") === "Москва" and col("actual_departure").isNotNull)
    .withColumn("date", expr("DATE(actual_departure)"))
    .withColumn("year", year(col("date")))
    .withColumn("month", month(col("date")))
    .withColumn("day", dayofweek(col("date")))
    .where(col("year") === "2016" and col("month") === "9")
    .groupBy("day")
    .agg(count("flight_id").as("num"))
    .agg(sum("num").as("sum_num"))
    .withColumn("response", floor(round(col("sum_num") / 30)))
    .select(lit(20).as("id"), col("response"))
    .orderBy("response")

  /*21.	Вывести топ 5 городов у которых среднее время перелета до пункта назначения больше 3 часов*/
  val hour3 = to_timestamp(lit("03:00:00")).cast("Long")
  val test_21_result = spark
    .table("school_de.bookings_flights_v")
    .where(col("status") === "Arrived")
    .withColumn("actual_duration_sec", to_timestamp(col("actual_duration")).cast("Long"))
    .groupBy("departure_city")
    .agg(avg("actual_duration_sec").as("avg"))
    .where(col("avg") > hour3)
    .orderBy(desc("avg")).limit(5)
    .select(lit(21).as("id"), col("departure_city").as("response"))
    .orderBy("response")


  val all_results = test_1_result
    .union(test_2_result)
    .union(test_3_result)
    .union(test_4_result)
    .union(test_5_result)
    .union(test_6_result)
    .union(test_7_result)
    .union(test_8_result)
    .union(test_9_result)
    .union(test_10_result)
    .union(test_11_result)
    .union(test_12_result)
    .union(test_13_result)
    .union(test_14_result)
    .union(test_15_result)
    .union(test_16_result)
    .union(test_17_result)
    .union(test_18_result)
    .union(test_19_result)
    .union(test_20_result)
    .union(test_21_result)

  all_results
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("path", "hdfs://ns-etl/warehouse/tablespace/external/hive/school_de_stg.db/results_svakulin")
    .saveAsTable("school_de_stg.results_svakulin")

  spark.stop()

}


