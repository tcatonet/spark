from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp, from_unixtime, count,col, date_format, current_date, months_between, datediff, current_timestamp,expr, to_timestamp
from pyspark.sql.types import *

spark = SparkSession \
    .builder \
    .appName("Projet") \
    .master("local[*]") \
    .getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY") \

mounth_to_int = { "Jan":"01", "Feb":"02", "Mar":"03", "Apr":"04", "May":"05", "Jun":"06", "Jul":"07", "Aug":"08", "Sep":"09", "Oct":"10", "Nov":"11", "Dec":"12" }

def format_date(date):
    print("DATA")
    print(date.__dict__)
    print("DATA")

    table_date = date.split(" ")
    year = table_date[4]
    mounth = mounth_to_int[table_date[1]]
    day = mounth_to_int[2]
    return year + "-" + mounth + "-" + day

 #text = spark.sparkContext.textFile("data/full.csv")
text = spark.read.format("csv").option("header", "true").load("data/full.csv")
#textRdd = text.flatMap(lambda line: line.split(" ")) \
#    .map(lambda word: (word, 1)) \
#    .reduceByKey(lambda a, b: a + b) \
 #   .sortBy(lambda a: a[1], ascending=False) \
#    .coalesce(1)


#textRdd.saveAsTextFile("results/wordcount.csv")
#print(textRdd.getNumPartitions())


#question1 = text.select("repo", "commit") \
 #   .groupBy("repo") \
#    .agg(count("commit").alias("Total_commit")) \
#    .orderBy("Total_commit", ascending=False) \
#    .limit(10) \
 #   .show()

#question2 = text.select("repo", "author") \
#    .where(col("repo") == "apache/spark") \
 #   .groupBy("author") \
 #   .agg(count("author").alias("Total_author")) \
#    .orderBy("Total_author", ascending=False) \
 #   .limit(1) \
#    .show()

print(current_date())
print(date_format('1970-01-01', "M"))
res = date_format(current_timestamp(),"yyyy MM dd")
print(res)

 #question3 = text.select(to_timestamp(format_date(col("date")), 'yyyy-MM-dd HH:mm:ss')).show()
    
    #& (months_between(current_date(),col("date")) != 24))




#question3 = text.select("repo", "author", "date") \
#    .where(col("repo") == "apache/spark") \
 #   .filter(format_date(col("date")) >= current_date() - expr("INTERVAL 7000 days")) \
#    .groupBy("author") \
 #   .agg(count("author").alias("Total_author")) \
#    .orderBy("Total_author", ascending=False) \
 #   .limit(50) \
 #   .show()

   # .filter(months_between(current_date(),col("date")) < 24) \



#data = [("1","2019-07-01"),("2","2019-06-24"),("3","2019-08-24")]
#df=spark.createDataFrame(data=data,schema=["id","date"])


df = spark.createDataFrame([('ZZa 2019-01-24',)], ['Date_col'])
df.select(date_format(col("Date_col"),"yyyy MM dd").alias("yyyy MM dd")).show()
