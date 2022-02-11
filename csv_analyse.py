from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col, count, months_between, to_date, current_timestamp, current_date, explode, split, desc, dense_rank
from constantes import * 
from conf import * 
from pyspark.sql.window import Window
import pyspark
import time

class Csv_analyse:

    def __init__ (self) :
        self.spark = SparkSession\
                    .builder\
                    .appName(APP_NAME)\
                    .master("local[6]")\
                    .config("spark.executor.memory", "4g") \
                    .config("spark.executor.cores", 6) \
                    .getOrCreate()

        self.spark.sql(PARSER_POLICY)
        self.dataframe_object = self.spark.read.format(FORMAT_OPTION) \
                                .option(HEADER_OPTION, HEADER_VALUE) \
                                .option(INFERSCHEMA_OPTION, INFERSCHEMA_VALUE) \
                                .load(GITHUB_DATA_PATH)


        self.stopword_list = []
        for stop_word in self.spark.read.text(STOPWORD_DATA_PATH).collect():
            self.stopword_list.append(stop_word.value)

        self.dataframe_object.write.mode("overwrite").parquet("./full.parquet")
        self.parDF1=self.spark.read.parquet("./full.parquet")
        self.parDF1.createOrReplaceTempView("fullTable")
        print(self.parDF1.rdd.getNumPartitions())


    def display_projet_with_max_commit(self):
        print("display_projet_with_max_commit")
        time_start = time.perf_counter()
        request = self.parDF1\
            .select(REPO, COMMIT) \
            .groupBy(REPO) \
            .agg(count(COMMIT).alias(TOTAL)) \
            .orderBy(TOTAL, ascending=False) \
            .limit(10) \
            .show()

        print(time.perf_counter() - time_start)


    def display_best_contributor(self):
        print("display_best_contributor")
        time_start = time.perf_counter()

        request = self.parDF1\
            .select(REPO, AUTHOR) \
            .where(col(REPO) == NAME_SPARK_REPO) \
            .groupBy(AUTHOR) \
            .agg(count(AUTHOR).alias(TOTAL)) \
            .orderBy(TOTAL, ascending=False) \
            .limit(1) \
            .show()
        print(time.perf_counter() - time_start)


    def display_commit_24_mounth(self):

        print("display_commit_24_mounth")
        time_start = time.perf_counter()

        request = self.parDF1\
            .select(AUTHOR, months_between(current_timestamp(), to_date('date', DATE_PATTERN)).alias(DIFF_MOUNTH))\
            .filter(col(REPO) == NAME_SPARK_REPO)\
            .filter(col(DIFF_MOUNTH) <= 24)\
            .groupBy(AUTHOR)\
            .agg(count(AUTHOR).alias(TOTAL))\
            .orderBy(col(TOTAL), ascending = False)\
            .limit(10)\
            .show()

        print(time.perf_counter() - time_start)


    def display_10_first_word_in_commit(self):
        print("display_10_first_word_in_commit")
        time_start = time.perf_counter()

 #       request = self.parDF1\
  #          .select(explode(split(MESSAGE, " ")).alias(WORD_LIST) )\
  #          .filter(~col(WORD_LIST).isin(self.stopword_list))\
#            .rdd.map(lambda x: (x,1))\
   #         .reduceByKey(lambda a,b: a+b)\
#            .toDF()

        request = self.parDF1\
            .select(explode(split(MESSAGE, " ")).alias(WORD_LIST) )\
            .filter(~col(WORD_LIST).isin(self.stopword_list))\
            .groupBy(WORD_LIST)\
            .agg(count(WORD_LIST).alias(TOTAL)) \
            .orderBy(col(TOTAL), ascending = False)\
            .limit(10).show()

        print(time.perf_counter() - time_start)
        
