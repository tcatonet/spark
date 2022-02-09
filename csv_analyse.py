from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col, count, months_between, to_date, current_timestamp, current_date, explode, split
from constantes import * 
from conf import * 

class Csv_analyse:

    def __init__(self) :
        self.spark = SparkSession\
                    .builder\
                    .appName(APP_NAME)\
                    .master(MASTER_OPTION)\
                    .getOrCreate()\

        self.spark.sql(PARSER_POLICY)

        self.dataframe_object = self.spark.read.format(FORMAT_OPTION) \
                                .option(HEADER_OPTION, HEADER_VALUE) \
                                .option(INFERSCHEMA_OPTION, INFERSCHEMA_VALUE) \
                                .load(GITHUB_DATA_PATH)

        self.stopword_list = []
        for stop_word in self.spark.read.text(STOPWORD_DATA_PATH).collect():
            self.stopword_list.append(stop_word.value)


    def display_projet_with_max_commit(self):
        print("display_projet_with_max_commit")
        request = self.dataframe_object\
            .select(REPO, COMMIT) \
            .groupBy(REPO) \
            .agg(count(COMMIT).alias(TOTAL)) \
            .orderBy(TOTAL, ascending=False) \
            .limit(10) \
            .show()
            
    def display_best_contributor(self):
        print("display_best_contributor")
        request = self.dataframe_object\
            .select(REPO, AUTHOR) \
            .where(col(REPO) == NAME_SPARK_REPO) \
            .groupBy(AUTHOR) \
            .agg(count(AUTHOR).alias(TOTAL)) \
            .orderBy(TOTAL, ascending=False) \
            .limit(1) \
            .show()


    def display_commit_24_mounth(self):
        print("display_commit_24_mounth")
        request = self.dataframe_object\
            .select(AUTHOR, months_between(current_timestamp(), to_date('date', DATE_PATTERN)).alias(DIFF_MOUNTH))\
            .filter(col(REPO) == NAME_SPARK_REPO)\
            .filter(col(DIFF_MOUNTH) <= 24)\
            .groupBy(AUTHOR)\
            .agg(count(AUTHOR).alias(TOTAL))\
            .orderBy(col(TOTAL), ascending = False)\
            .limit(10)\
            .show()


    def display_10_first_word_in_commit(self):
        print("display_10_first_word_in_commit")
        request = self.dataframe_object\
            .select(explode(split(MESSAGE, " ")).alias(WORD_LIST) )\
            .filter(~col(WORD_LIST).isin(self.stopword_list))\
            .filter(col(MESSAGE).isNotNull())\
            .groupBy(WORD_LIST)\
            .agg(count(WORD_LIST).alias(TOTAL)) \
            .orderBy(col(TOTAL), ascending = False)\
            .limit(10)\
            .show()
            