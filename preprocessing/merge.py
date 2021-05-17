from collections import defaultdict

import nltk
import pandas
import pyspark
from django.utils.functional import lazy
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType

import config
import pathmng
import utils
import json
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

from crawl.imdb import lazyCrawl

rotten_path = "file:///" + config.crawl_data_path + "\\rotten.csv"
cleaned_rotten_path = "file:///" + config.crawl_data_path + "\\rotten_cleaned\\"

imdb_path = "file:///" + config.crawl_data_path + "\\imdb.csv"
cleaned_imdb_path = "file:///" + config.crawl_data_path + "\\imdb_cleaned.csv"

normalize_title_func = udf(lambda x: utils.normalize_string(x))


class AwardsCheckExist:

    name_to_years = defaultdict(lambda: set())
    _singleton = None

    def __init__(self):
        df = pandas.read_csv(pathmng.wiki_best_actor_director_path)
        for index, row in df.iterrows():
            self.name_to_years[utils.normalize_string(row["name"])].add(row.year)

    def check(self, name, release_year):
        years = self.name_to_years[utils.normalize_string(name)]
        for year in years:
            if int(release_year) > year:
                return True
        return False

    @staticmethod
    def get_instance():
        if AwardsCheckExist._singleton is None:
            AwardsCheckExist._singleton = AwardsCheckExist()
        return AwardsCheckExist._singleton


def count_awards(*columns):
    def _count_awards(*columns):
        try:
            release_year, actor_col, director_col = columns
            actors = json.loads(actor_col.replace("'", "\""))
            directors = json.loads(director_col.replace("'", "\""))
            actor_director_set = set(actors) | set(directors)
            counter = 0
            for p in actor_director_set:
                if AwardsCheckExist.get_instance().check(p, release_year):
                    counter += 1
            return counter
        except:
            return 0

    return udf(lambda *columns: _count_awards(*columns))(*columns)


def re_crawl_and_get_imdb_detail_df(df: DataFrame):
    all_link = df.select("imdb_link").rdd.map(lambda row: row["imdb_link"]).collect()
    lazyCrawl.crawl_all_movie_detail(all_link)
    return spark.read.csv(utils.build_hadoop_local_path(path=pathmng.imdb_detail_path), header=True)


def build_movie_dataframe():
    df_rotten = utils.read_csv_with_pyspark(spark, pathmng.rotten_path)\
        .withColumn("title", normalize_title_func("title"))\
        .filter(col("audience_score").isNotNull())\
        .drop("runtime")\
        .withColumnRenamed("link", "rotten_link")\
        .withColumn("count_award", count_awards("release_year", "casts", "directors"))\
        .dropDuplicates(["title", "release_year"])

    df_imdb = spark.read.csv(path=imdb_path, header=True) \
            .withColumn("title", normalize_title_func("title"))\
            .filter(col("runtime").contains("min"))\
            .drop("genre")\
            .drop("budget")\
            .drop("opening_weekend_gross")\
            .drop("box_office_gross")\
            .drop("certificate")\
            .withColumn("runtime", udf(lambda x: int(str(x).replace(" min", "")), IntegerType())("runtime"))\
            .withColumnRenamed("link", "imdb_link")\

    df_rotten.show()
    df_imdb.show()

    df_main = df_rotten.join(df_imdb, on=["title", "release_year"]).dropDuplicates(["title", "release_year"])

    print("count df_main:", df_main.count())
    imdb_detail_df = re_crawl_and_get_imdb_detail_df(df_main).withColumnRenamed("link", "imdb_link")

    df_main = df_main.join(imdb_detail_df, on=["imdb_link"])

    df_main.show()

    print("Writing all movie data...")
    df_main.toPandas().to_csv(pathmng.all_cleaned_movie_path, index=False)

    return df_main


def normalize_data():
    df_main = utils.read_csv_with_pyspark(spark, pathmng.all_cleaned_movie_path)\
        .filter(col("budget").isNotNull() & col("plot_des").isNotNull() & col("theater_release_date").isNotNull())\
        .filter(col("box_office_gross").isNotNull() & col("opening_weekend_gross").isNotNull())\
        .filter(col("critic_score").isNotNull())\
        .withColumn("budget", udf(lambda _col: utils.string_to_int_by_filter_number(_col), IntegerType())("budget"))\
        .withColumn("box_office_gross", udf(lambda _col: utils.string_to_int_by_filter_number(_col), IntegerType())("box_office_gross"))\
        .withColumn("opening_weekend_gross", udf(lambda _col: utils.string_to_int_by_filter_number(_col), IntegerType())("opening_weekend_gross"))

    df_main = df_main.filter(col("box_office_gross").isNotNull())
    df_main.show()
    print("Size after normalized:", df_main.count())
    return df_main


spark = SparkSession.builder.master("local[*]").appName("oke").getOrCreate()
if __name__ == '__main__':
    # spark.createDataFrame([["a", "b"]], ["a", "b"]).toPandas().to_csv(pathmng.temp_path)
    # build_movie_dataframe()
    normalize_data()