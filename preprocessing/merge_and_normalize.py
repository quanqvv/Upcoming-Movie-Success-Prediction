from collections import defaultdict

import nltk
import pandas
import pyspark
from django.utils.functional import lazy
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, StringType

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
    return utils.read_csv_with_pyspark(spark=spark, path=pathmng.imdb_detail_path)


def build_movie_dataframe():
    df_rotten = utils.read_csv_with_pyspark(spark, pathmng.rotten_path)\
        .withColumn("title", normalize_title_func("title"))\
        .filter(col("audience_score").isNotNull())\
        .drop("runtime")\
        .withColumnRenamed("link", "rotten_link")\
        .withColumn("count_award", count_awards("release_year", "casts", "directors"))\
        .dropDuplicates(["title", "release_year"])\
        .filter(col("critic_score").isNotNull())

    # print("Rotten")
    # df_rotten.show()

    df_imdb = utils.read_csv_with_pyspark(spark, pathmng.imdb_path) \
            .withColumn("title", normalize_title_func("title"))\
            .filter(col("runtime").contains("min"))\
            .filter(col("plot_des").isNotNull())\
            .filter(col("user_rate").isNotNull())\
            .drop("genre", "imdb_rating")\
            .withColumn("runtime", udf(lambda x: int(str(x).replace(" min", "")), IntegerType())("runtime")) \
            .withColumnRenamed("link", "imdb_link")\
            .withColumnRenamed("user_rate", "audience_score")\

    print("Imdb")
    df_imdb.show()

    # df_main = df_rotten.join(df_imdb, on=["title", "release_year"]).dropDuplicates(["title", "release_year"])
    df_main = df_imdb.drop_duplicates(["title", "release_year"])

    df_duplicate = df_imdb.subtract(df_main)
    print("count df_duplicate:", df_duplicate.count())

    imdb_detail_df = re_crawl_and_get_imdb_detail_df(df_main)\
        .withColumnRenamed("link", "imdb_link")

    df_main = df_main.join(imdb_detail_df, on=["imdb_link"])\
        .withColumn("award", count_awards("release_year", "casts", "directors")) \

    # for _col in df_main.columns:
    #     if _col in df_rotten.columns and _col != "title" and _col != "release_year":
    #         df_main = df_main.drop(_col)
    # df_main = df_main.join(df_rotten, on = ["title", "release_year"])

    df_main.show()

    print("count df main", df_main.count())

    df_main.toPandas().to_csv(pathmng.all_cleaned_movie_path, index=False)

    return df_main


spark = SparkSession.builder.master("local[*]").config("spark.executor.memory", "3g").appName("oke").getOrCreate()
if __name__ == '__main__':
    # spark.createDataFrame([["a", "b"]], ["a", "b"]).toPandas().to_csv(pathmng.temp_path)
    build_movie_dataframe()
    # normalize_data(write=True)