from collections import defaultdict

import nltk
import pandas
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType

import config
import pathmng
import utils
import json
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize


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


# print(AwardsCheckExist.get_instance().check("EMIL JANNINGS", 1929))
# breakpoint()


def get_extract_release_year_func():
    def _release_year(*columns):
        for _col in columns:
            year = utils.filter_year(_col)
            if year is not None:
                return year

    return udf(lambda *columns: _release_year(*columns))


def count_awards(*columns):
    def _count_awards(*columns):
        try:
            release_year, actor_col, director_col = columns
            actor_director_set = set()
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


def get_rotten_and_imdb_joining_df():
    df_rotten = spark.read.csv(path=rotten_path, header=True)\
        .withColumn("title", normalize_title_func("title"))\
        .withColumn("release_year", get_extract_release_year_func()("theater_release_date", "dvd_release_date", "streaming_release_date"))\
        .filter(col("runtime").isNotNull())\
        .withColumn("runtime", udf(lambda runtime_col: utils.time_str_to_int(runtime_col), IntegerType())("runtime"))\
        .drop("runtime")\
        .filter(col("audience_score").isNotNull())\
        .withColumn("count_award", count_awards("release_year", "casts", "directors"))

    def filter_error():
        pass

    df_imdb = spark.read.csv(path=imdb_path, header=True)\
            .filter(col("runtime").contains("min"))\
            .drop("genre")\
            .withColumn("runtime", udf(lambda x: int(str(x).replace(" min", "")), IntegerType())("runtime"))\
            .withColumn("title", normalize_title_func("title"))

    df_rotten.show()
    # df_imdb.show()
    # df_main = df_rotten
    df_main = df_rotten.join(df_imdb, on=["title", "release_year"]).dropDuplicates(["title", "release_year"])
    print("count df_main:", df_main.count())
    return df_main


class ListEncoder:

    def __init__(self, all_elements):
        self.all_elements = all_elements
        self.element_to_index = {}
        for i in range(len(self.all_elements)):
            self.element_to_index[self.all_elements[i]] = i

    def get_bit_vector(self, elements):
        vector = [0] * len(self.all_elements)
        for ele in elements:
            if ele in self.element_to_index:
                vector[self.element_to_index[ele]] = 1
        return tuple(vector)


def build_main():
    df = get_rotten_and_imdb_joining_df()
    df = df.select("runtime", "count_award", "genre", "plot_des", "audience_score")
    genre_list = []

    paragraph_list = []
    word_list = []
    temp_row_list = df.select("genre", "plot_des").rdd\
        .collect()

    # get word list
    for row in temp_row_list:
        genre_list.extend(utils.get_list_from_str_json(row.genre))
        if row.plot_des is not None:
            paragraph_list.append(row.plot_des)
    for paragraph in paragraph_list:
        paragraph: str
        # normalize word
        word_list.extend(utils.get_words_without_stopword(utils.remove_non_alphabet(paragraph.lower().strip())))

    # build bit vector
    genre_encoder = ListEncoder(utils.filter_duplicate(genre_list))
    from collections import Counter
    occurrence_count = Counter(word_list)
    word_list = [_[0] for _ in occurrence_count.most_common(100)]
    word_encoder = ListEncoder(word_list)
    print(utils.filter_duplicate(genre_list))
    print(word_list)

    def each_row(row):
        res = [row.runtime, int(row.count_award)]
        res.extend(genre_encoder.get_bit_vector(utils.get_list_from_str_json(row.genre)))
        res.extend(word_encoder.get_bit_vector(str(row.plot_des).split(" ")))
        res.append(int(row.audience_score))
        return tuple(res)

    res = df.rdd.map(lambda row: each_row(row)).collect()

    import numpy as np
    print(np.array(res))
    np.save(pathmng.movie_vector_path, np.array(res))


if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").appName("oke").getOrCreate()
    build_main()
