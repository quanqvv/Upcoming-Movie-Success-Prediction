from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import count, col, udf
from pyspark.sql.types import IntegerType, StringType

import pathmng
import utils
from preprocessing.data_model import DataModel


def get_words_from_plot_des(paragraph_list):
    word_list = []
    for paragraph in paragraph_list:
        paragraph: str
        # normalize word
        word_list.extend(utils.get_words_without_stopword(utils.remove_non_alphabet(paragraph.lower().strip())))
    return utils.get_most_common(word_list, 90)


def build_main(normalize_func):
    # df = spark.read.csv(utils.build_hadoop_local_path(pathmng.all_cleaned_movie_path))
    # df = df.select("runtime", "count_award", "genre", "plot_des", "audience_score")
    df = normalize_func().replace("Not Rated", "Unrated")

    genre_list = []
    paragraph_list = []
    movie_studios = []
    mpaa_ratings = []

    temp_row_list = df.select("genre", "plot_des", "studio", "mpaa_rating").rdd \
        .collect()

    common_studio_df = df.select("studio") \
        .groupby("studio") \
        .agg(count("*").alias("count")) \
        .orderBy(col("count").desc()) \
        .limit(22)

    for row in temp_row_list:
        genre_list.extend(utils.get_list_from_str_json(row.genre))
        if row.plot_des is not None:
            paragraph_list.append(row.plot_des)
        if row.mpaa_rating is not None:
            mpaa_ratings.append(row.mpaa_rating)

    for row in common_studio_df.collect():
        if row.studio is not None:
            movie_studios.append(row.studio)

    plot_des_word_list = get_words_from_plot_des(paragraph_list)

    data_model = DataModel(utils.filter_duplicate(movie_studios),
                           utils.filter_duplicate(plot_des_word_list),
                           utils.filter_duplicate(mpaa_ratings),
                           utils.filter_duplicate(utils.get_most_common(genre_list, 22)))

    import pickle
    pickle.dump(data_model, open(pathmng.data_model_path, "wb"))

    print("Studio:", len(utils.filter_duplicate(movie_studios)), utils.filter_duplicate(movie_studios))
    print("MPAA Rating:", len(utils.filter_duplicate(mpaa_ratings)), utils.filter_duplicate(mpaa_ratings))
    print("Genre:", len(utils.filter_duplicate(genre_list)), utils.filter_duplicate(genre_list))
    print("Plot Des:", len(utils.filter_duplicate(plot_des_word_list)), utils.filter_duplicate(plot_des_word_list))

    full_feature = df.rdd.map(lambda row: tuple(data_model.get_full_feature(row)))
    features_for_audience_score= df.rdd.map(lambda row: tuple(data_model.get_feature_for_audience_score(row)))
    features_for_box_office_gross = df.rdd.map(lambda row: tuple(data_model.get_feature_for_box_office_gross(row)))

    import numpy as np
    np.save(pathmng.movie_full_feature_vector_path, np.array(full_feature.collect()))
    np.save(pathmng.movie_audience_score_vector_path, np.array(features_for_audience_score.collect()))
    np.save(pathmng.movie_box_office_gross_vector_path, np.array(features_for_box_office_gross.collect()))


def normalize_data(write=False):
    df_main = utils.read_csv_with_pyspark(spark, pathmng.all_cleaned_movie_path)\
        .filter(col("budget").isNotNull() & col("plot_des").isNotNull())\
        .filter(col("box_office_gross").isNotNull() & col("opening_weekend_gross").isNotNull())\
        .withColumn("budget", udf(lambda _col: utils.string_to_int_by_filter_number(_col), IntegerType())("budget"))\
        .withColumn("box_office_gross", udf(lambda _col: utils.string_to_int_by_filter_number(_col), IntegerType())("box_office_gross"))\
        .withColumn("theater_release_date", udf(lambda _col: utils.normalize_date_string(_col), StringType())(col("theater_release_date"))).filter(col("theater_release_date").isNotNull())\
        .withColumn("opening_weekend_gross", udf(lambda _col: utils.string_to_int_by_filter_number(_col), IntegerType())("opening_weekend_gross"))

    df_main = df_main.filter(col("box_office_gross").isNotNull())
    df_main = normalize_money(df_main, "budget", "box_office_gross", "opening_weekend_gross")

    print("Size after normalized:", df_main.count())
    if write:
        df_main.toPandas().to_csv(pathmng.all_cleaned_movie_path, index=False)
    df_main.drop("opening_weekend_gross").drop("dvd_release_date").drop("count_award").show()
    df_main = df_main.union(df_main)
    return df_main


def normalize_money(df: DataFrame, *money_column):
    func = udf(lambda money_col, release_year: utils.get_exchanged_usd(money_col, release_year), IntegerType())
    for _col in money_column:
        df = df.withColumn(_col, func(col(_col), col("release_year")))
    return df


def normalize_data_for_audience_score(write=True):
    # .filter(col("budget").isNotNull() & col("plot_des").isNotNull())
    df_main = utils.read_csv_with_pyspark(spark, pathmng.all_cleaned_movie_path)\
        .filter(col("plot_des").isNotNull())\
        .withColumn("budget", udf(lambda _col: utils.string_to_int_by_filter_number(_col), IntegerType())("budget"))\
        .withColumn("box_office_gross", udf(lambda _col: utils.string_to_int_by_filter_number(_col), IntegerType())("box_office_gross")) \
        .withColumn("theater_release_date", udf(lambda _col: utils.normalize_date_string(_col), StringType())(col("theater_release_date")))\
        .filter(col("theater_release_date").isNotNull()) \
        .withColumn("opening_weekend_gross", udf(lambda _col: utils.string_to_int_by_filter_number(_col), IntegerType())("opening_weekend_gross"))

    df_main = normalize_money(df_main, "budget", "box_office_gross", "opening_weekend_gross")\
        .filter("box_office_gross != 0")\


    # def normalize_budget(_col, _box_office):
    #     if _col == 0:
    #         return int(_box_office / 2)
    #     else:
    #         return _col
    # df_main = df_main.withColumn("budget", udf(normalize_budget, StringType())(col("budget"), col("box_office_gross")))
    # df_main = df_main.withColumn("audience_score", udf(lambda _col: float(_col) * 10, StringType())(col("audience_score"))).drop("imdb_link")

    print("Size after normalized:", df_main.count())

    if write:
        df_main.toPandas().to_csv(pathmng.all_cleaned_movie_path_oke, index=False)

    return df_main


if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").appName("oke").getOrCreate()
    # build_main(normalize_func=normalize_data)
    build_main(normalize_func=normalize_data_for_audience_score)
