from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col

import pathmng
import utils
from preprocessing.data_model import DataModel
from preprocessing.merge import normalize_data


def get_words_from_plot_des(paragraph_list):
    word_list = []
    for paragraph in paragraph_list:
        paragraph: str
        # normalize word
        word_list.extend(utils.get_words_without_stopword(utils.remove_non_alphabet(paragraph.lower().strip())))
    return utils.get_most_common(word_list, 90)


def build_main():
    # df = spark.read.csv(utils.build_hadoop_local_path(pathmng.all_cleaned_movie_path))
    # df = df.select("runtime", "count_award", "genre", "plot_des", "audience_score")
    df = normalize_data().replace("Not Rated", "Unrated")

    genre_list = []
    paragraph_list = []
    movie_studios = []
    mpaa_ratings = []

    temp_row_list = df.select("genre", "plot_des", "studio", "mpaa_rating").rdd\
        .collect()

    common_studio_df = df.select("studio")\
        .groupby("studio")\
        .agg(count("*").alias("count"))\
        .orderBy(col("count").desc())\
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

    print(utils.filter_duplicate(movie_studios))
    print(utils.filter_duplicate(mpaa_ratings))
    print(utils.filter_duplicate(genre_list))
    print(utils.filter_duplicate(plot_des_word_list))

    # row0 = df.rdd.take(1)[0]
    # data_model.get_feature_for_linear(row0)

    res = df.rdd.map(lambda row: tuple(data_model.get_feature_for_linear(row)))
    print("size vector", len(res.take(1)[0]))
    res = res.collect()
    import numpy as np
    np.save(pathmng.movie_vector_path, np.array(res))


if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").appName("oke").getOrCreate()
    build_main()
