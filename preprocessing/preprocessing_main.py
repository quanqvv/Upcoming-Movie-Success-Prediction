from pyspark.sql.functions import col, lit

import utils
import pathmng


def show_all():
    # df_imdb = utils.read_csv_with_pyspark(spark, pathmng.imdb_path).limit(10) \
    #     .filter(col("runtime").contains("min")) \
    #     .filter(col("plot_des").isNotNull()) \
    #     .filter(col("user_rate").isNotNull()) \
    #     .drop("genre", "user_rate", "imdb_rating") \
    #     .join(utils.read_csv_with_pyspark(spark=spark, path=pathmng.imdb_detail_path), ["link"]).drop("link")\
    #     .select("title",  "release_year", "mpaa_rating", "plot_des", "budget", "box_office_gross", "opening_weekend_gross")
    #
    # df_rotten = utils.read_csv_with_pyspark(spark, pathmng.rotten_path).limit(10) \
    #     .filter(col("audience_score").isNotNull()) \
    #     .drop("runtime").drop("dvd_release_date").drop("link")\
    #     .dropDuplicates(["title", "release_year"]) \
    #     .filter(col("critic_score").isNotNull())\
    #     .select("title", "release_year", "audience_score", "theater_release_date", "genre", "casts", "directors")
    #
    # df_wiki = utils.read_csv_with_pyspark(spark, pathmng.wiki_best_actor_director_path).\
    #     withColumn("role", lit("actor")).limit(10)
    #
    # df_rotten.show()
    # df_imdb.show()
    # df_wiki.show()

    # df_all = utils.read_csv_with_pyspark(spark, pathmng.all_cleaned_movie_path).show()
    df_all = utils.read_csv_with_pyspark(spark, pathmng.final_movie_path).show()


if __name__ == '__main__':
    spark = utils.get_spark()
    show_all()