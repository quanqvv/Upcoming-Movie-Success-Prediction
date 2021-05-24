import json

import pandas
from nltk import word_tokenize
from nltk.corpus import stopwords
from pyspark.sql import SparkSession
from selenium import webdriver
import config
import pathmng


def get_driver(direct_profile=config.browser_profile_path):
    options = webdriver.ChromeOptions()
    options.add_argument(f"--user-data-dir={direct_profile}")
    driver = webdriver.Chrome(config.driver_path, options=options)
    return driver


def remove_non_alphabet(text: str):
    import re
    return re.sub("[^a-zA-Z0-9 ]", "", text)


def normalize_string(text: str):
    text = remove_non_alphabet(text).strip().upper()
    return text


def filter_year(text: str):
    import re
    try:
        return re.search("[0-9]{4}", text).group()
    except:
        pass


def filter_number(text: str):
    import re
    try:
        return re.search("[0-9]*", text).group()
    except:
        pass


def time_str_to_int(text: str):
    try:
        splitted = text.split(" ")
        if len(splitted) == 1:
            return int(filter_number(splitted[0]))
        else:
            return int(filter_number(splitted[0])) * 60 + int(filter_number(splitted[1]))
    except:
        return 50


def get_list_from_str_json(text: str):
    try:
        res: list
        res = json.loads(text.replace("'", "\""))
        return res
    except:
        return []


all_stopwords = set(stopwords.words("english"))


def get_words_without_stopword(text):
    words = word_tokenize(text.strip())
    res = []
    for word in words:
        if word not in set(all_stopwords):
            res.append(word)
    return res


def filter_duplicate(_list):
    return list(set(_list))


def filter_duplicate_preserve_order(_list):
    return list(dict.fromkeys(_list))


def measure_accuracy(label_predicted, label_origin):
    error = 0
    for index in range(label_predicted.size):
        error += abs(label_predicted[index] - label_origin[index])/label_origin[index]

    print("Accuracy:", 1 - error/len(label_origin))


def try_catch(func):
    try:
        return func()
    except:
        pass


def split_list_to_group(number_per_group, _list):
    final = [_list[i * number_per_group:(i + 1) * number_per_group] for i in range((len(_list) + number_per_group - 1) // number_per_group)]
    return final


def build_hadoop_local_path(path):
    return "file:///"+path


def read_csv_with_pyspark(spark: SparkSession, path):
    return spark.createDataFrame(pandas.read_csv(path, dtype=str).astype(str)).replace("nan", None)


def save_pyspark_df_to_csv(df, path):
    df.toPandas().to_csv(path, index=False)


def normalize_date_string(_str):
    try:
        return get_datetime_from_string(_str).strftime("%Y-%m-%d")
    except:
        pass
    return None


def get_datetime_from_string(date_str):
    from datetime import datetime
    try:
        return datetime.strptime(date_str, '%b %d, %Y')
    except:
        try:
            return datetime.strptime(date_str, "%Y-%m-%d")
        except:
            try:
                return datetime.strptime(date_str, "%B %d %Y")
            except:
                try:
                    return datetime.strptime(date_str, "%d %B %Y")
                except:
                    pass
    return None


def string_to_int_by_filter_number(text: str):
    import re
    try:
        return int(re.sub("[^0-9]", "", text))
    except:
        return 0


def get_most_common(_list, num):
    from collections import Counter
    occurrence_count = Counter(_list)
    return [_[0] for _ in occurrence_count.most_common(num)]


def get_spark():
    return SparkSession.builder.master("local[*]").appName("oke").getOrCreate()


year_to_cpi = {}


def get_exchanged_usd(value, year, to_year=2020):
    if year is None or value is None:
        return 0
    try:
        year = int(year)
        if year_to_cpi == {}:
            df = pandas.read_csv(pathmng.consumer_price_index_path)
            for index, (_year, cpi) in df.iterrows():
                year_to_cpi[_year] = cpi
        return int(year_to_cpi[to_year] / year_to_cpi[year] * value)
    except Exception as e:
        return 0


if __name__ == '__main__':
    # print(filter_year("23 April 2021"))
    # print(get_datetime_from_string("April 27 2010 (USA)"))
    print(get_exchanged_usd(100.1, "2018"))


