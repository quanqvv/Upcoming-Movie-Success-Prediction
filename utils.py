import json

from nltk import word_tokenize
from nltk.corpus import stopwords
from pyspark.sql import SparkSession
from selenium import webdriver
import config


def get_driver(direct_profile=config.browser_profile_path):
    options = webdriver.ChromeOptions()
    options.add_argument(f"--user-data-dir={direct_profile}")
    driver = webdriver.Chrome(config.driver_path, options=options)
    return driver


def remove_non_alphabet(text: str):
    import re
    return re.sub("[^a-zA-Z ]", "", text)


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


def measure_accuracy(label_predicted, label_origin):
    error = 0
    for index in range(label_predicted.size):
        error += abs(label_predicted[index] - label_origin[index])/label_origin[index]

    print("Accuracy:", 1 - error/len(label_origin))


