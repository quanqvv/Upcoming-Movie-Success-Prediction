"""
 176 features: 6
pertaining to bit vectors for MPAA rating, 1 for movie runtime, 22 pertaining to bit vectors for movie studio, 12 for
release month, 4 for popular weekends, 8 for year bins, 1 for budget, 6 for awards (one for each of the following for
both Academy and Globe: sum of all award winners among the cast, director, and those involved in a Best Picture),
22 pertaining to bit vectors for genre, 90 pertaining to bit vectors for the most popular words in a movie’s plot
description (after removing stop words and words that seemed unrelated to movie content), and 4 bit vectors for the
movie’s place in a series.
"""
import math
from datetime import datetime
import math

from nltk import WordNetLemmatizer
from pattern.text import singularize

import utils
import re


class ListEncoder:

    def __init__(self, all_elements):
        self.all_elements = all_elements
        self.element_to_index = {}
        for i in range(len(self.all_elements)):
            self.element_to_index[self.all_elements[i]] = i

    def get_bit_vector(self, element_or_elements):
        if isinstance(element_or_elements, (list, tuple)) is False:
            element_or_elements = [element_or_elements]
        vector = [0] * len(self.all_elements)
        for ele in element_or_elements:
            if ele in self.element_to_index:
                vector[self.element_to_index[ele]] = 1
        return vector


def get_popular_weekend_vector(release_date):
    import holidays
    common_popular_weekend = ['Memorial Day', 'Independence Day',
                 'Thanksgiving', 'Day After Thanksgiving', 'Christmas Day']
    # print(holidays.UnitedStates().get(release_date))
    # print(holidays.US(state='CA', years=2021).values())
    this_holiday = holidays.UnitedStates().get(release_date)
    holiday = None
    if this_holiday is not None:
        for holiday in common_popular_weekend:
            if str(this_holiday).startswith(holiday) is True:
                break
    return ListEncoder(common_popular_weekend).get_bit_vector(holiday)


def get_release_month_vector(release_date: str):
    return ListEncoder([i for i in range(1, 13)])\
        .get_bit_vector([utils.get_datetime_from_string(release_date).month])

import nltk
nltk.download('wordnet')

def preprocess_word(word: str):
    word = word.lower()
    if word in utils.all_stopwords:
      return None
    word = re.sub("[^a-zA-Z ]", "", word)
    word = WordNetLemmatizer().lemmatize(word,'v')
    word = singularize(word)
    if len(word) == 1:
      return None
    return word


class DataModel:
    _singleton = None

    def __init__(self, movie_studio_list, plot_des_word_list, mpaa_rating_list, genre_list):
        if DataModel._singleton is not None:
            raise Exception("This object is singleton")
        self.movie_studio_list_encoder = ListEncoder(movie_studio_list)
        self.plot_des_word_list_encoder = ListEncoder(plot_des_word_list)
        self.mpaa_rating_list_encoder = ListEncoder(mpaa_rating_list)
        self.genre_list_encoder = ListEncoder(genre_list)

    def rebuild_plot_des_encoder(self, words):
        self.plot_des_word_list_encoder = ListEncoder(words)

    def rebuild_movie_studio_encoder(self, _list):
        self.movie_studio_list_encoder = ListEncoder(_list)

    def rebuild_mpaa_rating(self, _list):
        self.mpaa_rating_list_encoder = ListEncoder(_list)

    def build_feature_dict(self, row):
        release_date = utils.get_datetime_from_string(row["theater_release_date"])
        temp = 0
        return {
            "mpaa_rating": self.mpaa_rating_list_encoder.get_bit_vector(row["mpaa_rating"]),
            "runtime": (float(row["runtime"]) / 500,),
            "studio": self.movie_studio_list_encoder.get_bit_vector(row["studio"]),
            "release_month": get_release_month_vector(row["theater_release_date"]),
            # "popular_weekend": get_popular_weekend_vector(release_date.strftime("%Y-%m-%d")),
            "budget": (float(row["budget"]) / 12215500000,),
            "award": (int(row["award"]),),
            "genre": self.genre_list_encoder.get_bit_vector(utils.get_list_from_str_json(row["genre"])),
            "plot_des": self.plot_des_word_list_encoder.get_bit_vector([preprocess_word(_) for _ in row["plot_des"].split(" ")])
        }

    def get_other_features(self, row):
        genre_and_time_interval = \
            [('Sci-Fi', '120-150'),
             ('Animation', '90-120'),
             ('Adventure', '120-150'),
             ('Family', '90-120'),
             ('History', '90-120'),
             ('Action', '120-150'),
             ('War', '90-120'),
           ]
        vector = [0] * len(genre_and_time_interval)
        for index, (genre, time_interval) in enumerate(genre_and_time_interval):
            film_genres = utils.get_list_from_str_json(row["genre"])
            vector[index] = (genre in film_genres) and \
                            (int(time_interval.split("-")[0]) <= int(row["runtime"]) < int(time_interval.split("-")[1]))
            if vector[index] is True:
                vector[index] = 1
                # print(row)
                break
        # return vector
        return ()

    def get_other_features(self, row):
        genre_rating = [('Action', 'PG-13'),
                        ('Comedy', 'PG'),
                        ('Comedy', 'PG-13'),
                        ('Drama', 'PG-13'),
                        ('Drama', 'R'),
                        ('Comedy', 'R'),
                        ('Crime', 'R'),
                        ('Romance', 'R'),
                        ('Thriller', 'R'),
                        ('Action', 'R')]
        vector = [0] * len(genre_rating)
        for index, (genre, rating) in enumerate(genre_rating):
            film_genres = utils.get_list_from_str_json(row["genre"])
            vector[index] = genre in film_genres and row["mpaa_rating"] == rating
            if vector[index] is True:
                vector[index] = 1
                # print(row)
                # break
        # return vector
        return ()

    def build_feature_vector(self, row, *feature_names):

        feature_dict = self.build_feature_dict(row)
        if feature_names[0] == "*":
            feature_names = feature_dict.keys()
        vector = []
        for feature_name in feature_names:
            vector.extend(feature_dict[feature_name])
        vector.extend(self.get_other_features(row))
        return vector

    def get_full_feature(self, row: dict):
        vector = self.build_feature_vector(row, "*")
        if "box_office_gross" not in row:
            row["box_office_gross"] = 0
        if row["box_office_gross"] > 1.3*row["budget"]:
            vector.append(1)
        else:
            vector.append(0)
        return vector

    def get_input_feature(self, row):
        vector = self.build_feature_vector(row, "*")
        return vector

    @staticmethod
    def get_instance():
        DataModel._singleton: DataModel
        return DataModel._singleton

    def show(self):
        print(self.genre_list_encoder.all_elements)
        print(self.mpaa_rating_list_encoder.all_elements)
        print(self.plot_des_word_list_encoder.all_elements)
        print(self.movie_studio_list_encoder.all_elements)


def get_data_model():
    import pickle, pathmng
    data_model = pickle.load(open(pathmng.data_model_path, "rb"))
    data_model: DataModel
    data_model.rebuild_plot_des_encoder(['cop', 'run', 'couple', 'order', 'end', 'base', 'bos', 'car', 'school',
       'story', 'house', 'kidnap', 'play', 'give', 'seek', 'discover', 'alien',
       'survive', 'street', 'child'])
    data_model.rebuild_movie_studio_encoder(['Universal Pictures', 'Paramount Pictures', 'Columbia Pictures',
       'Warner Bros', 'Twentieth Century Fox', 'New Line Cinema',
       'Touchstone Pictures', 'MetroGoldwynMayer MGM', 'Walt Disney Pictures',
       'TriStar Pictures', 'Summit Entertainment', 'Dreamworks Pictures',
       'Screen Gems', 'Lionsgate', 'Miramax', 'Dimension Films',
       'Fox 2000 Pictures', 'Orion Pictures', 'Castle Rock Entertainment',
       'Fox Searchlight Pictures', 'Millennium Films',
       'New Regency Productions', 'Hollywood Pictures', 'Focus Features',
       'EuropaCorp', 'Revolution Studios', 'DreamWorks Animation',
       'Morgan Creek Entertainment', 'United Artists', 'Alcon Entertainment'])

    # data_model.rebuild_mpaa_rating(["R", "PG-13"])
    return data_model


if __name__ == '__main__':
    # print(ListEncoder([4, 5]).get_bit_vector(4))
    # print(get_popular_weekend_vector("31-05-2021"))
    # import re
    # print(re.compile(" |\|").split("a|a b|c"))
    # get_data_model().show()
    print()
