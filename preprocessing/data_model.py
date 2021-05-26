"""
 176 features: 6
pertaining to bit vectors for MPAA rating, 1 for movie runtime, 22 pertaining to bit vectors for movie studio, 12 for
release month, 4 for popular weekends, 8 for year bins, 1 for budget, 6 for awards (one for each of the following for
both Academy and Globe: sum of all award winners among the cast, director, and those involved in a Best Picture),
22 pertaining to bit vectors for genre, 90 pertaining to bit vectors for the most popular words in a movie’s plot
description (after removing stop words and words that seemed unrelated to movie content), and 4 bit vectors for the
movie’s place in a series.
"""
from datetime import datetime

import utils


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
    common_popular_weekend = [ 'Memorial Day', 'Independence Day',
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


class DataModel:
    _singleton = None

    def __init__(self, movie_studio_list, plot_des_word_list, mpaa_rating_list, genre_list):
        if DataModel._singleton is not None:
            raise Exception("This object is singleton")
        self.movie_studio_list_encoder = ListEncoder(movie_studio_list)
        self.plot_des_word_list_encoder = ListEncoder(plot_des_word_list)
        self.mpaa_rating_list_encoder = ListEncoder(mpaa_rating_list)
        self.genre_list_encoder = ListEncoder(genre_list)

    def build_feature_dict(self, row):
        release_date = utils.get_datetime_from_string(row["theater_release_date"])
        if release_date is None:
            print(row["theater_release_date"])
        return {
            "mpaa_rating": self.mpaa_rating_list_encoder.get_bit_vector(row["mpaa_rating"]),
            "runtime": (int(row["runtime"]),),
            "studio": self.movie_studio_list_encoder.get_bit_vector(row["studio"]),
            "release_month": get_release_month_vector(row["theater_release_date"]),
            "popular_weekend": get_popular_weekend_vector(release_date.strftime("%Y-%m-%d")),
            "budget": (int(row["budget"]),),
            "award": (int(row["award"]),),
            "genre": self.genre_list_encoder.get_bit_vector(utils.get_list_from_str_json(row["genre"])),
            "plot_des": self.plot_des_word_list_encoder.get_bit_vector(row["plot_des"].split(" "))
        }

    def build_feature_vector(self, row, *feature_names):

        feature_dict = self.build_feature_dict(row)
        if feature_names[0] == "*":
            feature_names = feature_dict.keys()
        vector = []
        for feature_name in feature_names:
            vector.extend(feature_dict[feature_name])
        return vector

    def get_full_feature(self, row: dict):
        vector = self.build_feature_vector(row, "*")
        vector.append(float(row["audience_score"]) * 100)
        vector.append(int(row["opening_weekend_gross"]))
        return vector

    def get_feature_for_audience_score(self, row: dict):
        vector = self.build_feature_vector(row, "genre", "mpaa_rating", "runtime", "award", "studio")
        vector.append(int(row["opening_weekend_gross"]))
        vector.append(float(row["audience_score"]) * 100)
        vector.append(int(row["box_office_gross"]))
        return vector

    def get_feature_for_box_office_gross(self, row: dict):
        vector = self.build_feature_vector(row, "genre", "mpaa_rating", "runtime", "studio", "award", "budget",
                                           "release_month")
        vector.append(int(row["opening_weekend_gross"]))
        vector.append(float(row["audience_score"]) * 100)
        vector.append(int(row["box_office_gross"]))
        return vector


    @staticmethod
    def get_instance():
        DataModel._singleton: DataModel
        return DataModel._singleton


class LinearFeature:
    pass


if __name__ == '__main__':
    print(ListEncoder([4, 5]).get_bit_vector(4))
    print(get_popular_weekend_vector("31-05-2021"))

