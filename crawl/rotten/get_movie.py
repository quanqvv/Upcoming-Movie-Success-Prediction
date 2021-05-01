from concurrent.futures import ThreadPoolExecutor

from numpy.testing._private.parameterized import param
from selenium import webdriver

import config
import time
import os
from bs4 import BeautifulSoup, Tag
from bs4 import BeautifulSoup
import difflib
import re
import requests
from urllib.request import urlopen
from lxml import etree
import pandas

movie_id_path = config.crawl_data_path + "\\rotten_movie_id.txt"
movie_metadata_path = config.crawl_data_path + "\\rotten.csv"
config.auto_create_path(movie_id_path, movie_metadata_path)


def get_movie_id(num=100):
    options = webdriver.ChromeOptions()
    options.add_argument(f"--user-data-dir={config.browser_profile_path}")
    driver = webdriver.Chrome(config.driver_path, options=options)


    driver.get("https://www.rottentomatoes.com/browse/dvd-streaming-all?minTomato=0&maxTomato=100&services=amazon;hbo_go;itunes;netflix_iw;vudu;amazon_prime;fandango_now&genres=1;2;4;5;6;8;9;10;11;13;18;14&sortBy=release")

    # click show more button
    time.sleep(2)
    while True:
        driver.find_element_by_xpath('//*[@id="show-more-btn"]/button').click()
        time.sleep(2)
        num_movies = len(driver.find_elements_by_xpath('//*[@class="movie_info"]/a'))
        print(f"Find {num_movies} movies")
        if num_movies > num:
            break

    movies = driver.find_elements_by_xpath('//*[@class="movie_info"]/a')
    movie_id_set = set()
    for ele in movies:
        movie_id_set.add(str(ele.get_attribute("href")).split("/")[-1])

    with open(movie_id_path, "w") as f:
        f.write("\n".join(list(movie_id_set)))


class MovieMetadata:

    def __init__(self):
        self.critic_score = None
        self.audience_score = None
        self.runtime = None
        self.MPAA_rating = None
        self.studio = None
        self.theater_release_date = None
        self.dvd_release_date = None
        self.streaming_release_date = None
        self.genres = []
        self.casts = set()
        self.directors = set()

    def __str__(self):
        return str(vars(self))

    def get_all_attr(self):
        res = []
        for attr, _ in self.__dict__.items():
            res.append(attr)
        return res

    def get_all_value(self):
        values = []
        for attr, value in self.__dict__.items():
            values.append(value)
        return values


class MovieScraper():
    def __init__(self, **kwargs):
        super(MovieScraper, self).__init__()
        self.metadata = MovieMetadata()
        if 'movie_url' in kwargs.keys():
            self.url = kwargs['movie_url']

    def extract_metadata(self):
        page_movie = urlopen(self.url)
        main_soup = BeautifulSoup(page_movie, "lxml")
        # print(main_soup.find("body"))

        # Score
        score = main_soup.find('score-board')
        self.metadata.critic_score = score.attrs['tomatometerscore']
        self.metadata.audience_score = score.attrs['audiencescore']

        # Movie Info
        movie_info_section = main_soup.find_all('div', class_='media-body')
        soup_movie_info = BeautifulSoup(str(movie_info_section[0]), "lxml")
        movie_info_length = len(soup_movie_info.find_all('li', class_='meta-row clearfix'))

        for i in range(movie_info_length):
            x = soup_movie_info.find_all('li', class_='meta-row clearfix')[i]
            soup = BeautifulSoup(str(x), "lxml")
            label = soup.find('div', class_='meta-label subtle').text.strip().replace(':', '')
            value = soup.find('div', class_='meta-value').text.strip()
            if label == 'Runtime':
                self.metadata.runtime = value.replace('$', '').replace(',', '')
            elif label == 'Release Date (Theaters)':
                self.metadata.theater_release_date = re.sub(r'\s\([^)]*\)', '', value).replace("\n\xa0limited", "")
            elif label == "Release Date (Streaming)":
                self.metadata.streaming_release_date = re.sub(r'\s\([^)]*\)', '', value).replace("\n\xa0limited", "")
            elif label == "Release Date (DVD)":
                self.metadata.streaming_release_date = re.sub(r'\s\([^)]*\)', '', value).replace("\n\xa0limited", "")
            elif label == "Director":
                self.metadata.directors.add(value.replace("\n", "").strip())
            elif label == "Genre":
                self.metadata.genres = value.replace(' ', '').replace('\n', '').split(',')

        # for res in main_soup.findNext("div", class_="media-body").findAll("div", {"data-qa": "cast-crew-item-link"}):
        #     print(res)
        for res in main_soup.findAll('div', class_='media-body'):
            try:
                tag = res.findNext("span")
                if tag.has_attr("title"):
                    self.metadata.casts.add(tag.text.strip())
            except:
                pass
        return self

    @staticmethod
    def closest(keyword, words):
        closest_match = difflib.get_close_matches(keyword, words, cutoff=0.6)
        return closest_match


def get_movie_data():
    movie_metadatas = []
    def crawl_movie_and_append(movie_id):
        try:
            values = MovieScraper(movie_url=f"https://www.rottentomatoes.com/m/{movie_id}").extract_metadata().metadata.get_all_value()
            movie_metadatas.append(values)
        except Exception as e:
            print(e)

    with open(movie_id_path, "r") as f:
        all_movie_id = f.read().split("\n")[0:10]
        with ThreadPoolExecutor(max_workers=10) as e:
            for movie_id in all_movie_id:
                e.submit(lambda: crawl_movie_and_append(movie_id))
    for x in movie_metadatas:
        print(x)
    movie_metadata_df = pandas.DataFrame(data=movie_metadatas, columns=MovieMetadata().get_all_attr())
    movie_metadata_df.to_csv(movie_metadata_path)
    print(movie_metadata_df)


if __name__ == '__main__':
    import os

    # get_movie_id(num=100)
    get_movie_data()