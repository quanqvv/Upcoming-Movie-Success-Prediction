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

import utils
import pathmng
from crawl import tool

movie_id_path = config.crawl_data_path + "\\rotten_movie_id.txt"
movie_metadata_path = pathmng.rotten_path
config.auto_create_path(movie_id_path, movie_metadata_path)


def get_movie_id(num=100):
    driver = utils.get_driver()
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

    driver.close()


class RottenMovieMetadata(tool.MovieMetadata):

    def __init__(self):
        self.title = None
        self.critic_score = None
        self.audience_score = None
        self.release_year = None
        self.runtime = None
        # self.studio = None
        self.theater_release_date = None
        self.dvd_release_date = None
        self.streaming_release_date = None
        self.genre = []
        self.casts = set()
        self.directors = set()
        self.link = None


class MovieScraper():
    def __init__(self, **kwargs):
        super(MovieScraper, self).__init__()
        self.metadata = RottenMovieMetadata()
        if 'movie_url' in kwargs.keys():
            self.url = kwargs['movie_url']

    def extract_metadata(self):
        page_movie = requests.get(self.url)
        main_soup = BeautifulSoup(page_movie.text, "html.parser")
        # print(main_soup.find("body"))

        self.metadata.link = self.url

        score = main_soup.find('score-board')
        self.metadata.critic_score = score.attrs['tomatometerscore']
        self.metadata.audience_score = score.attrs['audiencescore']
        self.metadata.title = score.findNext("h1", attrs={"slot": "title"}).text.strip()
        self.metadata.release_year = utils.filter_year(score.findNext("p", attrs={"slot": "info"}).text.strip())

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
                self.metadata.theater_release_date = re.sub(r'\s\([^)]*\)', '', value).replace("\n\xa0limited", "").replace("\n"+chr(160)+"wide", "")
            elif label == "Release Date (Streaming)":
                self.metadata.streaming_release_date = re.sub(r'\s\([^)]*\)', '', value).replace("\n\xa0limited", "").replace("\n"+chr(160)+"wide", "")
            elif label == "Release Date (DVD)":
                self.metadata.streaming_release_date = re.sub(r'\s\([^)]*\)', '', value).replace("\n\xa0limited", "").replace("\n"+chr(160)+"wide", "")
            elif label == "Director":
                self.metadata.directors = set([_.strip() for _ in value.replace("\n", "").split(",")])
            elif label == "Genre":
                self.metadata.genre = value.replace(' ', '').replace('\n', '').split(',')

        # for res in main_soup.findNext("div", class_="media-body").findAll("div", {"data-qa": "cast-crew-item-link"}):
        #     print(res)
        # Cast
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

    def crawl_one_movie(movie_id):
        try:
            print("Getting for", movie_id)
            metadata = MovieScraper(movie_url=f"https://www.rottentomatoes.com/m/{movie_id}").extract_metadata().metadata
            return metadata
        except Exception as e:
            print(e)

    with open(movie_id_path, "r") as f:
        all_movie_id = f.read().split("\n")
        # all_movie_id = ["the_last_5_years"]
        tool.streaming_crawl(all_movie_id, crawl_one_movie, movie_metadata_path, start_group=15)


if __name__ == '__main__':
    import os
    # get_movie_id(num=8000)
    get_movie_data()