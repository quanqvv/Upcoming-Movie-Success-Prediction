import re

import bs4
import pandas
import requests
import xlsxwriter
from bs4 import Tag

import config
from crawl import tool

list_data_init = [[]]
imdb_movie_path = config.crawl_data_path + "\\imdb.csv"
config.auto_create_path(imdb_movie_path)


def get_page_content_as_soup(url):
    page = requests.get(url, headers={"Accept-Language":"en-US"})
    return bs4.BeautifulSoup(page.text, "html.parser")


class ImdbMovieMetadata(tool.MovieMetadata):
    def __init__(self):
        self.title = None
        self.user_rate = None
        self.imdb_rating = None
        self.plot_des = None
        # self.budget = None
        self.release_year = None
        # self.box_office_gross = None
        # self.opening_weekend_gross = None
        self.genre = None
        self.runtime = None
        self.certificate = None
        self.link = None


def parse_data(string_url) :

    page = requests.get(string_url, headers={"Accept-Language": "en-US"})
    soup = bs4.BeautifulSoup(page.text, "html.parser")
    list_item_content = soup.find_all('div', "lister-item-content")

    movie_metadata_list = []

    for item in list_item_content:
        item: Tag
        movie_metadata = ImdbMovieMetadata()

        url = None

        if item.find('h3', "lister-item-header").a is not None:
            movie_metadata.title = item.find('h3', "lister-item-header").a.string.strip()
            url = item.find('h3', "lister-item-header").a["href"]
            release_year_str = item.find('h3', "lister-item-header").find_all("span")[1].string
            import re
            movie_metadata.release_year = int(re.search(r"\d+", release_year_str).group())

        if item.find('div', "inline-block ratings-imdb-rating") is not None:
            movie_metadata.user_rate = item.find('div', "inline-block ratings-imdb-rating")['data-value']
            if movie_metadata.user_rate == "":
                movie_metadata.user_rate = None

        # if item.find('div', "inline-block ratings-metascore") is not None:
        #     movie_metadata.imdb_rating = item.find('div', "inline-block ratings-metascore").span.string.strip()
        #     # if movie_metadata.imdb_rating == "":
        #     #     movie_metadata.imdb_rating = None
        #     movie_metadata.imdb_rating = None

        text_muted = item.find_all('p', "text-muted")[0]

        # if text_muted.find('span', "certificate") is not None:
        #     certificate = text_muted.find('span', "certificate").string.strip()
        #     movie_metadata.certificate = certificate

        if text_muted.find('span', "runtime") is not None:
            movie_metadata.runtime = text_muted.find('span', "runtime").string

        if (text_muted.find('span', "genre")) is not None:
            movie_metadata.genre = text_muted.find('span', "genre").string.strip()

        if (item.find_all('p', "text-muted")[1]).string is not None:
            movie_metadata.plot_des = item.find_all('p', "text-muted")[1].string.strip()

        # inside\
        if url is not None:
            # url = "https://www.imdb.com" + url
            # print(url)
            # soup_inside = get_page_content_as_soup(url)
            # print(soup_inside)
            # all_blocks = soup_inside.find(attrs={"id": "titleDetails"}).find_all("div", _class="txt-block")
            # print(all_blocks)
            # for block in all_blocks:
            #     block_str = block.find("h4").string
            #     if block_str == "Budget:":
            #         movie_metadata.budget = block_str
            #     elif block_str == "Opening Weekend USA:":
            #         movie_metadata.opening_weekend_gross = "ab"
            #     elif block_str == "Cumulative Worldwide Gross:":
            #         movie_metadata.box_office_gross = block_str
            # def filter_money(text: str):
            #     re.search()
            movie_metadata.link = url

        movie_metadata_list.append(movie_metadata)
    return movie_metadata_list


if __name__ == '__main__':
    movie_metadata_list = []
    for i in range(1, 40000, 50):
        string_url = "https://www.imdb.com/search/title/?release_date=2010-01-01,2020-12-31&runtime=60,300&start=" + str(i)
        print("crawled...", i+50, "pages" )
        res = parse_data(string_url)
        movie_metadata_list.extend(res)

    tool.save_metadata_to_csv(movie_metadata_list, imdb_movie_path)


    # df = pandas.DataFrame(list_data_init, columns=['title','user_rate', 'meta_score', 'certificate', 'run_time', 'genre', 'description'])
    # writer = pandas.ExcelWriter("D:\\20202\\phân tích nghiệp vụ thông minh\\data_movie.xlsx", engine='xlsxwriter')
    # df.to_excel(writer, sheet_name= "Sheet1", index=False)
    # writer.save()
    # df.to_csv(imdb_movie_path)
    # print(len(list_data_init))