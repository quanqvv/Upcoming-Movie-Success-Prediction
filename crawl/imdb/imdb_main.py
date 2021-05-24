import re

import bs4
import pandas
import requests
import xlsxwriter
from bs4 import Tag
import pathmng

import config
import utils
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
        self.release_year = None
        self.genre = None
        self.runtime = None
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
            if str(url).startswith("https://www.imdb.com") is False:
                url ="https://www.imdb.com" + url
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


def get_imdb_list_page_link():
    class PageLink(tool.MovieMetadata):
        def __init__(self, link):
            self.link = link
    driver = utils.get_driver()

    pre_link = "https://www.imdb.com/search/title/?release_date=2010-01-01,2020-12-31&runtime=60,300&start=9951&ref_=adv_nxt"
    pre_link = "https://www.imdb.com/search/title/?release_date=2000-01-01,2009-12-31&runtime=60,300"

    try:
        pre_df = pandas.read_csv(pathmng.imdb_next_link_path)
        pre_link = list(pre_df["link"])[-1]
    except:
        pass
    link_list = []

    driver.get(pre_link)
    import time
    for i in range(1, 1000):
        link = driver.find_element_by_xpath('//*[@class="lister-page-next next-page"]').get_attribute("href")
        link_list.append(PageLink(link))
        driver.find_element_by_xpath('//*[@class="lister-page-next next-page"]').click()
        time.sleep(2)
        if i % 50 == 0:
            tool.save_metadata_to_csv(utils.filter_duplicate_preserve_order(link_list), pathmng.imdb_next_link_path)
            link_list.clear()


if __name__ == '__main__':
    movie_metadata_list = []

    for year in range(1980, 1999):
        for i in range(1, 2000, 50):
            string_url = f"https://www.imdb.com/search/title/?release_date={year}-01-01,{year}-12-31&runtime=60,300&start=" + str(i)
            print("crawled...", i+50, "movies" )
            res = parse_data(string_url)
            movie_metadata_list.extend(res)
        tool.save_metadata_to_csv(movie_metadata_list, imdb_movie_path)
        movie_metadata_list.clear()

    # # crawl bắt đầu từ title 10001
    # for index, link in enumerate(list(pandas.read_csv(pathmng.imdb_next_link_path)["link"])):
    #     print("crawled...", (index+1) * 50, "movies" )
    #     res = parse_data(link)
    #     movie_metadata_list.extend(res)
    #
    #     if index % 50 == 49:
    #         tool.save_metadata_to_csv(movie_metadata_list, imdb_movie_path)
    #         movie_metadata_list.clear()
