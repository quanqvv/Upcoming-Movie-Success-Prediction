import json
import re
from concurrent.futures import ThreadPoolExecutor

import pandas
from bs4 import Tag

import pathmng
from crawl import tool
import requests
import bs4


class ImdbMovieDetail(tool.MovieMetadata):

    def __init__(self):
        self.link = None
        self.budget = None
        self.opening_weekend_gross = None
        self.box_office_gross = None
        self.studio = None
        self.mpaa_rating = None

        self.theater_release_date = None
        self.directors = set()
        self.casts = set()
        self.genre = []


def get_page_content(url, cookie=None):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
        "Accept-Language": "en-US"
    }
    if cookie:
        headers["cookie"] = cookie
    page = requests.get(url, headers=headers)
    return bs4.BeautifulSoup(page.text, "html.parser")


def crawl_imdb_pro(url: str):
    cookie = 'session-id=130-1485257-9983924; adblk=adblk_no; ubid-main=132-6940483-2822969; x-main="tyVMItbprLJKedcjM7QfzKPjSbg@RpVAyoKlUMEiGCHL@aTgWr8tDrVtWkHK?rR@"; at-main=Atza|IwEBINodx2soRWAwDCO-jFDtnGJ90L3PDJjp1ujbBZEl8OnqKBeCBn8N4GwB24EzNlI3fclrDDXe9M6FDqsxIeBTdXpjDLWAn6jWM2gRtWVA8FbEYgdY4pN5bmomErGdAoQ0Dm2Y1CZKdGFw590edwXJCv4kFGF0yzPYkkofu_63hJ7VrYW8ZJKTjBlfJIJa_kmRBGXf2a4usvYWQULfWpnWIQTI5Y1TvQmfXu7tANCiPTOmJw; sess-at-main="Lw7ODYlnFdHtso10O3rDcBM2KYNz0rvIqTHhlHniLXc="; uu=eyJpZCI6InV1YjZkNjcyZjc0ZmY0NDI0MThjNWMiLCJwcmVmZXJlbmNlcyI6eyJmaW5kX2luY2x1ZGVfYWR1bHQiOmZhbHNlfSwidWMiOiJ1cjEzMjU5Mjc4MCJ9; _uetsid=501765c0b5ed11eba95f4d411f36a30d; _uetvid=cebd5860ab3211ebbbecb34316e755c5; session-id-time=2082787201l; csm-hit=tb:1902MH6KJV33ZJ223VFR+s-1902MH6KJV33ZJ223VFR|1621135861310&t:1621135861310&adb:adblk_yes; session-token=VK20OFTSAEzGA0zF0C9OzByCm0kEGpAks7RBVKGlNxow1dlO8iKlzj1l7dGuNEV5Rb/0dU/y/72T5cXGg00r0XpFwo9kJdGQQobflvDCwRtzTLmw8BJDvxYImNwQkkYuEBQdm/dkPamqUdwLs56OuIOF3NOvwF5hQ8wKDfpPrbUcL+/KJHl0KO9l2jiWuLvYcOjcezzu5Hn9iICEJsMZmErnc/qnzfsLoiGBaG23j0c='
    soup = get_page_content(url.replace("www", "pro"), cookie)
    box_office_block = soup.find_all("div", attrs={"class": "a-row a-grid-vertical-align a-grid-top"})
    movie = ImdbMovieDetail()
    movie.link = url
    for ele in box_office_block:
        ele: Tag
        if ele.div.span.text.strip() == "Budget":
            movie.budget = ele.find_all("div")[1].text.strip()
        if ele.div.span.text.strip() == "Opening weekend":
            movie.opening_weekend_gross = ele.find_all("div")[1].text.strip()
        if ele.div.span.text.strip() == "Gross (World)":
            movie.box_office_gross = ele.find_all("div")[1].text.strip()

    movie.mpaa_rating = soup.find("span", id='certificate').text.strip()

    try:
        for ele in soup.find_all("div", attrs={"class": "a-section a-spacing-medium header_section"}):
            ele: Tag
            if ele.text.find("Production Company") != -1:
                movie.studio = ele.find("a", class_="a-size- a-align- a-link-").text
                break
    except:
        pass

    return movie


def crawl_imdb_normal(url):

    def get_data_as_list(key: str, json_data):
        try:
            if type(json_data[key]) is list:
                return [actor["name"] for actor in json_data[key]]
            else:
                return [json_data[key]["name"]]
        except Exception as e:
            pass
        return None

    soup = get_page_content(url)
    movie = ImdbMovieDetail()
    movie.link = url

    json_text = soup.find("script", type="application/ld+json").string
    json_data = json.loads(json_text)
    from collections import defaultdict
    json_data = defaultdict(lambda: None, json_data)
    # print(json_data)

    movie.directors = get_data_as_list("director", json_data)
    movie.casts = get_data_as_list("actor", json_data)
    movie.genre = json_data["genre"] if type(json_data["genre"]) is list else [json_data["genre"]]
    movie.mpaa_rating = json_data["contentRating"]

    detail_blocks = soup.find_all("div", attrs={"class": "txt-block"})
    for detail_block in detail_blocks:
        detail_block: Tag
        detail_name = detail_block.h4.text if detail_block.h4 is not None else "abc"
        if detail_name.find("Budget") != -1:
            movie.budget = detail_block.contents[2].strip()
        if detail_name.find("Opening Weekend") != -1:
            movie.opening_weekend_gross = detail_block.contents[2].strip()
        if detail_name.find("Worldwide Gross") != -1:
            movie.box_office_gross = detail_block.contents[2].strip()
        if detail_name.find("Production Co") != -1:
            movie.studio = detail_block.a.text.strip()
        if detail_name.find("Release Date") != -1:
            movie.theater_release_date = detail_block.contents[2].strip()
            movie.theater_release_date = re.sub("\(.*\)", "", str(movie.theater_release_date)).strip()
    return movie


def crawl_all_movie_detail(url_list):
    pre_url_list = []

    try:
        pre_df = pandas.read_csv(pathmng.imdb_detail_path)
        pre_url_list = list(pre_df["link"])
    except:
        pass

    crawling_urls = list(set(url_list) - set(pre_url_list))
    tool.streaming_crawl(list(crawling_urls), func=crawl_imdb_normal, path_save_file=pathmng.imdb_detail_path)


if __name__ == '__main__':
    temp = [
        "https://www.imdb.com/title/tt4154796/",
            "https://www.imdb.com/title/tt6723592",
            "https://www.imdb.com/title/tt11032374",
            "https://www.imdb.com/title/tt8332922/?ref_=adv_li_tt",
        "https://www.imdb.com/title/tt8106576",
    'https://www.imdb.com/title/tt5500158/',
    "https://www.imdb.com/title/tt6288650/"]
    # crawl_all_movie_detail(temp)
    print(crawl_imdb_normal(temp[-1]))