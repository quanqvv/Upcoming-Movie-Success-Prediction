import bs4
import requests

string_url = "https://www.imdb.com/search/title/?count=100&groups=top_1000&sort=user_rating%27"
page = requests.get(string_url)
soup = bs4.BeautifulSoup(page.text, "html.parser")
list_item_content = soup.find_all('div', "lister-item-content")
for item in list_item_content:
    user_rate = item.find('div', "inline-block ratings-imdb-rating")['data-value']
    meta_score = item.find('div', "inline-block ratings-metascore").span.string
    text_muted = item.find_all('p', "text-muted")[0]
    certificate = ""
    #print(type(text_muted.find('span', "certificate")) is not None )
    if (text_muted.find('span', "certificate")) is not None:
        certificate = text_muted.find('span', "certificate").string.strip()
    run_time = text_muted.find('span', "runtime").string
    genre = ""
    if ((text_muted.find('span', "genre")) is not None):
        genre = text_muted.find('span', "genre").string.strip()
    print(certificate)
    print(run_time)
    print(genre)
    description = ""
    if (item.find_all('p', "text-muted")[1]).string is not None:
        description = item.find_all('p', "text-muted")[1].string.strip()
    print(description)


