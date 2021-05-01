import bs4
import pandas
import requests
import xlsxwriter

import config

list_data_init = [[]]
imdb_movie_path = config.crawl_data_path + "\\imdb.csv"
config.auto_create_path(imdb_movie_path)

url = 'https://www.imdb.com/search/title/?count=100&groups=top_1000&sort=user_rating%27' # các bạn thay link của trang mình cần lấy dữ liệu tại đây
def get_page_content(url):
   page = requests.get(url, headers={"Accept-Language":"en-US"})
   return bs4.BeautifulSoup(page.text, "html.parser")
soup = get_page_content(url)
#print(soup)

def parse_data(string_url) :

    # certificate = []
    # runtime = []
    # genre = []
    # for movie in soup.findAll('p', class_='text-muted'):
    #     if ((not movie.findAll('span',class_='certificate')) &
    #     (not movie.findAll('span',class_='runtime')) &
    #     (not movie.findAll('span',class_='genre'))) & (not movie.findAll('span',class_='realse')) & (not movie.findAll('span',class_='title')) :
    #         continue
    # certificate.append('' if not [ce.text for ce in movie.findAll('span',class_='certificate')] else
    # [ce.text for ce in movie.findAll('span',class_='certificate')][0])
    # runtime.append('' if not [rt.text for rt in movie.findAll('span',class_='runtime')] else
    # [rt.text for rt in movie.findAll('span',class_='runtime')][0])
    # genre.append('' if not [gr.text for gr in movie.findAll('span',class_="genre")] else
    # [rt.text for rt in movie.findAll('span',class_='genre')][0])
    #
    #
    # movies = soup.findAll('h3', class_='lister-item-header')
    # titles = [movie.find('a').text for movie in movies]
    # release = [rs.find('span', class_="lister-item-year text-muted unbold").text for rs in movies]
    # certificate = [ce.text for ce in soup.findAll('span',class_='certificate')]
    # runtime = [rt.text for rt in soup.findAll('span',class_='runtime')]
    # genre = [gr.text for gr in soup.findAll('span',class_="genre")]
    # rates = [rate['data-value'] for rate in soup.findAll('div',class_='inline-block ratings-imdb-rating')]
    # print(len(titles), len(release), len(certificate), len(runtime), len(genre), len(rates))

    page = requests.get(string_url, headers={"Accept-Language": "en-US"})
    soup = bs4.BeautifulSoup(page.text, "html.parser")
    list_item_content = soup.find_all('div', "lister-item-content")
    for item in list_item_content:
        title = ""
        if(item.find('h3', "lister-item-header").a is not None):
            title = item.find('h3', "lister-item-header").a.string.strip()
        user_rate = ""
        if(item.find('div', "inline-block ratings-imdb-rating") is not None):
            user_rate = item.find('div', "inline-block ratings-imdb-rating")['data-value']
        meta_score = ""
        if item.find('div', "inline-block ratings-metascore") is not None:
            meta_score = item.find('div', "inline-block ratings-metascore").span.string
        text_muted = item.find_all('p', "text-muted")[0]
        certificate = ""
        # print(type(text_muted.find('span', "certificate")) is not None )
        if (text_muted.find('span', "certificate")) is not None:
            certificate = text_muted.find('span', "certificate").string.strip()
        run_time = ""
        if (text_muted.find('span', "runtime") is not None) :
            run_time = text_muted.find('span', "runtime").string
        genre = ""
        if ((text_muted.find('span', "genre")) is not None):
            genre = text_muted.find('span', "genre").string.strip()
        #print(genre)
        description = ""
        if (item.find_all('p', "text-muted")[1]).string is not None:
            description = item.find_all('p', "text-muted")[1].string.strip()
        # print(certificate)
        # print(run_time)
        # print(genre)
        list_data = [title, user_rate, meta_score, certificate, run_time, genre, description]
        #print(len(list_data))
        list_data_init.append(list_data)

        #print(list_data)
    #list_data_init.append(list_data)
#string_url = https://www.imdb.com/search/title/?count=100&groups=top_1000&sort=user_rating%27


for i in range(1, 100, 50):
    string_url = "https://www.imdb.com/search/title/?release_date=2010-01-01,2020-12-31&runtime=60,300&start=" + str(i)
    #print(string_url)
    print("crawled...", i+50, "pages" )
    parse_data(string_url)

df = pandas.DataFrame(list_data_init, columns=['title','user_rate', 'meta_score', 'certificate', 'run_time', 'genre', 'description'])
# writer = pandas.ExcelWriter("D:\\20202\\phân tích nghiệp vụ thông minh\\data_movie.xlsx", engine='xlsxwriter')
# df.to_excel(writer, sheet_name= "Sheet1", index=False)
# writer.save()
df.to_csv(imdb_movie_path)
print(len(list_data_init))