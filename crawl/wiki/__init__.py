import pathmng
import utils
from crawl import tool
from crawl.tool import MovieMetadata


class AwardMetadata(MovieMetadata):
    name = None
    year = None


def get_academy_award_for_best_people(driver, url):
    driver.get(url)
    data = []

    for ele in driver.find_elements_by_xpath("//tr"):
        try:

            year_eles = ele.find_elements_by_xpath(".//th[@rowspan]/a")
            actor = ele.find_element_by_xpath('.//td[@style="background:#FAEB86;"]//a').text
            pre_year = ""
            for year_ele in year_eles:
                # case 1
                year_text = str(year_ele.text)
                if year_text.find("/") != -1:
                    first, second = year_text.split("/")
                    second = first[0:2] + second

                    award1 = AwardMetadata()
                    award1.name = actor
                    award1.year = first

                    award2 = AwardMetadata()
                    award2.name = actor
                    award2.year = second

                    data.append(award1)
                    data.append(award2)
                else:
                    award = AwardMetadata()
                    if len(year_text) == 2:
                        year_text = pre_year[0:2] + year_text

                    award.name = actor
                    award.year = year_text
                    data.append(award)
                    pre_year = year_text

                print(f"Found {len(data)} data")
        except:
            pass
    return data


def get_Academy_Award_for_Best_Actor_Director():
    driver = utils.get_driver()
    urls = ["https://en.wikipedia.org/wiki/Academy_Award_for_Best_Actor",
            "https://en.wikipedia.org/wiki/Academy_Award_for_Best_Director"]

    data = []
    for url in urls[0:2]:
        data.extend(get_academy_award_for_best_people(driver, url))

    tool.save_metadata_to_csv(data, pathmng.wiki_best_actor_director_path)


def get_Academy_Award_for_Best_Picture():
    pass


if __name__ == '__main__':
    get_Academy_Award_for_Best_Actor_Director()
