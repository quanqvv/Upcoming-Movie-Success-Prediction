import requests
import bs4
import pandas

html = """
<p>   hgdfdg  gdgdf   </p>
<p> gdgdfgdfd </p>
"""
# soup = bs4.BeautifulSoup(html, "html.parser")
# #print(type(soup.find('p', "g)) is None)
# something = None
# content = (soup.find_all('p', "gdg")[1])
# print(type(content))
list = [[]]
list1 = [1,2]
list2 = [2,3]
list.append(list2)
list.append((list1))

print(list)
list.clear()
print(list)
list.append(list1)
print(list)