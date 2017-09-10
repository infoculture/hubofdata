import csv
import json
import requests
import codecs
import urllib.request
from bs4 import BeautifulSoup

packages_list_api = "https://hubofdata.ru/api/3/action/package_list"
r = requests.get(packages_list_api)
json_data = json.loads(r.text)
# with codecs.open("Data\hubofdata_packagelist.json", "r", encoding="utf-8") as f:
#     json_data = json.loads(json.loads(json.dumps(f.read())))
packages_list = json_data["result"]
# with codecs.open("Data\hubofdata_packagelist.json", "w", encoding="utf-8") as f:
#     json.dump(json_data, f, ensure_ascii=False, separators=(',', ':'))

package_api_url = "http://hubofdata.ru/api/3/action/package_show?id={}"

# debug - тестовая загрузка инфо по пакетам данных
# with codecs.open("Data\hubofdata_packages.json", "r", encoding="utf-8") as f:
#     json_str = json.loads(json.loads(json.dumps(f.read())))
# print (json_str)

json_content = dict()
id_len = len(str(len(packages_list)))
packages_dict  = dict()
try:
    for i, package in enumerate(packages_list):
        curr_url = package_api_url.format(package)
        r2 = requests.get(curr_url)
        id_str = "0" * (id_len - len(str(i))) + str(i)
        if r2.status_code == 200:
            package_json_data = json.loads(r2.text)["result"]
            packages_dict[id_str] = package_json_data
        else:
            packages_dict[id_str] = dict()
        print ("{0} - {1}, status: {2}".format(id_str, package, r2.status_code))
except:
    print ("Ошибка на наборе {}".format(id_str))

with codecs.open("Data\hubofdata_packages.json", "w", encoding="utf-8") as f:
    json.dump(packages_dict, f, ensure_ascii=False, separators=(',', ':'))