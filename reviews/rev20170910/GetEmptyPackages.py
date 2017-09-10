import csv
import json
import requests
import codecs
import re

def get_resource_size(url_str):
    file_size = -1
    try:
        r = requests.head(url_str, headers={'Accept-Encoding': 'identity'})
        if r.status_code == 200:
            file_size = r.headers['content-length']
        else:
            file_size = 0
    except:
        print("Some connection problems")
    return file_size

# убрать переносы строк из текстов с гитхаба
def remove_crlf(text):
    text = re.sub("(\\r( )*\\n)+", " ", text)
    text = text.replace("\r", " ").replace("\n", " ")
    text = re.sub("( )+", " ", text)
    return text

csv.register_dialect('customcsv', delimiter=',', quoting=csv.QUOTE_MINIMAL, quotechar='"', escapechar='\\')
with open("Data\empty_packages.csv", "w", encoding='utf-8', newline='') as f:
    csvfile = csv.writer(f, 'customcsv')
    csvfile.writerow(["package_name","package_title","author","resource_url","resource_title","resource_size"])

with codecs.open("Data\hubofdata_packages.json", "r", encoding="utf-8") as f:
    packages_data = json.loads(json.loads(json.dumps(f.read())))

for id_key, data in packages_data.items():
    resources = data["resources"]
    for rc in resources:
        if "warc" not in rc["url"]:
            sz = get_resource_size(rc["url"])
            try:
                print("{0} - {1} {2}".format(id_key, data["name"], rc["url"]))
                sz = int(sz)
                if sz == -1:
                    raise ValueError
                if sz < 100:
                    with open("Data\empty_packages.csv", "a", encoding='utf-8', newline='') as f:
                        csvfile = csv.writer(f, 'customcsv')
                        csvfile.writerow([remove_crlf(data["name"]), remove_crlf(data["notes"]),
                                          remove_crlf(data["author"]), rc["url"], remove_crlf(rc["description"]), sz])
            except:
                print("Ошибка получения размера для {}".format(rc["url"]))
        else: #warc-файлы исключены из проверки. вроде, они все целые, но похоже, скачиваются целиком при оценке размера
            print("{} is a warc file, skipped".format(rc["url"]))


