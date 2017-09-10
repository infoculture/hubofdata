import csv
import re

csv.register_dialect('customcsv', delimiter=',', quoting=csv.QUOTE_MINIMAL, quotechar='"', escapechar='\\')
with open("Data\empty_packages_data.csv", encoding='utf-8') as csvfile:
    readCSV = csv.reader(csvfile, 'customcsv')
    empty_packages = list(readCSV)
    empty_packages.pop(0)

# package_name,package_title,author,resource_url,resource_title,resource_size

packages_all = set()
packages_excel = set()
packages_zip = set()
packages_json = set()
packages_cdx_small = set()
packages_cdn_forbidden = set()
packages_rackcdn = set()

for ep in empty_packages:
    packages_all.add(ep[0])
    if ".xls" in ep[3].lower():
        packages_excel.add(ep[0])
    if ".zip" in ep[3].lower():
        packages_zip.add(ep[0])
    if ".json" in ep[3].lower():
        packages_json.add(ep[0])
    if "rackcdn.com" in ep[3].lower():
        packages_rackcdn.add(ep[0])
    if "cdn2.sdlabs.ru/preservation" in ep[3].lower():
        packages_cdn_forbidden.add(ep[0])
    if ".cdx" in ep[3].lower():
        packages_cdx_small.add(ep[0])

packages_other = packages_all - (packages_all & (packages_excel | packages_zip | packages_json |
                    packages_rackcdn | packages_cdn_forbidden | packages_cdx_small))

with open("Data\empty_packages_stat.csv", "w", encoding='utf-8', newline='') as f:
    csvfile = csv.writer(f, 'customcsv')
    csvfile.writerow(["Всего наборов с пустыми файлами: {}".format(len(packages_all))])
    csvfile.writerow(["Всего наборов с пустыми файлами excel (xls): {}".format(len(packages_excel))])
    csvfile.writerow(["    (перечень - в empty_packages_excel.csv)"])
    csvfile.writerow(["Всего наборов с пустыми файлами zip: {}".format(len(packages_zip))])
    csvfile.writerow(["    (перечень - в empty_packages_zip.csv)"])
    csvfile.writerow(["Всего наборов с пустыми файлами json: {}".format(len(packages_json))])
    csvfile.writerow(["    (перечень - в empty_packages_json.csv)"])
    csvfile.writerow(["Всего наборов с пустыми файлами по длинной ссылке: {}".format(len(packages_rackcdn))])
    csvfile.writerow(["    (ссылки вида http://d2f6aadeaff96aaafda4-614b9ac7aa1f2556da9aa7df0eee2346.r98.cf2.rackcdn.com/cikrf/data/raw/2007/pvr.pdf)"])
    csvfile.writerow(["    (перечень - в empty_packages_rackcdn.csv)"])
    csvfile.writerow(["Всего наборов с пустыми или мелкими файлами .cdx в : {}".format(len(packages_cdx_small))])
    csvfile.writerow(["Всего наборов с закрытым доступом в CDN: {}".format(len(packages_cdn_forbidden))])
    csvfile.writerow(["    (ссылки вида http://cdn2.sdlabs.ru/preservation/webcollect/lawenf/112.ru/112.ru.cdx.xz)"])
    csvfile.writerow(["Всего наборов с пустыми файлами, не вошедших в другие категории: {}".format(len(packages_other))])

with open("Data\empty_packages_all.csv", "w", encoding='utf-8', newline='') as f:
    csvfile = csv.writer(f, 'customcsv')
    for p in packages_all:
        csvfile.writerow([p])

with open("Data\empty_packages_excel.csv", "w", encoding='utf-8', newline='') as f:
    csvfile = csv.writer(f, 'customcsv')
    for p in packages_excel:
        csvfile.writerow([p])

with open("Data\empty_packages_zip.csv", "w", encoding='utf-8', newline='') as f:
    csvfile = csv.writer(f, 'customcsv')
    for p in packages_zip:
        csvfile.writerow([p])

with open("Data\empty_packages_json.csv", "w", encoding='utf-8', newline='') as f:
    csvfile = csv.writer(f, 'customcsv')
    for p in packages_json:
        csvfile.writerow([p])


with open("Data\empty_packages_rackcdn.csv", "w", encoding='utf-8', newline='') as f:
    csvfile = csv.writer(f, 'customcsv')
    for p in packages_rackcdn:
        csvfile.writerow([p])

with open("Data\empty_packages_cdn_forbidden.csv", "w", encoding='utf-8', newline='') as f:
    csvfile = csv.writer(f, 'customcsv')
    for p in packages_cdn_forbidden:
        csvfile.writerow([p])

with open("Data\empty_packages_cdx_small.csv", "w", encoding='utf-8', newline='') as f:
    csvfile = csv.writer(f, 'customcsv')
    for p in packages_cdx_small:
        csvfile.writerow([p])

with open("Data\empty_packages_other.csv", "w", encoding='utf-8', newline='') as f:
    csvfile = csv.writer(f, 'customcsv')
    for p in packages_other:
        csvfile.writerow([p])
