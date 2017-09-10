#!/usr/bin/env python
# coding: utf8
"""
EMISS data extractor
"""
import csv
import json
import os, os.path
from urllib import urlopen, unquote_plus, urlencode
import urllib2
from BeautifulSoup import BeautifulSoup, BeautifulStoneSoup

import mechanize
import cookielib
import time
BASE_URL = 'http://budget.gov.ru'

REGIONS_FILE = 'data/regions.csv'
CATALOG_FILE = 'data/datasets.csv'
LIST_URL = 'http://budget.gov.ru/data/opendata'
REGION_URL = 'http://budget.gov.ru/regions/passport?region=%s'
BUDGET_EXP_JSON = 'http://budget.gov.ru/ajax/JsonExp/index?curDate=01.11.2012&budgetType=-2&budgetLevel=%s'
BUDGET_DOH_JSON = 'http://budget.gov.ru/ajax/JsonData/index?id=-10&param=01.11.2012&param2=%s'

class DataExtractor:
    """Data extractor from ESIMO"""

    def __init__(self):
        pass

    def extract_catalog(self):
        u = urlopen(LIST_URL)
        data = u.read()
        u.close()
        soup = BeautifulSoup(data)
        alldata = soup.findAll('div', attrs={'class' : 'npa_unit clearfix'})
        for item in alldata:
            resources = []
            name = item.find('span', attrs={'class' : 'npa_header'}).string


        f = open(CATALOG_FILE, 'w')
        parsed = json.loads(data)
        f.write(json.dumps(parsed, indent=4, sort_keys=True))
        f.close()

    def extract_regions(self):
        f = open(REGIONS_FILE, 'w')
        keys = ['id','name']
        s = ('\t'.join(keys)).encode('utf8') + '\n'
        f.write(s)
        u = urlopen(BASE_URL)
        data = u.read()
        u.close()
        soup = BeautifulSoup(data)
        alldata = soup.findAll('div', attrs={'class' : 'RBCC_item'})
        for item in alldata:
            span = item.find('span')
            id = span['id']
            name = span.string
            r = [id, name]
            s = ('\t'.join(r)).encode('utf8') + '\n'
            f.write(s)
        f.close()

    def extract_regions_data(self):
        reader = csv.DictReader(open(REGIONS_FILE, 'r'), delimiter="\t")
        alldata = []
        for r in reader:
            url = BUDGET_DOH_JSON % r['id']
            data = urlopen(url).read()
            f = open('data/raw/dokhod_%s.json' %r['id'], 'w')
            d = json.loads(data)
            parsed = json.loads(data)
            alldata.append(parsed)
            f.write(json.dumps(parsed, indent=4, sort_keys=True))
            f.close()

            url = BUDGET_EXP_JSON % r['id']
            data = urlopen(url).read()
            f = open('data/raw/exp_%s.json' %r['id'], 'w')
            d = json.loads(data)
            parsed = json.loads(data)
            alldata.append(parsed)
            f.write(json.dumps(parsed, indent=4, sort_keys=True))
            f.close()
            print 'Processed', url



    def extract_all_raw(self):

        pass


if __name__ == "__main__":
    ext = DataExtractor()
#    ext.extract_all_raw()
#    ext.extract_regions()
    ext.extract_regions_data()


