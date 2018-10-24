#!/usr/bin/env python
# coding: utf8
"""
EMISS data extractor
"""
import csv
import os, os.path
from urllib.request import urlopen
from urllib.parse import unquote_plus
from bs4 import BeautifulSoup, BeautifulStoneSoup

import http.cookiejar
#import mechanize
import robobrowser
import time
import json
import re
from requests import Session
import requests
ORGLIST_FNAME = 'orglist.csv'
INDLIST_FNAME = 'indlist.csv'
LIST_URL = 'https://fedstat.ru/organizations/list.do'
ORG_PAT = 'https://fedstat.ru/indicators/org.do?id=%s'
DATA_PAT = 'https://fedstat.ru/indicator/%s.do'
BASE_URL = 'https://fedstat.ru'
FILE_URL = 'https://fedstat.ru/indicator/data.do'

ORG_XML_PAT = 'http://fedstat.ru/indicators/expand.do?expandId=%s&display=organization'
ORG_IND_PAT = 'http://fedstat.ru/indicators/expand.do?expandId=%s&id=%s&display=organization'


class DataExtractor:
    """Data extractor for emill"""
    def __init__(self):

        # Cookie Jar
        cj = http.cookiejar.LWPCookieJar()
        user_agent='User-agent Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.1) Gecko/2008071615 Fedora/3.0.1-1.fc9 Firefox/3.0.1'
        session = Session()
        session.headers.update({
            'Content-Encoding': 'gzip',
        })
        rb = robobrowser.RoboBrowser(session=session, user_agent=user_agent)
        rb.session.cookies.update(cj)

        self.rb = rb
        # Browser

    def extract_org_list(self):
        """Extracts org list"""
        f = open(ORGLIST_FNAME, 'w')
        keys = ['id', 'url', 'name']
        s = '\t'.join(keys) + '\n'
        f.write(s.encode('utf8'))
        u  = urlopen(LIST_URL)
        data = u.read()
        u.close()
        soup = BeautifulSoup(data, convertEntities=BeautifulStoneSoup.HTML_ENTITIES)
        listofli = soup.find('table', attrs={'id': 'row'}).findAll('tr')
        for liitem in listofli:
            tds = liitem.findAll('td')
            if len(tds) < 2: continue
            url = BASE_URL + tds[1].find('a')['href']
            id = tds[1].find('a')['href'].rsplit('=', 1)[-1]
            name = unquote_plus(tds[1].find('a').string)
            s = ('\t'.join([id, url, name]) + '\n').encode('utf8')
            f.write(s)
            print(s.strip())



    def __process_folder(self, url):
        allitems = []
        url = BASE_URL + '/indicators/' + url
        #        print url
        u = urlopen(url)
        data = u.read()
        soup = BeautifulSoup(data)
        folders = soup.findAll('folder')
        items = soup.findAll('item')
        for item in items:
            ds = BeautifulSoup(item['text'])
            a = ds.find('a', attrs={'class' : 'fastTreeItemName'})
            res = [item['id'], BASE_URL + a['href'], a.string]
            allitems.append(res)
        #            print ('\t'.join(res)).encode('utf8')
        for f in folders:
            allitems.extend(self.__process_folder(f['src']))
        return allitems

    def extract_ind_list(self):
        """Extracts list of indicators based on list of orgs"""
        f = open(INDLIST_FNAME, 'w')
        keys = ['id', 'url', 'name', 'orgid', 'orgname']
        s = '\t'.join(keys) + '\n'
        f.write(s.encode('utf8'))
        reader = csv.DictReader(open(ORGLIST_FNAME, 'r'), delimiter="\t")
        i = 0
        for item in reader:
            i += 1
            print(i)
            #            if i != 6: continue
            url = ORG_XML_PAT % item['id']
            #            print url
            u = urlopen(url)
            data = u.read()
            soup = BeautifulSoup(data)
            folders = soup.findAll('folder')
            allitems = []
            for folder in folders:
                allitems.extend(self.__process_folder(folder['src']))
            for theitem in allitems:
                theitem.append(item['id'])
                theitem.append(item['name'].decode('utf8'))
                s = ('\t'.join(theitem) + '\n').encode('utf8')
                f.write(s)
                print(s.strip())


    def extract_all_ind_data(self):
        reader = csv.DictReader(open(INDLIST_FNAME, 'r'), delimiter="\t")
        i = 0
        for item in reader:
            i += 1
            self.extract_ind_data(item['id'][1:])
            print(i, item['name'])


    def extract_ind_data(self, id):
        """Extracts selected indicator data"""
        if os.path.exists('data/data_%s_full.sdmx' % id) and os.path.exists('data/data_%s_full.xls' % id):
            return
        rb = self.rb
        print(DATA_PAT % id)
        r = rb.open(DATA_PAT % id)
        soup = BeautifulSoup(rb.response.content.decode("utf-8") )
        the_word='new FGrid'
        data=str(soup.findAll('script',text=lambda t: t and the_word in t)[0])

        found = re.search('title: \'(.+)', data).group()
        title=found.split(': ')[1]

        data_post=[('lineObjectIds','0')]
        try:
            found = re.search('left_columns: \[\n(.+)', data).group(1)
        except AttributeError:
                found = '' # apply your error handling

        for i in found.strip().split(', '):
            data_post.append(('lineObjectIds',i))


        try:
            found = re.search('top_columns: \[\n(.+)', data).group(1)
        except AttributeError:
            found = '' # apply your error handling


        for i in found.strip().split(', '):
            data_post.append(('columnObjectIds',i))

        i=0
        prefix=''
        suffix=''
        num=data.find('filters')

        for letter in data[num:].splitlines():
            if '{' in letter:
                i+=1
            if '}' in letter:
                i-=1
            if re.findall('\d+: {' ,letter):
                if i<=2:
                    prefix=letter.strip().split(':')[0]
                if i>2:
                    suffix=letter.strip().split(':')[0]
            data_post.append(('selectedFilterIds',prefix+'_'+suffix))
        data_post.append(('id', str(id)))
        params = (('format', 'sdmx'),)
        response = requests.post(FILE_URL, params=params, data=data_post)

        # Save as SDMX
        f = open('data/data_%s_full.sdmx' % id , 'wb')
        f.write(response.content)
        f.close()

        # Save as XLS
        params = (('format', 'excel'),)
        response = requests.post(FILE_URL, params=params, data=data_post)
        f = open('data/data_%s_full.xls' % id , 'wb')
        f.write(response.content)
        f.close()


if __name__ == "__main__":
    imp = DataExtractor()
    #imp.extract_ind_data('37429')
    imp.extract_ind_data('37430')
#    imp.extract_all_ind_data()
#    imp.extract_org_list()
#    imp.extract_ind_list()
