#!/usr/bin/env python
# coding: utf8
"""
EMISS data extractor
"""
import csv
import os, os.path
from urllib import urlopen, unquote_plus
from BeautifulSoup import BeautifulSoup, BeautifulStoneSoup

import mechanize
import cookielib
import time
ORGLIST_FNAME = 'orglist.csv'
INDLIST_FNAME = 'indlist.csv'
LIST_URL = 'http://fedstat.ru/organizations/list.do'
ORG_PAT = 'http://fedstat.ru/indicators/org.do?id=%s'
DATA_PAT = 'http://fedstat.ru/indicator/data.do?id=%s'
BASE_URL = 'http://fedstat.ru'

ORG_XML_PAT = 'http://fedstat.ru/indicators/expand.do?expandId=%s&display=organization'
ORG_IND_PAT = 'http://fedstat.ru/indicators/expand.do?expandId=%s&id=%s&display=organization'


class DataExtractor:
    """Data extractor for emill"""
    def __init__(self):

        # Browser
        br = mechanize.Browser()

        # Cookie Jar
        cj = cookielib.LWPCookieJar()
        br.set_cookiejar(cj)

        # Browser options
        br.set_handle_equiv(True)
        br.set_handle_gzip(True)
        br.set_handle_redirect(True)
        br.set_handle_referer(True)
        br.set_handle_robots(False)

        # Follows refresh 0 but not hangs on refresh > 0
        br.set_handle_refresh(mechanize._http.HTTPRefreshProcessor(), max_time=1)

        # Want debugging messages?
        #br.set_debug_http(True)
        #br.set_debug_redirects(True)
        #br.set_debug_responses(True)

        # User-Agent (this is cheating, ok?)
        br.addheaders = [('User-agent', 'Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.1) Gecko/2008071615 Fedora/3.0.1-1.fc9 Firefox/3.0.1')]
        self.br = br

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
            print s.strip()



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
            print i
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
                print s.strip()


    def extract_all_ind_data(self):
        reader = csv.DictReader(open(INDLIST_FNAME, 'r'), delimiter="\t")
        i = 0
        for item in reader:
            i += 1
            self.extract_ind_data(item['id'][1:])
            print i, item['name']


    def extract_ind_data(self, id):
        """Extracts selected indicator data"""
        if os.path.exists('data/data_%s_full.sdmx' % id) and os.path.exists('data/data_%s_full.xls' % id):
            return
        br = self.br
        # Save as SDMX
        r = br.open(DATA_PAT % id)
        br.select_form(nr=1)
        br.form.set_all_readonly(False)
        values = []
        for k in br.form.controls:
            if k.name[0:28] == '__checkbox_selectedFilterIds':
                values.append(k.value)
        i = 0
        for k in br.form.controls:
            if k.name == 'selectedFilterIds' and i == 0:
                i += 1
            elif k.name == 'selectedFilterIds':
                k.value = values
                break
        br.form['format'] = 'sdmx'
        br.submit()
        f = open('data/data_%s_full.sdmx' % id , 'w')
        f.write(br.response().read())
        f.close()


        # Save as XLS
        r = br.open(DATA_PAT % id)
        br.select_form(nr=1)
        br.form.set_all_readonly(False)
        values = []
        for k in br.form.controls:
            if k.name[0:28] == '__checkbox_selectedFilterIds':
                values.append(k.value)
        i = 0
        for k in br.form.controls:
            if k.name == 'selectedFilterIds' and i == 0:
                i += 1
            elif k.name == 'selectedFilterIds':
                k.value = values
                break
        br.form['format'] = 'excel'
        br.submit()
        f = open('data/data_%s_full.xls' % id , 'w')
        f.write(br.response().read())
        f.close()



if __name__ == "__main__":
    imp = DataExtractor()
    #imp.extract_ind_data('37429')
    imp.extract_all_ind_data()
#    imp.extract_org_list()
#    imp.extract_ind_list()
