#!/usr/bin/env python
# coding: utf8
"""
Importer data about police taken from EMISS (fedstat.ru) to open data hub.
"""
import ckanclient
import csv
import os, os.path
from urllib import urlopen
from BeautifulSoup import BeautifulSoup

API_KEY_FILENAME = "apikey.txt"

# Police datahub API
LIST_URL = 'http://data.ulgov.ru/index/list/'
API_URL = "http://datahub.opengovdata.ru/api"
DATASETS_FILENAME = 'datasets.csv'
BASE_URL = 'http://data.ulgov.ru'
PAGE_URLPAT = 'http://data.ulgov.ru/index/data/id/%s/'
TARGET_GROUP = 'ulgov'

class DataImporter:
    """Data importer class for data.mos.ru"""
    def __init__(self):
        self.apikey = open(API_KEY_FILENAME).read()
        self.ckan = ckanclient.CkanClient(base_location=API_URL, api_key=self.apikey)
        self.package_list = self.ckan.package_register_get()
        pass

    def collect_data(self):
        """Collects data from EMISS"""
        f = open(DATASETS_FILENAME, 'w')
        keys = ['id', 'url', 'name', 'dataurl', 'source']
        s = '\t'.join(keys) + '\n'
        f.write(s.encode('utf8'))

        u  = urlopen(LIST_URL)
        data = u.read()
        u.close()
        soup = BeautifulSoup(data)
        listofli = soup.find('div', attrs={'class': 'span8'}).find('ul').findAll('li')
        for liitem in listofli:
            url = 'http://data.ulgov.ru' + liitem.find('a')['href']
            id = liitem.find('a')['href'].strip('/').rsplit('/', 1)[-1]
            name = liitem.find('a').string
            source = liitem.find('small').find('em').string
            u = urlopen(url)
            ds = BeautifulSoup(u.read())
            u.close()
            au = ds.find('a', attrs={'class' : 'btn btn-large btn-primary'})
            dataurl = 'http://data.ulgov.ru' + au['href']
            s = ('\t'.join([id, url, name, dataurl, source]) + '\n').encode('utf8')
            f.write(s)
            print s.strip()
            del ds

    def import_datasets(self):
        """Processes all data from datasets.csv and creates package"""
        reader = csv.DictReader(open(DATASETS_FILENAME, 'r'), delimiter="\t")
        package_names = []
        for package in reader:
            package_names.append(self.register(package))
        self.update_group(TARGET_GROUP, package_names)
        pass

    def register(self, package):
        """Register or update dataset
        :param package:
        """
        key = 'dataulgovru_' + package['id']
        try:
            r = self.ckan.package_entity_get(key)
            status = 0

        except ckanclient.CkanApiNotFoundError, e:
            status = 404
        tags = [u'правительство ульяновской области', u'ульяновская область', u'официально']
        resources = [{'name': package['name'], 'format': '', 'url':  PAGE_URLPAT % package['id'],
                      'description': u'Страница на сайте ульяновской области'},
                     {'name': package['name'], 'format': 'CSV', 'url': package['dataurl'],
                      'description': u'Данные в формате CSV выгруженные с портала data.ulgov.ru'}]


        the_package = { 'name' : key, 'title' : package['name'],
                           'notes' : package['name'] + u'\n\n'.encode('utf8') + u'Данные с портала открытых данных Ульяновской области.'.encode('utf8'),
                           'tags' : tags,
                           'state' : 'active',
                           'resources': resources,
                           'group' : 'moscow',
                           'author' : 'Ivan Begtin',
                           'author_email' : 'ibegtin@infoculture.ru',
                           'maintainer' : 'Ivan Begtin',
                           'maintainer_email' : 'ibegtin@infoculture.ru',
                           'license_id' : 'other-nc',
                           'owner_org': "",
                           'extras':
                               {'govbody' : package['source']}
                        }
        if status == 404:
            try:
                self.ckan.package_register_post(the_package)
            except Exception, e:
                print 'Error importing', key
                print e, type(e)
                return key
                pass
            print "Imported", key
        else:
            package_entity = self.ckan.last_message
            if type(package_entity) == type(''): return None
            package_entity.update(the_package)
            for k in ['id', 'ratings_average', 'relationships', 'ckan_url', 'ratings_count']:
                del package_entity[k]
            self.ckan.package_entity_put(package_entity)
            print "Updated", key
#            print self.ckan.last_message
        return key

    def update_group(self, group_name, package_names, group_title="", description=""):
            #        print key
            try:
                group = self.ckan.group_entity_get(group_name)
                status = 0
            except ckanclient.CkanApiNotFoundError, e:
                status = 404
            if status == 404:
                group_entity = {'name' : group_name, 'title' : group_title, 'description' : description }
                self.ckan.group_register_post(group_entity)
            group = self.ckan.group_entity_get(group_name)
            for name in package_names:
                if name not in group['packages']:
                    group['packages'].append(name)
            self.ckan.group_entity_put(group)


if __name__ == "__main__":
    imp = DataImporter()
#    imp.collect_data()
    imp.import_datasets()