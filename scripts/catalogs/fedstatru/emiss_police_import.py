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
#API_URL = "http://data.openpolice.ru/api"
API_URL = "http://datahub.opengovdata.ru/api"
DATASETS_FILENAME = 'policelist.csv'
BASE_URL = 'http://fedstat'
PAGE_URLPAT = 'http://fedstat.ru/indicator/data.do?id=%s'
DIRECT_DOWNLOAD_URLPAT = "http://db5d6ea0e82f092e072c-37f3681af01a3d9c9cbb80b9eeb1d9f9.r33.cf2.rackcdn.com/data/%s"


class DataImporter:
    """Data importer class for data.mos.ru"""
    def __init__(self):
        self.apikey = open(API_KEY_FILENAME).read()
        self.ckan = ckanclient.CkanClient(base_location=API_URL, api_key=self.apikey)
        self.package_list = self.ckan.package_register_get()
        pass

    def collect_data(self):
        """Collects data from EMISS"""
        f = open('policelist.csv', 'w')
        keys = ['uniqid', 'id', 'name', 'filename', 'xlsfilename']
        s = '\t'.join(keys) + '\n'
        f.write(s.encode('utf8'))
        files = os.listdir('data_police')
        for filename in files:
            fname, ext = os.path.splitext(filename)
            if ext == '.xml':
                id = fname.split('_', 1)[-1]
                url = PAGE_URLPAT % id
                u = urlopen(url)
                data = u.read()
                u.close()
                soup = BeautifulSoup(data)
                title = soup.find('meta', attrs={'name' : 'description'})['content']
                xlsfilename = ''
                if os.path.exists('data_police/data_' + id + '.xls'):
                    xlsfilename = 'data_%s.xls' %(id)
                s = ('\t'.join([id, 'emiss_' + id, title, filename, xlsfilename]) + '\n').encode('utf8')
                f.write(s)
                print id, title

    def clear_emiss(self):
        """Clears all"""
        reader = csv.DictReader(open(DATASETS_FILENAME, 'r'), delimiter="\t")
        package_names = []
        for package in reader:
            print package['name']
            try:
                r = self.ckan.package_entity_delete(package['id'])
            except Exception, e:
                print '- error', e
        pass

    def import_emiss(self):
        """Processes all data from datasets.csv and creates package"""
        reader = csv.DictReader(open(DATASETS_FILENAME, 'r'), delimiter="\t")
        package_names = []
        for package in reader:
            package['id'] = package['id']
            package_names.append(self.register(package))
        self.update_group('stats', package_names)
        pass

    def register(self, package):
        """Register or update dataset
        :param package:
        """
        key = 'emiss_' + package['id']
        try:
            r = self.ckan.package_entity_get(key)
            status = 0

        except ckanclient.CkanApiNotFoundError, e:
            status = 404
        tags = [u'емисс', u'статистика', u'мвд россии', u'росстат']
        resources = [{'name': package['name'], 'format': '', 'url':  PAGE_URLPAT % package['uniqid'],
                      'description': u'Страница на сайте fedstat.ru (ЕМИСС)'},
                     {'name': package['name'], 'format': 'XML', 'url': DIRECT_DOWNLOAD_URLPAT % package['filename'],
                      'description': u'Данные в формате XML выгруженные из fedstat.ru'}]

        if len(package['xlsfilename']) > 0:
            resources.append({'name': package['name'], 'format': 'XLS', 'url': DIRECT_DOWNLOAD_URLPAT % package['xlsfilename'],
                      'description': u'Данные в формате XLS выгруженные из fedstat.ru'})

        the_package = { 'name' : key, 'title' : package['name'],
                           'notes' : u'Статистика с портала ЕМИСС (fedstat.ru).\n\n'.encode('utf8') + package['name'],
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
                               {'govbody' : u'Росстат'.encode('utf8')}
                        }
#            self.ckan.package_entity_update(package)
#
#        if True:
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
    imp.clear_emiss()
    imp.import_emiss()