#!/usr/bin/env python
# coding: utf8
"""
Importer data taken from Central Bank (www.cbr.ru) to the opendata hub
"""
import ckanclient
import csv
import os, os.path
from urllib import urlopen
from BeautifulSoup import BeautifulSoup

API_KEY_FILENAME = "apikey.txt"

# Police datahub API
API_URL = "http://datahub.opengovdata.ru/api"
DATASETS_FILENAME = 'refined/fullinds.csv'
DIRECT_DOWNLOAD_URLPAT = "http://9e6ea82e8717937529b0-8088da39de044d3f95103c0c7eebdb3b.r31.cf2.rackcdn.com/inddata/%s"


class DataImporter:
    """Data importer class for data.mos.ru"""
    def __init__(self):
        self.apikey = open(API_KEY_FILENAME).read()
        self.ckan = ckanclient.CkanClient(base_location=API_URL, api_key=self.apikey)
        self.package_list = self.ckan.package_register_get()
        self.started = True
#        self.start_key = 'cbrf_ind_479'
        pass


    def import_all(self):
        """Processes all data from datasets.csv and creates package"""
        reader = csv.DictReader(open(DATASETS_FILENAME, 'r'), delimiter="\t")
        package_names = []
        for package in reader:
            package_names.append(self.register(package))
        self.update_group('cbrf', package_names)
        pass

    def register(self, package):
        """Register or update dataset
        :param package:
        """
        package['id'] = package['ind_id']
        key = 'cbrf_ind_' + package['id']
        if not self.started:
            if key != self.start_key:
                print 'Already imported. Skipping'
                return key
            else:
                self.started = True
        package['name'] = package['table_name'] + '. ' + package['ind_name']
        filename = 'inddata_%s.csv' % package['id']

        try:
            r = self.ckan.package_entity_get(key)
            status = 0

        except ckanclient.CkanApiNotFoundError, e:
            status = 404
        tags = [u'цб рф', u'индикаторы', u'финансы', u'статистика']
        resources = [{'name': package['name'], 'format': 'CSV', 'url': DIRECT_DOWNLOAD_URLPAT % filename,
                      'description': u'Данные в формате CSV выгруженные с сайта ЦБ РФ (www.cbr.ru)'}]

        the_package = { 'name' : key, 'title' : package['name'],
                           'notes' : u'Данные с сайта ЦБ РФ.\n\n'.encode('utf8') + package['name'],
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
                               {'govbody' : u'ЦБ РФ'.encode('utf8'),
                                'ind_name' : package['ind_name'],
                                'ind_id' : package['ind_id'],
                                'table_name': package['table_name'],
                                'table_id' : package['table_id']
                                }
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
    imp.import_all()