#!/usr/bin/env python
# coding: utf8
"""
Importer data taken from perepis2010 to the opendata hub
"""
import ckanclient
import csv
import json

API_KEY_FILENAME = "apikey.txt"

API_URL = "http://datahub.opengovdata.ru/api"
DATASETS_FILENAME = 'data/catalog.json'
DIRECT_DOWNLOAD_URLPAT = "http://d2f6aadeaff96aaafda4-614b9ac7aa1f2556da9aa7df0eee2346.r98.cf2.rackcdn.com/perepis2010/data/raw/%s"
PAGE_URL = 'http://portal.esimo.ru/dataview/viewresource?resourceId=%s'


class DataImporter:
    """Data importer class for data.mos.ru"""
    def __init__(self):
        self.apikey = open(API_KEY_FILENAME).read()
        self.ckan = ckanclient.CkanClient(base_location=API_URL, api_key=self.apikey)
        self.package_list = self.ckan.package_register_get()
        self.started = True
        self.start_key = ''
        pass

    def delete_all(self):
        """Delete all data from datasets.csv and creates package"""
        f = open(DATASETS_FILENAME, 'r')
        data = f.read()
        f.close()
        reader = json.loads(data)
        package_names = []
        for package in reader['aaData']:
            id = package['resourceId']
            key = 'esimo_' + id.lower()
            try:
                self.ckan.package_entity_delete(key)
                print key, 'deleted'
            except Exception, e:
                print key, 'not deleted. Error'

        pass


    def import_all(self):
        """Processes all data from datasets.csv and creates package"""
        f = open(DATASETS_FILENAME, 'r')
        data = f.read()
        f.close()
        reader = json.loads(data)
        package_names = []
        for package in reader['aaData']:
            if package['datasourceStorageType'] in ['DATA_FILE', 'DBMS'] and package['accessConstraints'] != 'confidential':
                package_names.append(self.register(package))
        self.update_group('esimo', package_names)
        pass

    def register(self, package):
        """Register or update dataset
        :param package:
        """
        id = package['resourceId']
        key = 'esimo_' + id.lower()
        if not self.started:
            if key != self.start_key:
                print 'Already imported. Skipping'
                return key
            else:
                self.started = True
        filename = 'data_%s.csv' % id
        codesfilename = 'codes_%s.json' % id

        try:
            r = self.ckan.package_entity_get(key)
            status = 0

        except ckanclient.CkanApiNotFoundError, e:
            status = 404
        tags = [u'есимо', u'море']
        name = package['objectTitle'].encode('utf8')
        resources = [{'name': name, 'format': 'CSV', 'url': DIRECT_DOWNLOAD_URLPAT % filename,
                      'description': u'Данные в формате CSV выгруженные с сайта ЕСИМО (portal.esimo.ru)'},
                     {'name': u'Структура полей', 'format': 'JSON', 'url': DIRECT_DOWNLOAD_URLPAT % codesfilename,
                      'description': u'Описание метаданных в формате JSON'},
                     {'name': u'RSS лента', 'format': 'RSS', 'url': RSS_URLPAT % id,
                      'description': u'RSS лента обновлений массива данных'},
                     {'name': name, 'format': '', 'url': PAGE_URLPAT % id,
                      'description': u'Страница с описанием на сайте ЕСИМО (portal.esimo.ru)'}
        ]
        print name

        the_package = { 'name' : key, 'title' : name,
                           'notes' :  package['objectDescription'].encode('utf8') + u'\n\nДанные из системы ЕСИМО.\n\n'.encode('utf8'),
                           'tags' : tags,
                           'state' : 'active',
                           'resources': resources,
                           'author' : 'Ivan Begtin',
                           'author_email' : 'ibegtin@infoculture.ru',
                           'maintainer' : 'Ivan Begtin',
                           'maintainer_email' : 'ibegtin@infoculture.ru',
                           'license_id' : 'other-nc',
                           'owner_org': "",
                           'extras': package
                        }
#            self.ckan.package_entity_update(package)
#
#        if True:
        if status == 404:
            try:
                self.ckan.package_register_post(the_package)
            except Exception, e:
                print 'Error importing', key
                print e, type(e), e.message
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
#    imp.delete_all()
    imp.import_all()