#!/usr/bin/env python2.6
# -*- coding: utf-8 -*-
# Offlickr
# Hugo Haas <hugo@larve.net> -- http://larve.net/people/hugo/
# Homepage: http://code.google.com/p/offlickr/
# License: GPLv2
#
# Daniel Drucker <dmd@3e.org> contributed:
#   * wget patch
#   * backup of videos as well
#   * updated to Beej's Flickr API version 1.2 (required)
# 
# Beraldo Leal <beraldo at beraldoleal.com> contributed:
#   * Download photos in set with -s and -p option

import sys
import libxml2
import urllib
import getopt
import time
import os.path
import threading

# Beej's Python Flickr API
# http://beej.us/flickr/flickrapi/

from flickrapi import FlickrAPI
import logging

__version__ = '0.22 - 2009-03-20'
maxTime = '9999999999'

# Gotten from Flickr. the flickrSecret isn't really a secret.
# see http://markmail.org/message/mcgz7vbfw3lrjtbo

flickrAPIKey = '1391fcd0a9780b247cd6a101272acf71'
flickrSecret = 'fd221d0336de3b6d'


class Offlickr:

    def __init__(
        self,
        key,
        secret,
        uid,
        httplib=None,
        dryrun=False,
        verbose=False,
        ):
        """Instantiates an Offlickr object
        An API key is needed, as well as an API secret and a user id."""

        self.__flickrAPIKey = key
        self.__flickrSecret = secret
        self.__httplib = httplib

        # Get authentication token
        # note we must explicitly select the xmlnode parser to be compatible with FlickrAPI 1.2

        self.fapi = FlickrAPI(self.__flickrAPIKey, self.__flickrSecret,
                              format='xmlnode')
        (token, frob) = self.fapi.get_token_part_one()
        if not token:
            raw_input('Press ENTER after you authorized this program')
        self.fapi.get_token_part_two((token, frob))
        self.token = token
        self.flickrUserId = uid
        self.dryrun = dryrun
        self.verbose = verbose

    def __testFailure(self, rsp):
        """Returns whether the previous call was successful"""

        if rsp['stat'] == 'fail':
            print 'Error!'
            return True
        else:
            return False

    def getPhotoList(self, dateLo, dateHi):
        """Returns a list of photo given a time frame"""

        n = 0
        flickr_max = 500
        photos = []

        print 'Retrieving list of photos'
        while True:
            if self.verbose:
                print 'Requesting a page...'
            n = n + 1
            rsp = self.fapi.photos_search(
                api_key=self.__flickrAPIKey,
                auth_token=self.token,
                user_id=self.flickrUserId,
                per_page=str(flickr_max),
                page=str(n),
                min_upload_date=dateLo,
                max_upload_date=dateHi,
                )
            if self.__testFailure(rsp):
                return None
            if rsp.photos[0]['total'] == '0':
                return None
            photos += rsp.photos[0].photo
            if self.verbose:
                print ' %d photos so far' % len(photos)
            if len(photos) >= int(rsp.photos[0]['total']):
                break

        return photos

    def getGeotaggedPhotoList(self, dateLo, dateHi):
        """Returns a list of photo given a time frame"""

        n = 0
        flickr_max = 500
        photos = []

        print 'Retrieving list of photos'
        while True:
            if self.verbose:
                print 'Requesting a page...'
            n = n + 1
            rsp = \
                self.fapi.photos_getWithGeoData(api_key=self.__flickrAPIKey,
                    auth_token=self.token, user_id=self.flickrUserId,
                    per_page=str(flickr_max), page=str(n))
            if self.__testFailure(rsp):
                return None
            if rsp.photos[0]['total'] == '0':
                return None
            photos += rsp.photos[0].photo
            if self.verbose:
                print ' %d photos so far' % len(photos)
            if len(photos) >= int(rsp.photos[0]['total']):
                break

        return photos

    def getPhotoLocation(self, pid):
        """Returns a string containing location of a photo (in XML)"""

        rsp = \
            self.fapi.photos_geo_getLocation(api_key=self.__flickrAPIKey,
                auth_token=self.token, photo_id=pid)
        if self.__testFailure(rsp):
            return None
        doc = libxml2.parseDoc(rsp.xml)
        info = doc.xpathEval('/rsp/photo')[0].serialize()
        doc.freeDoc()
        return info

    def getPhotoLocationPermission(self, pid):
        """Returns a string containing location permision for a photo (in XML)"""

        rsp = \
            self.fapi.photos_geo_getPerms(api_key=self.__flickrAPIKey,
                auth_token=self.token, photo_id=pid)
        if self.__testFailure(rsp):
            return None
        doc = libxml2.parseDoc(rsp.xml)
        info = doc.xpathEval('/rsp/perms')[0].serialize()
        doc.freeDoc()
        return info

    def getPhotosetList(self):
        """Returns a list of photosets for a user"""

        rsp = self.fapi.photosets_getList(api_key=self.__flickrAPIKey,
                auth_token=self.token, user_id=self.flickrUserId)
        if self.__testFailure(rsp):
            return None
        return rsp.photosets[0].photoset

    def getPhotosetPhotos(self, pid):
        """Returns a list of photos in a photosets"""

        rsp = self.fapi.photosets_getPhotos(api_key=self.__flickrAPIKey,
                auth_token=self.token, user_id=self.flickrUserId, photoset_id=pid)
        if self.__testFailure(rsp):
            return None
        return rsp.photoset[0].photo

    def getPhotosetInfo(self, pid, method):
        """Returns a string containing information about a photoset (in XML)"""

        rsp = method(api_key=self.__flickrAPIKey,
                     auth_token=self.token, photoset_id=pid)
        if self.__testFailure(rsp):
            return None
        doc = libxml2.parseDoc(rsp.xml)
        info = doc.xpathEval('/rsp/photoset')[0].serialize()
        doc.freeDoc()
        return info

    def getPhotoMetadata(self, pid):
        """Returns an array containing containing the photo metadata (as a string), and the format of the photo"""

        if self.verbose:
            print 'Requesting metadata for photo %s' % pid
        rsp = self.fapi.photos_getInfo(api_key=self.__flickrAPIKey,
                auth_token=self.token, photo_id=pid)
        if self.__testFailure(rsp):
            return None
        doc = libxml2.parseDoc(rsp.xml)
        metadata = doc.xpathEval('/rsp/photo')[0].serialize()
        doc.freeDoc()
        return [metadata, rsp.photo[0]['originalformat']]

    def getPhotoComments(self, pid):
        """Returns an XML string containing the photo comments"""

        if self.verbose:
            print 'Requesting comments for photo %s' % pid
        rsp = \
            self.fapi.photos_comments_getList(api_key=self.__flickrAPIKey,
                auth_token=self.token, photo_id=pid)
        if self.__testFailure(rsp):
            return None
        doc = libxml2.parseDoc(rsp.xml)
        comments = doc.xpathEval('/rsp/comments')[0].serialize()
        doc.freeDoc()
        return comments

    def getPhotoSizes(self, pid):
        """Returns a string with is a list of available sizes for a photo"""

        rsp = self.fapi.photos_getSizes(api_key=self.__flickrAPIKey,
                auth_token=self.token, photo_id=pid)
        if self.__testFailure(rsp):
            return None
        return rsp

    def getOriginalPhoto(self, pid):
        """Returns a URL which is the original photo, if it exists"""

        source = None
        rsp = self.getPhotoSizes(pid)
        if rsp == None:
            return None
        for s in rsp.sizes[0].size:
            if s['label'] == 'Original':
                source = s['source']
        for s in rsp.sizes[0].size:
            if s['label'] == 'Video Original':
                source = s['source']
        return [source, s['label'] == 'Video Original']

    def __downloadReportHook(
        self,
        count,
        blockSize,
        totalSize,
        ):

        if not self.__verbose:
            return
        p = ((100 * count) * blockSize) / totalSize
        if p > 100:
            p = 100
        print '\r %3d %%' % p,
        sys.stdout.flush()

    def downloadURL(
        self,
        url,
        target,
        filename,
        verbose=False,
        ):
        """Saves a photo in a file"""

        if self.dryrun:
            return
        self.__verbose = verbose
        tmpfile = '%s/%s.TMP' % (target, filename)
        if self.__httplib == 'wget':
            cmd = 'wget -q -t 0 -T 120 -w 10 -c -O %s %s' % (tmpfile,
                    url)
            os.system(cmd)
        else:
            urllib.urlretrieve(url, tmpfile,
                               reporthook=self.__downloadReportHook)
        os.rename(tmpfile, '%s/%s' % (target, filename))


def usage():
    """Command line interface usage"""

    print 'Usage: Offlickr.py -i <flickr Id>'
    print 'Backs up Flickr photos and metadata'
    print 'Options:'
    print '\t-f <date>\tbeginning of the date range'
    print '\t\t\t(default: since you started using Flickr)'
    print '\t-t <date>\tend of the date range'
    print '\t\t\t(default: until now)'
    print '\t-d <dir>\tdirectory for saving files (default: ./dst)'
    print '\t-l <level>\tlevels of directory hashes (default: 0)'
    print '\t-p\t\tback up photos in addition to photo metadata'
    print '\t-n\t\tdo not redownload anything which has already been downloaded (only jpg checked)'
    print '\t-o\t\toverwrite photo, even if it already exists'
    print '\t-L\t\tback up human-readable photo locations and permissions to separate files'
    print '\t-s\t\tback up all photosets (time range is ignored)'
    print '\t-e\t\tback up videos ONLY'
    print '\t-w\t\tuse wget instead of internal Python HTTP library'
    print '\t-c <threads>\tnumber of threads to run to backup photos (default: 1)'
    print '\t-v\t\tverbose output'
    print '\t-N\t\tdry run'
    print '\t-h\t\tthis help message'
    print '\nDates are specified in seconds since the Epoch (00:00:00 UTC, January 1, 1970).'
    print '\nVersion ' + __version__


def fileWrite(
    dryrun,
    directory,
    filename,
    string,
    ):
    """Write a string into a file"""

    if dryrun:
        return
    if not os.access(directory, os.F_OK):
        os.makedirs(directory)
    f = open(directory + '/' + filename, 'w')
    f.write(string)
    f.close()
    print 'Written as', filename


class photoBackupThread(threading.Thread):

    def __init__(
        self,
        sem,
        i,
        total,
        id,
        title,
        offlickr,
        target,
        hash_level,
        getPhotos,
        getVideosOnly,
        doNotRedownload,
        overwritePhotos,
        ):

        self.sem = sem
        self.i = i
        self.total = total
        self.id = id
        self.title = title
        self.offlickr = offlickr
        self.target = target
        self.hash_level = hash_level
        self.getPhotos = getPhotos
        self.getVideosOnly = getVideosOnly
        self.doNotRedownload = doNotRedownload
        self.overwritePhotos = overwritePhotos
        threading.Thread.__init__(self)

    def run(self):
        backupPhoto(
            self.i,
            self.total,
            self.id,
            self.title,
            self.target,
            self.hash_level,
            self.offlickr,
            self.doNotRedownload,
            self.getPhotos,
            self.getVideosOnly,
            self.overwritePhotos,
            )
        self.sem.release()


def backupPhoto(
    i,
    total,
    id,
    title,
    target,
    hash_level,
    offlickr,
    doNotRedownload,
    getPhotos,
    getVideosOnly,
    overwritePhotos,
    ):

    [source, isVideo] = offlickr.getOriginalPhoto(id)

    if (getVideosOnly and not isVideo):
        return

    print str(i) + '/' + str(total) + ': ' + id + ': '\
         + title.encode('utf-8')
    td = target_dir(target, hash_level, id)
    if doNotRedownload and os.path.isfile(td + '/' + id + '.xml')\
         and os.path.isfile(td + '/' + id + '-comments.xml')\
         and (not getPhotos or getPhotos and os.path.isfile(td + '/'
               + id + '.jpg')):
        print 'Photo %s already downloaded; continuing' % id
        return

    # Get Metadata

    metadataResults = offlickr.getPhotoMetadata(id)
    if metadataResults == None:
        print 'Failed!'
        sys.exit(2)
    metadata = metadataResults[0]
    format = metadataResults[1]
    t_dir = target_dir(target, hash_level, id)

    # Write metadata

    fileWrite(offlickr.dryrun, t_dir, id + '.xml', metadata)

    # Get comments

    photoComments = offlickr.getPhotoComments(id)
    fileWrite(offlickr.dryrun, t_dir, id + '-comments.xml',
              photoComments)

    # Do we want the picture too?

    if not getPhotos:
        return


    if source == None:
        print 'Oopsie, no photo found'
        return

    # if it's a Video, we cannot trust the format that getInfo told us.
    # we have to make an extra round trip to grab the Content-Disposition
    isPrivateFailure = False
    if isVideo:
        sourceconnection = urllib.urlopen(source)
        try:
            format = sourceconnection.headers['Content-Disposition'].split('.')[-1].rstrip('"')
        except:
            print 'warning: private video %s cannot be backed up due to a Flickr bug' % str(id)
            format = 'privateVideofailure'
            isPrivateFailure = True

    filename = id + '.' + format

    isPrivateFailure = False
    if os.path.isfile('%s/%s' % (t_dir, filename))\
         and not overwritePhotos:
        print '%s already downloaded... continuing' % filename
        return
    if not isPrivateFailure:
        print 'Retrieving ' + source + ' as ' + filename
        offlickr.downloadURL(source, t_dir, filename, verbose=True)
        print 'Done downloading %s' % filename


def backupPhotos(
    threads,
    offlickr,
    target,
    hash_level,
    dateLo,
    dateHi,
    getPhotos,
    getVideosOnly,
    doNotRedownload,
    overwritePhotos,
    ):
    """Back photos up for a particular time range"""

    if dateHi == maxTime:
        t = time.time()
        print 'For incremental backups, the current time is %.0f' % t
        print "You can rerun the program with '-f %.0f'" % t

    photos = offlickr.getPhotoList(dateLo, dateHi)
    if photos == None:
        print 'No photos found'
        sys.exit(1)

    total = len(photos)
    print 'Backing up', total, 'photos'

    if threads > 1:
        concurrentThreads = threading.Semaphore(threads)
    i = 0
    for p in photos:
        i = i + 1
        pid = str(int(p['id']))  # Making sure we don't have weird things here
        if threads > 1:
            concurrentThreads.acquire()
            downloader = photoBackupThread(
                concurrentThreads,
                i,
                total,
                pid,
                p['title'],
                offlickr,
                target,
                hash_level,
                getPhotos,
                getVideosOnly,
                doNotRedownload,
                overwritePhotos,
                )
            downloader.start()
        else:
            backupPhoto(
                i,
                total,
                pid,
                p['title'],
                target,
                hash_level,
                offlickr,
                doNotRedownload,
                getPhotos,
                getVideosOnly,
                overwritePhotos,
                )


def backupLocation(
    threads,
    offlickr,
    target,
    hash_level,
    dateLo,
    dateHi,
    doNotRedownload,
    ):
    """Back photo locations up for a particular time range"""

    if dateHi == maxTime:
        t = time.time()
        print 'For incremental backups, the current time is %.0f' % t
        print "You can rerun the program with '-f %.0f'" % t

    photos = offlickr.getGeotaggedPhotoList(dateLo, dateHi)
    if photos == None:
        print 'No photos found'
        sys.exit(1)

    total = len(photos)
    print 'Backing up', total, 'photo locations'

    i = 0
    for p in photos:
        i = i + 1
        pid = str(int(p['id']))  # Making sure we don't have weird things here
        td = target_dir(target, hash_level, pid) + '/'
        if doNotRedownload and os.path.isfile(td + pid + '-location.xml'
                ) and os.path.isfile(td + pid
                 + '-location-permissions.xml'):
            print pid + ': Already there'
            continue
        location = offlickr.getPhotoLocation(pid)
        if location == None:
            print 'Failed!'
        else:
            fileWrite(offlickr.dryrun, target_dir(target, hash_level,
                      pid), pid + '-location.xml', location)
        locationPermission = offlickr.getPhotoLocationPermission(pid)
        if locationPermission == None:
            print 'Failed!'
        else:
            fileWrite(offlickr.dryrun, target_dir(target, hash_level,
                      pid), pid + '-location-permissions.xml',
                      locationPermission)


def backupPhotosets(threads, offlickr, getPhotos, getVideosOnly, target, hash_level, doNotRedownload, overwritePhotos):
    """Back photosets up"""

    photosets = offlickr.getPhotosetList()
    if photosets == None:
        print 'No photosets found'
        sys.exit(0)

    total = len(photosets)
    print 'Backing up', total, 'photosets'

    i = 0
    for p in photosets:
        i = i + 1
        pid = str(int(p['id']))  # Making sure we don't have weird things here
        print str(i) + '/' + str(total) + ': ' + pid + ': '\
             + p.title[0].text.encode('utf-8')

        # Get Metadata

        info = offlickr.getPhotosetInfo(pid,
                offlickr.fapi.photosets_getInfo)
        if info == None:
            print 'Failed!'
        else:
            fileWrite(offlickr.dryrun, target_dir(target + '/sets/' + pid, hash_level,
                      pid), 'set_' + pid + '_info.xml', info)
        photos = offlickr.getPhotosetInfo(pid,
                offlickr.fapi.photosets_getPhotos)
        if photos == None:
            print 'Failed!'
        else:
            fileWrite(offlickr.dryrun, target_dir(target + '/sets/' + pid, hash_level,
                      pid), 'set_' + pid + '_photos.xml', photos)
      
        photos = offlickr.getPhotosetPhotos(pid)
        if photos == None:
            print 'No photos found in this photoset'
        else:
           total = len(photos)
           print 'Backing up', total, 'photos'

           if threads > 1:
               concurrentThreads = threading.Semaphore(threads)
           i = 0
           for p in photos:
               i = i + 1
               photoid = str(int(p['id']))  # Making sure we don't have weird things here
               if threads > 1:
                   concurrentThreads.acquire()
                   downloader = photoBackupThread(
                       concurrentThreads,
                       i,
                       total,
                       photoid,
                       p['title'],
                       offlickr,
                       target + '/sets/' + pid + '/photos',
                       hash_level,
                       getPhotos,
                       getVideosOnly,
                       doNotRedownload,
                       overwritePhotos,
                       )
                   downloader.start()
               else:
                   backupPhoto(
                       i,
                       total,
                       photoid,
                       p['title'],
                       target + '/sets/' + pid + '/photos',
                       hash_level,
                       offlickr,
                       doNotRedownload,
                       getPhotos,
                       getVideosOnly,
                       overwritePhotos,
                       )


        # Do we want the picture too?


def target_dir(target, hash_level, id):
    dir = target
    i = 1
    while i <= hash_level:
        dir = dir + '/' + id[len(id) - i]
        i = i + 1
    return dir


def main():
    """Command-line interface"""

    # Default options

    flickrUserId = None
    dateLo = '1'
    dateHi = maxTime
    getPhotos = False
    getVideosOnly = False
    overwritePhotos = False
    doNotRedownload = False
    target = 'dst'
    photoLocations = False
    photosets = False
    verbose = False
    threads = 1
    httplib = None
    hash_level = 0
    dryrun = False

    # Parse command line

    try:
        (opts, args) = getopt.getopt(sys.argv[1:],
                'ehvponNLswf:t:d:i:c:l:', ['help'])
    except getopt.GetoptError:
        usage()
        sys.exit(2)
    for (o, a) in opts:
        if o in ('-h', '--help'):
            usage()
            sys.exit(0)
        if o == '-i':
            flickrUserId = a
        if o == '-p':
            getPhotos = True
        if o == '-e':
            getVideosOnly = True
        if o == '-o':
            overwritePhotos = True
        if o == '-n':
            doNotRedownload = True
        if o == '-L':
            photoLocations = True
        if o == '-s':
            photosets = True
        if o == '-f':
            dateLo = a
        if o == '-t':
            dateHi = a
        if o == '-d':
            target = a
        if o == '-w':
            httplib = 'wget'
        if o == '-c':
            threads = int(a)
        if o == '-l':
            hash_level = int(a)
        if o == '-N':
            dryrun = True
        if o == '-v':
            verbose = True

    # Check that we have a user id specified

    if flickrUserId == None:
        print 'You need to specify a Flickr Id'
        sys.exit(1)

    # Check that the target directory exists

    if not os.path.isdir(target):
        print target + ' is not a directory; please fix that.'
        sys.exit(1)

    offlickr = Offlickr(
        flickrAPIKey,
        flickrSecret,
        flickrUserId,
        httplib,
        dryrun,
        verbose,
        )

    if photosets:
        backupPhotosets(threads, offlickr, getPhotos, getVideosOnly, target, hash_level, doNotRedownload, overwritePhotos)
    elif photoLocations:
        backupLocation(
            threads,
            offlickr,
            target,
            hash_level,
            dateLo,
            dateHi,
            doNotRedownload,
            )
    else:
        backupPhotos(
            threads,
            offlickr,
            target,
            hash_level,
            dateLo,
            dateHi,
            getPhotos,
            getVideosOnly,
            doNotRedownload,
            overwritePhotos,
            )


if __name__ == '__main__':
    main()
