#!/usr/bin/env python
# coding: utf8
"""
Youtube
"""
import csv
import os, os.path
import gdata.youtube
import gdata.youtube.service
from gdata.youtube import Statistics
from pytube import YouTube
import json
import datetime


DEV_KEY = 'AI39si6pQhOiyPAkNDHJHUg4hrqmGb0W-rL53Rf95gUcLkv3au1aIRW1MKYao6U9Kq0V1ZSEYphUnAWkgZMYVkay6jsJncxg3Q'
CATALOG_FILE = 'accounts.csv'
MAX_ENTRIES = 50



class DataExtractor:
    """Data extractor from government twitters"""

    def __init__(self):
        self.yt_service = gdata.youtube.service.YouTubeService(additional_headers={'GData-Version': '1', 'X-GData-Key' : 'key=%s' % DEV_KEY})
        self.yt = YouTube()
        pass

    def get_videofeed(self, username, start_index):
        print '-- Extracting for %s and index %d' %(username, start_index)
        uri = 'http://gdata.youtube.com/feeds/api/users/%s/uploads?max-results=%d&start-index=%d' % (username, MAX_ENTRIES, start_index)
        feed = self.yt_service.GetYouTubeVideoFeed(uri)
        return feed, len(feed.entry)

    def get_commentsfeed(self, url, id):
        comment_feed = self.yt_service.GetYouTubeVideoCommentFeed(uri=url)
        results = []
        n = 0
        print 'Processing', url

        try:
            while comment_feed:
                print 'Working on comments'
                if len(comment_feed.entry) == 0:
                    print 'No comments, break'
                    break
                for comment_entry in comment_feed.entry:
                    n += 1
                    if n % 1 == 0:
                        print 'processed', n, 'from', url
#                    print comment_entry
                    results.append({'video_id' : id,
                                    'comment_id' : comment_entry.id.text,
                                    'author_name' : comment_entry.author[0].name.text,
                                    'author_url' : comment_entry.author[0].uri.text,
                                    'title' : comment_entry.title.text,
                                    'published' : comment_entry.published.text,
                                    'updated' : comment_entry.updated.text,
                                    'content' : comment_entry.content.text},
                                   )
                comment_feed = self.yt_service.Query(comment_feed.GetNextLink().href)
        except:
            pass
        return results



    def extract_videofeed_by_username(self, username):
        """Extracts youtube video feed by username"""
        now = datetime.datetime.now()
        results = {'total' : 0, 'archive_date' : now.isoformat(), 'items' : []}
        feed, num_items = self.get_videofeed(username, 1)
#        return
        for entry in feed.entry:
            data = self.get_item_data(username, entry)
            results['items'].append(data)
            print '-', data['title']
        if num_items != MAX_ENTRIES:
            results['total'] = len(results['items'])
            return results
        else:
            start_index = num_items
            while num_items == MAX_ENTRIES:
                feed, num_items = self.get_videofeed(username, start_index)
                start_index += num_items
                for entry in feed.entry:
                    data = self.get_item_data(username, entry)
                    results['items'].append(data)

                    print '-', data['title']
            results['total'] = len(results['items'])
            return results

    def get_item_data(self, username, entry):
        result = {}
        result['id'] = entry.id.text.rsplit('/', 1)[-1]
        result['title'] =  entry.media.title.text
        result['description'] =  entry.media.description.text
        result['published'] = entry.published.text
        result['category'] = entry.media.category[0].text
        result['tags'] = entry.media.keywords.text
        result['watch_page'] = entry.media.player.url
        result['flash_url'] = entry.GetSwfUrl()
        result['duration'] = entry.media.duration.seconds
        result['view_count'] = entry.statistics.view_count
        result['view_rating'] = entry.rating.average if entry.rating else ""
        result['thumbnails'] = []
        for thumbnail in entry.media.thumbnail:
            result['thumbnails'].append(thumbnail.url)
        if entry.comments:
            result['comments'] = {'url': entry.comments.feed_link[0].href,
                                  'count_hint' : entry.comments.feed_link[0].count_hint}
        result['filename'] = entry.id.text.rsplit('/', 1)[-1]
        return result

    def extract_video(self, username, id, url):
        filename = id
        filepath = 'raw/%s/' %(username, )
        if not os.path.exists(filepath):
            os.makedirs(filepath)
        if not os.path.exists(filepath + filename + '.mp4'):
            print 'Saving video', filepath + filename
            self.yt.url = url
            self.yt.filename = filename
            video = self.yt.get('mp4', '360p')
            video.download(filepath)
            print 'Video saved as', filename

    def extract_all_comments(self):
        now = datetime.datetime.now()
        reader = csv.DictReader(open(CATALOG_FILE, 'r'), delimiter="\t")
        for item in reader:
            filename = 'data/%s_comments.json' %(item['username'])
            print 'Processing', item['username']
            if os.path.exists(filename): continue

            results = {'total' : 0, 'archive_date' : now.isoformat(), 'items' : []}
            js = json.load(open('data/' + item['username'] + '.json', 'r'))
            for record in js['items']:
                if record.has_key('comments'):
                    url = record['comments']['url']
                    comments = self.get_commentsfeed(url, record['id'])
                    results['items'].extend(comments)
            results['total'] = len(results['items'])
            f = open(filename, 'w')
            f.write(json.dumps(results, sort_keys = False, indent = 4))
            f.close()

    def extract_all_raw(self):
        reader = csv.DictReader(open(CATALOG_FILE, 'r'), delimiter="\t")
        for item in reader:
            print 'Processing', item['username']
            filename = 'data/%s.json' %(item['username'])
            if os.path.exists(filename): continue
            data = self.extract_videofeed_by_username(item['username'])
            f = open(filename, 'w')
            f.write(json.dumps(data, sort_keys = False, indent = 4))
            f.close()


    def calc_all(self):
        reader = csv.DictReader(open(CATALOG_FILE, 'r'), delimiter="\t")
        total_video = 0
        total_comm = 0
        for item in reader:
            js = json.load(open('data/' + item['username'] + '.json', 'r'))
            total_video += int(js['total'])
            jsc = json.load(open('data/' + item['username'] + '_comments.json', 'r'))
            total_comm += int(jsc['total'])
        print total_comm, total_video

    def extract_all_video(self):
        reader = csv.DictReader(open(CATALOG_FILE, 'r'), delimiter="\t")
        for item in reader:
            if item['username'] != 'AccountsChamber': continue
            js = json.load(open('data/' + item['username'] + '.json', 'r'))
            for record in js['items']:
                id = record['id']
                url = record['watch_page']
                username = item['username']
                self.extract_video(username, id, url)


if __name__ == "__main__":
    ext = DataExtractor()
    ext.extract_all_raw()
    ext.extract_all_comments()
    ext.calc_all()
#    ext.extract_all_video()


