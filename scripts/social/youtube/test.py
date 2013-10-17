#!/usr/bin/env python
import sys
import gdata.youtube
import gdata.youtube.service
from gdata.youtube import Statistics

DEV_KEY = 'AI39si6pQhOiyPAkNDHJHUg4hrqmGb0W-rL53Rf95gUcLkv3au1aIRW1MKYao6U9Kq0V1ZSEYphUnAWkgZMYVkay6jsJncxg3Q'

# youtubeService = gdata.youtube.service.YouTubeService(additional_headers={'GData-Version': '1'}) 
# entry = youtubeService.GetYouTubeVideoEntry(video_id=b"FgSW_hrvdKo")

def GetAndPrintUserUploads(username):
    yt_service = gdata.youtube.service.YouTubeService(additional_headers={'GData-Version': '1'})
    uri = 'http://gdata.youtube.com/feeds/api/users/%s/uploads' % username
    PrintVideoFeed(yt_service.GetYouTubeVideoFeed(uri))

def PrintVideoFeed(feed):
    for entry in feed.entry:
        PrintEntryDetails(entry)

def PrintEntryDetails(entry):
    print entry
    print 'Video title: %s' % entry.media.title.text
    print 'Video published on: %s ' % entry.published.text
    print 'Video description: %s' % entry.media.description.text
    print 'Video category: %s' % entry.media.category[0].text
    print 'Video tags: %s' % entry.media.keywords.text
    print 'Video watch page: %s' % entry.media.player.url
    print 'Video flash player URL: %s' % entry.GetSwfUrl()
    print 'Video duration: %s' % entry.media.duration.seconds

    # non entry.media attributes
    print 'Video view count: %s' % entry.statistics.view_count
    if entry.rating:
       print 'Video rating: %s' % entry.rating.average
       print entry.rating.num_raters
    else:
       print 'Video rating: None'

    # show thumbnails
    for thumbnail in entry.media.thumbnail:
        print 'Thumbnail url: %s' % thumbnail.url

GetAndPrintUserUploads(sys.argv[1])
