#from pyslideshare import pyslideshare
from pyslideshare2 import pyslideshare
# Have all the secure keys in a file called localsettings.py
from localsettings import username, password, api_key, secret_key, proxy
    
obj = pyslideshare.pyslideshare(locals(), verbose=False)
#slide = pyslideshare.get
#slide = obj.get_slideshow_by_user(username_for='mos_ru')
#print slide
print obj.get_slideshow(slideshow_id=25438871)
obj.download_slideshow(slideshow_id=25438871, username=username, password=password)
