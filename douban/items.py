# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy

class DoubanItem(scrapy.Item):
    # define the fields for your item here like:
    topic_id = scrapy.Field()
    href = scrapy.Field()
    title = scrapy.Field()
    reply_count = scrapy.Field()
    reply_time = scrapy.Field() # will be showed at web page
    timestamp = scrapy.Field() # reply_time's timestamp, will be used for sort title
