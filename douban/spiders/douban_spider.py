# -*- coding: utf-8 -*-
import re
import time
import logging
import MySQLdb
from datetime import datetime

from scrapy.contrib.spiders import CrawlSpider, Rule
from scrapy.contrib.linkextractors import LinkExtractor

from douban.local_settings import MYSQL_INFO, START_URLS, \
    FILTER_TITLE, FILTER_USER

logging.basicConfig(
        filename = ('/tmp/douab.log'),
        level = logging.INFO,
        filemode = 'w',
        format = '[%(filename)s:%(lineno)d] %(asctime)s - %(levelname)s: %(message)s')

class DoubanSpider(CrawlSpider):

    name = "douban"
    allowed_domains = ["douban.com"]
    start_urls = START_URLS
    rules = [Rule(LinkExtractor(allow=['/topic/\d+']), 'parse_item')]

    con = MySQLdb.connect(
            host = MYSQL_INFO['host'],
            port = 3306,
            user = MYSQL_INFO['user'],
            passwd = MYSQL_INFO['passwd'],
            db = MYSQL_INFO['db'],
            charset = 'utf8')

    def __get_people_id_from_url(self, href):
        m =  re.search("^http://www.douban.com/group/people/([^/]+)/$", href)
        if m:
            return m.group(1)
        else:
            return None

    def __get_topic_id_from_url(self, href):
        m =  re.search("^http://www.douban.com/group/topic/([^/]+)/$", href)
        if m:
            return m.group(1)
        else:
            return None

    def __get_reply_time(self, reply_time):
        m = re.search("^(\d{2}-\d{2} \d{2}:\d{2})$", reply_time)
        if m:
            return m.group(1)
        else:
            return None

    def __should_continue(self, title, user_id, reply_count=0):

        for word in FILTER_TITLE:
            if word in title:
                return True

        for user in FILTER_USER:
            if user == user_id:
                return True

        if reply_count > 50:
            return True

        return False

    def parse_item(self, response):
        try:
            url = response.url
            title = response.xpath("//h1/text()").extract()
            people_url = response.xpath("//span[@class='from']/a/@href").extract()
            people_name = response.xpath("//span[@class='from']/a/text()").extract()
            create_time = response.xpath("//span[@class='color-green']/text()").extract()
            content = response.xpath("//div[@class='topic-content']/p/text()").extract()
            reply_count = response.xpath("count(//span[@class='pubtime'])").extract()

            reply_count = int(reply_count[0][0])
            if reply_count > 0:
                index = reply_count - 1
                last_reply_time = response.xpath("//span[@class='pubtime']/text()")[index].extract()
            else:
                last_reply_time = None

            if not (url and title and people_url and people_name and create_time and content):
                return

            title = title[0]
            people_url = people_url[0]
            people_name = people_name[0]
            create_time = create_time[0]
            content = content[0]

            topic_id = self.__get_topic_id_from_url(url)
            people_id = self.__get_people_id_from_url(people_url)
            timestamp = time.mktime(datetime.strptime(create_time, "%Y-%m-%d %H:%M:%S").timetuple())

            if not topic_id or not people_id or self.__should_continue(title, people_id):
                return

            topic_id = int(topic_id)
            timestamp = int(timestamp)
            if last_reply_time:
                reply_timestamp = time.mktime(datetime.strptime(last_reply_time, "%Y-%m-%d %H:%M:%S").timetuple())
                reply_timestamp = int(reply_timestamp)
            else:
                reply_timestamp = 0

            con = self.con
            with con:
                cur = con.cursor()
                cur.execute(
                            "INSERT INTO %s VALUES (%d, '%s', '%s', '%s', '%s', %d, %d, %d) \
                             ON DUPLICATE KEY UPDATE \
                                 reply_timestamp = %d, \
                                 reply_count = %d" % (MYSQL_INFO['topic_info_table'], topic_id, title,
                                 people_id, people_name, content, timestamp, reply_timestamp, reply_count,
                                 reply_timestamp, reply_count)
                           )
        except Exception as e:
            logging.error(e)
            return
