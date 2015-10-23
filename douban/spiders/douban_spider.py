# -*- coding: utf-8 -*-
import re
import time
import logging
import MySQLdb
from datetime import datetime

from scrapy.contrib.spiders import CrawlSpider, Rule
from scrapy.contrib.linkextractors import LinkExtractor

from douban.local_settings import MYSQL_INFO, START_URLS, \
    FILTER_TITLE

logging.basicConfig(
        filename = ('/tmp/crawl_douban.log'),
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
        m =  re.search("^http://www.douban.com/people/([^/]+)/$", href)
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

    def __should_continue(self, title, reply_count=0):
        for word in FILTER_TITLE:
            if word in title:
                return True

        if reply_count > 100:
            return True

        return False

    def parse_start_url(self, response):
        for node in response.xpath("//table[@class='olt']//tr"):
            try:
                href = node.xpath(".//td[1]/a/@href").extract()
                title = node.xpath(".//td[1]/a/@title").extract()
                people_href = node.xpath(".//td[2]/a/@href").extract()
                people_name = node.xpath(".//td[2]/a/text()").extract()
                reply_count = node.xpath(".//td[3]/text()").extract()
                reply_time = node.xpath(".//td[4]/text()").extract()

                if not (href and title and reply_count and reply_time \
                    and people_href and people_name):
                    continue

                href = href[0]
                title = title[0]
                people_href = people_href[0]
                people_name = people_name[0]
                reply_count = int(reply_count[0])
                reply_time = reply_time[0]

                topic_id = self.__get_topic_id_from_url(href)
                people_id = self.__get_people_id_from_url(people_href)
                reply_time = self.__get_reply_time(reply_time)

                if not topic_id or not people_id or not reply_time \
                    or not reply_count or self.__should_continue(title, reply_count):
                    continue

                topic_id = int(topic_id)
                reply_time = str(datetime.now().year) + '-' + reply_time
                reply_timestamp = int(time.mktime(datetime.strptime(reply_time, "%Y-%m-%d %H:%M").timetuple()))

                con = self.con
                with con:
                    cur = con.cursor()
                    cur.execute("INSERT INTO %s (id, title, people_id, people_name, reply_timestamp, reply_count)\
                                 VALUES (%d, '%s', '%s', '%s', %d, %d) \
                                 ON DUPLICATE KEY UPDATE \
                                 reply_timestamp = %d, \
                                 reply_count = %d" %
                                 (MYSQL_INFO['topic_table'],
                                  topic_id, title, people_id, people_name, reply_timestamp, reply_count,
                                  reply_timestamp, reply_count))
            except Exception as e:
                logging.error(e)
                continue
        return

    def parse_item(self, response):
        try:
            url = response.url
            create_time = response.xpath("//span[@class='color-green']/text()").extract()
            create_time = create_time[0]

            topic_id = self.__get_topic_id_from_url(url)
            timestamp = time.mktime(datetime.strptime(create_time, "%Y-%m-%d %H:%M:%S").timetuple())

            if not topic_id:
                return

            topic_id = int(topic_id)
            timestamp = int(timestamp)
            con = self.con
            with con:
                cur = con.cursor()
                cur.execute("UPDATE %s SET timestamp=%d WHERE id=%d" %
                             (MYSQL_INFO['topic_table'], timestamp, topic_id))
        except Exception as e:
            logging.error(e)
            return
