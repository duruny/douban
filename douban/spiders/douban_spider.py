# -*- coding: utf-8 -*-
import re
import sqlite3
import MySQLdb
import time
from datetime import datetime

from scrapy.contrib.spiders import CrawlSpider

from douban.local_settings import MYSQL_INFO

class DoubanSpider(CrawlSpider):

    name = "douban"
    allowed_domains = ["douban.com"]
    start_urls = [
        "http://www.douban.com/group/465554/",
        "http://www.douban.com/group/beijingzufang/",
        "http://www.douban.com/group/26926/",
        "http://www.douban.com/group/zhufang/",
        "http://www.douban.com/group/549574/",
        "http://www.douban.com/group/257523/",
        "http://www.douban.com/group/opking/",
        "http://www.douban.com/group/279962/",
        "http://www.douban.com/group/sweethome/",
        "http://www.douban.com/group/252218/",
        "http://www.douban.com/group/bjzufang/",
        "http://www.douban.com/group/cbdrent/",
        "http://www.douban.com/group/xiaotanzi/",
        "http://www.douban.com/group/374051/",
        "http://www.douban.com/group/325060/",
        "http://www.douban.com/group/276176/",
        "http://www.douban.com/group/550436/",
        "http://www.douban.com/group/519274/",
        "http://www.douban.com/group/516673/"
    ]

    filter_word = [
        u'\u642C\u5BB6', # 搬家
        u'\u5E08\u5085', # 师傅
        u'\u8BDA\u62DB', # 诚招
        u'\u6C42\u79DF', # 求租
        u'\u5DF2\u79DF', # 已租
        u'\u6DD8\u5B9D', # 淘宝
        u'\u7F8E\u56E2', # 美团
        u'\u7CEF\u7C73', # 糯米
        u'\u5929\u732B', # 天猫
        u'\u652F\u4ED8\u5B9D', # 支付宝
        u'\u5B66\u82F1\u8BED', # 学英语
        u'\u6C42\u5408\u79DF', # 求合租
        u'\u6C42\u6574\u79DF', # 求整租
        u'\u5FAE\u4FE1\u94B1\u5305', # 微信钱包
        u'\u4E34\u5E8A\u6D4B\u8BD5', # 临床测试
        u'\u6709\u507F\u8BBF\u8C08', # 有偿访谈
        u'\u6700\u9760\u8C31\u7684\u79DF\u5BA2', # 最靠谱的租客
        u'\u6700\u5FEB\u7684\u627E\u623F\u795E\u5668', # 最快的找房神器
        u'\u5317\u4EAC\u79DF\u623F\u5B8C\u5168\u653B\u7565', # 北京租房完全攻略
        u'\u7F8E\u7F8E\u7684\u623F\u5B50\u7B49\u9760\u8C31\u7684\u4F60', # 美美的房子等靠谱的你
    ]

    def __get_user_id_from_url(self, href):
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

    def parse(self, response):
        for node in response.xpath("//table[@class='olt']//tr"):

            href = node.xpath(".//td[1]/a/@href").extract()
            title = node.xpath(".//td[1]/a/@title").extract()

            user_href = node.xpath(".//td[2]/a/@href").extract()
            user_name = node.xpath(".//td[2]/a/text()").extract()

            reply_count = node.xpath(".//td[3]/text()").extract()
            reply_time = node.xpath(".//td[4]/text()").extract()

            should_continue = False

            if href and title and reply_count and reply_time:
                href = href[0]
                title = title[0]

                user_href = user_href[0]
                user_name = user_name[0]

                reply_count =reply_count[0]
                reply_time = reply_time[0]

                for word in self.filter_word:
                    if word in title:
                        should_continue = True
                        break

                if should_continue:
                    continue

                if reply_count and int(reply_count) > 50:
                    continue

                topic_id = self.__get_topic_id_from_url(href)
                user_id = self.__get_user_id_from_url(user_href)
                if not topic_id:
                    continue

                # make sure 'reply_time' is in the form of '05-23 13:05'
                reply_time = self.__get_reply_time(reply_time)
                if not reply_time:
                    continue

                reply_time = str(datetime.now().year) + '-' + reply_time
                timestamp = time.mktime(datetime.strptime(reply_time, "%Y-%m-%d %H:%M").timetuple())

                # for sqlite3
                #con = sqlite3.connect('/home/lian/zufang.db')

                con = MySQLdb.connect(
                        host = MYSQL_INFO['host'],
                        port = 3306,
                        user = MYSQL_INFO['user'],
                        passwd = MYSQL_INFO['passwd'],
                        db = 'mysite',
                        charset = 'utf8'
                    )

                with con:
                    cur = con.cursor()
                    try:
                        # for sqlite3
                        #cur.execute("INSERT OR IGNORE INTO zufang VALUES(%d, '%s', '%s', '%s', %d)" %
                        #        (int(topic_id), title, href, reply_time, timestamp))

                        cur.execute("INSERT INTO %s \
                                        VALUES (%d, '%s', '%s', '%s', %d) \
                                    ON DUPLICATE KEY UPDATE \
                                        reply_time='%s',\
                                        timestamp=%d" %
                                    (MYSQL_INFO['topic_table'], int(topic_id), title, href, reply_time, int(timestamp), reply_time, int(timestamp)))
                    except Exception as e:
                        continue
            else:
                continue
