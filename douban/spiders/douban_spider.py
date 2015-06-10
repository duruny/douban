# -*- coding: utf-8 -*-
import re
import time
import logging
import MySQLdb
from datetime import datetime

from scrapy.contrib.spiders import CrawlSpider

from douban.local_settings import MYSQL_INFO

logging.basicConfig(
        filename = ('/tmp/douab.log'),
        level = logging.INFO,
        filemode = 'w',
        format = '[%(filename)s:%(lineno)d] %(asctime)s - %(levelname)s: %(message)s'
    )

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
        "http://www.douban.com/group/bjzft/",
        "http://www.douban.com/group/516673/"
    ]

    filter_title = [
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
        u'\u8D22\u4ED8\u901A', # 财付通
        u'\u5145\u8BDD\u8D39', # 充话费
        u'\u62C9\u5361\u62C9', # 拉卡拉
        u'\u62C9\u624B\u7F51', # 拉手网
        u'\u5FAE\u4FE1\u94B1\u5305', # 微信钱包
        u'\u4E34\u5E8A\u6D4B\u8BD5', # 临床测试
        u'\u6709\u507F\u8BBF\u8C08', # 有偿访谈
        u'\u86CB\u58F3\u516C\u5BD3', # 蛋壳公寓
        u'\u6700\u9760\u8C31\u7684\u79DF\u5BA2', # 最靠谱的租客
        u'\u6700\u5FEB\u7684\u627E\u623F\u795E\u5668', # 最快的找房神器
        u'\u5317\u4EAC\u79DF\u623F\u5B8C\u5168\u653B\u7565', # 北京租房完全攻略
        u'\u7F8E\u7F8E\u7684\u623F\u5B50\u7B49\u9760\u8C31\u7684\u4F60', # 美美的房子等靠谱的你
    ]

    filter_user = [
        u'88396311',
        u'127156047',
        u'53407894',
        u'75880277',
        u'126992246',
        u'101681408',
        u'72361353',
        u'75389696',
        u'75741951',
        u'75866361',
        u'75866661',
        u'73293339',
        u'75859992',
        u'42471058',
        u'42471301',
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

    def __should_continue(self, href, title, user_id, reply_count):

        for word in self.filter_title:
            if word in title:
                return True

        for user in self.filter_user:
            if user == user_id:
                return True

        if int(reply_count) > 50:
            return True

        return False

    def parse(self, response):
        for node in response.xpath("//table[@class='olt']//tr"):

            try:
                href = node.xpath(".//td[1]/a/@href").extract()
                title = node.xpath(".//td[1]/a/@title").extract()
                user_href = node.xpath(".//td[2]/a/@href").extract()
                user_name = node.xpath(".//td[2]/a/text()").extract()
                reply_count = node.xpath(".//td[3]/text()").extract()
                reply_time = node.xpath(".//td[4]/text()").extract()

                if not (href and title and reply_count and reply_time \
                    and user_href and user_name):
                    continue

                href = href[0]
                title = title[0]
                user_href = user_href[0]
                user_name = user_name[0]
                reply_count =reply_count[0]
                reply_time = reply_time[0]

                topic_id = self.__get_topic_id_from_url(href)
                user_id = self.__get_user_id_from_url(user_href)
                reply_time = self.__get_reply_time(reply_time)

                if not topic_id or not user_id or not reply_time \
                    or not reply_count or self.__should_continue(href, title,
                            user_id, reply_count):
                    continue

                topic_id = int(topic_id)
                reply_time = str(datetime.now().year) + '-' + reply_time
                timestamp = int(time.mktime(datetime.strptime(reply_time, "%Y-%m-%d %H:%M").timetuple()))

                con = MySQLdb.connect(
                        host = MYSQL_INFO['host'],
                        port = 3306,
                        user = MYSQL_INFO['user'],
                        passwd = MYSQL_INFO['passwd'],
                        db = MYSQL_INFO['db'],
                        charset = 'utf8'
                    )

                with con:
                    cur = con.cursor()
                    cur.execute("INSERT INTO %s \
                                    VALUES (%d, '%s', %d, '%s', '%s', '%s') \
                                ON DUPLICATE KEY UPDATE \
                                    timestamp=%d" %
                                (MYSQL_INFO['topic_table'], topic_id, title, timestamp, user_id, user_name, '', timestamp))
            except Exception as e:
                logging.error(e)
                continue
