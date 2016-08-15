#!/usr/bin/python
# -*- coding:utf-8 -*-

import argparse
import json
import sys
import requests
import threading
import time
import openpyxl
import Queue
import traceback
import re
from lxml import html

parser = argparse.ArgumentParser(description=u'IT桔子')
parser.add_argument(
    u'-c',
    type=int,
    required=True,
    dest=u'thread_count',
    help=u'运行的线程数目')
parser.add_argument(
    u'-o',
    required=False,
    dest=u'output_file',
    default=u'./itjuzi.xlsx',
    help=u'保存的excel文件路径')

data = {}
headers = {u'User-Agent': u'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36'}
session = requests.Session()
task_queue = Queue.Queue()

def get(url, try_times=3):
    global session
    while try_times > 0:
        try_times -= 1
        try:
            r = session.get(url, headers=headers)
            return r.content.decode('utf-8')
        except Exception:
            pass
    return None

def get_doc(url, try_times=3):
    content = get(url, try_times)
    return html.fromstring(content) if content else None

def get_json(url, try_times=3):
    content = get(url, try_times)
    return json.loads(content) if content else None

def save_data_to_excel(data, filename):
    """
    把数据文件保存到excel
    """

    columns = [
        u'时间',
        u'公司',
        u'行业',
        u'地区',
        u'轮次',
        u'融资额',
        u'投资方',
    ]
    columns_cnt = len(columns)
    wb = openpyxl.Workbook()
    ws = wb.active

    row = 1
    for i in range(columns_cnt):
        ws.cell(row=row, column=i+1, value=columns[i])

    for page, page_data in data.iteritems():
        for entry in page_data:
            row += 1
            for i in range(columns_cnt):
                ws.cell(row=row, column=i+1, value=entry[columns[i]])

    wb.save(filename)

class Task(object):

    page = None
    """
    页码
    """

    def __init__(self, page):
        self.page = page

class WorkerThread(threading.Thread):

    id = None
    """
    该线程的编号
    """

    def __init__(self, id):
        super(WorkerThread, self).__init__()
        self.id = id

    def log(self, msg):
        sys.stdout.write(
            (u"[线程#%s]%s\n" % (self.id, msg)).encode('utf-8'))

    def run(self):
        global task_queue
        while not task_queue.empty():
            try:
                task = task_queue.get_nowait()
                self.log(u"负责抓取第%d页" % task.page)
                self.do_task(task)
            except Exception as e:
                self.log(u'抓取第%d页发生异常: %s' % (task.page, e.message))
                traceback.print_exc()

        self.log(u"退出")

    def do_task(self, task):
        global data
        url = u'https://www.itjuzi.com/investevents?page=%d' % task.page
        doc = get_doc(url)
        if doc is None:
            raise Exception(u'failed to get %s' % url)

        data[task.page] = []
        lis = doc.cssselect(u'body > div.thewrap > div:nth-child(3) > div.main > div:nth-child(3) > div > div:nth-child(1) > ul:nth-child(2) > li')
        for li in lis:
            entry = {
                u'时间':    li.cssselect(u'i')[0].cssselect(u'span')[0].text_content().strip(),
                u'公司':    li.cssselect(u'p.title > a > span')[0].text_content().strip(),
                u'行业':    li.cssselect(u'span.tags > a')[0].text_content().strip(),
                u'地区':    li.cssselect(u'span.loca > a')[0].text_content().strip(),
                u'轮次':    li.cssselect(u'i')[3].cssselect(u'span')[0].text_content().strip(),
                u'融资额':  li.cssselect(u'i')[4].text_content().strip(),
                u'投资方':  re.sub(u'\s+', u' / ', li.cssselect(u'i')[5].cssselect(u'span')[0].text_content().strip()),
            }
            print entry
            data[task.page].append(entry)
        self.log(u'第%d页 %d条记录' % (task.page, len(data[task.page])))

def main():
    # 解析命令行参数
    args = parser.parse_args()
    thread_count = args.thread_count
    output_file = args.output_file

    #  全局变量
    global data
    global task_queue

    # 获取总页数，填充任务队列
    url = u'https://www.itjuzi.com/investevents'
    doc = get_doc(url)
    if doc is None:
        raise Exception(u'无法访问: %s' % url)
    total_page = int(doc.cssselect('a[data-ci-pagination-page]')[-1].get(u'data-ci-pagination-page'))
    for i in range(total_page):
        task_queue.put(Task(page=i+1))

    #  启动线程
    for i in range(thread_count):
        t = WorkerThread(id=i)
        t.daemon = True
        t.start()

    # 等待所有线程结束
    # 为了使主线程能接收signal，采用轮询的方式
    while threading.activeCount() > 1:
        time.sleep(1)

    # 保存数据到excel
    save_data_to_excel(data, output_file)

if __name__ == '__main__':
    main()
