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
from lxml import html

parser = argparse.ArgumentParser(description=u'投资界')
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
    default=u'./pedaily.xlsx',
    help=u'保存的excel文件路径')

data = {}
session = requests.Session()
task_queue = Queue.Queue()
max_page = None

def get(url, try_times=3):
    global session
    while try_times > 0:
        try_times -= 1
        try:
            r = session.get(url)
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
        u'投资时间',
        u'投资方',
        u'受资方',
        u'轮次',
        u'行业分类',
        u'金额',
        u'URL',
        u'案例介绍',
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

    data = None
    """
    保存数据的dict
    """

    task_queue = None
    """
    任务队列
    """

    def __init__(self, id):
        super(WorkerThread, self).__init__()
        self.id = id

    def log(self, msg):
        sys.stdout.write(
            (u"[线程#%s]%s\n" % (self.id, msg)).encode('utf-8'))

    def run(self):
        global task_queue
        global max_page
        while not task_queue.empty():
            try:
                task = task_queue.get_nowait()
                if task.page > max_page:
                    break
                self.log(u"负责抓取第%d页" % task.page)
                self.do_task(task)
            except Exception as e:
                self.log(u'抓取第%d页发生异常: %s' % (task.page, e.message))
                traceback.print_exc()

        self.log(u"退出")

    def do_task(self, task):
        global data
        global max_page
        url = u'http://zdb.pedaily.cn/inv/%d/' % task.page
        doc = get_doc(url)
        if doc is None:
            raise Exception(u'failed to get %s' % url)

        data[task.page] = []
        trs = doc.cssselect(u'body > div.content > div > div.box-fix-c > div.box.box-content > table > tr')[1:]
        if len(trs) == 0:
            max_page = task.page

        for tr in trs:
            detail_url = u'http://zdb.pedaily.cn' + tr.cssselect(u'td.td6 > a')[0].get('href')
            doc = get_doc(detail_url)
            if doc is None:
                self.log(u'failed to get detail: %s' % detail_url)
                continue

            entry = {
                u'投资时间':    doc.cssselect(u'body > div.content > div > div.box-fix-c.index-focus > div.news-show > div > p:nth-child(1)')[0].text_content()[5:],
                u'投资方':      doc.cssselect(u'body > div.content > div > div.box-fix-c.index-focus > div.news-show > div > p:nth-child(2)')[0].text_content()[6:],
                u'受资方':      doc.cssselect(u'body > div.content > div > div.box-fix-c.index-focus > div.news-show > div > p:nth-child(3)')[0].text_content()[6:],
                u'轮次':        doc.cssselect(u'body > div.content > div > div.box-fix-c.index-focus > div.news-show > div > p:nth-child(4)')[0].text_content()[5:],
                u'行业分类':    doc.cssselect(u'body > div.content > div > div.box-fix-c.index-focus > div.news-show > div > p:nth-child(5) > a:nth-child(2)')[0].text_content(),
                u'金额':        doc.cssselect(u'body > div.content > div > div.box-fix-c.index-focus > div.news-show > div > p:nth-child(6)')[0].text_content()[5:],
                u'URL':         detail_url,
                u'案例介绍':    doc.cssselect(u'body > div.content > div > div.box-fix-c.index-focus > div.news-show > div > p:nth-child(8)')[0].text_content(),
            }
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
    global max_page

    # 获取总页数，填充任务队列
    url = u'http://zdb.pedaily.cn/inv/'
    doc = get_doc(url)
    if doc is None:
        raise Exception(u'无法访问: %s' % url)
    a = doc.cssselect('body > div.content > div > div.box-fix-c > div.box.box-content > div.box-page > div > a')[-2]
    total_page = int(a.text)
    max_page = total_page
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
