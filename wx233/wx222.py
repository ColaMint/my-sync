#!/usr/bin/python
# -*- coding:utf-8 -*-

import argparse
import json
import sys
import requests
import threading
import time
import re
from lxml import html
from Queue import Queue

parser = argparse.ArgumentParser(description='网校233')
parser.add_argument(
    '-t',
    type=int,
    required=True,
    dest='thread_count',
    help='运行的线程数目')
parser.add_argument(
    '-c',
    required=True,
    dest='cookie',
    help='cookie')
parser.add_argument(
    '-o',
    required=False,
    dest='output_file',
    default='./wx233.json',
    help='保存的json文件')

task_queue = Queue()
data = {}
session = requests.Session()

class Task(object):

    class_id = None
    """
    http://wx.233.com/tiku/exam/{class_id}
    """

    cert    = None
    """
    资格证书中文名称
    """

    def __init__(self, class_id, cert):
        self.class_id = class_id
        self.cert = cert

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
            (u"%s%s\n" %
             (u"[线程#%s]" %
              self.id,
              msg)).encode('utf-8'))

    def get(self, url, try_times=3):
        global session
        while try_times > 0:
            try_times -= 1
            try:
                r = session.get(url)
                return r.content.decode('utf-8')
            except Exception:
                pass
        return None

    def post(self, url, data, try_times=3):
        global session
        while try_times > 0:
            try_times -= 1
            try:
                r = session.post(url, data=data)
                return r.content.decode('utf-8')
            except Exception:
                pass
        return None

    def run(self):
        global task_queue
        while not task_queue.empty():
            try:
                task = task_queue.get_nowait()
            except Exception:
                break
            self.log(u"负责抓取 class_id: %s, cert: %s" % (task.class_id, task.cert))
            self.do_task(task)
        self.log(u"退出")

    def do_task(self, task):
        url = 'http://wx.233.com/tiku/exam/%s' % task.class_id
        body = self.get(url)
        doc = html.fromstring(body)
        mo_ni_kao_chang = doc.cssselect('body > div.nav-box1 > ul > li > a:nth-child(2)')[0]
        mo_ni_kao_chang_url = 'http://wx.233.com%s' % mo_ni_kao_chang.get('href')
        mo_ni_kao_chang_data = self.fetch_mo_ni_kao_chang(mo_ni_kao_chang_url)

    def fetch_mo_ni_kao_chang(self, url):
        """
        模拟考场
        """
        global session
        for p in range(200):
            purl = url + 'p=%s' % p
            body = self.get(purl)
            doc = html.fromstring(body)
            lis = doc.cssselect('body > div.le-pracon > div.le-pracleft > div.le-prabg.pracl-dalist > ul > li')
            if lis is None:
                break
            for li in lis:
                a = li.cssselect('div > h3 > a')[0]
                span = li.cssselect('div > p > span:nth-child(1)')[0]

                paper = a.text.strip()
                paper_id = a.get('href').split('/')[-1]
                exam = re.search(u'总题：(.*) 题', span.text).group(1)

                # 开始考试
                pay_paper_url = 'http://wx.233.com/tiku/Exam/PayPaper/'
                body = self.post(pay_paper_url, {'paperId': paper_id, 'modelStr': 'mk', 'exam': exam})

                do_url = 'http://wx.233.com/tiku/exam/do/%s' % paper_id
                body = self.get(do_url)
                doc = html.fromstring(body)
                page_rules_a = doc.cssselect('#page-rules > a')
                dan_xiang_xuan_ze_ti = None
                duo_xiang_xuan_ze_ti = None
                an_li_fen_xi_ti = None
                for a in page_rules_a:
                    if a.text.startswith(u'单项'):
                        dan_xiang_xuan_ze_ti = a.get('data-value')
                    elif a.text.startswith(u'多项'):
                        duo_xiang_xuan_ze_ti = a.get('data-value')
                    elif a.text.startswith(u'案例'):
                        an_li_fen_xi_ti = a.get('data-value')

                # 提交试卷
                pause_exam_url = 'http://wx.233.com/tiku/exam/pauseExam?paperId=%s&pauseType=1&modelStr=mk&_=%s' % (paper_id, int(time.time() * 1000))
                self.get(pause_exam_url)

                # 获取试题及答案
                if dan_xiang_xuan_ze_ti:
                    answer_url = 'http://wx.233.com/tiku/exam/getNewsList?paperId=%s&rulesId=%s&_=%s' % (paper_id, dan_xiang_xuan_ze_ti, int(time.time() * 1000))
                    body = self.get(answer_url)
                    questions = json.loads(body)['list']['questions']
                    for q in questions:
                        question = {
                            'exam_id': q['examId'],
                            'question': q['question'],
                            'option_list': q['optionList'],
                            'answer': q['answer'],
                            'analysis': q['analysis'],
                        }
                if duo_xiang_xuan_ze_ti:
                    answer_url = 'http://wx.233.com/tiku/exam/getNewsList?paperId=%s&rulesId=%s&_=%s' % (paper_id, duo_xiang_xuan_ze_ti, int(time.time() * 1000))
                    body = self.get(answer_url)
                if an_li_fen_xi_ti:
                    answer_url = 'http://wx.233.com/tiku/exam/getNewsList?paperId=%s&rulesId=%s&_=%s' % (paper_id, an_li_fen_xi_ti, int(time.time() * 1000))
                    body = self.get(answer_url)



def parse_cookie(cookie):
    cookies = {}
    for pair in cookie.split('; '):
        index = pair.index('=')
        k = pair[:index]
        v = pair[index + 1:]
        cookies[k] = v
    return requests.utils.cookiejar_from_dict(cookies)

def main():
    # 解析命令行参数
    args = parser.parse_args()
    thread_count = args.thread_count
    output_file = args.output_file
    cookies = parse_cookie(args.cookie)
    global task_queue
    global data
    global session

    session.cookies = cookies
    url = 'http://wx.233.com/uc/class'
    r = session.get(url)
    if r.status_code != 200:
        return

    doc = html.fromstring(r.content.decode('utf-8'))
    elements = doc.cssselect('[data-classid]')
    for e in elements:
        class_id = e.get('data-classid')
        cert = e.text
        task_queue.put(Task(class_id=class_id, cert=cert))

    # 启动线程
    for i in range(thread_count):
        t = WorkerThread(id=i)
        t.daemon = True
        t.start()

    # 等待所有线程结束
    # 为了使主线程能接收signal，采用轮询的方式
    while threading.activeCount() > 1:
        time.sleep(1)

    # 保存数据到文件
    with open(output_file, 'w') as f:
        f.write(json.dumps(data))

if __name__ == '__main__':
    main()
