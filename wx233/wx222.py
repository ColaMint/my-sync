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
                return r.content
            except Exception:
                pass
        return None

    def post(self, url, data, try_times=3):
        global session
        while try_times > 0:
            try_times -= 1
            try:
                r = session.post(url, data=data)
                return r.content
            except Exception:
                pass
        return None

    def run(self):
        global task_queue
        while not task_queue.empty():
            try:
                task = task_queue.get_nowait()
                self.log(u"负责抓取 class_id: %s, cert: %s" % (task.class_id, task.cert))
                self.do_task(task)
            except Exception as e:
                print e
        self.log(u"退出")

    def do_task(self, task):
        global data
        data['task.class_id'] = {}
        data['task.class_id']['cert'] = task.cert
        # 所有试卷
        papers_url = 'http://wx.233.com/tiku/exam/%s-0-0-0-0-0' % task.class_id
        papers = self.fetch_papers(papers_url)
        data['task.class_id']['papers'] = papers

        # 科目练习
        subjects_url = 'http://wx.233.com/tiku/chapter/%s' % task.class_id
        subjects = self.fetch_subjects(subjects_url)
        data['task.class_id']['subjects'] = subjects

    def fetch_papers(self, url):
        """
        获取试卷
        """
        papers = {}
        for p in range(200):
            purl = url + 'p=%s' % p
            self.log(u'获取试卷: %s' % purl)
            body = self.get(purl)
            doc = html.fromstring(body)
            lis = doc.cssselect('body > div.le-pracon > div.le-pracleft > div.le-prabg.pracl-dalist > ul > li')
            if lis is None:
                break
            for li in lis:
                a = li.cssselect('div > h3 > a')[0]
                span = li.cssselect('div > p > span:nth-child(1)')[0]

                paper_name = a.text.strip()
                paper_id = a.get('href').split('/')[-1]
                exam = re.search(u'总题：(.*) 题', span.text).group(1)
                paper = {'paper_id': paper_id, 'paper_name': paper_name, 'dan_xiang_xuan_ze_ti': [], 'duo_xiang_xuan_ze_ti': [], 'an_li_fen_xi_ti': []}

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
                        paper['dan_xiang_xuan_ze_ti'].append(self.parse_question(q))
                if duo_xiang_xuan_ze_ti:
                    answer_url = 'http://wx.233.com/tiku/exam/getNewsList?paperId=%s&rulesId=%s&_=%s' % (paper_id, duo_xiang_xuan_ze_ti, int(time.time() * 1000))
                    body = self.get(answer_url)
                    questions = json.loads(body)['list']['questions']
                    for q in questions:
                        paper['duo_xiang_xuan_ze_ti'].append(self.parse_question(q))
                if an_li_fen_xi_ti:
                    answer_url = 'http://wx.233.com/tiku/exam/getNewsList?paperId=%s&rulesId=%s&_=%s' % (paper_id, an_li_fen_xi_ti, int(time.time() * 1000))
                    body = self.get(answer_url)
                    questions = json.loads(body)['list']['questions']
                    for q in questions:
                        paper['an_li_fen_xi_ti'].append(self.parse_question(q))

                papers[paper_id] = paper

        return papers

    def fetch_subjects(self, url):
        """
        获取科目章节练习
        """
        self.log(u'获取科目章节练习: %s' % url)
        subjects = {}
        # 获取科目
        body = self.get(url)
        doc = html.fromstring(body)
        dds = doc.cssselect('body > div.le-pracon > div.le-prabg.pracl-nav > div > dl:nth-child(1) > dd')
        for dd in dds:
            a = dd.cssselect('a')[0]
            subject = a.text.strip()
            subject_url = 'http://wx.233.com' + a.get('href')
            subjects[subject] = []

            chapters = {}
            body = self.get(subject_url)
            doc = html.fromstring(body)
            trs = doc.cssselect('body > div.le-pracon > div.le-prabg.le-question > div.lo-tablecon > table > tr')
            for tr in trs:
                chapter_id = int(tr.get('data-chapterid'))
                pid = int(tr.get('data-pid'))
                exam_num = int(tr.get('data-examnum'))

                if pid == 0:
                    # 大章
                    chapter_name = tr.cssselect('a')[0].text.strip()
                    self.log(u'%s %s' % (subject, chapter_name))
                    chapter_exams = self.fetch_chapter_or_section_exams(chapter_id, exam_num) if exam_num > 0 else None
                    chapters[chapter_id]= {'chapter_id': chapter_id, 'chapter_name': chapter_name, 'chapter_exams': chapter_exams, 'sections': []}
                else:
                    # 小节
                    section_name = tr.cssselect('a')[0].text.strip()
                    self.log(u'%s %s' % (subject, section_name))
                    section_exams = self.fetch_chapter_or_section_exams(chapter_id, exam_num) if exam_num > 0 else None
                    section = {
                        'section_id': chapter_id,
                        'section_name': section_name,
                        'section_exams': section_exams
                    }
                    chapters[pid]['sections'].append(section)

            for _, chapter in sorted(chapters.iteritems(), key=lambda d:d[0]):
                subjects[subject].append(chapter)

        return subjects

    def fetch_chapter_or_section_exams(self, id, exam_num):
        exams = []
        # 生成练习
        start_url = 'http://wx.233.com/tiku/chapter/getChapterQuestion?chapterId=%s&questionFilter=do&questionType=-1&questionYear=-1&questionNum=%s&interfaceAction=fast&_=%s' % (id, exam_num, int(time.time() * 1000))
        body = self.get(start_url)
        log_id = json.loads(body)['list']['logId']

        # 暂停练习
        pause_url = 'http://wx.233.com/tiku/exam/pauseExercise?typeId=%s&pauseType=1&fromType=2&_=%s' % (log_id, int(time.time() * 1000))
        body = self.get(pause_url)

        # 获取练习和答案
        exam_url = 'http://wx.233.com/tiku/exam/getExerciseNewsList?typeId=%s&fromType=2&completedTf=1&_=%s' % (log_id, int(time.time() * 1000))
        body = self.get(exam_url)
        questions = json.loads(body)['list']['questions']
        for q in questions:
            exams.append(self.parse_question(q))

        return exams

    def parse_question(self, q):
        return {
            'exam_id': q['examId'],
            'exam_type': q['examType'],
            'question': q['question'],
            'option_list': q['optionList'],
            'answer': q['answer'],
            'analysis': q['analysis'],
        }

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