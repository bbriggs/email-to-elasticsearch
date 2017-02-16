from __future__ import print_function
import os
import sys
import re
import threading
from queue import Queue
import json
from email.parser import BytesParser
from elasticsearch import Elasticsearch

class Message(object):
    def __init__(self):
        return

    def parse(self,message):
        '''
        Break email into a json object with fields for each data point in the email
        Optionally insert them into ElasticSearch
        '''
        parsed = self.__to_json(self.__read(message))

        decoded = json.loads(parsed)

        return parsed

    def __to_json(self,message):
        jsonized = {}
        for key in message:
            jsonized[key] = message[key]
        jsonized['raw'] = str(message)
        return json.dumps(jsonized)

    def __read(self,message):
        with open(message,'rb') as f:
            msg = BytesParser().parse(f)
            return msg

    def insert(self,index,data,es):
        return(es.create(index=index,body=data,doc_type='email'))


class EmailParser(threading.Thread):
    '''
    Extension of Thread class
    Create message objects by reading emails off disk
    Call the message's insert() method to put it in the ES DB
    '''
    def __init__(self,tasks):
        super().__init__(target=self.run)
        self.tasks = tasks
        self.start()
        return

    def run(self):
        while True:
            email,es = self.tasks.get()
            try:
                message = Message()
                data = message.parse(email)
                print(message.insert('emails',data,es))
            except Exception as e:
                print(e)
            finally:
                self.tasks.task_done()


class EmailParserPool():
    '''
    Thread pool to manage EmailParser instances
    '''
    def __init__(self,num_threads):
        self.tasks = Queue(num_threads)
        for i in range(num_threads):
            EmailParser(self.tasks)
        return

    def add_task(self,pathname,es):
        self.tasks.put((pathname,es))

    def map(self, func, args_list):
        for args in args_list:
            self.add_task(func,args)

    def wait_completion(self):
        self.tasks.join()




class Crawler(object):
    def __init__(self,es,extension=".eml",threads=50):
        # Elasticsearch connection
        self.es = es
        self.extension = extension
        self.threads = threads
        # A handy switch for debugging
        self.count = 0
        self.pool = EmailParserPool(threads)
        return
    def crawl(self,directory):
        '''
        Recursively crawl a directory and find anything ending in the extension
        Add an EmailParser thread to work queue
        '''
        for f in os.listdir(directory):
            pathname = os.path.join(directory,f)
            if os.path.isdir(pathname):
                self.crawl(pathname)
            elif os.path.isfile(pathname):
                if f.lower().endswith(self.extension):
                    self.pool.add_task(pathname,self.es)

