# coding:utf-8
import json

import pymysql as pymysql
import requests


class Public:
    def __init__(self):
        pass

    def post_return(self, url, encoding='utf-8', data=None):
        try:
            r = requests.post(url, data)
            r.encoding = encoding
            return r.text
        except:
            return None

    def get_html_source(self, url):
        r = requests.get(url)
        r.encoding = 'utf-8'
        return r.text

    def save2file(self, content, filename):
        with open(filename, 'w+') as f:
            f.write(content)
        # print('{} is save to {}'.format(content, filename))

    def json2dict(self, data_str):
        dict_data = json.loads(data_str)
        return dict_data

    def save2mysql(self, host, database, user, password, sql, data, port=3306):
        db = pymysql.connect(host, user, password, database)
        cursor = db.cursor()
        try:
            cursor.executemany(sql, data)
            db.commit()
            # cursor.execute(sql)
        except Exception as error:
            print(error)
        finally:
            db.close()

    def mysql_exec(self, host, database, user, password, sql, port=3306):
        db = pymysql.connect(host, user, password, database)
        cursor = db.cursor()
        try:
            cursor.execute(sql)
            db.commit()
        except Exception as error:
            print(error)
        finally:
            db.close()


def get_mysql_conn(self, host, user, password, db, port=3306, charset='utf-8'):
    try:
        conn = pymysql.connect(host=host, port=port, user=user, passwd=password, db=db, charset=charset,
                               cursorclass=pymysql.cursors.DictCursor)
        return conn
    except Exception as error:
        print(error)
