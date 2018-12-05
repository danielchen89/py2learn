# -*- coding: utf-8 -*-
# flake8: noqa
from qiniu import Auth, put_file, etag
from qiniu import build_batch_delete, Auth, BucketManager
import qiniu.config
import sys
import logging
import os
import json
import MySQLdb
import time,datetime

"""
    #部署的包：
    pip install qiniu
    Centos7 安装 mysql-python
    #无mysql安装如下
    yum install mariadb-server mariadb  mariadb-devel 
    #有mysql 安装如下
    yum install MySQL-python

    #定时策略：一日一清
    0 6 * * * /usr/bin/python /script/qiniu_olddata_clean.py >> /script/qiniu_olddata_clean.log

    #安装策略：
    开发，测试，生产各部署一套
"""

logging.basicConfig(level=logging.INFO, stream=sys.stdout)

#需要填写你的 Access file_name 和 Secret file_name
access_key = 'xxxxxxxxxxxxxxxxx'
secret_key = 'xxxxxxxxxxxxxxxxxxx'
#构建鉴权对象
q = Auth(access_key, secret_key)
cleanDate = os.popen("date -d '(date +%Y%m%d) -7 days' +%Y-%m-%d").read().strip()

def mysqlQuery(dbhost,dbport,dbusername,dbpasswd,dbdatabase,bucket_name):
    # 打开数据库连接
    db = MySQLdb.connect(host=dbhost, port=dbport, user=dbusername, passwd=dbpasswd, charset='utf8')
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    # 使用execute方法执行SQL语句
    sql = "SELECT uin from %s.wechat;" % dbdatabase
    # 执行sql语句
    cursor.execute(sql)
    # 提交到数据库执行
    results = cursor.fetchall()
    # 创建dirList的目录
    uinList = []
    for row in results:
        uinList.append(str(row[0]))
    # 关闭数据库连接
    db.close()

    #需要列出的列表路径是uin+/db/为前缀的路径
    for uin in  uinList:
        uin_prefix=uin + "/db/"
        listAndCleanBucket(bucket_name,uin_prefix)
        # print(uin_prefix)


def delBucketFile(bucket_name,file_name):
    # 初始化BucketManager
    bucket = BucketManager(q)
    # 你要测试的空间， 并且这个key在你空间中存在
    bucket_name = bucket_name
    key = file_name
    # 删除bucket_name 中的文件 key
    ret, info = bucket.delete(bucket_name, key)
    print(info)
    assert ret == {}


#获取指定前缀文件列表
def listAndCleanBucket(bucket_name,uin_prefix):
    bucket = BucketManager(q)
    bucket_name = bucket_name
    # 前缀
    prefix = uin_prefix
    # 列举条目
    limit = 1000
    # 列举出除'/'的所有文件以及以'/'为分隔的所有前缀
    delimiter = None
    # 标记
    marker = None
    ret, eof, info = bucket.list(bucket_name, prefix, marker, limit, delimiter)

    for num in range(len(ret['items'])):
        fileTimeStamp = str(ret['items'][num]['putTime'])[0:10]
        timeArray = time.localtime(int(fileTimeStamp))
        #文件时间
        fileDate = time.strftime("%Y-%m-%d", timeArray)
        #文件路径
        fileDir = ret['items'][num]['key']
        print(num+1,fileDir,fileDate,cleanDate)
        # 如果file时间是七天前，删除
        if fileDate < cleanDate:
            delBucketFile(bucket_name, fileDir)

    assert len(ret.get('items')) is not None

def main():
    mysqlQuery('mysql的ip', 端口, '账号', '密码', '数据库名字','存储桶名字')
   
if __name__ == '__main__':
    main()

