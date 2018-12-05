# -*- coding=utf-8
from qcloud_cos import CosConfig
from qcloud_cos import CosS3Client
import sys
import logging
import os
import json
import MySQLdb

"""
    #部署的包：
    pip install -U cos-python-sdk-v5
    Centos7 安装 mysql-python
    #无mysql安装如下
    yum install mariadb-server mariadb  mariadb-devel 
    #有mysql 安装如下
    yum install MySQL-python

    #定时策略：一日一清
    0 2 * * * /usr/bin/python /script/cos_olddata_clean.py >> /script/cos_olddata_clean.log

    #安装策略：
    开发、测试、生产环境各部署一套
"""

logging.basicConfig(level=logging.INFO, stream=sys.stdout)

secret_id = 'xxxxxxxxxxxxxxxxxxxx'  # 替换为用户的 secretId
secret_key = 'xxxxxxxxxxxxxxxxxx'  # 替换为用户的 secretKey
region = 'ap-shanghai'  # 替换为用户的 Region
token = None  # 使用临时密钥需要传入 Token，默认为空，可不填
scheme = 'https'  # 指定使用 http/https 协议来访问 COS，默认为 https，可不填
config = CosConfig(Region=region, SecretId=secret_id, SecretKey=secret_key, Token=token, Scheme=scheme)
# 2. 获取客户端对象
client = CosS3Client(config)

def  mysqlQuery(dbhost,dbport,dbusername,dbpasswd,dbdatabase,bucket_name):
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

#删除文件
def delBucketFile(bucket_name,file_name):
    response = client.delete_object(
        Bucket=bucket_name,
        Key=file_name
    )

#列出并且操作桶内对象
def listAndCleanBucket(bucket_name,uin_prefix):
    #列出初始化的分片
    response = client.list_objects(
        Bucket=bucket_name,
        Prefix=uin_prefix,
    )

    #格式化json字符串
    jsonstr = json.dumps(response, sort_keys=True, indent=4, separators=(',', ':'))
    jsonDe = json.loads(jsonstr)
    if('Contents' in jsonDe):
        for num in range(len(jsonDe['Contents'])):
            fileDir = jsonDe['Contents'][num]['Key']
            fileSize = jsonDe['Contents'][num]['Size']
            #原始得到的fileDate是 u'2018-11-16'的格式的，加上encode('ascii')去掉u,得到'2018-11-16'格式的日期
            fileDate = jsonDe['Contents'][num]['LastModified'].split('T')[0].encode('ascii')
            cleanDate = os.popen("date -d '(date +%Y%m%d) -7 days' +%Y-%m-%d").read().strip()
            # absFileDir = "/" + fileDir
            # 打印所有的列表
            print(num + 1, fileDir, fileDate, cleanDate)
            # #如果file时间是七天前，删除
            if fileDate < cleanDate:
                delBucketFile(bucket_name,fileDir)

def main():
    mysqlQuery('mysql的ip', 端口, '账号', '密码', '数据库名字','存储桶名字')

if __name__ == '__main__':
    main()