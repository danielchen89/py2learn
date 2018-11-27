# -*- coding=utf-8
from qcloud_cos import CosConfig
from qcloud_cos import CosS3Client
import sys
import logging
import os
import json

logging.basicConfig(level=logging.INFO, stream=sys.stdout)

secret_id = 'xxxxxxxxxxx'  # 替换为用户的 secretId
secret_key = 'xxxxxxxxxxxxxxx'  # 替换为用户的 secretKey
region = 'ap-shanghai'  # 替换为用户的 Region
token = None  # 使用临时密钥需要传入 Token，默认为空，可不填
scheme = 'https'  # 指定使用 http/https 协议来访问 COS，默认为 https，可不填
config = CosConfig(Region=region, SecretId=secret_id, SecretKey=secret_key, Token=token, Scheme=scheme)
# 2. 获取客户端对象
client = CosS3Client(config)

"""
    文件流 简单上传
    （备用接口）
"""


def fileUpload(bucket_name, file_name):
    with open(file_name, 'rb') as fp:
        response = client.put_object(
            Bucket=bucket_name,  # Bucket由bucket_name-appid组成
            Body=fp,
            Key=file_name,
            StorageClass='STANDARD',
            ContentType='text/html; charset=utf-8'
        )
        print(response['ETag'])


# 根据文件大小自动选择简单上传或分块上传，分块上传具备断点续传功能。
def seniorFileUpload(bucket_name, file_name):
    response = client.upload_file(
        Bucket=bucket_name,
        LocalFilePath=file_name,
        Key=file_name,
        PartSize=10,
        MAXThread=10
    )
    print(response['ETag'])


def listBucket(bucket_name, data_dir):
    response = client.list_objects(
        Bucket=bucket_name,
        #       MaxKeys=1000,
    )
    # json格式化输出结果
    jsonstr = json.dumps(response, sort_keys=True, indent=4, separators=(',', ':'))
    jsonDe = json.loads(jsonstr)
    # 初始化上传失败的列表
    failUploadLists = []
    # 初始化本地上传路径列表
    localFileLists = []
    # 初始化远程上传路径列表
    uploadLists = []
    # 初始化传一半文件列表（标准文件大小为128M，文件可能由于是最新生产的小于128M，这类文件第二天要补传）
    halfUploadLists = []

    for num in range(len(jsonDe['Contents'])):
        fileDir = jsonDe['Contents'][num]['Key']
        fileSize = jsonDe['Contents'][num]['Size']
        absFileDir = "/" + fileDir
        uploadLists.append(absFileDir)
        # 打印所有的列表
        #         print(num+1,absFileDir,fileSize)

        # 判断file的大小如果为空，打印为空的列表
        if int(fileSize) == 0:
            failUploadLists.append(absFileDir)
        # 判断文件小于128M，打印文件列表
        if int(fileSize) < 134000000:
            halfUploadLists.append(absFileDir)

    # 列出本地指定路径下所有文件
    for file_name in os.listdir(data_dir):
        filepath = os.path.join(data_dir, file_name)
        localFileLists.append(filepath)

    # 移除有毒文件
    localFileLists.remove('/data/im/group_index')
    localFileLists.remove('/data/im/peer_index')
    halfUploadLists.remove('/data/im/cursor')

    # 1.对空列表文件进行重传
    for file_name in failUploadLists:
        seniorFileUpload(bucket_name, file_name)
    # 2.对本地文件不在腾讯云存储桶列表中的进行重新上传
    for file_name in localFileLists:
        if file_name not in uploadLists:
            seniorFileUpload(bucket_name, file_name)
    # 3.对之前传一半的文件进行重新上传
    for file_name in halfUploadLists:
        seniorFileUpload(bucket_name, file_name)


def main():
    listBucket('xxxxxxxxxxxxxxxxxxxxx', '/data/im')


if __name__ == '__main__':
    main()