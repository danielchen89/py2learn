#!/usr/bin/env python
# -*- coding:utf-8 -*-
# crontab每天执行:  0 0 * * * /usr/bin/python /script/logclean.py

import os, time, datetime

lists = []


def del_log(data_dir):
    # 清理时间为七天之前，打印时间戳
    clean_date = os.popen("date -d '(date +%Y%m%d) -7 days' +%Y-%m-%d").read().strip()
    # 打印不需要删除的文件列表，写入/tmp/slist中
    os.system("cd %s && ls -al | awk '{print $11}' | grep -v ^$ > /tmp/slist" % data_dir)
    for filename in os.listdir(data_dir):
        filepath = os.path.join(data_dir, filename)

        if os.path.isfile(filepath):
            # 打印需要清理的文件的时间戳
            file_date = os.popen("stat %s | sed -n '7p' | awk '{print $2}'" % filepath).read().strip()
            # 以log.为分隔符对需要清理的日志进行筛选
            log_len = len(filename.split('log.'))

            # 要是需要清理文件的时间戳比7天前还早，并且符合筛选的日志条件
            if file_date < clean_date and log_len > 1:
                lists.append(filepath)

    # 从列表中剔除特殊不需要删除文件
    with open("/tmp/slist") as sfilename:
        # 读取出的每行要加strip()去掉换行符
        for filename in sfilename:
            sfilepath = os.path.join(data_dir, filename.strip())
            if sfilepath in lists:
                lists.remove(sfilepath)
    # 获取最终需要删除的列表，删除其中的元素
    for list in lists:
        os.remove(list)
    print ("delete filename:", lists)


def main():
    # 列出需要清理的日志目录
    log_dirs = ['/data/logs/im']
    for log_dir in log_dirs:
        del_log(log_dir)


if __name__ == "__main__":
    main()
