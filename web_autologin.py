# -*- coding:utf-8 -*-
import time
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
"""
    自动登陆七牛云
	准备：
	windows安装好python环境,下载好chromedriver即可
    selenium查找元素文档地址: https://selenium-python-zh.readthedocs.io/en/latest/locating-elements.html
"""
options = webdriver.ChromeOptions()
driver=webdriver.Chrome(executable_path="D:\huzan\codingcode-windows-python2\chromedriver", chrome_options=options)
driver.get("https://sso.qiniu.com/")
#selenium反应很慢，最好等待一下
time.sleep(1)
driver.find_element_by_id('email').send_keys("xxxxxxxxxxxxxxxx")
driver.find_element_by_id('password').send_keys("xxxxxxxxxxxxxxxxx")
driver.find_element_by_id('login-button').click()
time.sleep(1)
# 点击对象存储
driver.get("https://portal.qiniu.com/bucket/asset/index")
