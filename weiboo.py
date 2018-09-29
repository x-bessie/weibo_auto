# -*- coding: utf-8 -*-
"""
Created on  Aug 3  08:56:45 2018

@author: Lina
"""
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support import expected_conditions as EC
import selenium.webdriver.support.ui as ui
from selenium.webdriver.common.action_chains import ActionChains
from time import sleep
from random import choice
import re
import os
from requests import Session 
import time 
from lxml import etree
import hashlib
import json
import requests
import pymysql

browser = webdriver.Chrome(executable_path='C:/Users/15631/Anaconda3/Scripts/chromedriver.exe') 
wait = ui.WebDriverWait(browser,10)

browser.get("http://s.weibo.com/")

#底层接口
home_url=''
#请求token
token=''
#请求的方式id_array
id_array='&id_array='
#条数
rows='&rows=50'
#开始时间
starttime='&starttime='
#结束时间
endtime='&endtim='
#搜索关键字keywords
word='&word='

#登陆
def login(username,password):
    try:
        wait.until(lambda browser: browser.find_element_by_xpath("//a[@node-type='loginBtn']"))
        browser.find_element_by_xpath("//a[@node-type='loginBtn']").click()

        wait.until(lambda browser: browser.find_element_by_xpath("//input[@name='username']"))
        user=browser.find_element_by_xpath("//input[@name='username']")
        user.clear()
        user.send_keys(username)
        time.sleep(1)

        psw=browser.find_element_by_xpath("//input[@name='password']")
        psw.clear()
        psw.send_keys(password)
        time.sleep(1)

        browser.find_element_by_xpath("//div[6]/a").click()
        time.sleep(3)
    except TimeoutException:
        login(username,password)
  #搜索  
#关键词的搜索
def search(searchWord):
    try:
        wait.until(lambda browser: browser.find_element_by_class_name("gn_name"))
    
        inputBtn = browser.find_element_by_class_name("searchInp_form")
        inputBtn.clear()
        inputBtn.send_keys(searchWord.strip().encode('gbk').decode("gbk"))
        browser.find_element_by_class_name('searchBtn').click()
    except TimeoutException:
        search(searchWord)
    # wait.until(lambda browser: browser.find_element_by_class_name("search_num"))
# 采集推文的链接
def geturl():
    
    wait.until(lambda browser: browser.find_element_by_class_name("W_pages")) #等加载
    time.sleep(3)
    body=browser.page_source  
    html=etree.HTML(body)   #获取网页源码并解析
    content=[]
    
    urls=html.xpath("//dl/div/div[@class='content clearfix']/div[@class='feed_from W_textb']/a[1]/@href")
    # Time=html.xpath("//div[@class='content clearfix']/div[@class='feed_from W_textb']/a[1]/@title")
    # str_Time=','.join(Time)
    print(len(urls))
    #这里开始弄一个MD5获取url之后的格式处理
    for url in urls:
        db=connectdb()
        weibo_url=url[0:32]
        weibo_urls='http:'+weibo_url
        # print(weibo_urls)

        if isinstance(weibo_urls,str):   #字符串转bytes
            byweibourl=weibo_urls.encode("utf-8")
            # print(byweibourl)
        md = hashlib.md5()
        md.update(byweibourl)
        aaa=md.hexdigest()
        # print(aaa,end=',')
        # print(type(aaa))
        cursor=db.cursor()
        sql_url="INSERT INTO weibo_all(AID,aURL) values(%s,%s)"
        cursor.execute(sql_url,(str(aaa),str(weibo_urls)))    
        db.commit()
        content.append(aaa) 
        # print(content)
    return content
# 翻页
def nextPages():
    # 等待下一页的出现，等待页面加载出下一页的类才执行动作
    wait.until(lambda browser: browser.find_element_by_class_name("W_pages"))
    if browser.find_element_by_class_name("W_pages")!=None:
        nums = len(browser.find_elements_by_xpath("//div[@class='layer_menu_list W_scroll']/ul/li"))#页面li的个数
        # print(nums)
        pg=browser.find_element_by_xpath("//div[@class='layer_menu_list W_scroll']/ul/li[%d]/a" %nums) 
        nx=browser.find_element(By.XPATH,'//a[text()="下一页"]')  #下一页,在第二页之后，就不是这个地址了，坑。找文本定位才是正解
        
        browser.execute_script("window.scrollTo(0,document.body.scrollHeight)")  #滑动到页面的底部的下一页位置,执行行为链
        ActionChains(browser).move_to_element(pg).click(nx).perform()

#数据库 
def connectdb():
    mysql_server='your sql_url'
    name='sql_name'
    password='your password'
    mysql_db='your db'
    db=pymysql.connect(mysql_server,name,password,mysql_db)
    return db 
#数据库中取关键字,不稳定，不能用先
def find_word(db):
    cursor=db.cursor()
    search_words="SELECT * from keyword ORDER BY id "
    cursor.execute(search_words)
    db.commit()
    add=[]
    words=cursor.fetchall()
    # str_word=list(tuple(words))
    # num=len(words)
    for i in words:
        str_word=list(tuple(i))
        list_pop=str_word.pop(1) #将数据库中sele出来的值做一个pop输出
        # print(str_word)
        add.append(list_pop)     
    return  add
#关键字的转格式
def getfind_word(db):
    list_word=find_word(db)
    # print(list_word) 
    str_word=','.join(list_word)
    # print(str_word) 
    return str_word
#关闭数据库
def closedb(db):
    db.close()

def main():
    
#连接数据库
    db=connectdb()
    # num=len(browser.find_elements_by_xpath("//div[@class='layer_menu_list W_scroll']/ul/li"))   #获取页数 
#登陆信息，在这里填入登陆信息
    login("",'')  
#搜索关键字    
    searchword="杭州"
    search(searchword)      
    # geturl() #test
    gettext =[]
    # getlist_url(db)   
#搜索页码的范围，可以设置
    for i in range(0,5):
        gettext=gettext +geturl()
        sleep(choice([1,2,3,4,5]))  
        nextPages()
        #url转md5之后的str格式
    str_gettext=','.join(gettext)
    # print(str_gettext)
#请求的json
    #对底层的请求处理，加了关键词
    getapi=home_url+token+id_array+str_gettext+rows+word+searchword
    # print(getapi)
   
    # print(postappi.status_code)
    time.sleep(5)
    #对采集的处理，不加关键词
    getscrapy=home_url+token+id_array+str_gettext+rows
    # browser.get(getapi)
    dict_result1=requests.post(getapi).json()
    dict_result2=requests.post(getscrapy).json()
    # querydb(db)
    # print(len(gettext))  #url的条数
#对请求的数据的筛选，时间是时间戳的形式，并且对比接口中的数据(底层)
    try:
        for i in range(len(gettext)):
        
            dict=dict_result1['results'][i]
            ID=dict['ID']
            #博主的ID 
            UID=dict['UID'] 
            #博文ID
            BlogID=dict['BlogID'] 
            #推文链接
            URL=dict['Url']
            #采集时间
            AddOn=dict['AddOn']
            Addon_stamp=float(AddOn/1000)
            AddArray=time.localtime(Addon_stamp)
            AddonTime=time.strftime("%Y-%m-%d %H:%M:%S", AddArray)
            #入库时间 
            AddTime=dict['AddTime']
            Addon_stamp=float(AddTime/1000)
            AddTimeArray=time.localtime(Addon_stamp)
            AddtimeTime=time.strftime("%Y-%m-%d %H:%M:%S", AddTimeArray)
            #推文发布时间 
            Time=dict['Time'] 
            # Time_stamp=float(Time/1000)
            timeArray=time.localtime(Time)
            Timetime=time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
            #关键字
            Keywords=dict['Keywords']
       
            # print('{} {} {} {} {} {} {} {}\n'.format(ID,UID,URL,BlogID,AddonTime,AddtimeTime,Timetime) )
    # 数据库操作
            cursor=db.cursor()
            cursor.execute(
                "INSERT INTO weibo_bottom(BID,UID,bURL,BlogID,AddOn,AddTime,Time,Keywords) values (%s,%s,%s,%s,%s,%s,%s,%s)",
                (str(ID),str(UID),str(URL),str(BlogID),str(AddonTime),str(AddtimeTime),str(Timetime),str(Keywords)))
            db.commit()
            if i >len(gettext):
                break
#采集的处理
    finally:
        for i in range(len(gettext)):
        
            dict=dict_result2['results'][i]
            ID=dict['ID']
            #博主的ID 
            UID=dict['UID'] 
            #博文ID
            BlogID=dict['BlogID'] 
            #推文链接
            URL=dict['Url']
            #采集时间
            AddOn=dict['AddOn']
            Addon_stamp=float(AddOn/1000)
            AddArray=time.localtime(Addon_stamp)
            AddonTime=time.strftime("%Y-%m-%d %H:%M:%S", AddArray)
            #入库时间 
            AddTime=dict['AddTime']
            Addon_stamp=float(AddTime/1000)
            AddTimeArray=time.localtime(Addon_stamp)
            AddtimeTime=time.strftime("%Y-%m-%d %H:%M:%S", AddTimeArray)
            #推文发布时间 
            Time=dict['Time'] 
            # Time_stamp=float(Time/1000)
            timeArray=time.localtime(Time)
            Timetime=time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
            #关键字
            Keywords=dict['Keywords']
       
            # print('{} {} {} {} {} {} {} {}\n'.format(ID,UID,URL,BlogID,AddonTime,AddtimeTime,Timetime,Keywords) )
        # 数据库操作
            cursor=db.cursor()
            cursor.execute(
                "INSERT INTO weibo_scrapy(SID,UID,sURL,BlogID,AddOn,AddTime,Time,Keywords) values (%s,%s,%s,%s,%s,%s,%s,%s)",
                (str(ID),str(UID),str(URL),str(BlogID),str(AddonTime),str(AddtimeTime),str(Timetime),str(Keywords)))
            db.commit()
            if i >len(gettext):
                break
    
    
if __name__ == '__main__':
    main()
 
    
   