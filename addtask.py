# -*- coding: utf-8 -*-
"""
Created on Fri Aug 10 17:38:59 2018
定时检测延迟或者采集漏的数据，并将延迟的数据插入库中（采集、底层）
@author: Lina
"""

import requests
from lxml import etree
import json
import pymysql
import hashlib
import time
import threading
from datetime import datetime
from threading import Timer


#请求的参数
def getarg():
    #底层接口
    home_url="http://hd.weibo.yunrunyuqing.com:38015/web/search/common/weibo/select?token="
    #请求token
    token="7f2a7d48-23ae-4d2f-b20c-7c5ebfb47d18"
    #请求的方式id_array
    id_array="&id_array="
    #条数
    rows="&rows=200"
    #开始时间
    starttime=""
    #结束时间
    endtime=""
    # print(home_url+token+id_array)
    return home_url+token+rows+id_array

#数据库连接
def connectiondb():
    mysql_server='localhost'
    name='root'
    password='123456'
    mysql_db='weibo_auto'
    db=pymysql.connect(mysql_server,name,password,mysql_db)
    return db

#关键字
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
        # print(str_word) #test
        add.append(list_pop)     
    return  add
def getfind_word(db):
    list_word=find_word(db)
    # print(list_word) 
    str_word=','.join(list_word)
    # print(str_word) 
    return str_word
#数据库中找到底层没有的连接
def query_id(db):
    cursor=db.cursor()
    query_id="SELECT weibo_bottom.BID,weibo_all.aUrl,weibo_all.AID from weibo_all LEFT JOIN weibo_bottom on weibo_all.AID=weibo_bottom.BID WHERE weibo_bottom.BID is NULL AND weibo_all.`statuss`=0"
    cursor.execute(query_id)
    db.commit()
    content=[]
    data=cursor.fetchall()
    for d in data:
        str_d=str(tuple(d))#从数据库中取出是tuple
        str_dd=str_d[14:51] #截取其中的url
        # print(str_dd,end=',')
        if isinstance(str_dd,str):   #字符串转bytes
            newurl=str_dd.encode("utf-8")
            # print(newurl)
        md = hashlib.md5()
        md.update(newurl)
        str_ddd=md.hexdigest()
        # print(str_ddd,end=',') 
        content.append(str_ddd)
    return content 

def query_id2(db):
    cursor=db.cursor()
    query_id2="SELECT weibo_scrapy.SID,weibo_all.aUrl,weibo_all.AID from weibo_all LEFT JOIN weibo_scrapy on weibo_all.AID=weibo_scrapy.SID WHERE weibo_scrapy.SID is NULL AND weibo_all.`status`=0"
    cursor.execute(query_id2)
    db.commit()
    lose2=[]
    data=cursor.fetchall()
    for d in data:
        str_d=str(tuple(d))#从数据库中取出是tuple
        str_dd=str_d[14:51] #截取其中的url
        # print(str_dd,end=',')
        if isinstance(str_dd,str):   #字符串转bytes
            newurl=str_dd.encode("utf-8")
            # print(newurl)
        md = hashlib.md5()
        md.update(newurl)
        str_ddd=md.hexdigest()
        # print(str_ddd,end=',') 
        lose2.append(str_ddd)
    return lose2                  

#拼接数据，数据格式的一个处理，第一种
def getlist_url(db):
    list_url=query_id(db)
    str_url=','.join(list_url)
    return str_url
#第二种
def getlist_url2(db):
    list_url2=query_id(db)
    str_url2=','.join(list_url2)
    return str_url2

#请求1
def urlrequest(db):
    
    if len(getlist_url(db))==0:
        print('采集库中没有缺漏值')
    else:
        url=getarg()+getlist_url(db)
    # print (getarg())
    # print(getlist_url(db))
        print(url)
        url_result=requests.post(url).json()
        print(url_result.status_code)
        print (url_result['total'])
        if url_result['total']==0:
            print('采集库中没有值')
        else:
            
            for i in range(len(getlist_url(db))):

                dict=url_result['results'][i]
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
                timeArray=time.localtime(Time)
                Timetime=time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
                #关键字
                Keywords=dict['Keywords']
                print('{} {} {} {} {} {} {} {}\n'.format(ID,UID,URL,BlogID,AddonTime,AddtimeTime,Timetime,Keywords) )
                # 数据库操作
                cursor=db.cursor()
                cursor.execute(
                    "INSERT INTO weibo_bottom(BID,UID,bURL,BlogID,AddOn,AddTime,Time,Keywords) values (%s,%s,%s,%s,%s,%s,%s,%s)",
                    (str(ID),str(UID),str(URL),str(BlogID),str(AddonTime),str(AddtimeTime),str(Timetime),str(Keywords)))
                db.commit()
                if i >len(getlist_url(db)):
                    break
        
#请求2
def urlrequest2(db):
    url2=getarg()+getlist_url2(db)+getfind_word(db)
    # print(url2)
    url_result2=requests.post(url2).json()

    print (url_result2['total'])
    if url_result2['total']==0:
        print('底层库中没有值')
    else:
            
        for i in range(len(getlist_url2(db))):

            dict=url_result2['results'][i]
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
            timeArray=time.localtime(Time)
            Timetime=time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
            #关键字
            Keywords=dict['Keywords']
            print('{} {} {} {} {} {} {} {}\n'.format(ID,UID,URL,BlogID,AddonTime,AddtimeTime,Timetime,Keywords) )
            # 数据库操作
            cursor=db.cursor()
            cursor.execute(
                "INSERT INTO weibo_scrapy(SID,UID,sURL,BlogID,AddOn,AddTime,Time,Keywords) values (%s,%s,%s,%s,%s,%s,%s,%s)",
                (str(ID),str(UID),str(URL),str(BlogID),str(AddonTime),str(AddtimeTime),str(Timetime),str(Keywords)))
            db.commit()
            if i >len(getlist_url(db)):
                break
    
def main():
    db=connectiondb()
    while 1:
        urlrequest(db)
        urlrequest2(db)
        time.sleep(60)

    

    
if __name__ == '__main__':
    main()


