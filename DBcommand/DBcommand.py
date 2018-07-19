#!/usr/bin/env python
#-*- coding:utf-8 -*-

import pymysql
import pandas as pd
from DBUtils.PooledDB import PooledDB
from sqlalchemy import create_engine
import os, time


class mysql2pd(object):
    def __init__(self,host,port,db,user,pwd,retry=3):
        '''
        :param host: 主机号ip
        :param port: 端口号
        :param db: 数据库
        :param user: 用户名
        :param pwd: 密码
        :param retry: 最大重新连接次数
        '''
        try:
            self.pool = PooledDB(pymysql,5,host=str(host),user=user,
                                 passwd=str(pwd),db=str(db),port=int(port),
                                 charset='utf8')
        except Exception as e:
            if retry > 0:
                retry -= 1
                time.sleep(10)
                mysql2pd(host,port,db,user,pwd,retry)
            else:
                raise e

        self.db = db
        self.user = user
        self.host = host
        self.pwd = pwd
        self.port = port

    def close(self):
        self.pool.close()

    def doget(self, sql):
        '''
        用于执行查询sql语句
        :param sql: 查询语句
        :return: dataframe形式的查询结果
        '''
        self.cxn = self.pool.connection()
        self.cursor = self.cxn.cursor()
        try:
            res = pd.read_sql(sql, self.cxn)
        except Exception as e:
            raise e
        return res

    def dopost(self, sql):
        '''
        用于执行对数据库改动的sql语句
        :param sql: 增删改的sql语句
        :return: 执行结果
        '''
        self.cxn = self.pool.connection()
        self.cursor = self.cxn.cursor()
        res = False
        try:
            self.cursor.execute(sql)
            self.cxn.commit()
            res = True
            print("执行成功：",sql)
        except Exception as e:
            print("执行失败：",sql)
            print("失败原因：")
            print(e)
            self.cxn.rollback()
        return res

    def showtables(self,keyword=None,showpars=False):
        '''
        显示数据库中的表
        :param keywords: 表名关键词
        :param showpars: 是否显示表的所有信息，若为否则只显示表名
        :return: 查询结果
        '''
        self.cxn = self.pool.connection()
        self.cursor = self.cxn.cursor()
        if not showpars:
            obj = 'table_name'
        else:
            obj = '*'
        sql = 'select '+obj+' from information_scheme'
        if keyword:
            sql += "where table_name like %"+keyword+"%"
        try:
            res = pd.read_sql(sql, self.cxn)
        except Exception as e:
            raise e
        return res

    def getdata(self,table,pars=None,tjs=None,blimit=None,elimit=None):
        '''
        从数据库中取出数据放到dataframe中
        :param table: 数据源表
        :param pars: list类型，列出想要提取的字段名，若为空则查询所有字段
        :param tjs: list类型，列出匹配的条件
        :param blimit: 数据行数最小值限制
        :param elimit: 数据行数最大值限制
        :return: dataframe类型查询结果
        '''
        self.cxn = self.pool.connection()
        self.cursor = self.cxn.cursor()
        if pars == None:
            items = '*'
        else:
            items = ','.join(pars)
        sql = 'select '+items+' from '+table
        if blimit != None or elimit != None:
            sql += ' limit '
            if blimit != None and elimit != None:
                sql += str(int(blimit)-1)+','+str(int(elimit)-int(blimit))
            elif elimit != None:
                sql += str(elimit)
            else:
                sql_count = "select table_rows from information_schema.table " \
                            "where table_name = '"+table+"'"
                self.cursor.execute(sql_count)
                n = self.cursor.fetchone()[0]
                sql += str(int(blimit)-1)+','+str(n-int(blimit))
        if tjs != None:
            if sql.find('where') != -1:
                sql = sql.replace('where','where '+' and '.join(tjs)+'and')
            else:
                sql += 'where '+' and '.join(tjs)
        try:
            res = pd.read_sql(sql, self.cxn)
        except Exception as e:
            raise e
        return res

    def addone(self,table,values,keys=None):
        '''
        增加一条记录
        :param table: 目标表
        :param values: list类型，要插入的值
        :param keys: list类型，字段名
        :return: 执行结果
        '''
        #self.cxn = self.pool.connection()
        #self.cursor = self.cxn.cursor()
        sql_insert = 'insert into '+table+''
        for i in range(0,len(values)):
            if not str(values[i]).isdigit() and values[i][0] !="'":
                values[i] = "'"+str(values[i])+"'"
        if keys != None:
            sql_insert += "("+",".join([str(key) for key in keys])+")"
        sql_insert += "values("+",".join(str(value) for value in values)+")"
        return self.dopost(sql_insert)

    def delete(self,table,find_dict):
        '''
        删除数据
        :param table: 目标表
        :param find_dict: where条件对 {'=':[('部门','业务组'),('性别','男')],'like':[('name','%冯%')]}
        :return: 执行结果
        '''
        #self.cxn = self.pool.connection()
        #self.cursor = self.cxn.cursor()
        sql = 'delete from '+table
        if find_dict:
            tj = []
            for key in find_dict.keys():
                for kv in find_dict[key]:
                    if not str(kv[1]).isdigit():
                        kk = ("'" + str(kv[1]) + "'").replace("''","'")
                    tj.append(str(kv[0]) + " "+key+" "+kk)
            sql += " where "+" and ".join(tj)
        return self.dopost(sql)

    def update(self,table,keyandvals,find_dict=None):
        '''
        更新数据
        :param table: 目标表
        :param keyandvals: 更新值，例如：{'age':'age-1','性别':'男'}
        :param find_dict: where条件对，例如：{'=':[('部门','业务组'),('性别','男')],'like':[('name','%冯%')]}
        :return: 执行结果
        '''
        #self.cxn = self.pool.connection()
        #self.cursor = self.cxn.cursor()
        sql = 'update '+table+' set '+','.join(str[k]+'='+str(v) for k,v in keyandvals.items())
        if find_dict:
            tj = []
            for key in find_dict.keys():
                for kv in find_dict[key]:
                    if not str(kv[1]).isdigit():
                        kk = ("'" + str(kv[1]) + "'").replace("''", "'")
                    tj.append(str(kv[0]) + " " + key + " " + kk)
            sql += " where " + " and ".join(tj)
        return self.dopost(sql)

    def addtable(self,table,pars):
        '''
        建表
        :param table: 表名
        :param pars: 字段属性，例如addtable("`xiaoming`",[("name","varchar(30)","not null"),("sex","varchar(30)"),("age","int(6)")])
        :return: 执行结果
        '''
        #self.cxn = self.pool.connection()
        #self.cursor = self.cxn.cursor()
        if table[0] != "`":
            table = "`"+table+"`"
        sql = "create table if not exists "+table+"("+",".join([" ".join(x) for x in pars])\
              +")ENGINE=InnoDB DEFAULT CHARSET=utf8"
        return self.dopost(sql)

    def write2mysql(self,df,table):
        self.cxn = self.pool.connection()
        self.cursor = self.cxn.cuesor()
        res = False
        try:
            engine = create_engine("mysql+pymysql://"+self.user+":"+self.pwd+"@"+self.host
                                   +":"+self.port+"/"+self.db+"?charset=utf8")
            df.to_sql(name=table,con=engine,if_exists='append',index=False,index_label=False)
            res = True
        except Exception as e:
            print(e)
        return res

class excel2pd(object):
    def __init__(self, path):
        #super(mysql2pd,self).__init__()
        self.excelpath = path

    def __call__(self):
        if os.path.exists(self.excelpath):
            if os.path.isfile(self.excelpath):
                try:
                    res = pd.read_excel(self.excelpath)
                except Exception as e:
                    print(e)
                    raise e
                return res




#test
if __name__ == "__main__":
    test = mysql2pd()
