#!/usr/bin/env python
#coding=gbk

#-------------------------
# Copyright (c) 2014
# Tanry Electronic Technology Co., Ltd.
# ChangSha, China
# All Rights Reserved.
# 功能：将从SQLSERVER中导出的数据文件通过python脚本导入进Oracle中
# 作者：cjt
# 时间：2014.7.16
#-------------------------

import random
import types
import os 
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.ZHS16GBK'

import cx_Oracle

def selBranch():
    cursor.execute("select * from d_t2_branch")
#     rows = cursor.fetchall()
#     for row in rows:
#         print row
#         for col in row:
#             print type(col)
#             print col#.decode('gbk')
#             if type(col) is types.StringType:  
#                 print col.encode('GBK')
     
# open connection to oracle
conn = cx_Oracle.connect('jono/jono@10.1.1.105/jono')
cursor = conn.cursor()

# selBranch()

sql = "SELECT sbi.ITEM_ID, sbi.BRANCH_ID, COUNT(*) FROM D_T2_SUPPLIER_BRANCH_ITEM sbi \
    WHERE sbi.PRIORITY = 0 GROUP BY sbi.ITEM_ID, sbi.BRANCH_ID HAVING COUNT(*) >1"
selSql = "SELECT sbi.ITEM_ID, sbi.BRANCH_ID, sbi.SUPPLIER_ID FROM D_T2_SUPPLIER_BRANCH_ITEM sbi \
    WHERE sbi.PRIORITY = 0 AND sbi.ITEM_ID = :1 AND sbi.BRANCH_ID = :2"
updateSql = "update D_T2_SUPPLIER_BRANCH_ITEM sbi set sbi.PRIORITY = 1 \
    where sbi.ITEM_ID = :1 and sbi.BRANCH_ID = :2 and sbi.SUPPLIER_ID ! = :3"

# sql = "update d_t0_OPTION set option_value = :1 where option_key = :2"
# sql = "INSERT INTO d_t0_OPTION(option_key, option_value) values(:1, :2)"
updateArgs = []

cursor.execute(sql)
rows = cursor.fetchall()
for row in rows:
    itemId = row[0]
    branchId = row[1]
    print itemId, branchId
    param = (itemId, branchId)
    cursor.execute(selSql, (itemId, branchId))
    row1 = cursor.fetchone()
    print row1
    updateArgs.append(row1)
#         for col in row:
#             print type(col)
#             print col#.decode('gbk')

cursor.executemany(updateSql, updateArgs)
cursor.execute("commit")
# cursor.execute("INSERT INTO d_t0_OPTION(option_key, option_value) VALUES ('SLKDFJ', '我们')")

cursor.close()
conn.close()
