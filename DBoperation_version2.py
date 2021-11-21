import os
import pymysql

data_dir = "data\\"  # csv所在目录
# 链接到mysql server
conn = pymysql.connect(host="localhost", port=3306, user='root', passwd='')
# 取得游标
cursor = conn.cursor()

for root, dirs, files in os.walk(data_dir):
    sql_cmd = "CREATE DATABASE `%s`" % root[-4:]
    cursor.execute(sql_cmd)
    conn.commit()
    sql_cmd = "USE `%s`" % root[-4:]
    cursor.execute(sql_cmd)
    conn.commit()
    for fil in files:
        cnt = 0
        if fil[-4:] == '.csv':
            sql_cmd = "CREATE TABLE `%s`(" % fil[:-5]
            f = open(root + '\\' + fil, encoding='utf-8')
            head = f.readline().split(',')
            f.close()
            cnt = 0
            for i in head:
                cnt += 1
                if cnt == len(head):
                    sql_cmd += '%s text' % i
                else:
                    sql_cmd += '%s text,' % i
            sql_cmd += ')ENGINE = MYISAM;'  # 字段数量超过innoDB存储引擎限制 切换引擎为MyISAM
            cursor.execute(sql_cmd)  # 执行数据库指令
            conn.commit()  # 提交到服务器

            sql_cmd = "load data infile '%s' into table %s fields terminated by ',';" % (
                root + '\\' + fil, fil[:-5])
            cursor.execute(sql_cmd)
            conn.commit()

cursor.close()
print("Success")
