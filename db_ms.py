import json
import pymysql
from sshtunnel import SSHTunnelForwarder
 
 
class DataBaseHandle:
    ''' 定义一个 MySQL 操作类'''
    def __init__(self, host='127.0.0.1', username='root', password='123456'
                 , database='dbname', port=3306):
        '''初始化数据库信息并创建数据库连接'''
        self.w_server = SSHTunnelForwarder(
                # 中间服务器地址
                ("remoteip", 22),
                ssh_username="root",
                ssh_password="123456",
                # ssh_pkey="~/.ssh/id_rsa",
                # ssh_private_key_password="~/.ssh/id_rsa",
                # 目标的地址与端口，因为目标地址就是中间地址所以写127.0.0.1或者localhost
                remote_bind_address=('xxx.rwlb.rds.aliyuncs.com', 3306),
                # remote_bind_address=('127.0.0.1', 3306),
                # 本地的地址与端口
                local_bind_address=('0.0.0.0', 3306)
                )
        # 启动ssh实例，后续的MySQL网络连接都将在这个环境下运行。
        self.w_server.start()
        # 后面开始对MySQL的数据进行初始化
        self.host = host
        self.username = username
        self.password = password
        self.database = database
        self.port = port
        self.db = pymysql.connect(host=self.host,
                                  user=self.username,
                                  passwd=self.password,
                                  database=self.database,
                                  port=self.port,
                                  charset='utf8')
 
    def insertDB(self, sql):
        ''' 插入数据库操作 '''
 
        self.cursor = self.db.cursor()
 
        try:
            # 执行sql
            self.cursor.execute(sql)
            # tt = self.cursor.execute(sql)  # 返回 插入数据 条数 可以根据 返回值 判定处理结果
            # print(tt)
            self.db.commit()
        except:
            # 发生错误时回滚
            self.db.rollback()
        finally:
            self.cursor.close()
 
    def deleteDB(self, sql):
        ''' 操作数据库数据删除 '''
        self.cursor = self.db.cursor()
 
        try:
            # 执行sql
            self.cursor.execute(sql)
            # tt = self.cursor.execute(sql) # 返回 删除数据 条数 可以根据 返回值 判定处理结果
            # print(tt)
            self.db.commit()
        except:
            # 发生错误时回滚
            self.db.rollback()
        finally:
            self.cursor.close()
 
    def updateDb(self, sql):
        ''' 更新数据库操作 '''
 
        self.cursor = self.db.cursor()
 
        try:
            # 执行sql
            self.cursor.execute(sql)
            # tt = self.cursor.execute(sql) # 返回 更新数据 条数 可以根据 返回值 判定处理结果
            # print(tt)
            self.db.commit()
        except:
            # 发生错误时回滚
            self.db.rollback()
        finally:
            self.cursor.close()
 
    def selectDb(self, sql):
        ''' 数据库查询 '''
        self.cursor = self.db.cursor()
        try:
            self.cursor.execute(sql)  # 返回 查询数据 条数 可以根据 返回值 判定处理结果
 
            data = self.cursor.fetchall()  # 返回所有记录列表
            # print(data)
 
            # # 结果遍历
            # for row in data:
            #     sid = row[0]
            #     name = row[1]
            #     # 遍历打印结果
            #     print('sid = %s,  name = %s' % (sid, name))
        except:
            print('Error: unable to fecth data')
        finally:
            self.cursor.close()
        return data
 
    def closeDb(self):
        ''' 数据库连接关闭 '''
        self.db.close()
        self.w_server.close()
 
 
if __name__ == '__main__':
    DbHandle = DataBaseHandle()
    data = DbHandle.selectDb("""
            select
                uid,
                name
            from users
        """)

    for row in data:
        uid = row[0]
        name = row[1]
        
        print(row,uid,name)

    DbHandle.closeDb()