import pandas as pd
import sshtunnel as sshtunnel
from clickhouse_driver import connect

server = sshtunnel.SSHTunnelForwarder(
    ('remoteip', 22),
    ssh_username='root',
    ssh_password='123456',
    # ssh_pkey="username.openssh", 
    # ssh_private_key_password="password",
    remote_bind_address=('xxx.clickhouse.ads.aliyuncs.com', 9000), # port is 9000 not 8123
    local_bind_address=('127.0.0.1', 8123)
)
server.start()
local_port = server.local_bind_port
# print(local_port)

conn = connect(host='127.0.0.1', port=local_port, user='root', password='123456', database='demo')

class ClickHouse:
    def __init__(self):
        self.client = conn

    def select(self, sql):
        # print(sql)
        cursor = self.client.cursor()
        cursor.execute(sql)
        res = cursor.fetchall()
        return res

ck = ClickHouse()
record = ck.select("""
    select
        uid,
        name
    from users
    """)
print(record)

df = pd.DataFrame(record)
print(df)
df.to_excel('example.xlsx')