import pandas as pd
from datetime import datetime,timedelta
import time

def readPdByName(fileName):
    records = []
    df = pd.read_excel('./source/' + fileName + '.xlsx',sheet_name=None)
    for k,v in df.items():
        items = v.to_dict(orient='records')
        for item in items:
            records.append(item)
    return records
	
def generateTimeHour():
    return str(time.strftime("%Y%m%d%H%M%S", time.localtime()))

def generateXlsx(datas,fileName):
    df = pd.DataFrame(datas)
    print(df)
    df.to_excel('./result/' + fileName + "-" + generateTimeHour() + '.xlsx')
    print(datetime.now(),'save to xlsx finish ', len(datas))

targetDatas = readPdByName('target')
print("target:",len(targetDatas))
poolDatas = readPdByName('pool')
print("pool:",len(poolDatas))

poolDataMap = {}
for sdata in poolDatas:
    uid = str(sdata['uid']).strip()
    poolDataMap[uid] = sdata
print("pool map:",len(poolDataMap))

result = []
for tdata in targetDatas:
    uid = str(tdata['uid']).strip()
    if poolDataMap.get(uid) is None:
        continue
    result.append(tdata)
print("result:",len(result))

generateXlsx(result,'tagtet-in-pool-data')