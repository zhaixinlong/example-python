import pandas as pd
from datetime import datetime
import time

host = "/xlsx/"

def generateTimeHour():
    return str(time.strftime("%Y%m%d%H", time.localtime()))

def generateTimeHourMinute():
    return str(time.strftime("%Y%m%d%H%m", time.localtime()))

def generateTimeDay():
    return str(time.strftime("%Y%m%d", time.localtime()))

def toHttpUrl(filePath):
    return host + filePath

def readPdByNameXlsx(fileNameXlsx):
    df = pd.read_excel('./file/' + fileNameXlsx)
    return  df.to_dict(orient='records')

def readPdByNameCsv(fileNameXlsx):
    df = pd.read_csv('./file/' + fileNameXlsx)
    return  df.to_dict(orient='records')

def generateXlsx(datas,fileName):
    df = pd.DataFrame(datas)
    print(df)
    fileName = fileName + "-" + generateTimeHour() + '.xlsx'
    df.to_excel('./xlsx/' + fileName)
    return toHttpUrl(fileName),fileName

def generateXlsxBydf(df,fileName):
    print(df)
    fileName = fileName + "-" + generateTimeHour() + '.xlsx'
    filePath = './xlsx/' + fileName
    df.to_excel(filePath)
    print(datetime.now(),'save to xlsx',filePath,' finish ')
    return toHttpUrl(fileName),fileName

def generateTxt(str,fileName):
    fileName = fileName.replace("/",'') + '.txt'
    filePath = './xlsx/' + fileName

    f1 = open(filePath,'a')
    f1.write(str)
    return toHttpUrl(fileName),fileName

def sortByKey(items,method):
    items.sort(key=method, reverse=True)
    return items

def readForPdByName(fileName):
    df = pd.read_excel('./xlsx/' + fileName)
    print(df)
    return  df

def getPdFilePath(fileName):
    fileName = fileName + "-" + generateTimeHour() + '.xlsx'
    return './xlsx/' + fileName,fileName

def mergeExcels(baseFileName,newFileName,mergeName):
    df_c=readForPdByName(baseFileName)
    df_t=readForPdByName(newFileName)

    filePath,fileName = getPdFilePath(mergeName)
    ew = pd.ExcelWriter(filePath,date_format=None,datetime_format=None, mode='w') # pylint: disable=abstract-class-instantiated

    with ew:
        df_c.to_excel(excel_writer=ew,sheet_name=baseFileName.strip().split("-")[0],index=None)
        df_t.to_excel(excel_writer=ew,sheet_name=newFileName.strip().split("-")[0],index=None)

    ew.save()
    ew.close()
    return toHttpUrl(fileName),fileName

def sumAllRowColumnForPd(df):
     # 对所有行进行求和
    row_sum = df.sum(numeric_only=True,axis=1)
    dfRow = pd.concat([df,pd.DataFrame({"总和":row_sum})],axis=1)

    # 对所有列进行求和
    column_sum = dfRow.sum(numeric_only=True,axis=0)
    print(column_sum.to_dict())
    column_sum[df.columns[0]] = "总和"
    dfColumn = dfRow.append(pd.DataFrame(column_sum.to_dict(),index=[0]))
    return dfColumn