import csv
import pandas as pd
from collections import Counter
import statistics
import sqlite3
import os
def etl(fileName):
    # Load CSV files
    # Process files to derive features
    # Upload processed data into a database
    with open(fileName,newline='') as experimentFile:
        fileReader=csv.reader(experimentFile,delimiter=',')
        userDict={}#create a user dictionary to collect user-number of experiments data
        userRunTime={}
        userCommonCompound={}
        i=0
        for row in fileReader:
            if i<1:
                i=i+1
                continue
            experimentId=int(row[0].strip('\t'))#get experiment id
            userId=int(row[1].strip('\t'))# get userid
            compounds=row[2].strip('\t')# get compounds list
            runTime=int(row[3].strip('\t'))# get runtime
            compoundsCount={}#not needed
            compoundList=[]
            for compound in compounds.split(';'):
                compoundSeen=int(compound)
                compoundList.append(compoundSeen)
                if compoundSeen in compoundsCount:
                    compoundsCount[compoundSeen]=compoundsCount[compoundSeen]+1
                else:
                    compoundsCount[compoundSeen]=1
            if userId in userDict:
                userDict[userId]=userDict[userId]+1
                userRunTime[userId]=userRunTime[userId]+runTime
                currentList=userCommonCompound[userId]
                for c in compoundList:
                    currentList.append(c)
                userCommonCompound[userId]=currentList
            else:
                userDict[userId]=1
                userRunTime[userId]=runTime
                userCommonCompound[userId]=compoundList
        featuresList=[]
        featuresList.append(userDict)
        #countAverageExperiments=sum(userDict.values())/len(userDict)
        userAvgRuntime={}
        for (key,value) in userRunTime.items():
            userAvgRuntime[key]=value/userDict[key]
        userCommonCompoundList=userCommonCompound
        for (key,listValue) in userCommonCompound.items():
                mode=statistics.mode(listValue)
                userCommonCompound[key]=mode
    #     with open(outputFileName,'w',newline='') as outputFile:
    #         fileWriter=csv.writer(outputFile,delimiter=',')
    #         print("hello")
    # with open(fileName,newline='') as experimentFile:
    #     fileReader2=csv.reader(experimentFile,delimiter=',')
    #     print(featuresList)List
    return(featuresList,userAvgRuntime,userCommonCompound,userCommonCompoundList)
# Your API that can be called to trigger your ETL process
def executeQueryOnDb(db, query):
    connection=sqlite3.connect(db)
    #create a cursor object to execute queries
    cursor=connection.cursor()
    cursor.execute(query)
    fetchedData=cursor.fetchall()
    connection.commit()#connection object makes a commit to the db
    connection.close()
    return(fetchedData)
def buildInsertQueries(listOfValues):
    ###
    1-2,2-5,3-9
    1-2.3,2-4.5,3-4.8
    1-1,2-90,3-87
    ###
    listUsersQueries=[]
    listUsCmpQueries=[]
    expDict=listOfValues[0]
    avgRunTimeDict=listOfValues[1]
    userCmpDict=listOfValues[2]
    userCompoundList=listOfValues[3]
    for userid,totalExp in expDict.items():
        avgRunTime=avgRunTimeDict[userid]
        userCmp=userCmpDict[userid]
        listOfCompounds=userCompoundList[userid]
        usersInsert=f"insert into users values({userid},{totalExp},{avgRunTime},{userCmp})"
        listUsersQueries.append(usersInsert)
        for compound in set(listOfCompounds):
            countCmp=listOfCompounds.count(compound)
            usersCmpInsert=f"insert into us_cmp values({userid},{compound},{countCmp})"
            listUsCmpQueries.append(usersCmpInsert)
    return(listUsersQueries+listUsCmpQueries)
        
    
def trigger_etl():
    # Trigger your ETL process here
    listOfFeatures=etl("C:/Users/varda/OneDrive/Documents/COLLEGE COURSEWORK/ETLpipeline/data/user_experiments.csv")
    queries=buildInsertQueries(listOfFeatures)
    dbName="ourdb.db"
    status=setupDb(dbName)
    for query in queries:
        executeQueryOnDb(dbName,query)
    return {"message": "ETL process started"}, 200

def setupDb(dbName):
    """
    if dbname nonexistent-create db+create tables using schemas
    else-don't do anything
        
    """
    if os.path.exists(dbName):
        return(0)
    schemaFile="initializeDB.sql"
    schemaQueries=open(schemaFile).read()
    schemaQueryList=schemaQueries.split(";")
    finalList=[]
    for q in schemaQueryList:
        q_split = q.splitlines()
        q_split = [item.lstrip().rstrip() for item in q_split]
        finalList.append(" ".join(q_split))
    for q in finalList:
        executeQueryOnDb(dbName,q)
    return(0)




    


    
if __name__=="__main__":
    trigger_etl()

