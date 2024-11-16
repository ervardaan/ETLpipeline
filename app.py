import csv
import pandas as pd
from collections import Counter
import statistics
def etl(fileName,outputFileName):
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
        for (key,listValue) in userCommonCompound.items():
                mode=statistics.mode(listValue)
                userCommonCompound[key]=mode
        # with open(outputFileName,'w',newline='') as outputFile:
        #     fileWriter=csv.writer(outputFile,delimiter=',')
        #     print("hello")
        #     for row in fileReader:
        #         print(row)
        return(featuresList,userAvgRuntime,userCommonCompound)
# Your API that can be called to trigger your ETL process
def trigger_etl():
    # Trigger your ETL process here
    etl("C:/Users/varda/OneDrive/Documents/COLLEGE COURSEWORK/ETLpipeline/data/user_experiments.csv","C:/Users/varda/OneDrive/Documents/COLLEGE COURSEWORK/ETLpipeline/data/users.csv")
    return {"message": "ETL process started"}, 200
if __name__=="__main__":
    trigger_etl()

