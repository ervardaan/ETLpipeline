import csv
import pandas as pd
def etl(fileName):
    # Load CSV files
    # Process files to derive features
    # Upload processed data into a database
    with open(fileName,newline='') as experimentFile:
        fileReader=csv.reader(experimentFile,delimiter=',')
        usersExperiments=pd.read_csv(fileName)
        userDict={}
        experimentAvg={}
        i=0
        for row in fileReader:
            if i<1:
                i=i+1
                continue
            experimentId=int(row[0].strip('\t'))
            userId=int(row[1].strip('\t'))
            compounds=row[2].strip('\t')
            runTime=int(row[3].strip('\t'))
            compoundList=[]
            compoundsCount={}
            for compound in compounds.split(';'):
                compoundSeen=int(compound)
                if compoundSeen in compoundsCount:
                    compoundsCount[compoundSeen]=compoundsCount[compoundSeen]+1
                else:
                    compoundsCount[compoundSeen]=1
            if userId in userDict:
                userDict[userId]=userDict[userId]+1
            else:
                userDict[userId]=1
        exp_id=usersExperiments.columns.tolist()[0].strip('\t')
        print(df)
        print(userDict)

        
    pass


# Your API that can be called to trigger your ETL process
def trigger_etl():
    # Trigger your ETL process here
    etl()
    return {"message": "ETL process started"}, 200
if __name__=="__main__":
    etl("data/user_experiments.csv")


