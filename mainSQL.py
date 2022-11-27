import mysql.connector
import math
import pandas as pd
import json
import requests

cnx = mysql.connector.connect(user='root', password='Harshsonita@123',
                              host='localhost',
                              database='new_schema', auth_plugin='mysql_native_password')

mycursor = cnx.cursor()
mycursor.execute("CREATE TABLE NAMENODE (parent VARCHAR(255), child VARCHAR(255), type VARCHAR(255))")
mycursor.execute("CREATE TABLE FILEATTRIBUTES(filename VARCHAR(255) PRIMARY KEY, partitionn VARCHAR(255), filesize VARCHAR(255))")

mycursor.execute("SHOW TABLES")

for x in mycursor:
  print(x)
cnx.close()


def makedir(dirname):
    names = dirname.split('/')
    q1 = "SELECT CHILD FROM NAMENODE"
    mycursor.execute(q1)
    myresult = mycursor.fetchall()
    visited= []
    flag0 = -1
    flag1 = 1
    if names[0] in myresult:
        existing_child = names[0]
        q2 = "SELECT PARENT FROM NAMENODE WHERE CHILD = {}".format(existing_child)
        mycursor.execute(q2)
        parent = mycursor.fetchall()
        if parent == '/':
            flag0 = 1
        elif names[0] == names[-1]:
            flag0 = 0
        visited.append(names[0])

    for x in names[1:]:
        if x in myresult:
            q3 = "SELECT PARENT FROM NAMENODE WHERE CHILD = {}".format(existing_child)
            mycursor.execute(q3)
            parent = mycursor.fetchall()
            if parent == visited[-1]:
                flag1 = 1
            elif x == names[-1]:
                flag1 = 0
                break

            visited.append(x)
        print("cannot create the new directory")
        return "no"
    if flag0 ==0:
        q = "INSERT INTO NAMENODE VALUES({},{})".format('/', names[-1])
        mycursor.execute(q)
        cnx.commit()

    if flag1 == 0:
        q = "INSERT INTO NAMENODE VALUES({},{})".format(names[-2], names[-1])
        mycursor.execute(q)
        cnx.commit()



def ls(dirname):
        if dirname == '/':
            q3 = "SELECT CHILD FROM NAMENODE WHERE PARENT = '{}'".format(dirname)
            mycursor.execute(q3)
            child = mycursor.fetchall()
            return child   
        names = dirname.split('/')
        name = names[-1]
        
        q3 = "SELECT CHILD FROM NAMENODE WHERE PARENT = '{}'".format(name)
        mycursor.execute(q3)
        child = mycursor.fetchall()
        print(child)


def loadData(name, filename, num_partitions):
    data_set = pd.read_csv(filename)
    length = data_set.shape[0]
    num_rows_per_part = math.ceil(length/num_partitions)
    partiton_path = ""
    for i in range(1, num_partitions + 1):
        if i == num_partitions:
            x = data_set.iloc[((i - 1) * num_rows_per_part + 1):len(data_set) + 1, :].to_json(orient="index")
        else:
            x = data_set.iloc[((i - 1) * num_rows_per_part + 1):(i * num_rows_per_part) + 1, :].to_json(orient="index")
        
        name1 = filename.split('.')[0] + str(i)
        p = "p" + str(i)
        tabname = name1 + p
        qCreate = "CREATE TABLE {}".format(tabname)
        mycursor.execute(qCreate)
        cnx.commit()


        cols = "`,`".join([str(i) for i in x.columns.tolist()])

        for i, row in x.iterrows():
            sql = "INSERT INTO `{}` (`"+cols+"`) VALUES (" + "%s,"*len(row-1)+"%s)".format(tabname)
            mycursor.execute(sql, tuple(row))
        

        
        ##url = 'https://project551-a12dc-default-rtdb.firebaseio.com/datanode/{}/{}/.json'.format(name, name1)
        ##partiton_path += '"{}":"https://project551-a12dc-default-rtdb.firebaseio.com/datanode/{}/{}/.json",'.format(p,name,name1)
       ## r1 = requests.put(url, data=q)
    path1 = partiton_path[:-1]
    path1 = "{" + path1 + "}"
    db = json.loads(path1)
    q = json.dumps(db)
    query = "INSERT INTO FILEATTRIBUTES VALUES (`filename`, `num_partitions`,`length`)"

   ## r2 = requests.put('https://project551-a12dc-default-rtdb.firebaseio.com/namenode/{}/{}/.json'.format(name,filename.split('.')[0]),data=q)




def getPartitions(filename):
    query = "SELECT Partitionn from FILEATTRIBUTES where filename = {}".format(filename)
    mycursor.execute(query)
    parent = mycursor.fetchall()
    return parent


    

def readPartitions(filename,partition):
        tabname = filename + "p"+str(partition)
        sql = "SELECT * FROM  {}".format(tabname)
        mycursor.execute(sql)
        res = mycursor.fetchall()
        print(res)

def rm(filename):
    n = getPartitions(filename)
    for i in range(n):
        tabname = filename + "p"+str(i)
        query = "DELETE FROM {}".format(tabname)
        mycursor.execute(query)
        cnx.commit()

    q = "DELETE FROM FILEATTRIBUTES WHERE FILENAME = {}".format(filename)
    mycursor.execute(q)
    cnx.commit()

def cat(filename):
    n = getPartitions(filename)
    for i in range(1, n+1):
        tabname = filename + "p"+str(i)
        sql = "SELECT * FROM  {}".format(tabname)
        mycursor.execute(sql)
        res = mycursor.fetchall()
        print(res)

            


        







        



