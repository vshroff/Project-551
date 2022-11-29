import mysql.connector
import math
import pandas as pd
import json
import requests

cnx = mysql.connector.connect(user='root', password='Harshsonita@123',
                              host='localhost',
                              database='new_schema', auth_plugin='mysql_native_password')

mycursor = cnx.cursor()
##mycursor.execute("CREATE TABLE NAMENODE (parent VARCHAR(255), child VARCHAR(255), type VARCHAR(255))")
##mycursor.execute("CREATE TABLE FILEATTRIBUTES(filename VARCHAR(255) PRIMARY KEY, partitionn VARCHAR(255), filesize VARCHAR(255))")

mycursor.execute("SHOW TABLES")

for x in mycursor:
    print(x)
cnx.close()


def makedir(dirname):
    names = dirname.split('/')
    q1 = "SELECT CHILD FROM NAMENODE"
    cnx = mysql.connector.connect(user='root', password='Harshsonita@123',
                                  host='localhost',
                                  database='new_schema', auth_plugin='mysql_native_password')

    # mycursor = cnx.cursor()
    # mycursor.execute(q1)
    # myresult = mycursor.fetchall()
    # visited = []
    # flag0 = -1
    # flag1 = 1

    if ".csv" in names[-1]:
        type = "file"
    else:
        type = "directory"
    query = "INSERT INTO NAMENODE VALUES(\"{}\",\"{}\",\"{}\")".format(names[-2], names[-1],type)
    print(query)
    mycursor = cnx.cursor()
    mycursor.execute(query)
    cnx.commit()


    # if names[0] in myresult:
    #     existing_child = names[0]
    #     q2 = "SELECT PARENT FROM NAMENODE WHERE CHILD = {}".format(existing_child)
    #     mycursor.execute(q2)
    #     parent = mycursor.fetchall()
    #     if parent == '/':
    #         flag0 = 1
    #     elif names[0] == names[-1]:
    #         flag0 = 0
    #     visited.append(names[0])
    #
    # for x in names[1:]:
    #     if x in myresult:
    #         q3 = "SELECT PARENT FROM NAMENODE WHERE CHILD = {}".format(existing_child)
    #         mycursor.execute(q3)
    #         parent = mycursor.fetchall()
    #         if parent == visited[-1]:
    #             flag1 = 1
    #         elif x == names[-1]:
    #             flag1 = 0
    #             break
    #
    #         visited.append(x)
    #     print("cannot create the new directory")
    #     return "no"
    # if flag0 == 0:
    #     q = "INSERT INTO NAMENODE VALUES({},{})".format('/', names[-1])
    #     mycursor.execute(q)
    #     cnx.commit()
    #
    # if flag1 == 0:
    #     q = "INSERT INTO NAMENODE VALUES({},{})".format(names[-2], names[-1])
    #     mycursor.execute(q)
    #     cnx.commit()


def ls(dirname):
    cnx = mysql.connector.connect(user='root', password='Harshsonita@123',
                                  host='localhost',
                                  database='new_schema', auth_plugin='mysql_native_password')

    mycursor = cnx.cursor()
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
    print(data_set.columns.to_list())
    length = data_set.shape[0]
    num_rows_per_part = math.ceil(length / num_partitions)
    partiton_path = ""
    flavours_of_cacao = """(Company
    varchar(50), SpecificBeanOrigin
    varchar(50), REF
    int, ReviewDate
    int, CocoaPercent
    varchar(50), CompanyLocation
    varchar(50), Rating
    float, BeanType
    varchar(50), BroadBeanOrigin
    varchar(50))"""

    for i in range(1, num_partitions + 1):
        if i == num_partitions:
            xy = data_set.iloc[((i - 1) * num_rows_per_part + 1):len(data_set) + 1, :]
        else:
            xy = data_set.iloc[((i - 1) * num_rows_per_part + 1):(i * num_rows_per_part) + 1, :]

        print(xy)
        tabname = filename.split('.')[0] + str(i)

        cnx = mysql.connector.connect(user='root', password='Harshsonita@123',
                                      host='localhost',
                                      database='new_schema', auth_plugin='mysql_native_password')

        cursor = cnx.cursor()

        qCreate = "CREATE TABLE {} {}".format(tabname, flavours_of_cacao)
        cursor.execute(qCreate)
        cnx.commit()

      ##  cols = "`,`".join(i for i in data_set.columns.tolist())

        for i, row in xy.iterrows():
            print(tuple(row))
            sql = "INSERT INTO `{}` VALUES  {}".format(tabname, tuple(row))
            print(sql)
            cursor.execute(sql)
            cnx.commit()

        ##url = 'https://project551-a12dc-default-rtdb.firebaseio.com/datanode/{}/{}/.json'.format(name, name1)
        ##partiton_path += '"{}":"https://project551-a12dc-default-rtdb.firebaseio.com/datanode/{}/{}/.json",'.format(p,name,name1)
    ## r1 = requests.put(url, data=q)
    path1 = partiton_path[:-1]
    path1 = "{" + path1 + "}"
    db = json.loads(path1)
    q = json.dumps(db)
    attributes = ('filename', 'num_partitions','length')
    query = "INSERT INTO FILEATTRIBUTES VALUES {}".format(attributes)
    cursor.execute(query)
    cnx.commit()


## r2 = requests.put('https://project551-a12dc-default-rtdb.firebaseio.com/namenode/{}/{}/.json'.format(name,filename.split('.')[0]),data=q)


def getPartitions(filename):
    cnx = mysql.connector.connect(user='root', password='Harshsonita@123',
                                  host='localhost',
                                  database='new_schema', auth_plugin='mysql_native_password')

    cursort = cnx.cursor()
    query = "SELECT Partitionn from FILEATTRIBUTES where filename = \"{}\" ".format(filename)
    print(query)
    cursort.execute(query)
    part = cursort.fetchall()
    print(part[0][0])
    return part[0][0]


def readPartitions(filename, partition):
    cnx = mysql.connector.connect(user='root', password='Harshsonita@123',
                                  host='localhost',
                                  database='new_schema', auth_plugin='mysql_native_password')

    cursorr = cnx.cursor()
    tabname = filename.split('.')[0] + str(partition)
    print(tabname)
    sql = "SELECT * FROM  {}".format(tabname)
    cursorr.execute(sql)
    res = cursorr.fetchall()
    print(res)


def rm(filename):
    cnx = mysql.connector.connect(user='root', password='Harshsonita@123',
                                  host='localhost',
                                  database='new_schema', auth_plugin='mysql_native_password')

    cursorrem = cnx.cursor()
    n = int(getPartitions(filename))
    for i in range(1, n+1):
        tabname = filename.split('.')[0] + str(i)
        query = "DROP TABLE IF EXISTS {}".format(tabname)
        print(query)
        cursorrem.execute(query)
        cnx.commit()

    q = "DELETE FROM FILEATTRIBUTES WHERE filename = \"{}\" ".format(filename)
    cursorrem.execute(q)
    cnx.commit()


def cat(filename):
    cnx = mysql.connector.connect(user='root', password='Harshsonita@123',
                                  host='localhost',
                                  database='new_schema', auth_plugin='mysql_native_password')

    cursorh = cnx.cursor()
    n = int(getPartitions(filename))
    for i in range(1, n + 1):
        tabname = filename.split('.')[0] + str(i)
        sql = "SELECT * FROM  {}".format(tabname)
        cursorh.execute(sql)
        res = cursorh.fetchall()
        print(res)

def getMaxChocolateRating(companyLocation):
    cnx = mysql.connector.connect(user='root', password='Harshsonita@123',
                                  host='localhost',
                                  database='new_schema', auth_plugin='mysql_native_password')

    cursora = cnx.cursor()
    n = int(getPartitions("flavors_of_cacao.csv"))
    maxl = []
    for i in range(1, n+1):
        tabname = "chocs" + str(i)
        q1 = "SELECT MAX(Rating) from {} WHERE COMPANYLOCATION IS {}".format(tabname, companyLocation)
        cursora.execute(q1)
        res = cursora.fetchall()
        maxl.append(res[0][0])
    return max(maxl)


def getCountries(score):
    cnx = mysql.connector.connect(user='root', password='Harshsonita@123',
                                  host='localhost',
                                  database='new_schema', auth_plugin='mysql_native_password')

    cursora = cnx.cursor()
    n = int(getPartitions())
    countries = []

    for i in range(1, n+1):
        tabname = "" + str(i)
        qr = "SELECT COUNTRYNAME FROM {} WHERE SCORE >= {}".format(tabname, score)
        cursora.execute(qr)
        res = cursora.fetchall()
        countries.append(res[0])
    return countries



















