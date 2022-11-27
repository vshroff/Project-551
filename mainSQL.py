import mysql.connector

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



