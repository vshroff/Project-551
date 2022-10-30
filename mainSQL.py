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



