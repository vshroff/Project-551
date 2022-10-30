import mysql.connector

cnx = mysql.connector.connect(user='root', password='Harshsonita@123',
                              host='localhost',
                              database='new_schema', auth_plugin='mysql_native_password')

mycursor = cnx.cursor()
##mycursor.execute("CREATE TABLE customers (name VARCHAR(255), address VARCHAR(255))")

mycursor.execute("SHOW TABLES")

for x in mycursor:
  print(x)
cnx.close()
