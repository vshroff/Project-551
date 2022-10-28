# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and setting

# Press the green button in the gutter to run the script

# See PyCharm help at https://www.jetbrains.com/help/pycharm/


from flask import Flask, render_template, request
import pandas as pd
import json
import requests
import sys
import math

app = Flask(__name__)

@app.route('/form')
def form():
    return render_template('form.html')


@app.route('/data/', methods=['POST', 'GET'])
def data():
    if request.method == 'GET':
        return f"The URL /data is accessed directly. Try going to '/form' to submit form"
    if request.method == 'POST':
        form_data = request.form
        print(form_data.items())
        ##makedir(form_data)
        for i, v in form_data.items():
            print(i,v)
        print(form_data.get('Name'), form_data.get('file'))
        loadData(form_data.get('Name'),form_data.get('file'))
        return render_template('data.html', form_data=form_data)

app.run(host='localhost', port=5000)

def loadData(name, filename):
    data_set = pd.read_csv(filename)
    length = data_set.shape[0]
    num_partitions = math.ceil(length/100)
    partiton_path =  ""
    for i in range(1,num_partitions+1):
        if i == num_partitions:
            x = data_set.iloc[((i-1)*100+1):len(data_set)+1, :].to_json(orient="index")
        else: 
            x = data_set.iloc[((i-1)*100+1):(i*100)+1, :].to_json(orient="index")
        db = json.loads(x)
        q = json.dumps(db)

        f = open('sample.json','w')
        f.write(q)
        f.close()
        name1 = "data"+str(i)
        p = "p"+str(i)
        url = 'https://project551-a12dc-default-rtdb.firebaseio.com/datanode/{}/.json'.format(name1)
        partiton_path += '"{}":"https://project551-a12dc-default-rtdb.firebaseio.com/datanode/{}/.json",'.format(p,name1)
        r1 = requests.put(url, data = q) 
    path1 = partiton_path[:-1]
    print(path1)
    path1 = "{"+path1+"}"
    r2 = requests.put('https://project551-a12dc-default-rtdb.firebaseio.com/namenode/{}/.json'.format(name1), data = json.dumps(path1))
#def makedir(form_data):
    ##print(form_data.items())
