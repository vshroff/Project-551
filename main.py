import json
import math
import pandas as pd
import requests
import sys
from flask import Flask, render_template, request

app = Flask(__name__, static_folder='staticfiles')

DATASET_PATH = "https://project551-a12dc-default-rtdb.firebaseio.com/"

def getPartitions(filename):
    namenode_path = DATASET_PATH + 'namenode/' + filename + '/.json'
    r1 = requests.get(namenode_path)
    return r1.json()

def readPartitions(filename,partition):
    datanode_path = DATASET_PATH + 'datanode/' + filename + partition+'/.json'
    r1 = requests.get(datanode_path)
    return r1.json()

def cat(filename):
    output = []
    namenode_path = DATASET_PATH + 'namenode/' + filename + '/.json'
    r1 = requests.get(namenode_path).json()
    for key,value in r1.items():
        output.append(requests.get(value).json())
    return output

def rm(filename):
    datanode_path = DATASET_PATH + 'datanode/' + filename + '/.json'
    namenode_path = DATASET_PATH + 'namenode/' + filename + '/.json'
    r1 = requests.get(namenode_path).json()
    requests.delete(namenode_path)
    for key, value in r1.items():
        requests.delete(value)


def makedir(dirname):
    datanode_path = DATASET_PATH + 'datanode/{}/.json'.format(dirname)
    namenode_path = DATASET_PATH + 'namenode/{}/.json'.format(dirname)
    data = json.loads('{"zero":0}')
    print(type(data))
    data = json.dumps(data)
    print(type(data))
    r1 = requests.put(datanode_path, data=data)
    r2 = requests.put(namenode_path, data=data)
    print(r1.url)
    print(r2.url)
    print(r1.text)
    print(r2.text)
    if r1.status_code == 200 and r2.status_code == 200:
        return 'yes'
    else:
        return "no"



def loadData(name, filename):
    data_set = pd.read_csv(filename)
    length = data_set.shape[0]
    num_partitions = math.ceil(length / 100)
    partiton_path = ""
    for i in range(1, num_partitions + 1):
        if i == num_partitions:
            x = data_set.iloc[((i - 1) * 100 + 1):len(data_set) + 1, :].to_json(orient="index")
        else:
            x = data_set.iloc[((i - 1) * 100 + 1):(i * 100) + 1, :].to_json(orient="index")
        db = json.loads(x)
        q = json.dumps(db)
        f = open('sample.json', 'w')
        f.write(q)
        f.close()
        name1 = filename.split('.')[0] + str(i)
        p = "p" + str(i)
        url = 'https://project551-a12dc-default-rtdb.firebaseio.com/datanode/{}/{}/.json'.format(name, name1)
        partiton_path += '"{}":"https://project551-a12dc-default-rtdb.firebaseio.com/datanode/{}/{}/.json",'.format(p,
                                                                                                                    name,
                                                                                                                    name1)
        r1 = requests.put(url, data=q)
        print(r1.text)
    path1 = partiton_path[:-1]
    path1 = "{" + path1 + "}"
    db = json.loads(path1)
    q = json.dumps(db)
    r2 = requests.put('https://project551-a12dc-default-rtdb.firebaseio.com/namenode/{}/{}/.json'.format(name,filename.split('.')[0]),data=q)


@app.route('/form')
def form():
    return render_template('form.html')


@app.route('/')
def upload():
    return render_template("file_form_upload.html")


@app.route('/success', methods=['POST'])
def success():
    if request.method == 'POST':
        f = request.files['file']
        f.save(f.filename)
        return render_template("success.html", name=f.filename)


@app.route('/data/', methods=['POST', 'GET'])
def data():
    if request.method == 'GET':
        return f"The URL /data is accessed directly. Try going to '/form' to submit form"
    if request.method == 'POST':
        form_data = request.form

        for i, v in form_data.items():
            print(i, v)
        print(form_data.get('MkdirName'))
        if 'MkdirName' in form_data and form_data.get('MkdirName'):
            r = makedir(form_data.get('MkdirName'))
            print(r)
        elif 'loadDataName' in form_data and 'filename' in form_data and form_data.get('loadDataName') and form_data.get('filename') :
            loadData(form_data.get('loadDataName'), form_data.get('filename'))
            return render_template('data.html', form_data=form_data)
        elif 'list' in form_data and form_data.get('list'):
            res = listFiles(form_data.get('list'))
            return render_template('display.html', form_data=res)
        elif 'remove' in form_data and form_data.get('remove'):
            print('file to rm ' + form_data.get('remove'))
            r = rm(form_data.get('remove'))
            print(r)
        elif 'cat' in form_data and form_data.get('cat'):
            print('file to cat ' + form_data.get('cat'))
            res = cat(form_data.get('cat'))
            return render_template('cat.html', form_data=res)
        elif 'getpart' in form_data and form_data.get('getpart'):
            print('file to getpart ' + form_data.get('getpart'))
            res = getPartitions(form_data.get('getpart'))
            print(res)
            return render_template('getpart.html', form_data=res)
        elif 'readpart' in form_data and form_data.get('readpart') and 'partition' in form_data and form_data.get('partition'):
            print('file to readpart ' + form_data.get('readpart'))
            res = readPartitions(form_data.get('readpart'),form_data.get('partition'))
            return render_template('readpart.html', form_data=res)
        
        return render_template('success.html')


@app.route('/list/', methods=['POST', 'GET'])
def list():
    if request.method == 'GET':
        return f"The URL /data is accessed directly. Try going to '/form' to submit form"
    if request.method == 'POST':
        form_data = request.form
        for i, v in form_data.items():
            print(i, v)
        print(form_data.get('list'))
        res = listFiles(form_data.get('list'))
        return render_template('display.html', form_data=res)


app.run(host='localhost', port=5000)


def listFiles(filePath):
    print("hyello")
    response = requests.get(
        DATASET_PATH + '/namenode/{}/.json?print=pretty'.format(
            filePath))
    response_dict = response.json()
    print(response_dict)
    return response_dict




