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

def getMaxChocolateRating(companyLocation):
    max_ratings = []
    getAllPartitions = getPartitions('sanjit/chocolate/flavors_of_cacao')
    count = 1
    for key,value in getAllPartitions.items():
        partitoned_data = requests.get(value)
        max_rating_partition = 0
        data_list = partitoned_data.json()

        if isinstance(data_list, type([])):
            data_list.remove(data_list[0])
            for l in data_list:
                if l['CompanyLocation'] == companyLocation:
                    max_rating_partition = max(max_rating_partition, l['Rating'])
                    max_ratings.append(max_rating_partition) #make it outside loop
        else:
            for l in data_list.values():
                if l['CompanyLocation'] == companyLocation:
                    max_rating_partition = max(max_rating_partition, l['Rating'])
                    max_ratings.append(max_rating_partition)
        count = count + 1
    return max(max_ratings)


def cat(filename):
    output = []
    namenode_path = DATASET_PATH + 'namenode/' + filename + '/.json'
    r1 = requests.get(namenode_path).json()
    return_value = ""
    for key,value in r1.items():
        for key2, val in value.items(): 
            return_value+=json.dumps(requests.get(val).json(), sort_keys = True, indent = 4, separators = (',', ': '))
    return return_value

def rm(filename):
    namenode_path = DATASET_PATH + 'namenode/' + filename + '/.json'
    print(namenode_path)
    r1 = requests.get(namenode_path).json()
    requests.delete(namenode_path)
    if r1:
        for key, value in r1.items():
            requests.delete(value)
    else:
        requests.delete(DATASET_PATH + 'datanode/' + filename + '/.json')


def makedir(dirname):
    datanode_path = DATASET_PATH + 'datanode/{}/.json'.format(dirname)
    namenode_path = DATASET_PATH + 'namenode/{}/.json'.format(dirname)
    data = json.loads('{"zero":0}')
    data = json.dumps(data)
    r1 = requests.put(datanode_path, data=data)
    r2 = requests.put(namenode_path, data=data)
    if r1.status_code == 200 and r2.status_code == 200:
        return 'yes'
    else:
        return "no"



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
        db = json.loads(x)
        q = json.dumps(db)
       
        name1 = filename.split('.')[0] + str(i)
        p = "p" + str(i)
        url = 'https://project551-a12dc-default-rtdb.firebaseio.com/datanode/{}/{}/.json'.format(name, name1)
        partiton_path += '"{}":"https://project551-a12dc-default-rtdb.firebaseio.com/datanode/{}/{}/.json",'.format(p,
                                                                                                                    name,
                                                                                                                    name1)
        r1 = requests.put(url, data=q)
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

        if 'MkdirName' in form_data and form_data.get('MkdirName'):
            r = makedir(form_data.get('MkdirName'))
  
        elif 'loadDataName' in form_data and 'filename' in form_data and 'num_partitions' in form_data and form_data.get('loadDataName') and form_data.get('filename')  and form_data.get('num_partitions') :
            loadData(form_data.get('loadDataName'), form_data.get('filename'), int(form_data.get('num_partitions')))
            return render_template('data.html', form_data=form_data)

        elif 'list' in form_data and form_data.get('list'):
            res = listFiles(form_data.get('list'))
            return render_template('display.html', form_data=res)

        elif 'remove' in form_data and form_data.get('remove'):
            r = rm(form_data.get('remove'))
            print(r)
        elif 'cat' in form_data and form_data.get('cat'):
            res = cat(form_data.get('cat'))
            return render_template('cat.html', response=res)

        elif 'getpart' in form_data and form_data.get('getpart'):
            res = getPartitions(form_data.get('getpart'))
            return render_template('getpart.html', form_data=res)

        elif 'readpart' in form_data and form_data.get('readpart') and 'partition' in form_data and form_data.get('partition'):
            res = readPartitions(form_data.get('readpart'),form_data.get('partition'))
            print(res)
            return render_template('readpart.html', form_data=res)

        elif 'chocrate' in form_data and form_data.get('chocrate'):
            res = getMaxChocolateRating(form_data.get('chocrate'))
            print(res)
            return render_template('chocRate.html', form_data=res)
        
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






