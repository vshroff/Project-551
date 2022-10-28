# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and setting

# Press the green button in the gutter to run the script

# See PyCharm help at https://www.jetbrains.com/help/pycharm/


from flask import Flask, render_template, request\



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
        return render_template('data.html', form_data=form_data)

app.run(host='localhost', port=5000)


##def makedir(form_data):
    ##print(form_data.items())
