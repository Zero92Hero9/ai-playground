# This is a sample Python script.
import datetime

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

import datetime
import os
from flask import Flask, jsonify
from openai import OpenAI


app = Flask(__name__)
api_key = os.getenv('OPENAI_API_KEY')
client = OpenAI(api_key = api_key)


@app.route("/")
def ping():
    # Use a breakpoint in the code line below to debug your script.
    return "Ping Ok !!! - " + str(datetime.datetime.now())


@app.route("/countryList", methods = ['GET'])
def country_list():
    value = client.chat.completions.create(messages=[{'role': 'system', 'content': 'only country names in the world '
                                                                                   'without any other text'}],
                                           model='gpt-4o-mini')
    choices = value.to_dict().get("choices")[0]
    d = dict(choices)
    data = {'countries' : dict(d.get('message')).get('content').split('\n')}
    response = jsonify(data)
    response.headers.add('Access-Control-Allow-Origin', 'http://localhost:3000')
    return response


@app.route("/stateList", methods = ['GET'])
def state_list():
    value = client.chat.completions.create(messages=[{'role': 'system', 'content': 'only state names in the USA without'
                                                                                   ' any other text '}],
                                           model='gpt-4o-mini')
    choices = value.to_dict().get("choices")[0]
    d = dict(choices)
    data = {'states' : dict(d.get('message')).get('content').split('\n')}
    response = jsonify(data)
    response.headers.add('Access-Control-Allow-Origin', 'http://localhost:3000')

@app.route("/cityList", methods = ['GET'])
def city_list():
    state = request.args.get('state')
    prompt = 'No other text, only cities in the ' + state + ' in alphabetic order'
    value = client.chat.completions.create(messages=[{'role': 'system', 'content': prompt}],
                                           model='gpt-4o-mini')
    choices = value.to_dict().get("choices")[0]
    d = dict(choices)
    data = {'cities' : dict(d.get('message')).get('content').split('\n')}
    response = jsonify(data)
    response.headers.add('Access-Control-Allow-Origin', 'http://localhost:3000')
    return response
    return response

