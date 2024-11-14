import datetime
import hashlib
import os
import uuid
import glob

import numpy as np
import requests
from google.auth.transport import requests as r
from google.oauth2 import service_account
from qdrant_client import QdrantClient, models
from qdrant_client.models import PointStruct


client = QdrantClient('intech-qdrant.nonprod-tmaws.io')
collection_name = 'rk_test_event_offers'


def get_token_from_service_account():
    credentials = service_account.Credentials.from_service_account_file('account.json', scopes = ['https://www.googleapis.com/auth/cloud-platform'])
    request = r.Request()
    credentials.refresh(request)
    return credentials.token


def upsert_embeddings(text):
    request_url = 'https://us-central1-aiplatform.googleapis.com/v1/projects/intense-plate-406615/locations/us' \
                  '-central1/publishers/google/models/textembedding-gecko:predict'
    post_body = '{"instances": [{"task_type": "SEMANTIC_SIMILARITY","content": \''+ text +'\'}]}'
    response = requests.post(request_url, headers={'Authorization': 'Bearer ' + get_token_from_service_account()},
                             data=post_body)
    if not collection_exists(collection_name):
        client.create_collection(collection_name = collection_name,
                             vectors_config=models.VectorParams(size = 768, distance=models.Distance.COSINE))

    event_id = "G5v7Z9tus2oQ4" + str(datetime.datetime.now())
    event_point_id = str(uuid.UUID(hashlib.md5(event_id.encode("utf-8")).hexdigest()))
    client.upsert(
        collection_name=collection_name,
        wait=True,
        points=[
            PointStruct(id=event_point_id, vector=response.json()['predictions'][0]['embeddings']['values'], payload={"payload": text})
        ],
    )


def get_embeddings(text, task_type):
    request_url = 'https://us-central1-aiplatform.googleapis.com/v1/projects/intense-plate-406615/locations/us' \
                  '-central1/publishers/google/models/textembedding-gecko:predict'
    post_body = '{"instances": [{"task_type": \'SEMANTIC_SIMILARITY\',"content": \'' + text + '\'}]}'
    response = requests.post(request_url, headers={'Authorization': 'Bearer ' + get_token_from_service_account()},
                             data=post_body)

    return response


def get_embeddings_clustering(text):
    request_url = 'https://us-central1-aiplatform.googleapis.com/v1/projects/intense-plate-406615/locations/us' \
                  '-central1/publishers/google/models/textembedding-gecko:predict'
    post_body = '{"instances": [{"task_type": "CLUSTERING","content": \''+ text +'\'}]}'
    response = requests.post(request_url, headers={'Authorization': 'Bearer ' + get_token_from_service_account()},
                             data=post_body)

    return response


def collection_exists(name):
    return client.collection_exists(collection_name = name)


def count_tokens(text):
    request_url = 'https://us-central1-aiplatform.googleapis.com/v1/projects/intense-plate-406615/locations/us' \
                  '-central1/publishers/google/models/textembedding-gecko:countTokens'
    post_body = '{"instances": [{"content": \'' + text + '\'}]}'
    response = requests.post(request_url, headers={'Authorization': 'Bearer ' + get_token_from_service_account()},
                             data=post_body)
    print(response.json())


def setup(task_type):
    path = './event-files'
    list_of_files = glob.glob('./event-files/*.json')
    embeddings = []

    for file_name in list_of_files:
        file_contents = ''
        with open(file_name, 'r') as f:
            file_contents = f.read().replace('\n', '')
            response = get_embeddings(file_contents, task_type=task_type)
            if response.status_code == 200:
                embeddings.append(response.json()['predictions'][0]['embeddings']['values'])
            else:
                print(response.json())
    with open(path + '/' + 'embeddings.txt', 'w') as fw:
        for embedding in embeddings:
            for e in embedding:
                fw.write(str(e) + ',')
            fw.write('\n')


def read_embeddings():
    embeddings = []
    count = 0
    with open('./event-files/embeddings.txt', 'r') as file:
        for line in file.readlines():
            embeddings.insert(count, line)
            count += 1
    return embeddings


#setup('CLUSTERING')
embeddings = read_embeddings()
for embedding in embeddings:
    embeddings_in_floats = [float(x) for x in embedding.rstrip('\n').split(',') if x != '']
    X = np.array(embeddings_in_floats, dtype=np.float32)
    print(X.shape)