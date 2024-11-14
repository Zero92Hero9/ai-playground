import vertexai
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import asyncio
import concurrent.futures

from os.path import expanduser
from google.oauth2 import service_account
from vertexai.language_models import TextEmbeddingModel, TextEmbeddingInput


offers_file_path = expanduser('~') + '/devel/autoqa/parquet/'
offers_embeddings_file_path = expanduser('~') + '/devel/autoqa/parquet/' + 'offer_embeddings_vertex_ai.parquet'
PROJECT_ID='intense-plate-406615'
REGION='us-central1'
credentials = service_account.Credentials.from_service_account_file('account.json', scopes = ['https://www.googleapis.com/auth/cloud-platform'])

vertexai.init(project=PROJECT_ID,
              location=REGION,
              credentials = credentials
             )
embedding_model = TextEmbeddingModel.from_pretrained('textembedding-gecko@003')


def get_file_contents(file_name):
    df = pd.read_parquet(offers_file_path + file_name, engine='pyarrow')
    return df


def get_embeddings(text):
    return embedding_model.get_embeddings(text)[0].values


def build_embeddings_df(embeddings, contents):
    columns = ['eventid', 'offerid', 'embeddings']
    data = contents.copy()
    data['content'] = embeddings
    return data


contents = get_file_contents('offer_doc_full.parquet')
embeddings_data = pd.DataFrame(columns=['eventid', 'offerid', 'embeddings'])
asyncio.ga
for index, row in contents.iterrows():
    embeddings_data['eventid'] = row['eventid']
    embeddings_data['offerid'] = row['offerid']
    embeddings_data['embeddings'] = get_embeddings([row['content']])

print(embeddings_data)

