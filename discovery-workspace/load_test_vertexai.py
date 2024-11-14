import numpy as np
import vertexai

from google.oauth2 import service_account
from vertexai.language_models import TextEmbeddingModel, TextEmbeddingInput

PROJECT_ID='intense-plate-406615'
REGION='us-central1'
credentials = service_account.Credentials.from_service_account_file('account.json', scopes = ['https://www.googleapis.com/auth/cloud-platform'])

vertexai.init(project=PROJECT_ID,
              location=REGION,
              credentials = credentials
             )
embedding_model = TextEmbeddingModel.from_pretrained('textembedding-gecko@003')

for s in range(0,200):
    text_input = TextEmbeddingInput(text = 'apple', task_type = 'CLUSTERING')
    embedding = embedding_model.get_embeddings([text_input])[0].values
    if s % 20 == 0:
        print(s