import faiss
from sentence_transformers import SentenceTransformer

import pickle
import os, types, logging
import io
import json

import pandas as pd
import numpy as np

from flask import Flask, request, jsonify


def vector_search(query, model, index, num_results=10):
    vector = model.encode(list(query))
    faiss.normalize_L2(vector)
    D, I = index.search(np.array(vector).astype("float32"), k=num_results)
    return D, I


def read_data(data):
    df = pd.read_csv(data)
    df.insert(0,'id',df.index)
    return df

def load_bert_model(path='paraphrase-distilroberta-base-v2'):
    return SentenceTransformer(path)

def load_faiss_index(path_to_faiss):
    with open(path_to_faiss, "rb") as h:
        data = pickle.load(h)
    return faiss.deserialize_index(data)


def find_matches(user_input):
    try:
        log_file_app = os.path.join('logs', 'app.log')
        logging.basicConfig(filename=log_file_app, level=logging.DEBUG, format=f'%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s')
        
        input_file = os.path.join('data', 'input.csv')
        index_file = os.path.join('models', 'faiss_index_distilroberta-cosine.pickle')

        data = read_data(input_file)
        model = load_bert_model()
        faiss_index = load_faiss_index(index_file)

        logging.info('Document, Index file and Model loaded successfully.')

        D, I = vector_search([user_input], model, faiss_index, num_results=1)

        frame = data[
          (data.id >= 0)
        ]

        id_ = I.flatten().tolist()[0]
        if id_ in set(frame.id):
          f = frame[(frame.id == id_)]
        else:
          pass

        logging.info(f'User Input: {user_input}')
        logging.info(f'Index: {id_}')

        resp_data = {'user_input': user_input, 'predicted_question': f.iloc[0].question, 'predicted_answer': f.iloc[0].answer, 'url': f.iloc[0].url, 'source': f.iloc[0].source, 'similarity': D.flatten().tolist()[0]}
        logging.info(f'Response: {resp_data}')
        return jsonify(resp_data)

    except Exception as Argument:
        logging.error(str(Argument))
        resp_data = {'predicted_answer': 'I cannot answer your query due to a backend error. Please ask again after some time.'}
        return jsonify(resp_data)
