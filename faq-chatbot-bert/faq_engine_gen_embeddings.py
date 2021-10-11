import os, logging
import io
import pandas as pd

import torch
from sentence_transformers import SentenceTransformer

import faiss
import numpy as np
import pickle
from pathlib import Path

from flask import jsonify
import openpyxl


def vector_search(query, model, index, num_results=10):
    vector = model.encode(list(query))
    faiss.normalize_L2(vector)
    D, I = index.search(np.array(vector).astype("float32"), k=num_results)
    return D, I

def id2details(df, I, column):
    return [list(df[df.id == idx][column]) for idx in I[0]]


# input_excel = 'faqs_input.xlsx'
def generate_index(input_excel):
    try:
        log_file_ge = os.path.join('logs', 'generate_embeddings.log')
        logging.basicConfig(filename=log_file_ge, level=logging.DEBUG, format=f'%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s')

        df = pd.read_excel(input_excel, engine='openpyxl')
        u = df.select_dtypes(object)
        df[u.columns] = u.apply(lambda x: x.str.encode('utf-8').str.decode('ascii', 'ignore'))
        data_file_path = os.path.join('data', 'input.csv')
        df.to_csv(data_file_path, index=None, header=True, encoding='utf-8-sig')
        logging.info('Input file cleanup and conversion to utf-8 encoded csv Successful.')

        df.insert(0,'id',df.index)
        logging.info(f"Number of rows in input file: {df.id.unique().shape[0]}")
        df = df.dropna(axis=0, subset=['question'])

        # model_path = './models/paraphrase-distilroberta-base-v2/'
        # model = SentenceTransformer(model_path)
        model = SentenceTransformer('paraphrase-distilroberta-base-v2')
        logging.info(f'Model being used: {model}')
        if torch.cuda.is_available():
            model = model.to(torch.device("cuda"))
        logging.info(f'Device used: {model.device}')

        embeddings = model.encode(df.question.to_list(), show_progress_bar=False)
        logging.info(f'Shape of the vectorised abstract: {embeddings[0].shape}')

        embeddings = np.array([embedding for embedding in embeddings]).astype("float32")
        quantizer = faiss.IndexFlatIP(embeddings.shape[1])
        index = faiss.IndexIVFFlat(quantizer, embeddings.shape[1], int(np.sqrt(embeddings.shape[0])), faiss.METRIC_INNER_PRODUCT)
        faiss.normalize_L2(embeddings)
        index.train(embeddings)
        index = faiss.IndexIDMap(index)
        index.add_with_ids(embeddings, df.id.values)
        logging.info(f"Number of vectors in the Faiss index: {index.ntotal}")

        index_file_path = os.path.join('models', 'faiss_index_distilroberta-cosine.pickle')
        with open(index_file_path, 'wb') as h:
            pickle.dump(faiss.serialize_index(index), h)

        logging.info('Index saved successfully.')
        return 'Index file saved successfully.'

    except Exception as Argument:
        logging.error(str(Argument))
        return f'Index file creation failed: {Argument}'
