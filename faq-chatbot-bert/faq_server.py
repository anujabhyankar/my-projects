from flask import Flask, request, jsonify, render_template
import pandas as pd
import json, os, shutil
from datetime import date, datetime
import faq_engine_app_server, faq_engine_gen_embeddings

app = Flask(__name__)

@app.route("/get_faq", methods = ['POST'])
def faq_request():
    # user_input_raw = json.loads(request.get_json())
    user_input_raw = request.get_json()
    user_input = user_input_raw['utterance']
    return faq_engine_app_server.find_matches(user_input)


@app.route("/bert_embeddings", methods = ['GET', 'POST'])
def gen_embeddings():
    if request.method == 'POST':
        input_file = request.files['file']

        date_mask = '%d%m%Y'
        time_mask = '%H%M'
        dte = datetime.now().strftime(date_mask)
        tme = datetime.now().strftime(time_mask)
        
        src_dir_data = 'data'
        dst_dir_data = os.path.join('data', 'archive')
        file_names_data = os.listdir(src_dir_data)
        for file_name in file_names_data:
            if file_name.endswith('.csv'):
                shutil.copy(os.path.join(src_dir_data, file_name), dst_dir_data)
                os.remove(os.path.join(src_dir_data, file_name))
                old_file_data = os.path.join(dst_dir_data, file_name)
                new_file_data = os.path.join(dst_dir_data, f'{file_name}.{dte}-{tme}')
                os.rename(old_file_data, new_file_data)

        src_dir = 'models'
        dst_dir = os.path.join('models', 'archive')
        file_names = os.listdir(src_dir)
        for file_name in file_names:
            if file_name.endswith('.pickle'):
                shutil.copy(os.path.join(src_dir, file_name), dst_dir)
                os.remove(os.path.join(src_dir, file_name))
                old_file = os.path.join(dst_dir, file_name)
                new_file = os.path.join(dst_dir, f'{file_name}.{dte}-{tme}')
                os.rename(old_file, new_file)

        result_response = faq_engine_gen_embeddings.generate_index(input_file)
        return render_template('upload.html', result_response=result_response)
    return render_template('upload.html')


if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5003)
