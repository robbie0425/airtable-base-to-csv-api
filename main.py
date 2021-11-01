from flask import Flask, request, make_response
from flask_cors import CORS
import pandas as pd
import functions
import uuid

app = Flask(__name__)
CORS(app)

@app.route('/')
def hello_there():
    return """daily coffee - airtable csv getter rest api"""

@app.route('/all-csv')
def get_base_csv():
    base_id = request.args.get('bid')
    if base_id is None:
        info = {"daily coffee api error": {"error": "please pass a bid value"}}
        code = 400
        return info, code
    elif base_id == '<your_base_id>':
        info = {"info": "<your_base_id> is an example value. Please visit https://grdc.notion.site/CSV-Exporter-for-Airtable-16afca474b954f588eff2e6b91620a4a to see how you can enable this api to work with your own Airtable base id"}
        code = 400
        return info, code

    api_key = request.args.get('apik')
    if api_key is None:
        info = {"daily coffee api error": {"error": "please pass an apik value"}}
        code = 400
        return info, code
    elif api_key == '<your_api_key>':
        info = {"info": "<your_api_key> is an example value. Please visit https://grdc.notion.site/CSV-Exporter-for-Airtable-16afca474b954f588eff2e6b91620a4a to see how you can enable this api to work with your own Airtable api key"}
        code = 400
        return info, code


    table_name = request.args.get('tname')
    if table_name is None:
        info = {"daily coffee api error": {"error": "please pass a tname value"}}
        code = 400
        return info, code
    
    table_name.replace(' ', '%20')

    view_name = request.args.get('vname')

    error_log = None

    try:
        info, code, df = functions.get_df(api_key=api_key, base_id=base_id, table_name=table_name, view_name=view_name)
    except Exception as e:
        error_log = str(e)
        # print this error log for debug
        info = {"daily coffee api error": {"error": "Something went wrong."}}
        code = 400
        df = None

    if code != 200:
        return info, code
    else:
        resp = make_response(df.to_csv(index=False))
        resp.headers["Content-Disposition"] = "attachment; filename={0}.csv".format(str(uuid.uuid4()))
        resp.headers["Content-Type"] = "text/csv"
        return resp

if __name__ == '__main__':
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.
    app.run(host='127.0.0.1', port=8080, debug=True)