

from flask import Flask
from flask import request
import update_tdn
import requests
import log_maker
import os
from logger import stdout_handler, stderr_handler
import logging

root = logging.getLogger()
root.setLevel(logging.DEBUG)
root.addHandler(stderr_handler)
root.addHandler(stdout_handler)
app = Flask(__name__)


@app.route('/tdsn_maker', methods=['GET'])
def tdsn_maker():
    try:
        if request.method == 'GET':
            tdn = update_tdn.TDN()
            res = tdn.main_entry()
            if 'status' in res and res['status'] == '1':
                out = requests.get(os.environ['update_url'])
                log_maker.logger.info('send')
                print('OK', out.text)
                return "OK "+out.text
            else:
                log_maker.logger.info('updated is failure!')
                print('Failure')
                return "Failure"
    except Exception as e:
        print(str(e))
        log_maker.logger.info(str(e))
        return "except"


app.run('0.0.0.0', os.environ['tm_port'])
