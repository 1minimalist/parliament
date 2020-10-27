from flask import Flask
import requests
import json
import time
from datetime import date
from dateutil.relativedelta import relativedelta
import logging

app = Flask(__name__)

@app.route("/parse")
def parse_data():
    return 'Hi'

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)