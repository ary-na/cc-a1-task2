# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# [START gae_python38_render_template]
# [START gae_python3_render_template]
import datetime

from flask import Flask, render_template
from google.cloud import bigquery

# Code sourced and adapted from:

# [2] cherba, S. Ramey, D. Duck and peterh, "How to use GOOGLE_APPLICATION_CREDENTIALS with gcloud on a server?",
# Server Fault, 2022. [Online]. Available: https://serverfault.com/questions/848580/how-to-use-google-application
# -credentials-with-gcloud-on-a-server. [Accessed: 14- Sep- 2022].

import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/ariannajafiyamchelo/Desktop/cc-a1-task2-362308-054849c295bb.json"

app = Flask(__name__)


# Code sourced and adapted from:

# [3] M. Berlyant, "Get sum of max per user", Stack Overflow, 2022. [Online]. Available:
# https://stackoverflow.com/questions/53286810/get-sum-of-max-per-user#53286904. [Accessed: 14- Sep- 2022].

# [4] notnoop, "SQL - How to find the highest number in a column?", Stack Overflow, 2022. [Online]. Available:
# https://stackoverflow.com/questions/1547125/sql-how-to-find-the-highest-number-in-a-column. [Accessed: 14- Sep-
# 2022].

# [5] "Aggregate functions  |  BigQuery  |  Google Cloud", Google Cloud,
# 2022. [Online]. Available: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions. [
# Accessed: 14- Sep- 2022].

# [6] S. Geron, "How do I fix the error "select list expression [...] references which is neither grouped nor
# aggregated" in BigQuery?", Stack Overflow, 2022. [Online]. Available:
# https://stackoverflow.com/questions/67195989/how-do-i-fix-the-error-select-list-expression-references-which-is
# -neither. [Accessed: 14- Sep- 2022].

# [7] "BigQuery API Client Libraries  |  Google Cloud", Google Cloud, 2022. [Online]. Available:
# https://cloud.google.com/bigquery/docs/reference/libraries#linux-or-macos. [Accessed: 14- Sep- 2022].

@app.route('/')
def root():
    client = bigquery.Client()

    query = """
    SELECT
  time_ref,
  MAX(trade_value) AS trade_value
FROM (
  SELECT
    time_ref,
    SUM(value) AS `trade_value`
  FROM
    `cc-a1-task2-362308.task2_dataset.gsquarterlySeptember20`
  WHERE
    account = "Exports"
    OR account = "Imports"
  GROUP BY
    time_ref)
GROUP BY
  time_ref
ORDER BY
  trade_value DESC
LIMIT
  10
    """

    query_job = client.query(query)

    return render_template('index.html', rows=query_job, i=0)


@app.route('/task22')
def task22():
    return render_template('task22.html')


@app.route('/task23')
def task23():
    return render_template('task23.html')


if __name__ == '__main__':
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.
    # Flask's development server will automatically serve static files in
    # the "static" directory. See:
    # http://flask.pocoo.org/docs/1.0/quickstart/#static-files. Once deployed,
    # App Engine itself will serve those files as configured in app.yaml.
    app.run(host='127.0.0.1', port=8080, debug=True)
# [END gae_python3_render_template]
# [END gae_python38_render_template]
