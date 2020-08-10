
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This exercise ensures a couple things:
# 1) You have a working python environment with the dependencies installed
# 2) You have some known resources defined (e.g. the bq_sme_dataset)
# 3) You have working credentials with sufficient rights to your project
#
# This exercise requires no modification to the source code to complete.
from google.cloud import bigquery


DEFAULT_DATASET = "bq_sme_dataset"
SAMPLE_TABLE = "prework_exercise1_table"

def main():
    client = bigquery.Client()
    setup_dataset(client)
    setup_sample_table(client)
   

def setup_dataset(client):
     # Define a dataset for future exercises
    dest_dataset = bigquery.Dataset("{}.{}".format(client.project, DEFAULT_DATASET))
    dest_dataset.location = "US"
    dest_dataset.description = "Dataset for BQ SME Exercises"

    # Create dataset, but don't throw an error if it already exists.
    ds = client.create_dataset(dest_dataset, exists_ok=True)
    print("Dataset {} exists in location {} with description: {}".format(ds.dataset_id, ds.location, ds.description))


def setup_sample_table(client):
    # Define a CTAS which will produce some data from github commits
    sql = """
    CREATE OR REPLACE TABLE {}.{}
    (
        repo_name STRING,
        event_ts TIMESTAMP,
        author_email STRING,
        committer_email STRING,
        commit_id STRING
    )
    PARTITION BY DATE(event_ts)
    OPTIONS(
        partition_expiration_days=3650,
        description="a sample table based on github commits"
    )
    AS 
    SELECT
        repo,
        TIMESTAMP_MILLIS(author.date.seconds*1000 + CAST(author.date.nanos / 1000 AS INT64)),
        author.email,
        committer.email,
        commit
    FROM
        `bigquery-public-data.github_repos.commits`
    CROSS JOIN UNNEST(repo_name) as repo
    WHERE
        repo LIKE '%goog%'
    """.format(DEFAULT_DATASET, SAMPLE_TABLE)

    # create a query job
    query_job = client.query(sql)
    print("Starting job {}".format(query_job.job_id))

    # wait for it to finish
    query_job.result()

    print("Query {} was a {} and was billed {} bytes".format(query_job.job_id, query_job.statement_type, query_job.total_bytes_billed))


if __name__ == '__main__':
    main()