
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

# In this exercise, you'll be authoring a query.
from google.cloud import bigquery


DEFAULT_DATASET = "bq_sme_dataset"
SAMPLE_TABLE = "prework_exercise1_table"

def main():
    client = bigquery.Client()
    query_info_schema(client)


def query_info_schema(client):
    # Define the SQL query we're going to be using.  This example
    # is intentionally imcomplete, modify it to report expected values.
    sql = """
    SELECT 

    FROM
      `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
    WHERE
      job_type = 'QUERY'
      AND DATE(creation_time) >= DATE('2020-08-01')

    """.format(DEFAULT_DATASET, SAMPLE_TABLE)

    # create a query job
    query_job = client.query(sql)
    print("Starting job {}".format(query_job.job_id))

    # wait for it to finish and print row results
    # There's some python formatting directives here for padding and truncating values formatting.
    # We also convert all values to strings for a simple reason: you can't format a None value (aka
    # a NULL), so this simplifies processing logic.
    for row in query_job.result():
        print('{:30.30} {:20.20} {:15} {:15}'.format(str(row.user_email), str(row.statement_type), str(row.jobcount), str(row.total_bytes_processed)))

if __name__ == '__main__':
    main()