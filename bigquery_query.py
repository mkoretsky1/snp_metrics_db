import os
import sys
import subprocess

from google.cloud import bigquery

def shell_do(command, log=False, return_log=False):
    print(f'Executing: {(" ").join(command.split())}', file=sys.stderr)

    res=subprocess.run(command.split(), stdout=subprocess.PIPE)

    if log:
        print(res.stdout.decode('utf-8'))
    if return_log:
        return(res.stdout.decode('utf-8'))

if __name__ == '__main__':

    # set credentials
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'genotools-02f64a1e10be.json'

    # initialize big query client
    client = bigquery.Client()

    # define metrics table
    metrics_table = 'genotools.snp_metrics.metrics'

    # test query
    test_query = f'SELECT * FROM `{metrics_table}` WHERE maf IS NOT NULL AND maf != 0.0 and maf != 0.0625 LIMIT 100'
    print(test_query)

    # query from gcloud and print results
    query_results = client.query(test_query).to_dataframe()

    print(query_results)