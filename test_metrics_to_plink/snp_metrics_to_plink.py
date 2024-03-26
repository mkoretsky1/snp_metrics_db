import pandas as pd
import numpy as np
from io import StringIO

from google.cloud import storage

def blob_as_csv(bucket, path, sep='\s+', header='infer'):  # reads in file from google cloud folder
    blob = bucket.get_blob(path)
    blob = blob.download_as_bytes()
    blob = str(blob, 'utf-8')
    blob = StringIO(blob)
    df = pd.read_csv(blob, sep=sep, header=header)
    return df

def get_gcloud_bucket(bucket_name):  # gets folders from Google Cloud
    storage_client = storage.Client(project='genotools')
    bucket = storage_client.get_bucket(bucket_name)
    return bucket

snp_metrics_bucket = 'snp_metrics_db'
snp_metrics = get_gcloud_bucket(snp_metrics_bucket)

test_file_path = 'hold_query/GBA_AAC.csv'

test_file = blob_as_csv(snp_metrics, test_file_path, sep=',')

print(test_file.shape)
print(test_file.columns)

print(test_file['snpid'].unique())
print(test_file['gt'].value_counts())

ped = pd.DataFrame()
ped['FID'] = 0
ped['IID'] = test_file['gp2sampleid']
ped['FID'] = 0
ped['PAT'] = 0
ped['MAT'] = 0
ped['SEX'] = test_file['sex']
ped['PHENO'] = test_file['phenotype']
ped['PHENO'] = np.where(ped['PHENO'] == 'Control', 1, 2)

ped = ped.drop_duplicates(ignore_index=True)
print(ped.shape)

map = pd.DataFrame(columns=['chromosome','snpid','position'])

sample_snps = ['Seq_rs2071053.3_ilmnrev_ilmnF2BT','rs11264374','rs11264353']

for snp in test_file['snpid'].unique():
    test_file_subset = test_file[test_file['snpid'] == snp]

    map = pd.concat([map, test_file_subset[['chromosome','snpid','position']]], axis=0)

    test_file_subset = test_file_subset[['gp2sampleid','gt','a1','a2']]
    join = ped.merge(test_file_subset, how='inner', left_on=['IID'], right_on=['gp2sampleid'])
    join[f'{snp}_1'] = np.where(join['gt'] == 'NC', 0, join['gt'])
    join[f'{snp}_2'] = np.where(join['gt'] == 'NC', 0, join['gt'])
    join[f'{snp}_1'] = np.where((join['gt'] == 'AA') | (join['gt'] == 'AB'), join['a1'], join[f'{snp}_1'])
    join[f'{snp}_2'] = np.where(join['gt'] == 'AA', join['a1'], join[f'{snp}_2']) 
    join[f'{snp}_1'] = np.where(join['gt'] == 'BB', join['a2'], join[f'{snp}_1'])
    join[f'{snp}_2'] = np.where((join['gt'] == 'BB') | (join['gt'] == 'AB'), join['a2'], join[f'{snp}_2'])
    join = join.drop(columns=['gp2sampleid','gt','a1','a2'], axis=1)
    ped = join

print(ped.head())
print(ped.shape)

map = map.drop_duplicates(ignore_index=True)
map = map.rename({'position':'bp'}, axis=1)
map['pos'] = 0
map = map[['chromosome','snpid','pos','bp']]
print(map.head())
print(map.shape)

ped_path = '/Users/koretskymj/Desktop/test_metrics_to_plink/test.ped'
ped.to_csv(ped_path, sep='\t', index=None, header=None)

map_path = '/Users/koretskymj/Desktop/test_metrics_to_plink/test.map'
map.to_csv(map_path, sep='\t', index=None, header=None)