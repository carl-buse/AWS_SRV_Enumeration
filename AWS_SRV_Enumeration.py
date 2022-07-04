import boto3 
import json
import pandas as pd
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('profile_name', help='name of the AWS profile to use')

args = parser.parse_args()
profile_name = args.profile_name

session = boto3.session.Session(profile_name=profile_name)
client = session.client('ce')
time_period = {'Start': '2022-05-01', 'End': '2022-05-15'}
results = client.get_cost_and_usage(
    TimePeriod=time_period, 
    Granularity='MONTHLY', 
    Metrics=['BlendedCost'], 
    GroupBy=[{'Type':'DIMENSION', 'Key':'SERVICE'}, {'Type': 'DIMENSION', 'Key': 'LINKED_ACCOUNT'}]
)

parsed = []
for r in results['ResultsByTime']:
    for g in r['Groups']:
        parsed.append({
            'account_id': g['Keys'][1],
            'service_name': g['Keys'][0],
            'cost': g['Metrics']['BlendedCost']['Amount']
        })

parsed_df = pd.DataFrame(parsed).sort_values(by=['account_id', 'cost'], ascending=False)
parsed_df.to_csv('{}_audit_perms.csv'.format(profile_name), index=None)