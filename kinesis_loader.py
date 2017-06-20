import time
import boto3
from datetime import datetime
import json
import random
import sys
from os import environ
from itertools import islice

class KinesisLoader:
    def __init__(self, batch_size=500, maximum_records=1000):
        """ The default batch_size here is to match the maximum allowed by Kinesis in a PutRecords request """

        PROD_ID = environ.get('AWS_ACCESS_KEY_ID')
        PROD_KEY = environ.get('AWS_SECRET_ACCESS_KEY')
        assert PROD_ID and PROD_KEY

        self.kinesis_client = boto3.client(
        'kinesis',
        aws_access_key_id=PROD_ID,
        aws_secret_access_key=PROD_KEY,
        )
        self.batch_size = min(batch_size, 500)
        self.maximum_records = maximum_records

        #parse the user input
        num_nodes = int(sys.argv[1])
        burst_per_min = int(sys.argv[2])
        observation_types = int(sys.argv[3])

        out_generator =  self.generate_records(num_nodes,burst_per_min,observation_types)
        threshold_per_min = int(num_nodes*observation_types/60)
        print("threshold per min: " , threshold_per_min)

        if(threshold_per_min >= 500):
            print("\nWe need to add more shrad to handle this situation\n")
            exit()

        while True:
            #sleep for .10 seconds to avoid having the problem of ProvisionedThroughputExceededException
            time.sleep(0.10)
            start_time = time.time()
            counter = 0
            records_batch = []

            records_batch = list(islice(out_generator,threshold_per_min))
            print("batch: ",len(records_batch))
            request = {
                'Records': records_batch,
                'StreamName': 'MansiStream'
            }
            response = self.kinesis_client.put_records(**request)

            self.submit_batch_until_successful(records_batch, response)
            counter += len(records_batch)

            print('Batch inserted. Total records: {}'.format(str(counter)))
            print("--- %s seconds ---" % (time.time() - start_time))


    def submit_batch_until_successful(self, batch, response):
            """ If needed, retry a batch of records, backing off exponentially until it goes through"""
            retry_interval = 0.5

            failed_record_count = response['FailedRecordCount']
            while failed_record_count:
                time.sleep(retry_interval)

                # Failed records don't contain the original contents - we have to correlate with the input by position
                failed_records = [batch[i] for i, record in enumerate(response['Records']) if 'ErrorCode' in record]

                print('Incrementing exponential back off and retrying {} failed records'.format(str(len(failed_records))))
                retry_interval = min(retry_interval * 2, 10)
                request = {
                    'Records': failed_records,
                    'StreamName': 'workflow_data_stream'
                }

                result = self.kinesis_client.put_records(**request)
                self.submit_batch_until_successful(records_batch, response)

                failed_record_count = result['FailedRecordCount']

    @staticmethod
    def get_kinesis_record():
            message = ('{"node_id": "000","node_config": "23f","datetime": "'+datetime.utcnow().isoformat().split('+')[0]+'",' \
                              '"sensor": "TMP112","data": {"Temperature": '+str(random.uniform(60,90))+'}}')
            raw_data = json.dumps(message)
            encoded_data = bytes(raw_data,'utf-8')
            kinesis_record = {
                'Data': encoded_data,
                'PartitionKey': 'arbitrary'
            }
            '''print (message)'''
            return kinesis_record


    def make_observation(self,feature_dict, node_id):
        observation = {
        'datetime': datetime.utcnow().isoformat().split('+')[0],
        'network': 'array_of_things_chicago',
        'meta_id': 1,
        'data': self.make_observation_data(feature_dict['property_list']),
        'node_id': node_id,
        'sensor': feature_dict['sensor'][random.randrange(0,len(feature_dict['sensor']))]

        }
        return observation

    def make_observation_data(self,property_list):
        return {property_name: random.randint(1,50) for property_name in property_list}

    def generate_records(self,num_nodes,burst_per_min,observation_types):
        print("num of nodes: ", num_nodes)
        print("num of bursts per min: ", burst_per_min)
        print("num of observation types: ", observation_types)

        temperature_data = {
            'property_list' : ['temperature'],
            'sensor': ['pr103j2', 'tmp421', 'tmp112', 'tsys01']
        }

        intensity_data = {
            'property_list' : ['intensity'],
            'sensor': ['tsl260rd', 'mlx75305','apds-9006-020', 'tsl260rd','ml8511']
        }

        bmi160_data = {
            'property_list': ['orient_y','orient_z','accel_z','orient_x','accel_y','accel_x'],
            'sensor': ['bmi160']
        }

        htu21d_data = {
            'property_list': ['temperature','humidity'],
            'sensor': ['htu21d', 'sht25', 'hih6130']
        }

        pressure_data = {
            'property_list': ['temperature','pressure'],
            'sensor': ['hmc5883l', 'bmp180', 'lps25h']
        }

        humidity_data = {
            'property_list': ['humidity'],
            'sensor': ['hih4030']
        }
        gas_concentration = {
            'property_list': ['o3', 'co', 'reducing_gases','h2s','no2','so2','oxidizing_gases'],
            'sensor': ['chemsense']
        }

        magnetic_field = {
            'property_list' : ['x','y','z'],
            'sensor': ['mma8452q','hmc5883l']
        }

        obs_array = [temperature_data,intensity_data,bmi160_data,htu21d_data,pressure_data,humidity_data,gas_concentration,magnetic_field]
        while True:
            for i in range(1,num_nodes):
                print("node: ",i)
                for j in range(0,observation_types):

                    message = self.make_observation(obs_array[j],i)
                    print(message)
                    raw_data = json.dumps(message)
                    encoded_data = bytes(raw_data,'utf-8')
                    kinesis_record = {
                        'Data': encoded_data,
                        'PartitionKey': 'arbitrary'
                    }
                    yield kinesis_record

x = KinesisLoader()
