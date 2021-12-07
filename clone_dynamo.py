
#  Copyright 2021 Baskaran N
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in all
#  copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#  SOFTWARE.

import hashlib
import json
import os
from datetime import datetime

import boto3
import click
from tqdm import tqdm

"""
General purpose script to copy table items from one table to another. 
The destination table is assumed to already existing and has been setup with the
appropriate sort/partition keys which match the source table.
"""


class DynamoUtils:
    def __init__(self, params):
        self.state_file_path = os.path.join(os.path.expanduser('~'), '.aws_tools', 'dynamo_clone.json')
        self.job_id = hashlib.sha256(str(params).encode()).hexdigest()
        self.params = params
        self.last_state = {}
        self.last_evaluated_key = None
        self.startup_check()

    @staticmethod
    def _now():
        return datetime.now().strftime("%m/%d/%Y %H:%M:%S")

    def startup_check(self):
        os.makedirs(os.path.join(os.path.expanduser("~"), '.aws_tools'), exist_ok=True)
        with open(self.state_file_path, mode='r+') as state_file:
            try:
                self.last_state = json.load(state_file)
            except json.decoder.JSONDecodeError:
                pass

            if self.job_id not in self.last_state:
                self.last_state.update(
                    {self.job_id: {'state': 'NEW', 'start_time': self._now()}})

            elif self.last_state[self.job_id] and self.last_state[self.job_id]['state'] == 'RUNNING':
                if click.confirm(f"Resume from last recorded state at "
                                 f"{self.last_state[self.job_id]['end_time']} ?", default=True):
                    self.last_evaluated_key = self.last_state[self.job_id]['last_evaluated_key']
                else:
                    self.last_state.update(
                        {self.job_id: {'state': 'NEW', 'start_time': self._now()}})

    def clone_dynamodb_table(self):

        dynamo_source_resource = boto3.resource('dynamodb', region_name=self.params.src_region,
                                                aws_access_key_id=self.params.src_access_id,
                                                aws_secret_access_key=self.params.src_access_key)
        dynamo_target_resource = boto3.resource('dynamodb', region_name=self.params.dst_region,
                                                aws_access_key_id=self.params.dst_access_id,
                                                aws_secret_access_key=self.params.dst_access_key)
        source_table_name = self.params.dst_table
        target_table_name = self.params.dst_table

        src_table = dynamo_source_resource.Table(source_table_name)
        dst_table = dynamo_target_resource.Table(target_table_name)
        print(f"Starting copy {src_table} ➡️ {dst_table}")
        try:
            with tqdm(total=src_table.item_count, desc='👨🏼‍💻 copying', unit='Items') as progress_bar:
                with dst_table.batch_writer() as batch:

                    while True:
                        if self.last_evaluated_key:
                            scan_response = src_table.scan(ExclusiveStartKey=self.last_evaluated_key)
                        else:
                            scan_response = src_table.scan()

                            for item in scan_response['Items']:
                                batch.put_item(Item=item)
                            progress_bar.update(1)

                        if 'LastEvaluatedKey' in scan_response:
                            self.last_evaluated_key = scan_response['LastEvaluatedKey']
                            self.last_state[self.job_id].update(
                                {'state': 'RUNNING', 'last_evaluated_key': self.last_evaluated_key,
                                 'end_time': self._now()})

                        else:
                            self.last_state.pop(self.job_id)
                            break

            print("✅ Done")
        finally:
            with open(self.state_file_path, mode='w+', encoding='utf-8') as state_file:
                json.dump(self.last_state, state_file, indent=4, ensure_ascii=False)


def main():
    import argparse

    parser = argparse.ArgumentParser(description='Copy dynamodb table items')
    parser.add_argument("--src-region", required=True)
    parser.add_argument("--dst-region")
    parser.add_argument("--src-table", required=True)
    parser.add_argument("--dst-table")
    parser.add_argument("--src-access-id", required=True)
    parser.add_argument("--src-access-key", required=True)
    parser.add_argument("--dst-access-id")
    parser.add_argument("--dst-access-key")
    args = parser.parse_args()
    DynamoUtils(params=args).clone_dynamodb_table()


if __name__ == '__main__':
    main()
