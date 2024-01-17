import boto3
import os
import json
import sys
import time
from datetime import datetime
import uuid


client = boto3.client('stepfunctions', region_name='ap-southeast-2')

state_machine_arn = os.getenv('state_machine_arn')
name = os.getenv('name')
steps = os.getenv('steps')

today = datetime.now().strftime('%Y-%m-%d')
uid = str(uuid.uuid4())
execution_name = f'{today}-{name}-{uid}'
step_input = {'steps': steps}

execution = client.start_execution(
    stateMachineArn=state_machine_arn,
    input=json.dumps(step_input),
    name=execution_name
)
execution_arn = execution['executionArn']

print('Execution', execution_arn, flush=True)
print('Execution Steps', steps, flush=True)

execution = client.describe_execution(executionArn=execution_arn)
print('Execution status', execution.get('status'), flush=True)
while execution.get('status') not in ('SUCCEEDED', 'FAILED', 'TIMED_OUT', 'ABORTED'):
    time.sleep(120)
    execution = client.describe_execution(executionArn=execution_arn)
    print('Execution status', execution.get('status'), flush=True)

if execution.get('status') == "SUCCEEDED":
    sys.exit(0)
else:
    print('Error', execution.get('error'), flush=True)
    print('Cause', execution.get('cause'), flush=True)
    sys.exit(1)
