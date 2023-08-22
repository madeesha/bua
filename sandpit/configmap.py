import subprocess
from typing import List, Dict

import yaml

if __name__ == '__main__':
    process = subprocess.run(
        ["aws", "sts", "get-caller-identity"], capture_output=True
    )
    response: Dict = yaml.load(process.stdout, yaml.Loader)
    arn = response['Arn']
    process = subprocess.run(
        ["kubectl", "-n", "kube-system", "get", "configmap/aws-auth", "-o", "yaml"], capture_output=True)
    response = yaml.load(process.stdout, yaml.Loader)
    with open('eks/aws-auth-orig.yml', 'w') as fp:
        yaml.dump(response, fp)
    map_roles: List[Dict] = yaml.load(response['data']['mapRoles'], yaml.Loader)
    for entry in map_roles:
        if entry['rolearn'] == arn:
            with open('eks/aws-auth-new.yml', 'w') as fp:
                yaml.dump(response, fp)
            exit(0)
    map_roles.append({
        'groups': ['system:masters'],
        'rolearn': arn,
        'username': 'Admin'
    })
    response['data']['mapRoles'] = yaml.dump(map_roles)
    with open('eks/aws-auth-new.yml', 'w') as fp:
        yaml.dump(response, fp)
