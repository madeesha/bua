import boto3

from bua.actions.kube import KubeCtl

if __name__ == '__main__':
    session = boto3.Session(region_name='ap-southeast-2')
    sts = session.client('sts')
    eks = session.client('eks')
    config = {
        'region': session.region_name,
        'cluster': 'anstead',
        'eksport': 16443
    }
    kube = KubeCtl(config, sts, eks, session)
    step = {}
    data = {
        'deployment': [ 'meterdata' ],
        'namespace': 'core'
    }
    kube.scale_down(step, data)
