import base64
import re

import yaml
import kubernetes
from boto3 import Session
from botocore.signers import RequestSigner


class KubeCtl:

    KUBE_FILEPATH = '/tmp/kubeconfig'

    def __init__(self, config, sts_client, eks_client, session: Session, kubes=kubernetes):
        self.config = config
        self.sts = sts_client
        self.eks = eks_client
        self.session = session
        self.region = session.region_name
        self.cluster = config['cluster']
        self.port = config.get('eksport')
        self.kubes = kubes

    def _get_bearer_token(self):
        """
        Get the bearer token from STS

        https://github.com/kubernetes-sigs/aws-iam-authenticator#api-authorization-from-outside-a-cluster

        :return:
        """

        STS_TOKEN_EXPIRES_IN = 180

        client = self.session.client('sts', region_name=self.region)
        service_id = client.meta.service_model.service_id

        signer = RequestSigner(
            service_id,
            self.region,
            'sts',
            'v4',
            self.session.get_credentials(),
            self.session.events
        )

        params = {
            'method': 'GET',
            'url': 'https://sts.{}.amazonaws.com/?Action=GetCallerIdentity&Version=2011-06-15'.format(self.region),
            'body': {},
            'headers': {
                'x-k8s-aws-id': self.cluster
            },
            'context': {}
        }

        signed_url = signer.generate_presigned_url(
            params,
            region_name=self.region,
            expires_in=STS_TOKEN_EXPIRES_IN,
            operation_name=''
        )

        base64_url = base64.urlsafe_b64encode(signed_url.encode('utf-8')).decode('utf-8')

        # remove any base64 encoding padding:
        return 'k8s-aws-v1.' + re.sub(r'=*', '', base64_url)

    def _create_kube_config(self, cluster):
        """
        Create the kubernetes config file with the bearer token for EKS
        """
        token = self._get_bearer_token()
        cluster_info = self.eks.describe_cluster(name=cluster)
        certificate = cluster_info['cluster']['certificateAuthority']['data']
        endpoint = cluster_info['cluster']['endpoint']
        arn = cluster_info['cluster']['arn']

        if self.port is not None:
            endpoint = f'{endpoint}:{self.port}'

        kube_content = dict()
        kube_content['apiVersion'] = 'v1'
        kube_content['clusters'] = [
            {
                'cluster':
                    {
                        'server': endpoint,
                        'certificate-authority-data': certificate
                    },
                'name': arn

            }]
        kube_content['contexts'] = [
            {
                'context':
                    {
                        'cluster': arn,
                        'user': arn
                    },
                'name': arn
            }]
        kube_content['current-context'] = arn
        kube_content['kind'] = 'Config'
        kube_content['preferences'] = dict()
        kube_content['users'] = [
            {
                'name': arn,
                'user': {
                    'auth-provider': {
                        'name': 'gcp',
                        'config': {
                            'access-token': token,
                        }
                    }
                }
            }]

        # Write kubeconfig
        with open(KubeCtl.KUBE_FILEPATH, 'w') as outfile:
            yaml.dump(kube_content, outfile, default_flow_style=False)

    def scale_replicas(self, step, data):
        self._create_kube_config(self.cluster)
        deployments = data['deployment']
        namespace = data['namespace']
        replicas = int(data['replicas'])
        self.kubes.config.load_kube_config(KubeCtl.KUBE_FILEPATH)
        apps_api = self.kubes.client.AppsV1Api()
        for name in deployments:
            deployment = apps_api.read_namespaced_deployment(name=name, namespace=namespace)
            print(name, deployment.spec.replicas, 'replicas')
            if deployment.spec.replicas != replicas:
                deployment.spec.replicas = replicas
                apps_api.patch_namespaced_deployment(name=name, namespace=namespace, body=deployment)
        return "COMPLETE", f"{','.join(deployments)} scaled to {replicas} replicas"

    def check_replicas(self, step, data):
        self._create_kube_config(self.cluster)
        deployments = data['deployment']
        namespace = data['namespace']
        replicas = int(data['replicas'])
        self.kubes.config.load_kube_config(KubeCtl.KUBE_FILEPATH)
        apps_api = self.kubes.client.AppsV1Api()
        for name in deployments:
            deployment = apps_api.read_namespaced_deployment(name=name, namespace=namespace)
            print(deployment)
        return "COMPLETE", f"Checked replicas"

    def scale_down(self, step, data):
        self._create_kube_config(self.cluster)
        deployments = data['deployment']
        namespace = data['namespace']
        self.kubes.config.load_kube_config(KubeCtl.KUBE_FILEPATH)
        apps_api = self.kubes.client.AppsV1Api()
        for name in deployments:
            deployment = apps_api.read_namespaced_deployment(name=name, namespace=namespace)
            print(name, deployment.spec.replicas, 'replicas')
            if deployment.spec.replicas > 0:
                deployment.spec.replicas = 0
                apps_api.patch_namespaced_deployment(name=name, namespace=namespace, body=deployment)
        return "COMPLETE", f"{','.join(deployments)} scaled down to 0 replicas"

    def scale_nodegroup(self, step, data):
        cluster_name = data['cluster_name']
        node_group_name = data['node_group_name']
        min_size = data['min_size']
        max_size = data['max_size']
        desired_size = data['desired_size']
        response = self.eks.update_nodegroup_config(
            clusterName=cluster_name,
            nodegroupName=node_group_name,
            scalingConfig={
                'minSize': min_size,
                'maxSize': max_size,
                'desiredSize': desired_size
            }
        )
        print(response)
        data['eks_update_id'] = response['update']['id']
        status = response['update']['status']
        if 'InProgress' == status:
            return "RUNNING", f'Update nodegroup in progress'
        if 'Failed' == status:
            return "FAILED", f'Update nodegroup failed'
        if 'Cancelled' == status:
            return "FAILED", f'Update nodegroup was cancelled'
        if 'Successful' == status:
            return "COMPLETE", f'Updated nodegroup configuration'
        return "TERMINATE", f'Unknown status {response["update"]["status"]}'

    def wait_for_scale_nodegroup(self, step, data):
        cluster_name = data['cluster_name']
        node_group_name = data['node_group_name']
        eks_update_id = data['eks_update_id']
        response = self.eks.describe_update(
            name=cluster_name,
            updateId=eks_update_id,
            nodegroupName=node_group_name
        )
        print(response)
        status = response['update']['status']
        if 'InProgress' == status:
            return "RETRY", f'Update nodegroup in progress'
        if 'Failed' == status:
            return "FAILED", f'Update nodegroup failed'
        if 'Cancelled' == status:
            return "FAILED", f'Update nodegroup was cancelled'
        if 'Successful' == status:
            return "COMPLETE", f'Updated nodegroup configuration'
        return "TERMINATE", f'Unknown status {response["update"]["status"]}'
