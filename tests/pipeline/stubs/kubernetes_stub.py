class KubernetesConfigStub:
    def load_kube_config(self, path):
        pass


class KubernetesSpecStub:
    def __init__(self, replicas):
        self.replicas = replicas


class KubernetesDeploymentStub:
    def __init__(self, replicas):
        self.spec = KubernetesSpecStub(replicas)


class KubernetesAppsV1ApiStub:
    def __init__(self, replicas):
        self.replicas = replicas

    def read_namespaced_deployment(self, *args, **kwargs):
        return KubernetesDeploymentStub(self.replicas)

    def patch_namespaced_deployment(self, *args, **kwargs):
        return KubernetesDeploymentStub(self.replicas)


class KubernetesClientStub:
    def __init__(self, replicas):
        self.replicas = replicas

    def AppsV1Api(self):
        return KubernetesAppsV1ApiStub(self.replicas)


class KubernetesStub:
    def __init__(self, replicas):
        self.config = KubernetesConfigStub()
        self.client = KubernetesClientStub(replicas)
