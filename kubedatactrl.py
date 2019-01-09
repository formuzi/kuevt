import logging
import os
import traceback
import pykka
import ConfigParser


from kubernetes import client, config, watch
from neoclient import Neo4jClient

conf = ConfigParser.ConfigParser()
logging.basicConfig(filename='debug.log', level=logging.DEBUG, format='%(asctime)s %(levelname)s: %(message)s')
LOG = logging.getLogger(__name__)


class KubeDataController(object):
    def __init__(self):
        """
        loads authentication and cluster information and init API
        this is simple to load config file from minikube
        use configuration.api_key['authorization'] = $token for API key
        """
        if 'KUBERNETES_PORT' in os.environ:
            config.load_incluster_config()
        else:
            config.load_kube_config()
        self.v1api = client.CoreV1Api()
        self.v1ext = client.ExtensionsV1beta1Api()
        self.conf = ConfigParser.ConfigParser()
        conf.read('config.ini')
        self.neoclient = Neo4jClient(conf.get('neo4j', 'connect_url'), conf.get('neo4j', 'user'),
                                     conf.get('neo4j', 'password'))


    def watch_node(self):
        """
        watch node event and create record to neo4j database
        :return:
        """
        w = watch.Watch()
        for event in w.stream(self.v1api.list_node):
            evt_type = event['type']
            if 'ADDED' == evt_type:
                # extract properties from Event
                props = self._build_base_props(event)
                # create or update Node to neo4j database
                self.neoclient.create_node(props)
            else:
                # skip Node update/delete action for now
                pass

    def watch_namespace(self):
        """
        watch namespaces and create to neo4j database
        :return:
        """
        w = watch.Watch()
        for event in w.stream(self.v1api.list_namespace):
            evt_type = event['type']
            if evt_type in ['ADDED', 'MODIFIED']:
                # extract properties from Event
                props = self._build_base_props(event)
                props['status'] = event['object'].status.phase
                # create or update Node to database
                self.neoclient.upsert_namespace(props)
            else:
                # skip namespace delete for now
                pass

    def watch_pod(self):
        """
        watch pod create/update/delete event and operate neo4j database record accordingly
        :return:
        """
        w = watch.Watch()
        for event in w.stream(self.v1api.list_pod_for_all_namespaces):
            # get objects which could have relationship with pod
            labels = event['object'].metadata.labels
            namespace = event['object'].metadata.namespace
            node_name = event['object'].spec.node_name
            evt_type = event['type']
            if evt_type in ['ADDED', 'MODIFIED']:
                # extract properties from Event
                props = self._build_base_props(event)
                props['status'] = event['object'].status.phase
                props['pod_ip'] = event['object'].status.pod_ip
                self.neoclient.upsert_pod(props, labels=labels, namespace=namespace, node_name=node_name)
            elif 'DELETED' == evt_type:
                self.neoclient.remove_pod(event['object'].metadata.name)

    def watch_deployment(self):
        """
        watch deployment object and CUD db record
        :return:
        """
        w = watch.Watch()
        for event in w.stream(self.v1ext.list_deployment_for_all_namespaces):
            labels = event['object'].metadata.labels
            namespace = event['object'].metadata.namespace
            selector = event['object'].spec.selector.match_labels
            evt_type = event['type']
            if evt_type in ['ADDED', 'MODIFIED']:
                # extract properties from Event
                props = self._build_base_props(event)
                props['replicas'] = event['object'].spec.replicas
                props['ready_replicas'] = event['object'].status.ready_replicas if event['object'].status.ready_replicas else 0
                self.neoclient.upsert_deployment(props, labels=labels, namespace=namespace, selector=selector)
            elif 'DELETED' == evt_type:
                self.neoclient.remove_deployment(event['object'].metadata.name)


    def _build_base_props(self, event):
        """
        extract common properties from event object
        :param event: Watcher stream event
        :return: dictionary of props
        """
        props = {}
        obj = event['object']
        props['name'] = obj.metadata.name
        props['kind'] = obj.kind
        props['creation_time'] = obj.metadata.creation_timestamp.strftime("%Y-%m-%d %H:%M")
        return props


class EventActor(pykka.ThreadingActor):
    def __init__(self, controller):
        super(EventActor, self).__init__()
        self.controller = controller

    def on_receive(self, message):
        try:
            if message.get('type') == 'namespace':
                self.controller.watch_namespace()
            elif message.get('type') == 'node':
                self.controller.watch_node()
            elif message.get('type') == 'pod':
                self.controller.watch_pod()
            elif message.get('type') == 'deployment':
                self.controller.watch_deployment()
            else:
                # skip for now
                return None
        except Exception as e:
            LOG.exception('Error occur when process event %s' % traceback.format_exc())
            raise Exception('Event error %s' % e.message)


if __name__ == '__main__':
    ctrl = KubeDataController()
    try:
        for evt in ['node', 'namespace', 'pod', 'deployment']:
            actor = EventActor(ctrl).start(ctrl)
            actor.tell({'type': evt})

    except Exception as e:
        LOG.exception('Exception occur %s' % e.message)
        # Clean up
        pykka.ActorRegistry.stop_all()

