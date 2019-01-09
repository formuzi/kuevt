import logging
import traceback
import time

from neo4j.v1 import GraphDatabase, ServiceUnavailable

LOG = logging.getLogger(__name__)


class Neo4jClient:
    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def create_node(self, props):
        """
        create node
        :param props: properties of node like name, creation time
        :return: True if success or False when exceptions
        """
        try:
            with self._driver.session() as session:
                session.write_transaction(lambda tx: tx.run(
                    "MERGE (:Node {name: $name, kind: $kind, created: $created})",
                    name=props['name'], kind=props['kind'], created=props['creation_time']))
            return True
        except ServiceUnavailable:
            LOG.exception('failed to create Node to database %s, %s' % (props['name'], traceback.format_exc()))
            return False

    def upsert_namespace(self, props):
        """
        create and update namespace
        :param props: properties of namespace
        :return: True if success or False when exceptions
        """
        try:
            with self._driver.session() as session:
                session.write_transaction(lambda tx: tx.run(
                    "MERGE (:Namespace {name: $name, kind: $kind, created: $created})",
                    name=props['name'], kind=props['kind'], created=props['creation_time']))
                if props['status']:
                    session.write_transaction(lambda tx: tx.run(
                        "MATCH (n:Namespace {name: $name}) "
                        "SET n.status = $status", name=props['name'], status=props['status']))
            return True
        except ServiceUnavailable:
            LOG.exception(
                'failed to create/update Namespace to database %s, %s' % (props['name'], traceback.format_exc()))
            return False

    def upsert_pod(self, props, **kwargs):
        """
        create or update pod
        :param args: properties include name, pod_ip, status
                     **kwargs are relationships of pod
                     Pod -- runsOn --> Node
                     Pod -- associateTo --> Namespace
        :return:
        """
        try:
            with self._driver.session() as session:
                # create pod
                session.write_transaction(lambda tx: tx.run(
                    "MERGE (:Pod {name: $name, kind: $kind, created: $created})",
                    name=props['name'], kind=props['kind'], created=props['creation_time']))

                # update pod IP, status after finish scheduling
                if props['pod_ip'] and props['status']:
                    session.write_transaction(lambda tx: tx.run(
                        "MATCH (p:Pod {name: $name}) "
                        "SET p.status = $status, p.pod_ip=$pod_ip", name=props['name'], pod_ip=props['pod_ip'],
                        status=props['status']))

                labels = kwargs.get('labels')
                if labels:
                    label_property = []
                    for key, value in labels.items():
                        label_property.append('%s=%s' % (key, value))
                    session.write_transaction(self.update_pod_label, props['pod_ip'], label_property)

                    # link to deployment if pod created by deployment
                    deploys = session.read_transaction(self.get_deployments)
                    if deploys:
                        for dp in deploys:
                            selector = dp['selector']
                            if selector:
                                condition = 'WHERE '
                                for i in range(len(selector)):
                                    condition += '"%s" in p.labels ' % selector[i]
                                    if i < len(selector) - 1:
                                        condition += "AND "
                                session.write_transaction(lambda tx: tx.run(
                                    "MATCH (d:Deployment {name: $name}) "
                                    "MATCH (p:Pod) %s"
                                    "MERGE (d)-[:scheduledTo]->(p)" % condition, name=dp['name']
                                ))

                node_name = kwargs.get('node_name')
                if node_name:
                    # link pod with node
                    session.write_transaction(self.add_pod_to_node, props['pod_ip'], node_name)
                namespace = kwargs.get('namespace')
                if namespace:
                    # link pod with namespace
                    session.write_transaction(self.add_pod_to_namespace, props['pod_ip'], namespace)
            return True
        except Exception:
            LOG.exception('failed to create/update Pod %s, %s' % (props['name'], traceback.format_exc()))
            return False

    def remove_pod(self, pod_name):
        """
        remove pod from database if got delete event
        :param pod_name: pod unique name
        :return:
        """
        try:
            with self._driver.session() as session:
                session.write_transaction(lambda tx: tx.run(
                    "MATCH (p:Pod {name: $name}) "
                    "DETACH DELETE p", name=pod_name
                ))
            return True
        except Exception:
            LOG.exception('failed to remove Pod %s, %s' % (pod_name, traceback.format_exc()))
            return False

    def upsert_deployment(self, props, **kwargs):
        """
        create or update deployment
        :param args: properties include name, replica, ready_replicas
                     **kwargs are relationships
                     Deployment -- scheduleTo --> Pod
                     Deployment -- associateTo --> Namespace
        :return:
        """
        try:
            with self._driver.session() as session:
                # create deployment
                session.write_transaction(lambda tx: tx.run(
                    "MERGE (:Deployment {name: $name, kind: $kind, created: $created})",
                    name=props['name'], kind=props['kind'], created=props['creation_time']))

                if props['ready_replicas'] and props['replicas']:
                    session.write_transaction(lambda tx: tx.run(
                        "MATCH (d:Deployment {name: $name}) "
                        "SET d.replicas = $replicas, d.ready_replicas = $ready_replicas", name=props['name'],
                        replicas=props['replicas'], ready_replicas=props['ready_replicas']
                    ))
                labels = kwargs.get('labels')
                if labels:
                    label_property = []
                    for key, value in labels.items():
                        label_property.append('%s=%s' % (key, value))
                    session.write_transaction(lambda tx: tx.run(
                        "MATCH (d:Deployment {name: $name}) "
                        "SET d.labels = $labels", name=props['name'], labels=label_property
                    ))
                namespace = kwargs.get('namespace')
                if namespace:
                    # link deploy with namespace
                    session.write_transaction(lambda tx: tx.run(
                        "MATCH (d:Deployment {name: $name}) "
                        "MATCH (n:Namespace {name: $namespace}) "
                        "MERGE (d)-[:associateTo]->(n)", name=props['name'], namespace=namespace
                    ))
                selector = kwargs.get('selector')
                if selector:
                    selector_property = []
                    for key, value in selector.items():
                        selector_property.append('%s=%s' % (key, value))
                    session.write_transaction(lambda tx: tx.run(
                        "MATCH (d:Deployment {name: $name}) "
                        "SET d.selector = $selector", name=props['name'], selector=selector_property
                    ))

            return True
        except Exception:
            LOG.exception('failed to create/update Deployment %s, %s' % (props['name'], traceback.format_exc()))
            return False


    def remove_deployment(self, name):
        """
        remove deployment from database if got delete event
        :param name: deployment name
        :return:
        """
        try:
            with self._driver.session() as session:
                session.write_transaction(lambda tx: tx.run(
                    "MATCH (d:Deployment {name: $name}) "
                    "DETACH DELETE d", name=name
                ))
            return True
        except Exception:
            LOG.exception('failed to remove deployment %s, %s' % (name, traceback.format_exc()))
            return False

    @staticmethod
    def get_deployments(tx):
        return list(tx.run("MATCH (d:Deployment) return d.name as name, d.selector as selector"))

    @staticmethod
    def update_pod_label(tx, pod_ip, labels):
        """
        update pod label
        :param tx:
        :param pod_ip: pod uniq ip
        :param labels array of label
        :return: none
        """
        tx.run("MATCH (p:Pod {pod_ip: $pod_ip}) "
               "SET p.labels = $labels",
               pod_ip=pod_ip, labels=labels)

    @staticmethod
    def add_pod_to_node(tx, pod_ip, node_name):
        """
        create pod -> node relationship if not exist
        :param tx:
        :param pod_ip: pod uniq ip
        :param node_name:
        :return: none
        """
        tx.run("MATCH (p:Pod {pod_ip: $pod_ip}) "
               "MATCH (n:Node {name: $node_name}) "
               "MERGE (p)-[:runsOn]->(n)",
               pod_ip=pod_ip, node_name=node_name)

    @staticmethod
    def add_pod_to_namespace(tx, pod_ip, namespace):
        """
        create pod -> namespace relationship if missing
        :param tx:
        :param pod_ip:
        :param namespace:
        :return:
        """
        tx.run("MATCH (p:Pod {pod_ip: $pod_ip}) "
               "MATCH (n:Namespace {name: $namespace}) "
               "MERGE (p)-[:associateTo]->(n)",
               pod_ip=pod_ip, namespace=namespace)