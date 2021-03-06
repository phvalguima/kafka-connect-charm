# Copyright 2021 pguimaraes
# See LICENSE file for licensing details.

import unittest
from mock import patch
from mock import PropertyMock

import base64

import src.charm as charm
from ops.testing import Harness

import wand.contrib.java as java
import wand.apps.kafka as kafka
import wand.apps.relations.kafka_mds as kafka_mds
import wand.apps.relations.kafka_listener as kafka_listener
import wand.apps.relations.kafka_schema_registry as kafka_sr
import wand.apps.relations.kafka_connect as kafka_connect
import wand.apps.relations.relation_manager_base as kafka_rel_base

TO_PATCH_LINUX = [
    "userAdd",
    "groupAdd"
]

TO_PATCH_FETCH = [
    'apt_install',
    'apt_update',
    'add_source'
]

TO_PATCH_HOST = [
    'service_resume',
    'service_running',
    'service_restart',
    'service_reload'
]

CONFIG_CHANGED="""
bootstrap.servers=ansiblebroker1.example.com:9092
config.storage.replication.factor=3
config.storage.topic=connect-cluster-configs
confluent.license.topic=_confluent-command
confluent.metadata.basic.auth.user.info=connect_worker:password123
confluent.metadata.bootstrap.server.urls=https://ansiblebroker1.example.com:8090
confluent.metadata.http.auth.credentials.provider=BASIC
confluent.monitoring.interceptor.topic=_confluent-monitoring
connector.client.config.override.policy=All
consumer.bootstrap.servers=ansiblebroker1.example.com:9092
consumer.confluent.monitoring.interceptor.bootstrap.servers=ansiblebroker1.example.com:9092
consumer.confluent.monitoring.interceptor.sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required username="connect_worker" password="password123" metadataServerUrls="https://ansiblebroker1.example.com:8090";
consumer.confluent.monitoring.interceptor.sasl.login.callback.handler.class=io.confluent.kafka.clients.plugins.auth.token.TokenUserLoginCallbackHandler
consumer.confluent.monitoring.interceptor.sasl.mechanism=OAUTHBEARER
consumer.confluent.monitoring.interceptor.security.protocol=SASL_SSL
consumer.confluent.monitoring.interceptor.ssl.truststore.location=/var/ssl/private/kafka_connect.truststore.jks
consumer.confluent.monitoring.interceptor.ssl.truststore.password=confluentkeystorestorepass
consumer.interceptor.classes=io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor
consumer.sasl.login.callback.handler.class=io.confluent.kafka.clients.plugins.auth.token.TokenUserLoginCallbackHandler
consumer.sasl.mechanism=OAUTHBEARER
consumer.security.protocol=SASL_SSL
consumer.ssl.key.password=confluentkeystorestorepass
consumer.ssl.keystore.location=/var/ssl/private/kafka_connect.keystore.jks
consumer.ssl.keystore.password=confluentkeystorestorepass
consumer.ssl.truststore.location=/var/ssl/private/kafka_connect.truststore.jks
consumer.ssl.truststore.password=confluentkeystorestorepass
group.id=connect-cluster
internal.key.converter=org.apache.kafka.connect.json.JsonConverter
internal.key.converter.schemas.enable=false
internal.value.converter=org.apache.kafka.connect.json.JsonConverter
internal.value.converter.schemas.enable=false
key.converter=io.confluent.connect.avro.AvroConverter
key.converter.schema.registry.ssl.key.password=confluentkeystorestorepass
key.converter.schema.registry.ssl.keystore.location=/var/ssl/private/kafka_connect.keystore.jks
key.converter.schema.registry.ssl.keystore.password=confluentkeystorestorepass
key.converter.schema.registry.ssl.truststore.location=/var/ssl/private/kafka_connect.truststore.jks
key.converter.schema.registry.ssl.truststore.password=confluentkeystorestorepass
key.converter.schema.registry.url=https://ansibleschemaregistry1.example.com:8081
listeners=https://0.0.0.0:8083
listeners.https.ssl.key.password=confluentkeystorestorepass
listeners.https.ssl.keystore.location=/var/ssl/private/kafka_connect.keystore.jks
listeners.https.ssl.keystore.password=confluentkeystorestorepass
listeners.https.ssl.truststore.location=/var/ssl/private/kafka_connect.truststore.jks
listeners.https.ssl.truststore.password=confluentkeystorestorepass
offset.flush.interval.ms=10000
offset.storage.replication.factor=3
offset.storage.topic=connect-cluster-offsets
plugin.path=/usr/share/java
producer.bootstrap.servers=ansiblebroker1.example.com:9092
producer.confluent.monitoring.interceptor.bootstrap.servers=ansiblebroker1.example.com:9092
producer.confluent.monitoring.interceptor.sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required username="connect_worker" password="password123" metadataServerUrls="https://ansiblebroker1.example.com:8090";
producer.confluent.monitoring.interceptor.sasl.login.callback.handler.class=io.confluent.kafka.clients.plugins.auth.token.TokenUserLoginCallbackHandler
producer.confluent.monitoring.interceptor.sasl.mechanism=OAUTHBEARER
producer.confluent.monitoring.interceptor.security.protocol=SASL_SSL
producer.confluent.monitoring.interceptor.ssl.truststore.location=/var/ssl/private/kafka_connect.truststore.jks
producer.confluent.monitoring.interceptor.ssl.truststore.password=confluentkeystorestorepass
producer.interceptor.classes=io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor
producer.sasl.login.callback.handler.class=io.confluent.kafka.clients.plugins.auth.token.TokenUserLoginCallbackHandler
producer.sasl.mechanism=OAUTHBEARER
producer.security.protocol=SASL_SSL
producer.ssl.key.password=confluentkeystorestorepass
producer.ssl.keystore.location=/var/ssl/private/kafka_connect.keystore.jks
producer.ssl.keystore.password=confluentkeystorestorepass
producer.ssl.truststore.location=/var/ssl/private/kafka_connect.truststore.jks
producer.ssl.truststore.password=confluentkeystorestorepass
public.key.path=/var/ssl/private/public.pem
rest.advertised.host.name=ansibleconnect1.example.com
rest.advertised.listener=https
rest.advertised.port=8083
rest.extension.classes=io.confluent.connect.security.ConnectSecurityExtension,io.confluent.connect.secretregistry.ConnectSecretRegistryExtension
rest.port=8083
rest.servlet.initializor.classes=io.confluent.common.security.jetty.initializer.InstallBearerOrBasicSecurityHandler
sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required username="connect_worker" password="password123" metadataServerUrls="https://ansiblebroker1.example.com:8090";
sasl.login.callback.handler.class=io.confluent.kafka.clients.plugins.auth.token.TokenUserLoginCallbackHandler
sasl.mechanism=OAUTHBEARER
security.protocol=SASL_SSL
ssl.key.password=confluentkeystorestorepass
ssl.keystore.location=/var/ssl/private/kafka_connect.keystore.jks
ssl.keystore.password=confluentkeystorestorepass
ssl.truststore.location=/var/ssl/private/kafka_connect.truststore.jks
ssl.truststore.password=confluentkeystorestorepass
status.storage.replication.factor=3
status.storage.topic=connect-cluster-status
value.converter=io.confluent.connect.avro.AvroConverter
value.converter.schema.registry.ssl.key.password=confluentkeystorestorepass
value.converter.schema.registry.ssl.keystore.location=/var/ssl/private/kafka_connect.keystore.jks
value.converter.schema.registry.ssl.keystore.password=confluentkeystorestorepass
value.converter.schema.registry.ssl.truststore.location=/var/ssl/private/kafka_connect.truststore.jks
value.converter.schema.registry.ssl.truststore.password=confluentkeystorestorepass
value.converter.schema.registry.url=https://ansibleschemaregistry1.example.com:8081""" # noqa


class TestCharm(unittest.TestCase):
    maxDiff = None

    def _patch(self, obj, method):
        _m = patch.object(obj, method)
        mock = _m.start()
        self.addCleanup(_m.stop)
        return mock

    def _simulate_render(self, ctx=None, templ_file=""):
        import jinja2
        env = jinja2.Environment(loader=jinja2.FileSystemLoader('templates'))
        templ = env.get_template(templ_file)
        doc = templ.render(ctx)
        return doc

    def setUp(self):
        super().setUp()

    @patch.object(charm, "open_port")
    @patch.object(kafka, "open_port")
#    @patch.object(kafka_rel_base, "binding_addr",
#                  new_callable=PropertyMock)
    @patch.object(kafka_rel_base.RelationManagerBase, "advertise_addr",
                  new_callable=PropertyMock)
    @patch.object(charm.KafkaConnectCharm,
                  "render_service_override_file",
                  new_callable=PropertyMock)
    # Mock the password generation method and replace for the same pwd
    @patch.object(java, "genRandomPassword")
    @patch.object(charm, "genRandomPassword")
    @patch.object(charm.KafkaConnectCharm, "_generate_keystores")
    @patch.object(charm, "service_running")
    @patch.object(charm, "service_reload")
    @patch.object(charm, "service_restart")
    @patch.object(charm, "service_resume")
    @patch.object(kafka_connect.KafkaConnectProvidesRelation,
                  "advertise_addr",
                  new_callable=PropertyMock)
    # Ignore this method since it just copies the key content to the file
    @patch.object(kafka_mds.KafkaMDSRequiresRelation, "get_public_key",
                  new_callable=PropertyMock)
    # Needed for the host.name parameter
    @patch.object(charm, "get_hostname")
    # Needed for the host.name parameter
    @patch.object(kafka, "get_hostname")
    @patch.object(charm, "CreateTruststore")
    # Ignore any set_TLS_auth calls as it is not relevant for this check
    @patch.object(kafka_sr.KafkaSchemaRegistryRequiresRelation,
                  "set_TLS_auth",
                  new_callable=PropertyMock)
    @patch.object(kafka_listener.KafkaListenerRequiresRelation,
                  "set_TLS_auth",
                  new_callable=PropertyMock)
    @patch.object(kafka.KafkaJavaCharmBase, "set_folders_and_permissions",
                  new_callable=PropertyMock)
    @patch.object(kafka.KafkaJavaCharmBase, "install_packages",
                  new_callable=PropertyMock)
    @patch.object(kafka.KafkaJavaCharmBase, "_on_install",
                  new_callable=PropertyMock)
    @patch.object(base64, "b64decode")
    @patch.object(charm, "render")
    def test_config_changed_no_conn_rel(self,
                                        mock_render,
                                        mock_b64_decode,
                                        mock_install,
                                        mock_install_pkgs,
                                        mock_set_folders_perms,
                                        mock_set_tls_auth,
                                        mock_sr_set_tls_auth,
                                        mock_create_ts,
                                        mock_get_hostname_kafka,
                                        mock_get_hostname,
                                        mock_get_public_key,
                                        mock_advertise_addr,
                                        mock_svc_resume,
                                        mock_svc_restart,
                                        mock_svc_reload,
                                        mock_svc_running,
                                        mock_gen_jks,
                                        mock_gen_pwd,
                                        mock_java_gen_pwd,
                                        mock_render_svc_override,
                                        mock_kafka_advertise_addr,
                                        mock_kafka_open_port,
                                        mock_open_port):
        # Certs will use the b64decode method
        mock_b64_decode.return_value = b'aaaa'
        mock_gen_pwd.return_value = "confluentkeystorestorepass"
        mock_java_gen_pwd.return_value = "confluentkeystorestorepass"
        mock_get_hostname.return_value = "ansibleconnect1.example.com"
        mock_get_hostname_kafka.return_value = mock_get_hostname.return_value
        self.harness = Harness(charm.KafkaConnectCharm)
        #
        # CONFIG
        #
        self.harness.update_config({
            "user": "test",
            "group": "test",
            "keystore-path": "/var/ssl/private/kafka_connect.keystore.jks",
            "truststore-path": "/var/ssl/private/kafka_connect.truststore.jks",
            "keystore-sr-path": "/var/ssl/private/kafka_connect.keystore.jks",
            "truststore-sr-path": "/var/ssl/private/kafka_connect.truststore.jks", # noqa
            "listener-keystore-path": "/var/ssl/private/kafka_connect.keystore.jks", # noqa
            "listener-truststore-path": "/var/ssl/private/kafka_connect.truststore.jks", # noqa
            "sasl-protocol": "LDAP",
            "mds_public_key_path": "/var/ssl/private/public.pem",
            "mds_user": "connect_worker",
            "mds_password": "password123",
            "confluent_license_topic": "_confluent-command",
            "ssl_cert": "SSL_CERT",
            "ssl_key": "SSL_KEY",
            "ssl_sr_cert": "SSL_CERT",
            "ssl_sr_key": "SSL_KEY",
            "ssl_listener_cert": "SSL_CERT",
            "ssl_listener_key": "SSL_KEY",
            "connect-distributed-properties": """  rest.extension.classes: "io.confluent.connect.security.ConnectSecurityExtension,io.confluent.connect.secretregistry.ConnectSecretRegistryExtension"
  rest.servlet.initializor.classes: io.confluent.common.security.jetty.initializer.InstallBearerOrBasicSecurityHandler
  status.storage.replication.factor: 3
  status.storage.topic: connect-cluster-status
  offset.flush.interval.ms: 10000
  offset.storage.replication.factor: 3
  offset.storage.topic: connect-cluster-offsets
  config.storage.replication.factor: 3
  config.storage.topic: connect-cluster-configs
""" # noqa
        })
        # MDS RELATION SETUP
        mds_id = self.harness.add_relation("mds", "broker")
        self.harness.add_relation_unit(mds_id, "broker/0")
        self.harness.update_relation_data(mds_id, "broker", {
            "public-key": "abc"
        })
        self.harness.update_relation_data(mds_id, "broker/0", {
            "mds_url": "https://ansiblebroker1.example.com:8090"
        })
        # LISTENER RELATION SETUP
        lst_id = self.harness.add_relation("listeners", "broker")
        self.harness.add_relation_unit(lst_id, 'broker/0')
        self.harness.update_relation_data(lst_id, 'broker/0', {
            "tls_cert": "certcert",
            "bootstrap-data": '''{ "kafka_connect_charm": {
                "bootstrap_server": "ansiblebroker1.example.com:9092",
                "cert_present": true,
                "sasl_present": true,
                "secprot": "SASL_SSL",
                "SASL": {
                  "jaas.config": "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required username=\\\"connect_worker\\\" password=\\\"password123\\\" metadataServerUrls=\\\"https://ansiblebroker1.example.com:8090\\\";",
                  "protocol": "OAUTHBEARER",
                  "publicKeyPath": "filepath",
                  "publicKey": "base64-public-key",
                  "confluent": {
                    "login.callback": "io.confluent.kafka.clients.plugins.auth.token.TokenUserLoginCallbackHandler"
                  }
                },
                "cert": "certcert"
            }}''' # noqa
        })
        # SCHEMA REGISTRY RELATION SETUP
        sr_id = self.harness.add_relation("schemaregistry", "sr")
        self.harness.add_relation_unit(sr_id, 'sr/0')
        self.harness.update_relation_data(sr_id, 'sr', {
            "url": "https://ansibleschemaregistry1.example.com:8081",
            "converter": "io.confluent.connect.avro.AvroConverter"
        })
        self.harness.update_relation_data(sr_id, 'sr/0', {
            "tls_cert": "certcert"
        })
        # CONFLUENT CENTER RELATION SETUP
        c3_id = self.harness.add_relation("c3", "c3")
        self.harness.add_relation_unit(c3_id, 'c3/0')
        self.harness.update_relation_data(c3_id, 'c3/0', {
            "bootstrap-server": "ansiblebroker1.example.com:9092"
        })
        self.harness.set_leader(True)
        self.harness.begin_with_initial_hooks()
        connect = self.harness.charm
        self.addCleanup(self.harness.cleanup)
        # If cluster relation events (-joined, -changed) happens before
        # certificate events, then cluster-* will be deferred.
        # Run reemit to ensure they are run.
        connect.framework.reemit()
        dist_props = connect._render_connect_distribute_properties()
        print(dist_props)
        # Check if CreateTruststore was called because
        # of missing connect relation
        mock_create_ts.assert_called()
        simulate_render = self._simulate_render(
            ctx={
                "dist_props": dist_props
            },
            templ_file='connect-distributed.properties.j2')
        # simulate_render = "\n".join(sorted(simulate_render.split("\n")))
        print(simulate_render)
        SERVER_PROPS = CONFIG_CHANGED.split("\n")
        SERVER_PROPS.sort()
        distributed_props = simulate_render.split("\n")
        distributed_props.sort()
        self.assertEqual(SERVER_PROPS, distributed_props)
#        self.assertSetEqual(
#            set(CONFIG_CHANGED.split("\n")),
#            set(simulate_render.split("\n")))
