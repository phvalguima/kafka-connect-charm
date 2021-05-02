#!/usr/bin/env python3
# Copyright 2021 pguimaraes
# See LICENSE file for licensing details.

import base64
import logging
import yaml

from ops.main import main
from ops.model import (
    BlockedStatus,
    ActiveStatus,
    MaintenanceStatus
)

from charmhelpers.core.templating import render
from charmhelpers.core.host import (
    service_resume,
    service_running,
    service_restart,
    service_reload
)

from wand.apps.relations.tls_certificates import (
    TLSCertificateRequiresRelation
)
from wand.apps.kafka import KafkaJavaCharmBase
from wand.apps.relations.kafka_mds import (
    KafkaMDSRequiresRelation
)
from wand.apps.relations.kafka_listener import (
    KafkaListenerRequiresRelation,
    KafkaListenerRelationNotSetError
)
from wand.apps.relations.kafka_schema_registry import (
    KafkaSchemaRegistryRequiresRelation
)
from wand.apps.relations.kafka_connect import (
    KafkaConnectProvidesRelation,
    KafkaConnectRelationNotUsedError
)
from wand.apps.relations.kafka_relation_base import (
    KafkaRelationBaseTLSNotSetError
)
from wand.security.ssl import PKCS12CreateKeystore
from wand.security.ssl import genRandomPassword

logger = logging.getLogger(__name__)


class KafkaConnectCharmUnsupportedParamError(Exception):

    def __init__(self, message):
        super().__init__(message)


class KafkaConnectCharmNotValidOptionSetError(Exception):

    def __init__(self, option):
        super().__init__("Option {} has no valid content".format(option))


class KafkaConnectCharmMissingRelationError(Exception):

    def __init__(self, relation_name):
        super().__init__("Missing relation to: {}".format(relation_name))


class KafkaConnectCharm(KafkaJavaCharmBase):

    CONFLUENT_PACKAGES = [
        "confluent-common",
        "confluent-rest-utils",
        "confluent-metadata-service",
        "confluent-ce-kafka-http-server",
        "confluent-kafka-rest",
        "confluent-server-rest",
        "confluent-telemetry",
        "confluent-server",
        "confluent-hub-client",
        "confluent-kafka-connect-replicator",
        "confluent-security",
        "confluent-rebalancer",
        "confluent-control-center-fe",
        "confluent-control-center",
        "confluent-schema-registry"
    ]

    def __init__(self, *args):
        super().__init__(*args)
        self.framework.observe(self.on.install, self._on_install)
        self.framework.observe(self.on.config_changed,
                               self._on_config_changed)
        self.framework.observe(self.on.listeners_relation_joined,
                               self.on_listeners_relation_joined)
        self.framework.observe(self.on.listeners_relation_changed,
                               self.on_listeners_relation_changed)
        self.framework.observe(self.on.schemaregistry_relation_joined,
                               self.on_schemaregistry_relation_joined)
        self.framework.observe(self.on.schemaregistry_relation_changed,
                               self.on_schemaregistry_relation_changed)
        self.framework.observe(self.on.mds_relation_joined,
                               self.on_mds_relation_joined)
        self.framework.observe(self.on.mds_relation_changed,
                               self.on_mds_relation_changed)
        self.framework.observe(self.on.certificates_relation_joined,
                               self.on_certificates_relation_joined)
        self.framework.observe(self.on.certificates_relation_changed,
                               self.on_certificates_relation_changed)
        self.framework.observe(self.on.update_status,
                               self._on_update_status)
        self.framework.observe(self.on.connect_relation_joined,
                               self.on_connect_relation_joined)
        self.framework.observe(self.on.connect_relation_changed,
                               self.on_connect_relation_changed)
        # Relation Managers
        self.listener = KafkaListenerRequiresRelation(
            self, 'listeners')
        self.sr = KafkaSchemaRegistryRequiresRelation(
            self, 'schemaregistry')
        self.certificates = \
            TLSCertificateRequiresRelation(self, 'certificates')
        self.connect = KafkaConnectProvidesRelation(self, 'connect')
        self.mds = KafkaMDSRequiresRelation(self, "mds")
        self.get_ssl_methods_list = [
            self.get_ssl_cert, self.get_ssl_key,
            self.get_ssl_listener_cert, self.get_ssl_listener_key,
            self.get_ssl_schemaregistry_cert,
            self.get_ssl_schemaregistry_key]
        self.ks.set_default(ssl_cert="")
        self.ks.set_default(ssl_key="")
        self.ks.set_default(ssl_listener_cert="")
        self.ks.set_default(ssl_listener_key="")
        self.ks.set_default(ks_listener_pwd=genRandomPassword())
        self.ks.set_default(ts_listener_pwd=genRandomPassword())
        self.ks.set_default(ssl_schemaregistry_cert="")
        self.ks.set_default(ssl_schemaregistry_key="")
        self.ks.set_default(ks_schemaregistry_pwd=genRandomPassword())
        self.ks.set_default(ts_schemaregistry_pwd=genRandomPassword())
        self.ks.set_default(has_exception="")
        self.ks.set_default(listener_plaintext_pwd=genRandomPassword(24))

    def _check_if_ready_to_start(self):
        self.model.unit.status = \
            ActiveStatus("{} running".format(self.service))
        return True

    def _on_install(self, event):
        super()._on_install(event)
        self.model.unit.status = MaintenanceStatus("Starting installation")
        logger.info("Starting installation")
        packages = []
        # TODO(pguimares): implement install_tarball logic
        # self._install_tarball()
        if self.distro == "confluent":
            packages = self.CONFLUENT_PACKAGES
        else:
            self.ks.has_exception = "Distro installation not implemented"
            raise Exception("Not Implemented Yet")
        super().install_packages('openjdk-11-headless', packages)
        make_dirs = ["/var/log/schema-registry"]
        self.set_folders_and_permissions(make_dirs)

    def on_connect_relation_joined(self, event):
        self.connect.on_connect_relation_joined(event)

    def on_connect_relation_changed(self, event):
        self.connect.on_connect_relation_changed(event)

    def on_schemaregistry_relation_joined(self, event):
        if not self._cert_relation_set(
                event, self.sr):
            return
        self.sr.on_schema_registry_relation_joined(event)
        self._on_config_changed(event)

    def on_schemaregistry_relation_changed(self, event):
        if not self._cert_relation_set(
                event, self.sr):
            return
        self.sr.on_schema_registry_relation_changed(event)
        self._on_config_changed(event)

    def _generate_listener_request(self):
        req = {}
        if self.is_sasl_enabled():
            # TODO: implement it
            req["SASL"] = {}
        req["is_public"] = False
        if self.is_ssl_enabled():
            req["cert"] = self.get_ssl_cert()
        if len(self.ks.listener_plaintext_pwd) == 0:
            self.ks.listener_plaintext_pwd = genRandomPassword(24)
        req["plaintext_pwd"] = self.ks.listener_plaintext_pwd
        self.listener.set_request(req)

    def on_listeners_relation_joined(self, event):
        # If no certificate available, defer this event and wait
        if not self._cert_relation_set(event, self.listener):
            return
        self._on_config_changed(event)

    def on_listeners_relation_changed(self, event):
        self.on_listeners_relation_joined(event)

    def on_certificates_relation_joined(self, event):
        self.certificates.on_tls_certificate_relation_joined(event)
        self._on_config_changed(event)

    def on_certificates_relation_changed(self, event):
        self.certificates.on_tls_certificate_relation_changed(event)
        self._on_config_changed(event)

    def on_mds_relation_joined(self, event):
        # If no certificate available, defer this event and wait
        if not self._cert_relation_set(event, self.mds):
            return
        self._on_config_changed(event)

    def on_mds_relation_changed(self, event):
        self.on_mds_relation_joined(event)

    def _on_update_status(self, event):
        if len(self.ks.has_exception) > 0:
            self.model.unit.status = \
                BlockedStatus(self.ks.has_exception)
            return
        if not service_running(self.service):
            self.model.unit.status = \
                BlockedStatus("{} not running".format(self.service))
            return
        self.model.unit.status = \
            ActiveStatus("{} is running".format(self.service))

    def is_sasl_enabled(self):
        # TODO: Implement this
        return False

        def is_rbac_enabled(self):
            if self.distro == "apache":
                return False
        return False

    # STORE GET METHODS
    def get_ssl_keystore(self):
        path = self.config.get("keystore-path", "")
        return path

    def get_ssl_truststore(self):
        path = self.config.get("truststore-path", "")
        return path

    def get_ssl_listener_keystore(self):
        path = self.config.get("listener-keystore-path", "")
        return path

    def get_ssl_listener_truststore(self):
        path = self.config.get("listener-truststore-path", "")
        return path

    def get_ssl_schemaregistry_keystore(self):
        path = self.config.get("keystore-sr-path", "")
        return path

    def get_ssl_schemaregistry_truststore(self):
        path = self.config.get("truststore-sr-path", "")
        return path

    # SSL GET METHODS
    def get_ssl_listener_cert(self):
        return self._get_ssl(self.listener, "cert")

    def get_ssl_listener_key(self):
        return self._get_ssl(self.listener, "key")

    def get_ssl_cert(self):
        return self._get_ssl(self.connect, "cert")

    def get_ssl_key(self):
        return self._get_ssl(self.connect, "key")

    def get_ssl_schemaregistry_cert(self):
        return self._get_ssl(self.sr, "cert")

    def get_ssl_schemaregistry_key(self):
        return self._get_ssl(self.sr, "key")

    def _get_ssl(self, relation, ty):
        prefix = ""
        if isinstance(relation, KafkaListenerRequiresRelation):
            prefix = "ssl_listener"
        elif isinstance(relation, KafkaSchemaRegistryRequiresRelation):
            prefix = "ssl_sr"
        elif isinstance(relation, KafkaConnectProvidesRelation):
            prefix = "ssl"
        if len(self.config.get(prefix + "_cert")) > 0 and \
           len(self.config.get(prefix + "_key")) > 0:
            if ty == "cert":
                return base64.b64decode(
                    self.config[prefix + "_cert"]).decode("ascii")
            else:
                return base64.b64decode(
                    self.config[prefix + "_key"]).decode("ascii")

        if not relation or not self.certificates:
            raise KafkaRelationBaseTLSNotSetError(
                "_get_ssl relatio {} or certificates"
                " not available".format(relation))
        certs = self.certificates.get_server_certs()
        c = certs[relation.binding_addr][ty]
        if ty == "cert":
            c = c + \
                self.certificates.get_chain()
        logger.debug("SSL {} for {}"
                     " from tls-certificates: {}".format(ty, prefix, c))
        return c

    def _generate_keystores(self):
        ks = [[self.ks.ssl_cert, self.ks.ssl_key, self.ks.ks_password,
               self.get_ssl_cert, self.get_ssl_key, self.get_ssl_keystore],
              [self.ks.ssl_listener_cert, self.ks.ssl_listener_key,
               self.ks.ks_listener_pwd,
               self.get_ssl_listener_cert, self.get_ssl_listener_key,
               self.get_ssl_listener_keystore],
              [self.ks.ssl_schemaregistry_cert, self.ks.ssl_schemaregistry_key,
               self.ks.ks_schemaregistry_pwd,
               self.get_ssl_schemaregistry_cert, self.get_ssl_schemaregistry_key,
               self.get_ssl_schemaregistry_keystore]]
        # INDICES WITHIN THE TUPLE:
        CERT, KEY, PWD, GET_CERT, GET_KEY, GET_KEYSTORE = 0, 1, 2, 3, 4, 5
        # Generate the keystores if cert/key exists
        for t in ks:
            if t[CERT] == t[GET_CERT]() and \
               t[KEY] == t[KEY]():
                # Certs and keys are the same
                logger.info("Same certs and keys for {}".format(t[CERT]))
                continue
            t[CERT] = t[GET_CERT]()
            t[KEY] = t[GET_KEY]()
            if len(t[CERT]) > 0 and len(t[KEY]) > 0 and t[GET_KEYSTORE]():
                logger.info("Create PKCS12 cert/key for {}".format(t[CERT]))
                filename = genRandomPassword(6)
                PKCS12CreateKeystore(
                    t[GET_KEYSTORE](),
                    t[PWD],
                    t[GET_CERT](),
                    t[GET_KEY](),
                    user=self.config["user"],
                    group=self.config["group"],
                    mode=0o640,
                    openssl_chain_path="/tmp/" + filename + ".chain",
                    openssl_key_path="/tmp/" + filename + ".key",
                    openssl_p12_path="/tmp/" + filename + ".p12",
                    ks_regenerate=self.config.get(
                        "regenerate-keystore-truststore", False))
            elif not t[GET_KEYSTORE]():
                logger.debug("Keystore not found on Iteration: {}".format(t))

    def _get_service_name(self):
        if self.distro == 'confluent':
            self.service = 'confluent-kafka-connect.service'
        elif self.distro == "apache":
            self.service = "kafka-connect.service"
        return self.service

    def _render_connect_distribute_properties(self, event):
        logger.info("Start to render connect-distributed properties")
        dist_props = \
            yaml.safe_load(self.config.get(
                "connect-distributed-properties", ""
            )) or {}
        dist_props["confluent.license.topic"] = \
            self.config.get("confluent_license_topic")
        dist_props["connector.client.config.override.policy"] = \
            self.config.get("connector-client-config-override-policy")
        dist_props["internal.key.converter"] = \
            self.config.get(
                "internal-converter",
                "org.apache.kafka.connect.json.JsonConverter")
        dist_props["internal.value.converter"] = \
            self.config.get(
                "internal-converter",
                "org.apache.kafka.connect.json.JsonConverter")
        # This is always set to false as the conversion will happen between
        # kafka connect and brokers and not involve Schema Registry
        dist_props["internal.key.converter.schemas.enable"] = False
        dist_props["internal.value.converter.schemas.enable"] = False
        dist_props["group.id"] = self.config.get("group-id", "connect-cluster")

        # MDS Relation
        if not self.mds.relation:
            # MDS is only available for Confluent
            logger.warning("MDS relation not detected")
        elif self.mds.relation and self.distro != "confluent":
            raise KafkaConnectCharmUnsupportedParamError(
                "kafka distro {} does not support MDS relation".format(self.distro)
            )
        else:
            # MDS relation present
            mds_opts = self.mds.get_options()
            dist_props = {**dist_props, **mds_opts}

        # Schema Registry Relation
        if not self.sr.relation:
            raise KafkaConnectCharmMissingRelationError("schemaregistry")
        dist_props["key.converter"] = self.sr.converter
        dist_props["value.converter"] = self.sr.converter
        dist_props["key.converter.schema.registry.url"] = self.sr.url
        dist_props["value.converter.schema.registry.url"] = self.sr.url
        if self.get_ssl_schemaregistry_cert() and \
           self.get_ssl_schemaregistry_key():
            if self.get_ssl_schemaregistry_keystore():
                dist_props["key.converter.schema.registry.ssl.key.password"] = \
                    self.ks.ks_schemaregistry_pwd
                dist_props["key.converter.schema.registry.ssl.keystore.password"] = \
                    self.ks.ks_schemaregistry_pwd
                dist_props["key.converter.schema.registry.ssl.keystore.location"] = \
                    self.get_ssl_schemaregistry_keystore()
            if self.get_ssl_schemaregistry_truststore():
                self.sr.set_TLS_auth(
                    self.get_ssl_schemaregistry_cert(),
                    self.get_ssl_schemaregistry_truststore(),
                    self.ks.ts_schemaregistry_pwd,
                    user=self.config["user"],
                    group=self.config["group"],
                    mode=0o640)
                logger.info("Schema Registry: using custom truststore")
                dist_props["key.converter.schema.registry.ssl.truststore.password"] = \
                    self.ks.ts_schemaregistry_pwd
                dist_props["key.converter.schema.registry.ssl.truststore.location"] = \
                    self.get_ssl_schemaregistry_truststore()
            # Same logic, but setting value options now
            dist_props["value.converter.schema.registry.ssl.key.password"] = \
                self.ks.ks_schemaregistry_pwd
            dist_props["value.converter.schema.registry.ssl.keystore.password"] = \
                self.ks.ks_schemaregistry_pwd
            dist_props["value.converter.schema.registry.ssl.keystore.location"] = \
                self.get_ssl_schemaregistry_keystore()
            if self.get_ssl_schemaregistry_truststore():
                logger.info("Schema Registry: using custom truststore")
                dist_props["value.converter.schema.registry.ssl.truststore.password"] = \
                    self.ks.ts_schemaregistry_pwd
                dist_props["value.converter.schema.registry.ssl.truststore.location"] = \
                    self.get_ssl_schemaregistry_truststore()
        elif self.sr.url.lower().startswith("https") and \
                self.sr.get_param("client_auth"):
            # Schema Registry URL set for HTTPS and client-auth enabled
            if not self.get_ssl_schemaregistry_cert():
                raise KafkaConnectCharmNotValidOptionSetError("schemaregistry-cert")
            if not self.get_ssl_schemaregistry_key():
                raise KafkaConnectCharmNotValidOptionSetError("schemaregistry-key")
            if not self.get_ssl_schemaregistry_keystore():
                raise KafkaConnectCharmNotValidOptionSetError("schemaregistry-keystore")

        # External connection
        dist_props["bootstrap.servers"] = self.listener.get_bootstrap_servers()
        if self.get_ssl_cert() and self.get_ssl_key():
            # As server side, we need the keystore at least
            if len(self.get_ssl_keystore()) == 0:
                raise KafkaConnectCharmNotValidOptionSetError("keystore-path")
            try:
                if len(self.config.get("truststore-path", "")) > 0:
                    self.connect.set_TLS_auth(
                        self.get_ssl_cert(),
                        self.get_ssl_truststore(),
                        self.ks.ts_password,
                        user=self.config["user"],
                        group=self.config["group"],
                        mode=0o640)
                   # Seems we can have only one truststore / keystore for both
                   # listeners and the connect API endpoint.
                   # TODO: export the certs from connect relation and import to the other relation

#                    dist_props["ssl.truststore.location"] = self.get_ssl_truststore()
#                    dist_props["ssl.truststore.password"] = self.ks.ts_password
                # That ensures that only if set_TLS_auth is successfully executed, then
                # the configs below will be added
#                dist_props["ssl.keystore.location"] = self.get_ssl_keystore()
#                dist_props["ssl.keystore.password"] = self.ks.ks_password
            except KafkaConnectRelationNotUsedError as e:
                # not necessarily this endpoint will be used. Log and move along
                logger.info(str(e))

        # Broker listeners setup
        if not self.listener.relation:
            raise KafkaConnectCharmMissingRelationError("listeners")
        dist_props["consumer.bootstrap.servers"] = self.listener.get_bootstrap_servers()
        dist_props["consumer.confluent.monitoring.interceptor."
                   "bootstrap.servers"] = self.listener.get_bootstrap_servers()
        # Producer
        dist_props["producer.bootstrap.servers"] = self.listener.get_bootstrap_servers()
        dist_props["producer.confluent.monitoring.interceptor."
                   "bootstrap.servers"] = self.listener.get_bootstrap_servers()
        if self.get_ssl_listener_cert() and self.get_ssl_listener_key():
            # TODO: review the protocol below if SASL set
            dist_props["consumer.security.protocol"] = "SSL"
            dist_props["producer.security.protocol"] = "SSL"
            if self.listener.tls_client_auth_enabled():
                if not self.get_ssl_listener_keystore():
                    # Client auth requested but keystore not set
                    raise KafkaConnectCharmNotValidOptionSetError("listener-keystore-path")
            dist_props["listeners.https.ssl.truststore.location"] = \
                self.get_ssl_listener_truststore()
            dist_props["listeners.https.ssl.truststore.password"] = \
                self.ks.ts_listener_pwd
            # TODO: review the protocol below if SASL set
            dist_props["consumer.confluent.monitoring.interceptor."
                       "ssl.keystore.location"] = self.get_ssl_listener_keystore()
            dist_props["consumer.confluent.monitoring.interceptor."
                       "ssl.keystore.password"] = self.ks.ks_listener_pwd
            dist_props["consumer.confluent.monitoring.interceptor."
                       "ssl.keystore.location"] = self.get_ssl_listener_keystore()
            dist_props["consumer.confluent.monitoring.interceptor."
                       "ssl.keystore.password"] = self.ks.ks_listener_pwd
            dist_props["consumer.ssl.keystore.location"] = self.get_ssl_listener_keystore()
            dist_props["consumer.confluent.monitoring.interceptor."
                       "ssl.keystore.password"] = self.ks.ks_listener_pwd
            dist_props["consumer.ssl.keystore.location"] = self.get_ssl_listener_keystore()
            dist_props["consumer.ssl.keystore.password"] = self.ks.ks_listener_pwd
            # Producer
            dist_props["producer.confluent.monitoring.interceptor."
                       "ssl.keystore.location"] = self.get_ssl_listener_keystore()
            dist_props["producer.confluent.monitoring.interceptor."
                       "ssl.keystore.password"] = self.ks.ks_listener_pwd
            dist_props["producer.confluent.monitoring.interceptor."
                       "ssl.keystore.location"] = self.get_ssl_listener_keystore()
            dist_props["producer.confluent.monitoring.interceptor."
                       "ssl.keystore.password"] = self.ks.ks_listener_pwd
            dist_props["producer.ssl.keystore.location"] = self.get_ssl_listener_keystore()
            dist_props["producer.confluent.monitoring.interceptor."
                       "ssl.keystore.password"] = self.ks.ks_listener_pwd
            dist_props["producer.ssl.keystore.location"] = self.get_ssl_listener_keystore()
            dist_props["producer.ssl.keystore.password"] = self.ks.ks_listener_pwd
            if self.get_ssl_listener_truststore():
                self.listener.set_TLS_auth(
                    self.get_ssl_listener_cert(),
                    self.get_ssl_listener_truststore(),
                    self.ks.ts_listener_pwd,
                    user=self.config["user"],
                    group=self.config["group"],
                    mode=0o640)
                logger.info("Using custom truststore instead of java's default")
                dist_props["listeners.https.ssl.truststore.location"] = \
                    self.get_ssl_listener_truststore()
                dist_props["listeners.https.ssl.truststore.password"] = \
                    self.ks.ts_listener_pwd
                dist_props["consumer.confluent.monitoring.interceptor."
                           "ssl.truststore.location"] = self.get_ssl_listener_truststore()
                dist_props["consumer.confluent.monitoring.interceptor."
                           "ssl.truststore.password"] = self.ks.ts_listener_pwd
                dist_props["consumer.confluent.monitoring.interceptor."
                           "ssl.truststore.location"] = self.get_ssl_listener_truststore()
                dist_props["consumer.confluent.monitoring.interceptor."
                           "ssl.truststore.password"] = self.ks.ts_listener_pwd
                dist_props["consumer.ssl.truststore.location"] = self.get_ssl_listener_truststore()
                dist_props["consumer.confluent.monitoring.interceptor."
                           "ssl.truststore.password"] = self.ks.ts_listener_pwd
                dist_props["consumer.ssl.truststore.location"] = self.get_ssl_listener_truststore()
                dist_props["consumer.ssl.truststore.password"] = self.ks.ts_listener_pwd
                # Producer
                dist_props["producer.confluent.monitoring.interceptor."
                           "ssl.truststore.location"] = self.get_ssl_listener_truststore()
                dist_props["producer.confluent.monitoring.interceptor."
                           "ssl.truststore.password"] = self.ks.ts_listener_pwd
                dist_props["producer.confluent.monitoring.interceptor."
                           "ssl.truststore.location"] = self.get_ssl_listener_truststore()
                dist_props["producer.confluent.monitoring.interceptor."
                           "ssl.truststore.password"] = self.ks.ts_listener_pwd
                dist_props["producer.ssl.truststore.location"] = self.get_ssl_listener_truststore()
                dist_props["producer.confluent.monitoring.interceptor."
                           "ssl.truststore.password"] = self.ks.ts_listener_pwd
                dist_props["producer.ssl.truststore.location"] = self.get_ssl_listener_truststore()
                dist_props["producer.ssl.truststore.password"] = self.ks.ts_listener_pwd

# TODO: Define those values later and switch the security protocol above
# producer.confluent.monitoring.interceptor.sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required username="connect_worker" password="password123" metadataServerUrls="https://ansiblebroker3.example.com:8090,https://ansiblebroker1.example.com:8090,https://ansiblebroker2.example.com:8090"; # noqa
# producer.confluent.monitoring.interceptor.sasl.login.callback.handler.class=io.confluent.kafka.clients.plugins.auth.token.TokenUserLoginCallbackHandler # noqa 
# producer.confluent.monitoring.interceptor.sasl.mechanism=OAUTHBEARER # noqa
# producer.confluent.monitoring.interceptor.security.protocol=SASL_SSL # noqa

            # REST Access to the Connector API
            dist_props["rest.port"] = self.config.get("clientPort", 8083)
            dist_props["rest.advertised.port"] = \
                self.config.get("clientPort", 8083)
            dist_props["listeners"] = "{}:{}".format(
                self.config.get("listener"), self.config.get("clientPort")
            )
            if len(self.config.get("rest-extension-classes", "")) > 0:
                dist_props["rest.extension.classes"] = \
                    self.config.get("rest-extension-classes")
                dist_props["rest.servlet.initializor.classes"] = \
                    self.config.get("rest-extension-classes")
            if self.config.get("listener", "").startswith("https"):
                dist_props["rest.advertised.listener"] = "https"
                # TODO: change to SASL if needed
                dist_props["security.protocol"] = "SSL"
                if self.get_ssl_truststore():
                    dist_props["ssl.truststore.location"] = \
                        self.get_ssl_listener_truststore()
                    dist_props["ssl.truststore.password"] = \
                        self.ks.ts_listener_pwd
#                    dist_props["listeners.https.ssl.truststore.location"] = \
#                        dist_props["ssl.truststore.location"]
#                    dist_props["listeners.https.ssl.truststore.password"] = \
#                        dist_props["ssl.truststore.password"]
            else:
                dist_props["rest.advertised.listener"] = "http"
            # TODO: certify we need to set the key below
            # dist_props["public.key.path"] = /var/ssl/private/public.pem

# TODO: Define those values later if SASL is enabled
# rest.servlet.initializor.classes=io.confluent.common.security.jetty.initializer.InstallBearerOrBasicSecurityHandler # noqa
# sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required username="connect_worker" password="password123" metadataServerUrls="https://ansiblebroker3.example.com:8090,https://ansiblebroker1.example.com:8090,https://ansiblebroker2.example.com:8090"; # noqa
# sasl.login.callback.handler.class=io.confluent.kafka.clients.plugins.auth.token.TokenUserLoginCallbackHandler # noqa
# sasl.mechanism=OAUTHBEARER # noqa

        logger.debug("Options are: {}".format(",".join(dist_props)))
        render(source="connect-distributed.properties.j2",
               target="/etc/kafka/connect-distributed.properties",
               owner=self.config.get('user'),
               group=self.config.get("group"),
               perms=0o640,
               context={
                   "dist_props": dist_props
               })

    def _render_connect_log4j_properties(self):
        root_logger = self.config.get("log4j-root-logger", None) or \
            "INFO, stdout, connectAppender"
        self.model.unit.status = MaintenanceStatus("Rendering log4j...")
        logger.debug("Rendering log4j")
        render(source="connect_log4j.properties.j2",
               target="/etc/kafka/connect-log4j.properties",
               owner=self.config.get('user'),
               group=self.config.get("group"),
               perms=0o640,
               context={
                   "root_logger": root_logger
               })

    def _render_systemd_service(self):
        render(source="service.conf.j2",
               target="/lib/systemd/system/confluent-kafka-connect.service",
               owner="root",
               group="root",
               perms=0o644,
               context={
                   "user": self.config.get("user", "cp-kafka"),
                   "group": self.config.get("group", "confluent")
               })

    def _on_config_changed(self, event):
        if not self._cert_relation_set(event):
            return
        self.model.unit.status = \
            MaintenanceStatus("generate certs and keys if needed")
        logger.debug("Running _generate_keystores()")
        self._generate_keystores()
        self.model.unit.status = \
            MaintenanceStatus("Generate Listener settings")
        self._generate_listener_request()
        self.model.unit.status = \
            MaintenanceStatus("Render connect-distributed.properties")
        logger.debug("Running render_connect_distributed_properties()")
        try:
            self._render_connect_distribute_properties(event)
        except KafkaConnectCharmNotValidOptionSetError as e:
            # Returning as we need to wait for a config change and that
            # will trigger a new event
            self.model.unit.status = str(e)
            self.ks.has_exception = str(e)
            return
        except KafkaConnectCharmMissingRelationError as e:
            # same reason as above, waiting for an add-relation
            self.model.unit.status = str(e)
            self.ks.has_exception = str(e)
            return
        except KafkaListenerRelationNotSetError as e:
            logger.warn("Listener relation not ready yet: {}".format(str(e)))
            event.defer()
            return
        self.model.unit.status = MaintenanceStatus("Render log4j properties")
        logger.debug("Running log4j properties renderer")
        self._render_connect_log4j_properties()
        self.model.unit.status = \
            MaintenanceStatus("Render service override conf file")
        logger.debug("Render override.conf")
        self.render_service_override_file(
            target="/etc/systemd/system/"
                   "{}.service.d/override.conf".format(self.service))
        self._render_systemd_service()
        if self._check_if_ready_to_start():
            logger.info("Service ready or start, restarting it...")
            # Unmask and enable service
            service_resume(self.service)
            # Reload and restart
            service_reload(self.service)
            service_restart(self.service)
            logger.debug("finished restarting")
        if not service_running(self.service):
            logger.warning("Service not running that "
                           "should be: {}".format(self.service))
            BlockedStatus("Service not running {}".format(self.service))
        # Restart has_exception for the next event
        self.ks.has_exception = ""


if __name__ == "__main__":
    main(KafkaConnectCharm)
