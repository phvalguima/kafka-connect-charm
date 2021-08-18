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

from charmhelpers.core.hookenv import (
    open_port
)

from wand.apps.relations.tls_certificates import (
    TLSCertificateRequiresRelation,
    TLSCertificateDataNotFoundInRelationError
)
from wand.apps.kafka import (
    KafkaJavaCharmBase,
    KafkaCharmBaseFeatureNotImplementedError,
    KafkaJavaCharmBasePrometheusMonitorNode,
    KafkaJavaCharmBaseNRPEMonitoring
)
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
    KafkaConnectProvidesRelation
)
from wand.apps.relations.kafka_confluent_center import (
    KafkaC3RequiresRelation
)
from wand.apps.relations.kafka_relation_base import (
    KafkaRelationBaseTLSNotSetError
)
from wand.security.ssl import genRandomPassword, CreateTruststore
from wand.contrib.linux import (
    get_hostname
)

from loadbalancer_interface import LBProvider


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
        self.framework.observe(self.on.cluster_relation_joined,
                               self.on_cluster_relation_joined)
        self.framework.observe(self.on.cluster_relation_changed,
                               self.on_cluster_relation_changed)
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
        self.framework.observe(self.on.c3_relation_joined,
                               self.on_c3_relation_joined)
        self.framework.observe(self.on.c3_relation_changed,
                               self.on_c3_relation_changed)
        # Relation Managers
        self.listener = KafkaListenerRequiresRelation(
            self, 'listeners')
        self.sr = KafkaSchemaRegistryRequiresRelation(
            self, 'schemaregistry')
        self.certificates = \
            TLSCertificateRequiresRelation(self, 'certificates')
        self.connect = KafkaConnectProvidesRelation(self, 'connect')
        self.mds = KafkaMDSRequiresRelation(self, "mds")
        self.c3 = KafkaC3RequiresRelation(self, "c3")
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
        self.ks.set_default(ports=[])
        self.ks.set_default(ssl_certs=[])
        self.ks.set_default(lb_port=0)
        # Load balancer settings
        self.lb_provider = LBProvider(self, "lb-provider")
        self.framework.observe(self.lb_provider.on.available,
                               self._on_lb_provider_available)
        # LMA integrations
        self.prometheus = \
            KafkaJavaCharmBasePrometheusMonitorNode(
                self, 'prometheus-manual',
                port=self.config.get("jmx-exporter-port", 9404),
                internal_endpoint=self.config.get(
                    "jmx_exporter_use_internal", False),
                labels=self.config.get("jmx_exporter_labels", None))
        self.nrpe = KafkaJavaCharmBaseNRPEMonitoring(
            self,
            svcs=[self._get_service_name()],
            endpoints=[],
            nrpe_relation_name='nrpe-external-master')

    def _on_lb_provider_available(self, event):
        if not (self.unit.is_leader() and self.lb_provider.is_available):
            return
        # Check if there is any configuration changes:
        if self.ks.lb_port == self.config["clientPort"] and \
           self.ks.lb_api_ip == self.config["api_ip"] and \
           self.ks.api_is_public == self.config["api_is_public"]:
            # configs are the same, just return
            return
        request = self.lb_provider.get_request("lb-consumer")
        request.protocol = request.protocols.tcp
        request.port_mapping = {
            self.config["clientPort"]: self.config["clientPort"]
        }
        if len(self.config.get("api_ip", "")) > 0:
            request.ingress_address = self.config["api_ip"]
        request.public = self.config["api_is_public"]
        self.lb_provider.send_request(request)
        # Now, save the data for the next request
        self.ks.lb_port = self.config["clientPort"]
        self.ks.lb_api_ip = self.config["api_ip"]
        self.ks.api_is_public = self.config["api_is_public"]

    def is_jmxexporter_enabled(self):
        if self.prometheus.relations:
            return True
        return False

    def on_cluster_relation_joined(self, event):
        self._on_config_changed(event)

    def on_cluster_relation_changed(self, event):
        self._on_config_changed(event)
        if not self.prometheus.relations:
            return
        # Update prometheus relation given it may mean a new unit
        # is available.
        if len(self.prometheus.relations) > 0:
            self.prometheus.on_prometheus_relation_changed(event)

    def _get_url(self):
        """ Returns the URL to be used to access Connect services"""
        return self.config["rest_url"] if len(self.config["rest_url"]) > 0 \
            else get_hostname(self.connect.advertise_addr)

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
        self.sr.on_schema_registry_relation_joined(event)
        self._on_config_changed(event)

    def on_schemaregistry_relation_changed(self, event):
        self.sr.on_schema_registry_relation_changed(event)
        self._on_config_changed(event)

    def on_c3_relation_joined(self, event):
        self._on_config_changed(event)

    def on_c3_relation_changed(self, event):
        self._on_config_changed(event)

    def _generate_listener_request(self):
        req = {}
        if self.is_sasl_enabled():
            if self.is_sasl_ldap_enabled():
                req["SASL"] = {
                    "protocol": "OAUTHBEARER",
                    "jaas.config": self._get_ldap_settings(
                        self.mds.get_bootstrap_servers()
                    ),
                    "confluent": {
                        "login.callback": "io.confluent.kafka.clients."
                                          "plugins.auth.token.TokenUser"
                                          "LoginCallbackHandler"
                    }
                }
            elif self.is_sasl_kerberos_enabled():
                raise KafkaCharmBaseFeatureNotImplementedError(
                    "Missing implementation of kerberos for Connect")
        req["is_public"] = False
        if self.is_sasl_enabled() and self.get_ssl_listener_truststore():
            req["secprot"] = "SASL_SSL"
        elif not self.is_sasl_enabled() and \
                self.get_ssl_listener_truststore():
            req["secprot"] = "SSL"
        elif self.is_sasl_enabled() and not \
                self.get_ssl_listener_truststore():
            req["secprot"] = "SASL_PLAINTEXT"
        else:
            req["secprot"] = "PLAINTEXT"
        if len(self.get_ssl_listener_cert()) > 0 and \
           len(self.get_ssl_listener_key()) > 0:
            req["cert"] = self.get_ssl_listener_cert()
        # Set the plaintext password
        if len(self.ks.listener_plaintext_pwd) == 0:
            self.ks.listener_plaintext_pwd = genRandomPassword(24)
        req["plaintext_pwd"] = self.ks.listener_plaintext_pwd
        self.listener.set_request(req)
        return req

    def on_listeners_relation_joined(self, event):
        self._on_config_changed(event)

    def on_listeners_relation_changed(self, event):
        try:
            self.listener.on_listener_relation_changed(event)
        except KafkaRelationBaseTLSNotSetError:
            logger.warning(
                "Detected some of the remote apps on listener relation "
                "but still waiting for setup on this unit.")
            # We need certs correctly configured to be able to set listeners
            # because certs are configured on other peers.
            # Defer this event until operator updates certificate info.
            self.model.unit.status = BlockedStatus(
                "Missing certificate info: listeners")
            event.defer()
            return
        self._on_config_changed(event)

    def on_certificates_relation_joined(self, event):
        self.certificates.on_tls_certificate_relation_joined(event)
        # Relation just joined, request certs for each of the relations
        # That will happen once. The certificates will be generated, then
        # it will trigger a -changed Event on certificates, which will
        # call the config-changed logic once again.
        # That way, the certificates will be added to the truststores
        # and shared across the other relations.

        # In case several relations shares the same set of IPs (spaces),
        # the last relation will get to set the certificate values.
        # Therefore, the order of the list below is relevant.
        for r in [self.listener,
                  self.sr,
                  self.connect]:
            self._cert_relation_set(None, r)
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

        # Call the method from JavaCharmBase
        super()._generate_keystores(ks)

    def _get_service_name(self):
        if self.distro == 'confluent':
            self.service = 'confluent-kafka-connect'
        elif self.distro == "apache":
            self.service = "kafka-connect"
        return self.service

    def is_ssl_enabled(self):
        """Returns true if the API endpoint has the SSL enabled"""
        return len(self.get_ssl_cert()) > 0 and \
            len(self.get_ssl_key()) > 0 and \
            len(self.get_ssl_keystore())

    def _render_connect_distribute_properties(self):
        """
        Render connect-distributed.properties:
        1) Read the options set using connect-distributed-properties
        2) Set service options
        2.1) Set SSL options
        3) Set Listener-related information
        4) Set schema-registry information
        5) Set metadata and C3 informaiton
        6) Render configs"""

        # 1) Read the options set using connect-distributed-properties
        logger.info("Start to render connect-distributed properties")
        dist_props = \
            yaml.safe_load(self.config.get(
                "connect-distributed-properties", ""
            )) or {}
        dist_props["confluent.license.topic"] = \
            self.config.get("confluent_license_topic")
        dist_props["connector.client.config.override.policy"] = \
            self.config.get("connector-client-config-override-policy")
        dist_props["plugin.path"] = self.config.get("plugin-path", "")
        dist_props["group.id"] = self.config.get("group-id", "")

        # 2) Set the connect endpoint:
        # 2.1) Recover certificates
        # TODO(pguimaraes): recover extra certs set by actions
        extra_certs = []

        # REST Access to the Connector API
        # Set the connect relation:
        if self.unit.is_leader():
            if len(self.config["api_url"]) > 0:
                self.connect.url = self.config["api_url"]
            else:
                self.connect.url = get_hostname(self.connect.advertise_addr)
        dist_props["rest.advertised.host.name"] = \
            self.connect.url

        dist_props["rest.port"] = self.config.get("clientPort", 8083)
        dist_props["rest.advertised.port"] = \
            self.config.get("clientPort", 8083)
        dist_props["listeners"] = "{}:{}".format(
            self.config.get("listener"), self.config.get("clientPort")
        )
        if self.is_ssl_enabled():
            dist_props["rest.advertised.listener"] = "https"
            dist_props["listeners.https.ssl.key.password"] = \
                self.ks.ks_password
            dist_props["listeners.https.ssl.keystore.password"] = \
                self.ks.ks_password
            dist_props["listeners.https.ssl.keystore.location"] = \
                self.get_ssl_keystore()

            if self.connect.relations:
                # If there is a relation, then set TLS certificate
                self.connect.set_TLS_auth(
                    self.get_ssl_cert(),
                    self.get_ssl_truststore(),
                    self.ks.ts_password,
                    user=self.config["user"],
                    group=self.config["group"],
                    mode=0o640)

            if len(self.get_ssl_truststore()) > 0:
                dist_props["listeners.https.ssl.truststore.password"] = \
                    self.ks.ts_password
                dist_props["listeners.https.ssl.truststore.location"] = \
                    self.get_ssl_truststore()
                # With or without a Connect relation, set the truststore
                # First, create a list out of current certs exchanged via
                # cluster relation or passed via actions
                crt_list = list(self.ks.ssl_certs) + extra_certs
                if len(crt_list) == 0:
                    crt_list = [self.get_ssl_cert()]
                # Now, get all the certs added via connect relation:
                if self.connect.relations:
                    for r in self.connect.relations:
                        for u in r.units:
                            if "tls_cert" in r.data[u]:
                                crt_list.append(r.data[u]["tls_cert"])
                ts_regenerate = \
                    self.config["regenerate-keystore-truststore"]
                # Finally, create the Truststore
                CreateTruststore(
                    self.get_ssl_truststore(),
                    self.ks.ts_password,
                    crt_list,
                    ts_regenerate=ts_regenerate,
                    user=self.config["user"],
                    group=self.config["group"],
                    mode=0o640)
        else:
            dist_props["rest.advertised.listener"] = "http"

        # 3) Broker listeners setup
        if not self.listener.relations:
            raise KafkaConnectCharmMissingRelationError("listeners")
        if len(self.get_ssl_listener_keystore()) > 0 and \
           len(self.get_ssl_listener_key()) > 0:
            # Will use key and keystore for listeners client auth
            self.listener.set_TLS_auth(
                self.get_ssl_listener_cert(),
                self.get_ssl_listener_truststore(),
                self.ks.ts_listener_pwd,
                user=self.config["user"],
                group=self.config["group"],
                mode=0o640)
        if len(self.get_ssl_listener_truststore()) > 0 and \
           len(self.get_ssl_listener_cert()) > 0:
            # Not running a SR cluster, then manually add
            # unit's own cert
            crt_list = [self.get_ssl_listener_cert()]
            if self.listener.relations:
                for r in self.listener.relations:
                    for u in r.units:
                        if "tls_cert" in r.data[u]:
                            crt_list.append(r.data[u]["tls_cert"])
            if len(crt_list) > 0:
                # We do have a certificate list to trust
                # Came from both cluster peers and SR relation
                CreateTruststore(
                    self.get_ssl_listener_truststore(),
                    self.ks.ts_listener_pwd,
                    crt_list,
                    ts_regenerate=True,
                    user=self.config["user"],
                    group=self.config["group"],
                    mode=0o640)
        # Generate options
        listener_opts = self.listener.generate_options(
            self.get_ssl_listener_keystore(),
            self.ks.ks_listener_pwd,
            self.get_ssl_listener_truststore(),
            self.ks.ts_listener_pwd,
            prefix="")
        if listener_opts:
            dist_props = {**dist_props, **listener_opts}
            # Also add listener endpoints for producer and consumer
            dist_props = {**dist_props, **{
                "consumer.{}".format(k): v for k, v in listener_opts.items()
            }}
            dist_props = {**dist_props, **{
                "producer.{}".format(k): v for k, v in listener_opts.items()
            }}

        # 4) Schema Registry Relation
        # Although the options below may not be directly related to the
        # Schema Registry, moving them here because it makes sense
        dist_props["internal.key.converter"] = \
            self.config.get(
                "internal-converter",
                "org.apache.kafka.connect.json.JsonConverter")
        dist_props["internal.value.converter"] = \
            self.config.get(
                "internal-converter",
                "org.apache.kafka.connect.json.JsonConverter")
        # Set it to False if the internal-convert option is configured
        dist_props["internal.key.converter.schemas.enable"] = \
            str(len(dist_props["internal.key.converter"]) == 0).lower()
        dist_props["internal.value.converter.schemas.enable"] = \
            str(len(dist_props["internal.value.converter"]) == 0).lower()
        if not self.sr.relation:
            raise KafkaConnectCharmMissingRelationError("schemaregistry")
        dist_props["key.converter"] = self.sr.converter
        dist_props["value.converter"] = self.sr.converter
        dist_props["key.converter.schema.registry.url"] = self.sr.url
        dist_props["value.converter.schema.registry.url"] = self.sr.url
        sr_config = self.sr.generate_configs(
            self.get_ssl_schemaregistry_truststore(),
            self.ks.ts_schemaregistry_pwd,
            len(self.get_ssl_schemaregistry_key()) > 0 and # noqa
            len(self.get_ssl_schemaregistry_keystore()) > 0,
            self.get_ssl_schemaregistry_keystore(),
            self.ks.ks_schemaregistry_pwd)
        if sr_config:
            dist_props = {**dist_props, **{
                "key.converter.{}".format(k): v for k, v in sr_config.items()
            }}
            dist_props = {**dist_props, **{
                "value.converter.{}".format(k): v for k, v in sr_config.items()
            }}
        if len(self.get_ssl_schemaregistry_keystore()) > 0 and \
           len(self.get_ssl_schemaregistry_key()) > 0:
            # Will use key and keystore for listeners client auth
            self.sr.set_TLS_auth(
                self.get_ssl_schemaregistry_cert(),
                self.get_ssl_schemaregistry_truststore(),
                self.ks.ts_schemaregistry_pwd,
                user=self.config["user"],
                group=self.config["group"],
                mode=0o640)
        if len(self.get_ssl_schemaregistry_truststore()) > 0 and \
           len(self.get_ssl_schemaregistry_cert()) > 0:
            # Not running a SR cluster, then manually add
            # unit's own cert
            crt_list = [self.get_ssl_schemaregistry_cert()]
            if self.sr.relations:
                for r in self.sr.relations:
                    for u in r.units:
                        if "tls_cert" in r.data[u]:
                            crt_list.append(r.data[u]["tls_cert"])
            if len(crt_list) > 0:
                # We do have a certificate list to trust
                # Came from both cluster peers and SR relation
                CreateTruststore(
                    self.get_ssl_schemaregistry_truststore(),
                    self.ks.ts_schemaregistry_pwd,
                    crt_list,
                    ts_regenerate=True,
                    user=self.config["user"],
                    group=self.config["group"],
                    mode=0o640)

        # 5) MDS and C3 relations
        # MDS Relation
        if not self.mds.relation:
            # MDS is only available for Confluent
            logger.warning("MDS relation not detected")
        elif self.mds.relation and self.distro != "confluent":
            raise KafkaConnectCharmUnsupportedParamError(
                "kafka distro {} does not support MDS "
                "relation".format(self.distro)
            )
        # MDS relation present
        mds_opts = self.mds.generate_configs(
            self.config["mds_public_key_path"],
            self.config.get("mds_user", ""),
            self.config.get("mds_password", "")
        )
        if (self.mds.relations or self.c3.relations) and \
           self.distro != "confluent":
            self.model.unit.status = \
                BlockedStatus("Not using confluent but MDS or"
                              " confluent center relations used")
            return
        if mds_opts:
            dist_props = {**dist_props, **mds_opts}
        # Set Confluent Center information
        c3_config = self.c3.generate_configs(
            self.get_ssl_listener_truststore(),
            self.ks.ts_listener_pwd,
            self._get_ldap_settings(self.mds.get_bootstrap_servers()),
            dist_props.get("security.protocol", "PLAINTEXT"),
            sasl_oauthbearer_enabled=self.is_sasl_ldap_enabled())
        if c3_config:
            # Remove the "client." prefix on some of the options
            c3_config = {
                k.replace("client.", ""): v for k, v in c3_config.items()
            }
            # Now, add c3_config twice
            # one for the producer and another for consumer
            # 1st, clean some options that are not needed:
            del c3_config["producer.interceptor.classes"]
            del c3_config["consumer.interceptor.classes"]
            dist_props = {**dist_props, **{
                "producer.{}".format(k): v for k, v in c3_config.items()
            }}
            dist_props = {**dist_props, **{
                "consumer.{}".format(k): v for k, v in c3_config.items()
            }}
            # Final clean up on configs from C3:
            dist_props["producer.interceptor.classes"] = \
                "io.confluent.monitoring.clients.interceptor." + \
                "MonitoringProducerInterceptor"
            dist_props["consumer.interceptor.classes"] = \
                "io.confluent.monitoring.clients.interceptor." + \
                "MonitoringConsumerInterceptor"
            dist_props["confluent.monitoring.interceptor.topic"] = \
                "_confluent-monitoring"
            if "consumer.sasl.jaas.config" in dist_props:
                del dist_props["consumer.sasl.jaas.config"]
            if "producer.sasl.jaas.config" in dist_props:
                del dist_props["producer.sasl.jaas.config"]
            inter = "confluent.monitoring.interceptor.topic"
            if "consumer." + inter in dist_props:
                del dist_props["consumer." + inter]
            if "producer." + inter in dist_props:
                del dist_props["producer." + inter]

        # 6) Render the options
        logger.debug("Options are: {}".format(",".join(dist_props)))
        render(source="connect-distributed.properties.j2",
               target="/etc/kafka/connect-distributed.properties",
               owner=self.config.get('user'),
               group=self.config.get("group"),
               perms=0o640,
               context={
                   "dist_props": dist_props
               })
        return dist_props

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
        return root_logger

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
        """Runs the changes on configuration files.

        Status management:
        Config changed sets unit status to Maintenance. Once it is configured
        and all settings are correctly issued, open ports, restart and move it
        to ActiveStatus.

        Any rendering method can move it to BlockedStatus. In this case, a
        check must be run after each render_* function and if model is now
        blocked, stop the configuration.

        No need to defer the event given each -hook call also calls this
        config_changed.

        Following steps will be taken:

        1) Check for any missing relations
        2) Check if TLS is set and configured correctly
        3) Prepare context: generate the configuration files
        4) Open ports
        5) Restart cycle
        6) Rerun load balancer"""

        # 1) Check for any missing relations
        if not self.listener.relations:
            self.model.unit.status = BlockedStatus(
                "Waiting for listener relation")
            # Abandon event as new relation will trigger config-changed
            return
        if not self.sr.relations:
            self.model.unit.status = BlockedStatus(
                "Waiting for schema registry relation")
            # Abandon event as new relations will trigger config-changed
            return
        if self.distro != "confluent" and \
           (self.c3.relations or self.mds.relations):
            self.model.unit.status = BlockedStatus(
                "Confluent-only relation set but distro is not confluent")
            # Abandon event as new relations will trigger config-changed
            return

        # 2) Check if TLS is set and configured correctly
        # Prepare context: generate the configuration files
        ctx = {}
        self.model.unit.status = \
            MaintenanceStatus("generate certs and keys if needed")
        try:
            self._generate_keystores()
        except TLSCertificateDataNotFoundInRelationError:
            logger.warn("Generate keystore could not find certificate "
                        "relation data. Missing 'common_name'")
            logger.debug("Certificate relation data is: {}".format(
                self.certificates.relation.data[self.unit]
            ))
            self.model.unit.status = \
                BlockedStatus("Missing data in certificates relation")
        if not isinstance(self.model.unit.status, MaintenanceStatus):
            logger.info("_generate_keystores() blocked the status, return")
            return

        # 2.1) Share certs with cluster peers
        if len(self.framework.model.relations["cluster"]) > 0:
            cluster_rel = self.framework.model.relations["cluster"][0]
            if cluster_rel:
                # Update certificate information:
                if len(self.get_ssl_cert()) > 0:
                    cluster_rel.data[self.unit]["cert"] = self.get_ssl_cert()
                    self.ks.ssl_certs = []
                    # And read each certificate data, store on a StoredState
                    for u in cluster_rel.units:
                        if "cert" in cluster_rel.data[u]:
                            self.ks.ssl_certs.append(
                                cluster_rel.data[u]["cert"])

        # 3) Prepare context
        self.model.unit.status = \
            MaintenanceStatus("Generate Listener settings")
        self._generate_listener_request()
        if not isinstance(self.model.unit.status, MaintenanceStatus):
            logger.info("Generate listener request blockd the status, return")
            return

        # Config change will generate JAAS and render svc override confs
        self.model.unit.status = \
            MaintenanceStatus("Render JAAS configs")
        ctx = super()._on_config_changed(event)
        if not isinstance(self.model.unit.status, MaintenanceStatus):
            logger.info("JAAS conf blocked status, return")
            return

        # Running main config render
        self.model.unit.status = \
            MaintenanceStatus("Render connect-distributed.properties")
        try:
            logger.debug("Running render_connect_distributed_properties()")
            ctx["c_opts"] = self._render_connect_distribute_properties()
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
        if not isinstance(self.model.unit.status, MaintenanceStatus):
            logger.info("Render connect distributed blocked status, return")
            return

        # Render log4j config
        self.model.unit.status = MaintenanceStatus("Render log4j properties")
        logger.debug("Running log4j properties renderer")
        ctx["log4j_opts"] = self._render_connect_log4j_properties()
        if not isinstance(self.model.unit.status, MaintenanceStatus):
            logger.info("render log4j blocked status, return")
            return

        self.model.unit.status = \
            MaintenanceStatus("Render service override conf file")
        logger.debug("Render override.conf")
        ctx["service"] = self.render_service_override_file(
            target="/etc/systemd/system/"
                   "{}.service.d/override.conf".format(self.service))
        self._render_systemd_service()
        if not isinstance(self.model.unit.status, MaintenanceStatus):
            logger.info("render svc override blocked status, return")
            return

        # Restart has_exception for the next event
        self.ks.has_exception = ""
        # 4) Open ports
        # 4.1) Close original ports
        # for p in self.ks.ports:
        #     if p > 0:
        #         close_port(p)
        # 4.2) Open ports for the newly found listeners
        open_port(self.config.get("clientPort", 8083))
        self.ks.ports = [self.config.get("clientPort", 8083)]

        # 5) Restart cycle
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

        # 6) Rerun load balancer config
        self._on_lb_provider_available(event)


if __name__ == "__main__":
    main(KafkaConnectCharm)
