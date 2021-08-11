ORIGINAL_SERVER_CONFIG="""
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