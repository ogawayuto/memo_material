FROM quay.io/debezium/connect:3.4

# Install Confluent Avro Converter and Schema Registry client
USER root

# Download Confluent Avro Converter and all required dependencies
RUN cd /kafka/libs && \
    curl -sO https://packages.confluent.io/maven/io/confluent/kafka-connect-avro-converter/7.8.1/kafka-connect-avro-converter-7.8.1.jar && \
    curl -sO https://packages.confluent.io/maven/io/confluent/kafka-connect-avro-data/7.8.1/kafka-connect-avro-data-7.8.1.jar && \
    curl -sO https://packages.confluent.io/maven/io/confluent/kafka-avro-serializer/7.8.1/kafka-avro-serializer-7.8.1.jar && \
    curl -sO https://packages.confluent.io/maven/io/confluent/kafka-schema-serializer/7.8.1/kafka-schema-serializer-7.8.1.jar && \
    curl -sO https://packages.confluent.io/maven/io/confluent/kafka-schema-converter/7.8.1/kafka-schema-converter-7.8.1.jar && \
    curl -sO https://packages.confluent.io/maven/io/confluent/kafka-schema-registry-client/7.8.1/kafka-schema-registry-client-7.8.1.jar && \
    curl -sO https://packages.confluent.io/maven/io/confluent/common-config/7.8.1/common-config-7.8.1.jar && \
    curl -sO https://packages.confluent.io/maven/io/confluent/common-utils/7.8.1/common-utils-7.8.1.jar && \
    curl -sO https://repo1.maven.org/maven2/org/apache/avro/avro/1.12.0/avro-1.12.0.jar && \
    curl -sO https://repo1.maven.org/maven2/org/apache/commons/commons-compress/1.27.1/commons-compress-1.27.1.jar && \
    curl -sO https://repo1.maven.org/maven2/com/google/guava/guava/33.0.0-jre/guava-33.0.0-jre.jar && \
    curl -sO https://repo1.maven.org/maven2/com/google/guava/failureaccess/1.0.2/failureaccess-1.0.2.jar

USER kafka
