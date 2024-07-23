/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineageClientUtils;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testcontainers.utility.DockerImageName;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.from_unixtime;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@Tag("integration-test")
class SparkStreamingTest {
  private static final String SPARK_3_OR_ABOVE = "^[3-9].*";
  private static final String SPARK_VERSION = "spark.version";

  @Getter
  static class InputMessage {
    private final String id;
    private final long epoch;

    public InputMessage(String id, long epoch) {
      this.id = id;
      this.epoch = epoch;
    }
  }

  @Getter
  @EqualsAndHashCode
  static class SchemaRecord {
    private final String name;
    private final String type;

    public SchemaRecord(String name, String type) {
      this.name = name;
      this.type = type;
    }
  }

  @Getter
  static class KafkaTestContainer {
    private final KafkaContainer kafka;
    private final String sourceTopic;
    private final String targetTopic;
    private final String bootstrapServers;

    public KafkaTestContainer(
        KafkaContainer kafka, String sourceTopic, String targetTopic, String bootstrapServers) {
      this.kafka = kafka;
      this.sourceTopic = sourceTopic;
      this.targetTopic = targetTopic;
      this.bootstrapServers = bootstrapServers;
    }

    public void stop() {
      kafka.stop();
    }

    public void close() {
      kafka.close();
    }

    public boolean isRunning() {
      return kafka.isRunning();
    }
  }

  @Getter
  static class PostgreSQLTestContainer {
    private final PostgreSQLContainer<?> postgres;

    public PostgreSQLTestContainer(PostgreSQLContainer<?> postgres) {
      this.postgres = postgres;
    }

    public void stop() {
      postgres.stop();
    }

    public String getNamespace() {
      return postgres.getHost() + ":" + postgres.getMappedPort(5432).toString();
    }
  }

  @Nested
  class Kafka {
    @Test
    @EnabledIfSystemProperty(named = SPARK_VERSION, matches = SPARK_3_OR_ABOVE)
    void testKafkaSourceToKafkaSink()
        throws TimeoutException, StreamingQueryException, IOException {
      KafkaTestContainer kafkaContainer = setupKafkaContainer();

      String bootstrapServers = kafkaContainer.getBootstrapServers();

      UUID testUuid = UUID.randomUUID();
      log.info("TestUuid is {}", testUuid);

      OpenLineageEndpointHandler handler = new OpenLineageEndpointHandler();
      HttpServer server = createHttpServer(handler);

      SparkSession spark = createSparkSession(server.getAddress().getPort());
      spark.sparkContext().setLogLevel("ERROR");

      String userDirProperty = System.getProperty("user.dir");
      Path userDirPath = Paths.get(userDirProperty);

      Path checkpointsDir =
          userDirPath.resolve("tmp").resolve("checkpoints").resolve(testUuid.toString());

      Dataset<Row> sourceStream =
          readKafkaTopic(spark, kafkaContainer.sourceTopic, bootstrapServers)
              .transform(this::processKafkaTopic);

      StreamingQuery streamingQuery =
          sourceStream
              .writeStream()
              .format("kafka")
              .option("topic", kafkaContainer.targetTopic)
              .option("kafka.bootstrap.servers", bootstrapServers)
              .option("checkpointLocation", checkpointsDir.toString())
              .trigger(Trigger.ProcessingTime(Duration.ofSeconds(4).toMillis()))
              .start();

      streamingQuery.awaitTermination(Duration.ofSeconds(20).toMillis());

      spark.stop();

      kafkaContainer.stop();

      kafkaContainer.close();

      Awaitility.await().atMost(Duration.ofSeconds(60)).until(() -> !kafkaContainer.isRunning());
      Awaitility.await()
          .atMost(Duration.ofSeconds(60))
          .until(() -> spark.sparkContext().isStopped());

      List<RunEvent> events =
          handler.eventsContainer.stream()
              .map(OpenLineageClientUtils::runEventFromJson)
              .collect(Collectors.toList());

      List<RunEvent> sqlEvents =
          events.stream()
              .filter(
                  x -> "STREAMING".equals(x.getJob().getFacets().getJobType().getProcessingType()))
              .collect(Collectors.toList());

      assertEquals(6, sqlEvents.size());

      List<RunEvent> nonEmptyInputEvents =
          events.stream().filter(x -> !x.getInputs().isEmpty()).collect(Collectors.toList());

      assertEquals(6, nonEmptyInputEvents.size());

      List<SchemaRecord> expectedInputSchema =
          Arrays.asList(
              new SchemaRecord("key", "binary"),
              new SchemaRecord("value", "binary"),
              new SchemaRecord("topic", "string"),
              new SchemaRecord("partition", "integer"),
              new SchemaRecord("offset", "long"),
              new SchemaRecord("timestamp", "timestamp"),
              new SchemaRecord("timestampType", "integer"));

      List<SchemaRecord> expectedOutputSchema =
          Arrays.asList(new SchemaRecord("key", "binary"), new SchemaRecord("value", "string"));

      nonEmptyInputEvents.forEach(
          event -> {
            assertEquals(1, event.getInputs().size());
            assertEquals(kafkaContainer.sourceTopic, event.getInputs().get(0).getName());
            assertTrue(event.getInputs().get(0).getNamespace().startsWith("kafka://prod-cluster:"));

            OpenLineage.SchemaDatasetFacet inputSchema =
                event.getInputs().get(0).getFacets().getSchema();

            List<SchemaRecord> inputSchemaFields = mapToSchemaRecord(inputSchema);

            assertEquals(expectedInputSchema, inputSchemaFields);

            assertEquals(1, event.getOutputs().size());
            assertEquals(kafkaContainer.targetTopic, event.getOutputs().get(0).getName());

            assertTrue(event.getOutputs().get(0).getNamespace().startsWith("kafka://prod-cluster:"));

            OpenLineage.SchemaDatasetFacet outputSchema =
                event.getOutputs().get(0).getFacets().getSchema();

            List<SchemaRecord> outputSchemaFields = mapToSchemaRecord(outputSchema);

            assertEquals(expectedOutputSchema, outputSchemaFields);
          });
    }

    @Test
    @EnabledIfSystemProperty(named = SPARK_VERSION, matches = SPARK_3_OR_ABOVE)
    void testKafkaSourceToBatchSink()
        throws IOException, TimeoutException, StreamingQueryException {
      KafkaTestContainer kafkaContainer = setupKafkaContainer();

      String bootstrapServers = kafkaContainer.getBootstrapServers();

      UUID testUuid = UUID.randomUUID();
      log.info("TestUuid is {}", testUuid);

      OpenLineageEndpointHandler handler = new OpenLineageEndpointHandler();
      HttpServer server = createHttpServer(handler);

      SparkSession spark = createSparkSession(server.getAddress().getPort());
      spark.sparkContext().setLogLevel("ERROR");

      Dataset<Row> sourceStream =
          readKafkaTopic(spark, kafkaContainer.sourceTopic, bootstrapServers)
              .transform(this::processKafkaTopic);

      StreamingQuery streamingQuery =
          sourceStream
              .writeStream()
              .foreachBatch(
                  (batch, batchId) -> {
                    batch.write().format("csv").mode("append").save("/tmp/batch_sink");
                  })
              .start();

      streamingQuery.awaitTermination(Duration.ofSeconds(20).toMillis());
      List<RunEvent> events =
          handler.eventsContainer.stream()
              .map(OpenLineageClientUtils::runEventFromJson)
              .collect(Collectors.toList());

      assertEquals(7, events.size());

      List<RunEvent> kafkaInputEvents =
          events.stream().filter(x -> !x.getInputs().isEmpty()).collect(Collectors.toList());

      assertEquals(2, kafkaInputEvents.size());

      kafkaInputEvents.forEach(
          event -> {
            assertEquals(1, event.getInputs().size());
            assertEquals(kafkaContainer.sourceTopic, event.getInputs().get(0).getName());
            assertTrue(event.getInputs().get(0).getNamespace().startsWith("kafka://prod-cluster:"));
          });

      List<RunEvent> outputEvents =
          events.stream().filter(x -> !x.getOutputs().isEmpty()).collect(Collectors.toList());

      assertEquals(4, outputEvents.size());

      outputEvents.forEach(
          event -> {
            assertEquals(1, event.getOutputs().size());
            assertEquals("/tmp/batch_sink", event.getOutputs().get(0).getName());
            assertEquals("file", event.getOutputs().get(0).getNamespace());
          });

      kafkaContainer.stop();
      spark.stop();
    }

    @Test
    @EnabledIfSystemProperty(named = SPARK_VERSION, matches = SPARK_3_OR_ABOVE)
    void testKafkaSourceToJdbcBatchSink()
        throws IOException, TimeoutException, StreamingQueryException {
      KafkaTestContainer kafkaContainer = setupKafkaContainer();
      PostgreSQLTestContainer postgresContainer = startPostgresContainer();

      postgresContainer.postgres.getJdbcUrl();

      String bootstrapServers = kafkaContainer.getBootstrapServers();

      UUID testUuid = UUID.randomUUID();
      log.info("TestUuid is {}", testUuid);

      OpenLineageEndpointHandler handler = new OpenLineageEndpointHandler();
      HttpServer server = createHttpServer(handler);

      SparkSession spark = createSparkSession(server.getAddress().getPort());
      spark.sparkContext().setLogLevel("ERROR");

      Dataset<Row> sourceStream =
          readKafkaTopic(spark, kafkaContainer.sourceTopic, bootstrapServers)
              .transform(this::processKafkaTopic);

      StreamingQuery streamingQuery =
          sourceStream
              .writeStream()
              .foreachBatch(
                  (batch, batchId) -> {
                    batch
                        .write()
                        .format("jdbc")
                        .option("url", postgresContainer.getPostgres().getJdbcUrl())
                        .option("driver", "org.postgresql.Driver")
                        .option("dbtable", "public.test")
                        .option("user", postgresContainer.getPostgres().getUsername())
                        .option("password", postgresContainer.getPostgres().getPassword())
                        .mode("append")
                        .save();
                  })
              .start();

      streamingQuery.awaitTermination(Duration.ofSeconds(20).toMillis());

      List<RunEvent> events =
          handler.eventsContainer.stream()
              .map(OpenLineageClientUtils::runEventFromJson)
              .collect(Collectors.toList());

      assertEquals(7, events.size());

      List<RunEvent> kafkaInputEvents =
          events.stream().filter(x -> !x.getInputs().isEmpty()).collect(Collectors.toList());

      assertEquals(2, kafkaInputEvents.size());

      kafkaInputEvents.forEach(
          event -> {
            assertEquals(1, event.getInputs().size());
            assertEquals(kafkaContainer.sourceTopic, event.getInputs().get(0).getName());
            assertTrue(event.getInputs().get(0).getNamespace().startsWith("kafka://prod-cluster:"));
          });

      List<RunEvent> outputEvents =
          events.stream().filter(x -> !x.getOutputs().isEmpty()).collect(Collectors.toList());

      assertEquals(4, outputEvents.size());

      outputEvents.forEach(
          event -> {
            assertEquals(1, event.getOutputs().size());
            assertEquals("openlineage.public.test", event.getOutputs().get(0).getName());
            assertTrue(event.getOutputs().get(0).getNamespace().startsWith("postgres://prod-cluster"));
          });

      postgresContainer.stop();
      kafkaContainer.stop();
      spark.stop();
    }

    @Test
    @EnabledIfSystemProperty(named = SPARK_VERSION, matches = SPARK_3_OR_ABOVE)
    void testKafkaClusterResolveNamespace() throws IOException, TimeoutException, StreamingQueryException {
      List<KafkaTestContainer> kafkaBrokers = setupKafkaCluster(3);

      OpenLineageEndpointHandler handler = new OpenLineageEndpointHandler();
      HttpServer httpServer = createHttpServer(handler);

      SparkSession spark = createSparkSession(httpServer.getAddress().getPort());

      spark.sparkContext().setLogLevel("WARN");

      spark
        .readStream()
        .format("kafka")
        .option("subscribe", kafkaBrokers.get(0).getSourceTopic())
        .option("kafka.bootstrap.servers", kafkaBrokers.stream().map(KafkaTestContainer::getBootstrapServers).collect(Collectors.joining(",")))
        .option("startingOffsets", "earliest")
        .load()
        .transform(this::processKafkaTopic)
        .writeStream().format("console").start()
        .awaitTermination(Duration.ofSeconds(10).toMillis());

      List<RunEvent> events = handler.eventsContainer.stream().
              map(OpenLineageClientUtils::runEventFromJson).
              collect(Collectors.toList());

      assertTrue(events.stream().anyMatch(x -> !x.getInputs().isEmpty()));

      events.stream().filter(x -> !x.getInputs().isEmpty()).forEach(
              event -> {
                assertEquals(1, event.getInputs().size());
                assertTrue(event.getInputs().get(0).getNamespace().startsWith("kafka://prod-cluster:"));
      });
    }

    private HttpServer createHttpServer(HttpHandler handler) throws IOException {

      int httpServerPort = new Random().nextInt(1000) + 10000;

      HttpServer server = HttpServer.create(new InetSocketAddress(httpServerPort), 0);
      server.createContext("/api/v1/lineage", handler);
      server.setExecutor(null);
      server.start();

      return server;
    }

    private Dataset<Row> processKafkaTopic(Dataset<Row> input) {
      StructType schema = StructType.fromDDL("id STRING, epoch LONG");
      return input
          .selectExpr("CAST(value AS STRING) AS value")
          .select(from_json(col("value"), schema).as("event"))
          .select(col("event.id").as("id"), col("event.epoch").as("epoch"))
          .select(col("id"), from_unixtime(col("epoch")).as("timestamp"))
          .select(functions.struct(col("id"), col("timestamp")).as("converted_event"))
          .select(
              expr("CAST('1' AS BINARY) AS key"),
              functions.to_json(col("converted_event")).as("value"));
    }

    private Dataset<Row> readKafkaTopic(SparkSession spark, String topic, String bootstrapServers) {
      return spark
          .readStream()
          .format("kafka")
          .option("subscribe", topic)
          .option("kafka.bootstrap.servers", bootstrapServers)
          .option("startingOffsets", "earliest")
          .load();
    }

    private PostgreSQLTestContainer startPostgresContainer() {
      PostgreSQLContainer<?> postgres =
          new PostgreSQLContainer<>(DockerImageName.parse("postgres:13"))
              .withDatabaseName("openlineage")
              .withPassword("openlineage")
              .withUsername("openlineage");

      postgres.start();

      return new PostgreSQLTestContainer(postgres);
    }

    private KafkaTestContainer setupKafkaContainer() {
      KafkaContainer kafka =
          new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));
      kafka.start();

      int kafkaTopicPrefix = new Random().nextInt(1000);
      String kafkaSourceTopic = "source-topic-" + kafkaTopicPrefix;
      String kafkaTargetTopic = "target-topic-" + kafkaTopicPrefix;

      String bootstrapServers = kafka.getBootstrapServers();

      createTopics(bootstrapServers, Arrays.asList(kafkaSourceTopic, kafkaTargetTopic));
      populateTopic(bootstrapServers, kafkaSourceTopic);

      return new KafkaTestContainer(kafka, kafkaSourceTopic, kafkaTargetTopic, bootstrapServers);
    }

    private List<KafkaTestContainer> setupKafkaCluster(Integer numberOfBrokers) {
      Network network = Network.newNetwork();
      String confluentPlatformVersion = "7.6.1";
      Integer internalTopicsRf = 2;

      GenericContainer zookeeper =
              new GenericContainer<>(DockerImageName.parse("confluentinc/cp-zookeeper")
                      .withTag("7.6.1"))
                      .withNetwork(network)
                      .withNetworkAliases("zookeeper")
                      .withEnv("ZOOKEEPER_CLIENT_PORT", String.valueOf(KafkaContainer.ZOOKEEPER_PORT));

      List<KafkaContainer> brokers =
              IntStream
                      .range(0, numberOfBrokers)
                      .mapToObj(brokerNum -> new KafkaContainer(
                              DockerImageName.
                                      parse("confluentinc/cp-kafka").
                                      withTag(confluentPlatformVersion))
                              .withNetwork(network)
                              .withNetworkAliases("broker-" + brokerNum)
                              .dependsOn(zookeeper)
                              .withExternalZookeeper("zookeeper:" + KafkaContainer.ZOOKEEPER_PORT)
                              .withEnv("KAFKA_BROKER_ID", brokerNum + "")
                              .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", internalTopicsRf + "")
                              .withEnv("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", internalTopicsRf + "")
                              .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", internalTopicsRf + "")
                              .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", internalTopicsRf + "")
                              .withStartupTimeout(Duration.ofMinutes(1))
                      )
                      .collect(Collectors.toList());

      brokers.forEach(KafkaContainer::start);

      int kafkaTopicPrefix = new Random().nextInt(1000);
      String kafkaSourceTopic = "source-topic-" + kafkaTopicPrefix;
      String kafkaTargetTopic = "target-topic-" + kafkaTopicPrefix;

      String bootstrapServers = brokers.
              stream().
              map(KafkaContainer::getBootstrapServers).
              collect(Collectors.joining(","));

      createTopics(bootstrapServers, Arrays.asList(kafkaSourceTopic, kafkaTargetTopic));
      populateTopic(bootstrapServers, kafkaSourceTopic);

      return brokers.stream().map(
                kafkaContainer -> new KafkaTestContainer(kafkaContainer, kafkaSourceTopic, kafkaTargetTopic, kafkaContainer.getBootstrapServers())
        ).collect(Collectors.toList());
    }

    private SparkSession createSparkSession(Integer httpServerPort) {
      String userDirProperty = System.getProperty("user.dir");
      Path userDirPath = Paths.get(userDirProperty);
      UUID testUuid = UUID.randomUUID();
      log.info("TestUuid is {}", testUuid);

      Path derbySystemHome =
          userDirPath.resolve("tmp").resolve("derby").resolve(testUuid.toString());
      Path sparkSqlWarehouse =
          userDirPath.resolve("tmp").resolve("spark-sql-warehouse").resolve(testUuid.toString());

      return SparkSession.builder()
          .appName("kafka-source-kafka-sink")
          .master("local[*]")
          .config("spark.extraListeners", OpenLineageSparkListener.class.getCanonicalName())
          .config("spark.driver.host", "localhost")
          .config("spark.driver.extraJavaOptions", "-Dderby.system.home=" + derbySystemHome)
          .config("spark.sql.warehouse.dir", sparkSqlWarehouse.toString())
          .config("spark.ui.enabled", false)
          .config("spark.openlineage.transport.type", "http")
          .config("spark.openlineage.transport.url", "http://localhost:" + httpServerPort)
          .config("spark.openlineage.facets.disabled", "[spark_unknown;]")
          .config("spark.openlineage.dataset.namespaceResolvers.prod-cluster.type", "hostList")
          .config(
                  "spark.openlineage.dataset.namespaceResolvers.prod-cluster.hosts", "[localhost]")
          .getOrCreate();
    }

    private List<SchemaRecord> mapToSchemaRecord(OpenLineage.SchemaDatasetFacet schema) {
      return schema.getFields().stream()
          .map(field -> new SchemaRecord(field.getName(), field.getType()))
          .collect(Collectors.toList());
    }

    private void populateTopic(String bootstrapServers, String topic) {
      Properties props = new Properties();
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      props.put(
          ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
      props.put(
          ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
      props.put(ProducerConfig.LINGER_MS_CONFIG, "100");

      ObjectMapper om = new ObjectMapper().findAndRegisterModules();
      try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
        List<Future<RecordMetadata>> futures =
            IntStream.range(0, 100)
                .mapToObj(
                    ignored ->
                        new InputMessage(
                            UUID.randomUUID().toString(), Instant.now().getEpochSecond()))
                .map(message -> serialize(om, message))
                .map(json -> new ProducerRecord<String, String>(topic, json))
                .map(
                    x ->
                        producer.send(
                            x,
                            (ignored, e) -> {
                              if (e != null) {
                                log.error("Failed to publish a message", e);
                              }
                            }))
                .collect(Collectors.toList());

        for (Future<RecordMetadata> future : futures) {
          future.get();
        }
      } catch (ExecutionException | InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    @SneakyThrows
    private String serialize(ObjectMapper mapper, InputMessage message) {
      return mapper.writeValueAsString(message);
    }

    private void createTopics(String bootstrapServers, Collection<String> topics) {
      try (AdminClient adminClient =
          AdminClient.create(
              ImmutableMap.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
        List<NewTopic> newTopics =
            topics.stream()
                .distinct()
                .map(topicName -> new NewTopic(topicName, 1, (short) 1))
                .collect(Collectors.toList());

        CreateTopicsResult result = adminClient.createTopics(newTopics);
        result.all().get();
      } catch (ExecutionException | InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }
}

@Tag("integration-test")
class OpenLineageEndpointHandler implements HttpHandler {
  List<String> eventsContainer = new ArrayList<>();

  public OpenLineageEndpointHandler() {}

  @Override
  public void handle(HttpExchange exchange) throws IOException {
    InputStreamReader isr =
        new InputStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8);
    BufferedReader br = new BufferedReader(isr);
    String value = br.readLine();

    eventsContainer.add(value);

    exchange.sendResponseHeaders(200, 0);
    try (Writer writer =
        new OutputStreamWriter(exchange.getResponseBody(), StandardCharsets.UTF_8)) {
      writer.write("{}");
    }
  }

  public List<String> getEvents() {
    return eventsContainer;
  }
}
