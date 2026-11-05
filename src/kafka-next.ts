/**
 * KafkaNext: High-level wrapper around kafkajs with TypeScript-first APIs.
 */

import { Kafka, KafkaConfig, Producer, Consumer, ConsumerConfig, Admin, EachMessagePayload, ZodSchema } from "kafkajs";
import { z } from "zod";
import pino from "pino";

import { TypedProducer, createTypedProducer } from "./typed/producer";
import { TypedConsumer, createTypedConsumer } from "./typed/consumer";
import { TypedConsumerGroup, createConsumerGroup } from "./typed/consumer-group";
import { SchemaRegistry, SchemaRegistryConfig } from "./schema-registry";
import { MonitoringConfig, MetricsCollector, createMetricsCollector } from "./monitoring";
import { RetryPolicy, DeadLetterQueue } from "./resilience";

export interface KafkaNextConfig extends KafkaConfig {
  schemaValidation?: boolean;
  schemaRegistry?: SchemaRegistryConfig;
  monitoring?: MonitoringConfig;
  retryPolicy?: RetryPolicy;
  deadLetterQueue?: DeadLetterQueue;
  logger?: pino.Logger;
}

export interface KafkaNext {
  kafka: Kafka;
  admin: Admin;
  producer: Producer;
  typedProducer: <T extends ZodSchema>(schema: T) => TypedProducer<T>;
  consumer: (config?: ConsumerConfig) => Consumer;
  typedConsumer: <T extends ZodSchema>(config: ConsumerConfig, schema: T) => TypedConsumer<T>;
  consumerGroup: <T extends ZodSchema>(groupId: string, config: ConsumerConfig, schema: T) => TypedConsumerGroup<T>;
  metrics: MetricsCollector;
  schemaRegistry: SchemaRegistry;
  disconnect: () => Promise<void>;
}

export interface CreateKafkaNextOptions {
  config: KafkaNextConfig;
  onProducerConnect?: (producer: Producer) => Promise<void>;
  onConsumerConnect?: (consumer: Consumer) => Promise<void>;
}

export async function createKafkaNext(options: CreateKafkaNextOptions): Promise<KafkaNext> {
  const { config, onProducerConnect, onConsumerConnect } = options;

  const logger = config.logger || pino({ level: "info" });
  const kafka = new Kafka(config);

  const admin = kafka.admin();
  await admin.connect();

  const producer = kafka.producer({
    allowAutoCreateTopics: true,
    transactionTimeout: 30000,
    ...config.producer,
  });
  await producer.connect();
  if (onProducerConnect) await onProducerConnect(producer);

  const metrics = createMetricsCollector(config.monitoring);
  const schemaRegistry = new SchemaRegistry(config.schemaRegistry || {});

  return {
    kafka,
    admin,
    producer,
    typedProducer: <T extends ZodSchema>(schema: T) => createTypedProducer(producer, schema, { logger }),
    consumer: (consumerConfig?: ConsumerConfig) => kafka.consumer({ groupId: "default", ...consumerConfig }),
    typedConsumer: <T extends ZodSchema>(consumerConfig: ConsumerConfig, schema: T) =>
      createTypedConsumer(kafka.consumer(consumerConfig), schema, { logger }),
    consumerGroup: <T extends ZodSchema>(groupId: string, consumerConfig: ConsumerConfig, schema: T) =>
      createConsumerGroup(kafka, groupId, schema, { consumerConfig, logger }),
    metrics,
    schemaRegistry,
    async disconnect() {
      await producer.disconnect();
      await admin.disconnect();
    },
  };
}