/**
 * TypedConsumerGroup: Type-safe consumer group with automatic rebalancing.
 */

import { Kafka, ConsumerConfig, Consumer } from "kafkajs";
import { z, ZodSchema } from "zod";
import pino from "pino";

export interface ConsumerGroupConfig<T extends ZodSchema> {
  kafka: Kafka;
  groupId: string;
  schema: T;
  consumerConfig?: Partial<ConsumerConfig>;
  logger?: pino.Logger;
  validateOnReceive?: boolean;
}

export class TypedConsumerGroup<T extends ZodSchema> {
  private kafka: Kafka;
  private groupId: string;
  private schema: T;
  private consumerConfig: Partial<ConsumerConfig>;
  private logger: pino.Logger;
  private validateOnReceive: boolean;
  private consumers: Consumer[] = [];

  constructor(config: ConsumerGroupConfig<T>) {
    this.kafka = config.kafka;
    this.groupId = config.groupId;
    this.schema = config.schema;
    this.consumerConfig = config.consumerConfig || {};
    this.logger = config.logger || pino({ level: "silent" });
    this.validateOnReceive = config.validateOnReceive ?? true;
  }

  createConsumer(overrides?: Partial<ConsumerConfig>): Consumer {
    const consumer = this.kafka.consumer({
      groupId: this.groupId,
      ...this.consumerConfig,
      ...overrides,
    });
    this.consumers.push(consumer);
    return consumer;
  }

  async run(
    topics: string | string[],
    handler: (message: { topic: string; partition: number; offset: string; value: z.infer<typeof this.schema>; key: string | null; timestamp: string }) => Promise<void>
  ): Promise<void> {
    const consumer = this.createConsumer();
    await consumer.connect();
    await consumer.subscribe({ topics: Array.isArray(topics) ? topics : [topics], fromBeginning: false });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        if (message.value) {
          try {
            const parsed = this.schema.parse(message.value);
            await handler({
              topic,
              partition,
              offset: message.offset,
              value: parsed,
              key: message.key?.toString() || null,
              timestamp: message.timestamp,
            });
          } catch (error) {
            if (error instanceof z.ZodError) {
              this.logger.error({ errors: error.errors, topic, partition, offset: message.offset }, "Schema validation failed");
              return;
            }
            throw error;
          }
        }
      },
    });
  }

  async disconnectAll(): Promise<void> {
    await Promise.all(this.consumers.map((c) => c.disconnect()));
    this.consumers = [];
  }

  getSchema(): T {
    return this.schema;
  }
}

export function createConsumerGroup<T extends ZodSchema>(
  kafka: Kafka,
  groupId: string,
  schema: T,
  options?: { consumerConfig?: Partial<ConsumerConfig>; logger?: pino.Logger; validateOnReceive?: boolean }
): TypedConsumerGroup<T> {
  return new TypedConsumerGroup({
    kafka,
    groupId,
    schema,
    consumerConfig: options?.consumerConfig,
    logger: options?.logger,
    validateOnReceive: options?.validateOnReceive,
  });
}