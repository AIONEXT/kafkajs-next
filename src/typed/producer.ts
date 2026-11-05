/**
 * TypedProducer: Type-safe Kafka producer with Zod schema validation.
 */

import { Producer, ProducerRecord, KafkaJSProducerOptions } from "kafkajs";
import { z, ZodSchema } from "zod";
import pino from "pino";

export interface TypedProducerConfig<T extends ZodSchema> {
  producer: Producer;
  schema: T;
  logger?: pino.Logger;
  validateOnSend?: boolean;
}

export class TypedProducer<T extends ZodSchema> {
  private producer: Producer;
  private schema: T;
  private logger: pino.Logger;
  private validateOnSend: boolean;

  constructor(config: TypedProducerConfig<T>) {
    this.producer = config.producer;
    this.schema = config.schema;
    this.logger = config.logger || pino({ level: "silent" });
    this.validateOnSend = config.validateOnSend ?? true;
  }

  async send(record: ProducerRecord): Promise<void> {
    const messages = record.messages || [];

    if (this.validateOnSend) {
      for (const message of messages) {
        if (message.value) {
          try {
            this.schema.parse(message.value);
          } catch (error) {
            if (error instanceof z.ZodError) {
              this.logger.error({ errors: error.errors }, "Schema validation failed");
              throw new ValidationError("Message validation failed", error.errors);
            }
            throw error;
          }
        }
      }
    }

    await this.producer.send(record);
  }

  async sendBatch(records: ProducerRecord[]): Promise<void> {
    await this.producer.sendBatch(records);
  }

  async disconnect(): Promise<void> {
    await this.producer.disconnect();
  }

  getSchema(): T {
    return this.schema;
  }

  getProducer(): Producer {
    return this.producer;
  }
}

export class ValidationError extends Error {
  constructor(message: string, public readonly zodErrors: z.ZodIssue[]) {
    super(message);
    this.name = "ValidationError";
  }
}

export function createTypedProducer<T extends ZodSchema>(
  producer: Producer,
  schema: T,
  options?: { logger?: pino.Logger; validateOnSend?: boolean }
): TypedProducer<T> {
  return new TypedProducer({
    producer,
    schema,
    logger: options?.logger,
    validateOnSend: options?.validateOnSend,
  });
}