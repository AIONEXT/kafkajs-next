/**
 * TypedConsumer: Type-safe Kafka consumer with Zod schema validation.
 */

import { Consumer, ConsumerConfig, EachMessagePayload, KafkaJSConsumerOptions } from "kafkajs";
import { z, ZodSchema } from "zod";
import pino from "pino";

export type TypedMessageHandler<T> = (
  message: EachMessagePayload & { parsedValue: z.infer<T> }
) => Promise<void>;

export interface TypedConsumerConfig<T extends ZodSchema> {
  consumer: Consumer;
  schema: T;
  logger?: pino.Logger;
  validateOnReceive?: boolean;
  onValidationError?: (error: ValidationError, payload: EachMessagePayload) => Promise<void>;
}

export class TypedConsumer<T extends ZodSchema> {
  private consumer: Consumer;
  private schema: T;
  private logger: pino.Logger;
  private validateOnReceive: boolean;
  private onValidationError?: TypedConsumerConfig<T>["onValidationError"];

  constructor(config: TypedConsumerConfig<T>) {
    this.consumer = config.consumer;
    this.schema = config.schema;
    this.logger = config.logger || pino({ level: "silent" });
    this.validateOnReceive = config.validateOnReceive ?? true;
    this.onValidationError = config.onValidationError;
  }

  async subscribe(topics: string | string[], fromBeginning = false): Promise<void> {
    await this.consumer.subscribe({ topics: Array.isArray(topics) ? topics : [topics], fromBeginning });
  }

  async run(handler: TypedMessageHandler<T>): Promise<void> {
    await this.consumer.run({
      eachMessage: async (payload) => {
        if (this.validateOnReceive && payload.message.value) {
          try {
            const parsed = this.schema.parse(payload.message.value);
            await handler({ ...payload, parsedValue: parsed });
          } catch (error) {
            if (error instanceof z.ZodError) {
              const validationError = new ValidationError("Message validation failed", error.errors);
              this.logger.error({ errors: error.errors }, "Schema validation failed");

              if (this.onValidationError) {
                await this.onValidationError(validationError, payload);
              }
              return; // Skip invalid message
            }
            throw error;
          }
        } else {
          // No validation, pass through
          await handler(payload as EachMessagePayload & { parsedValue: z.infer<T> });
        }
      },
    });
  }

  async disconnect(): Promise<void> {
    await this.consumer.disconnect();
  }

  getSchema(): T {
    return this.schema;
  }

  getConsumer(): Consumer {
    return this.consumer;
  }
}

export class ValidationError extends Error {
  constructor(message: string, public readonly zodErrors: z.ZodIssue[]) {
    super(message);
    this.name = "ValidationError";
  }
}

export function createTypedConsumer<T extends ZodSchema>(
  consumer: Consumer,
  schema: T,
  options?: { logger?: pino.Logger; validateOnReceive?: boolean; onValidationError?: TypedConsumerConfig<T>["onValidationError"] }
): TypedConsumer<T> {
  return new TypedConsumer({
    consumer,
    schema,
    logger: options?.logger,
    validateOnReceive: options?.validateOnReceive,
    onValidationError: options?.onValidationError,
  });
}