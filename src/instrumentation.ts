/**
 * Instrumented producer/consumer with automatic metrics.
 */

import { Producer, Consumer, ProducerRecord, EachMessagePayload } from "kafkajs";
import { MetricsCollector } from "./monitoring";
import pino from "pino";

export function createInstrumentedProducer(
  producer: Producer,
  metrics: MetricsCollector,
  logger?: pino.Logger
): Producer {
  const originalSend = producer.send.bind(producer);
  const originalSendBatch = producer.sendBatch.bind(producer);

  producer.send = async (record: ProducerRecord) => {
    const start = Date.now();
    try {
      const result = await originalSend(record);
      const size = record.messages.reduce((sum, m) => sum + (m.value?.length || 0), 0);
      metrics.recordProducerMessage(size, Date.now() - start);
      return result;
    } catch (error) {
      metrics.recordProducerFailure();
      throw error;
    }
  };

  producer.sendBatch = async (records: ProducerRecord[]) => {
    const start = Date.now();
    try {
      const result = await originalSendBatch(records);
      const size = records.reduce((sum, r) =>
        sum + r.messages.reduce((s, m) => s + (m.value?.length || 0), 0), 0);
      metrics.recordProducerMessage(size, Date.now() - start);
      return result;
    } catch (error) {
      metrics.recordProducerFailure();
      throw error;
    }
  };

  return producer;
}

export function createInstrumentedConsumer(
  consumer: Consumer,
  metrics: MetricsCollector,
  logger?: pino.Logger
): Consumer {
  const originalRun = consumer.run.bind(consumer);

  consumer.run = async (config: any) => {
    const originalEachMessage = config.eachMessage;
    config.eachMessage = async (payload: EachMessagePayload) => {
      const start = Date.now();
      try {
        await originalEachMessage?.(payload);
        metrics.recordConsumerMessage(payload.message.value?.length || 0, Date.now() - start);
      } catch (error) {
        metrics.recordConsumerFailure();
        throw error;
      }
    };
    return originalRun(config);
  };

  return consumer;
}