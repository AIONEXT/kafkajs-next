/**
 * Monitoring and metrics collection.
 */

import pino from "pino";

export interface MonitoringConfig {
  enabled?: boolean;
  interval?: number;
  logger?: pino.Logger;
  prometheus?: {
    enabled: boolean;
    port?: number;
    path?: string;
  };
  customMetrics?: Record<string, () => number>;
}

export interface MetricsSnapshot {
  timestamp: number;
  producer: {
    messagesSent: number;
    messagesFailed: number;
    bytesSent: number;
    latencyMs: { p50: number; p95: number; p99: number };
  };
  consumer: {
    messagesReceived: number;
    messagesProcessed: number;
    messagesFailed: number;
    latencyMs: { p50: number; p95: number; p99: number };
    lag: number;
  };
  custom: Record<string, number>;
}

export class MetricsCollector {
  private config: MonitoringConfig;
  private logger: pino.Logger;
  private interval?: NodeJS.Timeout;
  private metrics: MetricsSnapshot = {
    timestamp: Date.now(),
    producer: { messagesSent: 0, messagesFailed: 0, bytesSent: 0, latencyMs: { p50: 0, p95: 0, p99: 0 } },
    consumer: { messagesReceived: 0, messagesProcessed: 0, messagesFailed: 0, latencyMs: { p50: 0, p95: 0, p99: 0 }, lag: 0 },
    custom: {},
  };

  constructor(config: MonitoringConfig = {}) {
    this.config = { enabled: true, interval: 60000, ...config };
    this.logger = this.config.logger || pino({ level: "silent" });
  }

  start(): void {
    if (!this.config.enabled) return;

    this.interval = setInterval(() => {
      this.metrics.timestamp = Date.now();
      if (this.config.customMetrics) {
        for (const [key, fn] of Object.entries(this.config.customMetrics)) {
          this.metrics.custom[key] = fn();
        }
      }
      this.logger.info(this.metrics, "Metrics snapshot");
    }, this.config.interval);
  }

  stop(): void {
    if (this.interval) {
      clearInterval(this.interval);
      this.interval = undefined;
    }
  }

  recordProducerMessage(size: number, latencyMs: number): void {
    this.metrics.producer.messagesSent++;
    this.metrics.producer.bytesSent += size;
    // Simplified latency tracking
  }

  recordProducerFailure(): void {
    this.metrics.producer.messagesFailed++;
  }

  recordConsumerMessage(size: number, latencyMs: number): void {
    this.metrics.consumer.messagesReceived++;
    this.metrics.consumer.messagesProcessed++;
  }

  recordConsumerFailure(): void {
    this.metrics.consumer.messagesFailed++;
  }

  setLag(lag: number): void {
    this.metrics.consumer.lag = lag;
  }

  getSnapshot(): MetricsSnapshot {
    return { ...this.metrics };
  }
}

export function createMetricsCollector(config?: MonitoringConfig): MetricsCollector {
  return new MetricsCollector(config);
}