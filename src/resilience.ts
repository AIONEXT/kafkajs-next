/**
 * Retry policies and dead letter queue handling.
 */

export interface RetryPolicy {
  maxRetries: number;
  initialDelayMs: number;
  maxDelayMs: number;
  backoffMultiplier: number;
  retryableErrors?: string[];
}

export interface DeadLetterQueueConfig {
  enabled: boolean;
  topic: string;
  maxRetriesBeforeDlq?: number;
  includeOriginalMessage?: boolean;
  includeError?: boolean;
}

export interface RetryContext {
  attempt: number;
  error: Error;
  timestamp: number;
}

export const DEFAULT_RETRY_POLICY: RetryPolicy = {
  maxRetries: 3,
  initialDelayMs: 1000,
  maxDelayMs: 30000,
  backoffMultiplier: 2,
  retryableErrors: ["TIMEOUT", "NETWORK_ERROR", "CONNECTION_FAILED"],
};

export const DEFAULT_DLQ_CONFIG: DeadLetterQueueConfig = {
  enabled: true,
  topic: "dead-letter",
  maxRetriesBeforeDlq: 3,
  includeOriginalMessage: true,
  includeError: true,
};

export function calculateDelay(policy: RetryPolicy, attempt: number): number {
  const delay = policy.initialDelayMs * Math.pow(policy.backoffMultiplier, attempt);
  return Math.min(delay, policy.maxDelayMs);
}

export function isRetryable(error: Error, policy: RetryPolicy): boolean {
  if (!policy.retryableErrors || policy.retryableErrors.length === 0) return true;
  return policy.retryableErrors.some((e) => error.message.includes(e) || error.name.includes(e));
}

export async function withRetry<T>(
  fn: () => Promise<T>,
  policy: RetryPolicy = DEFAULT_RETRY_POLICY,
  onRetry?: (context: RetryContext) => void
): Promise<T> {
  let lastError: Error;

  for (let attempt = 0; attempt <= policy.maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      lastError = error as Error;

      if (attempt === policy.maxRetries) break;

      if (!isRetryable(lastError, policy)) {
        throw lastError;
      }

      const delay = calculateDelay(policy, attempt);

      if (onRetry) {
        onRetry({ attempt, error: lastError, timestamp: Date.now() });
      }

      await new Promise((resolve) => setTimeout(resolve, delay));
    }
  }

  throw lastError!;
}

export class DeadLetterQueue {
  private config: DeadLetterQueueConfig;
  private queue: Array<{ message: any; error: Error; timestamp: number; retries: number }> = [];

  constructor(config: DeadLetterQueueConfig = DEFAULT_DLQ_CONFIG) {
    this.config = config;
  }

  async add(message: any, error: Error, retries: number): Promise<void> {
    if (!this.config.enabled) return;

    if (retries >= (this.config.maxRetriesBeforeDlq ?? 3)) {
      this.queue.push({
        message: this.config.includeOriginalMessage ? message : undefined,
        error: this.config.includeError ? error : undefined,
        timestamp: Date.now(),
        retries,
      });
      // In production, send to Kafka topic
    }
  }

  getQueue(): typeof this.queue {
    return [...this.queue];
  }

  clear(): void {
    this.queue = [];
  }
}