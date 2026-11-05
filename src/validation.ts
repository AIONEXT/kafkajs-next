/**
 * Message validation utilities.
 */

import { z, ZodSchema, ZodError, ZodIssue } from "zod";

export interface ValidationError extends Error {
  zodErrors: ZodIssue[];
}

export function validateMessage<T>(schema: ZodSchema<T>, value: unknown): T {
  try {
    return schema.parse(value);
  } catch (error) {
    if (error instanceof ZodError) {
      const validationError = new Error("Message validation failed") as ValidationError;
      validationError.zodErrors = error.errors;
      throw validationError;
    }
    throw error;
  }
}

export function safeValidateMessage<T>(schema: ZodSchema<T>, value: unknown): { success: true; data: T } | { success: false; error: ValidationError } {
  const result = schema.safeParse(value);
  if (result.success) {
    return { success: true, data: result.data };
  }
  const validationError = new Error("Message validation failed") as ValidationError;
  validationError.zodErrors = result.error.errors;
  return { success: false, error: validationError };
}

export class SchemaValidator {
  private schemas: Map<string, ZodSchema> = new Map();

  register(name: string, schema: ZodSchema): void {
    this.schemas.set(name, schema);
  }

  validate(name: string, value: unknown): any {
    const schema = this.schemas.get(name);
    if (!schema) {
      throw new Error(`Schema not found: ${name}`);
    }
    return validateMessage(schema, value);
  }

  safeValidate(name: string, value: unknown): { success: true; data: any } | { success: false; error: ValidationError } {
    const schema = this.schemas.get(name);
    if (!schema) {
      const error = new Error(`Schema not found: ${name}`) as ValidationError;
      error.zodErrors = [];
      return { success: false, error };
    }
    return safeValidateMessage(schema, value);
  }
}