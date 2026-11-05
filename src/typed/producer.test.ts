import { describe, it, expect, vi, beforeEach } from "vitest";
import { z } from "zod";
import { TypedProducer, createTypedProducer, ValidationError } from "./producer";

const TestSchema = z.object({
  id: z.number(),
  name: z.string(),
  email: z.string().email(),
});

describe("TypedProducer", () => {
  let mockProducer: any;
  let typedProducer: TypedProducer<typeof TestSchema>;

  beforeEach(() => {
    mockProducer = {
      send: vi.fn().mockResolvedValue(undefined),
      disconnect: vi.fn().mockResolvedValue(undefined),
    };
    typedProducer = createTypedProducer(mockProducer, TestSchema);
  });

  it("should send valid messages", async () => {
    const validMessage = { id: 1, name: "Test", email: "test@example.com" };
    await typedProducer.send({
      topic: "test-topic",
      messages: [{ value: validMessage }],
    });

    expect(mockProducer.send).toHaveBeenCalledWith({
      topic: "test-topic",
      messages: [{ value: validMessage }],
    });
  });

  it("should reject invalid messages", async () => {
    const invalidMessage = { id: "not-a-number", name: "Test" };

    await expect(
      typedProducer.send({
        topic: "test-topic",
        messages: [{ value: invalidMessage }],
      })
    ).rejects.toThrow(ValidationError);
  });

  it("should skip validation when disabled", async () => {
    const noValidateProducer = createTypedProducer(mockProducer, TestSchema, { validateOnSend: false });
    const invalidMessage = { id: "not-a-number", name: "Test" };

    await noValidateProducer.send({
      topic: "test-topic",
      messages: [{ value: invalidMessage }],
    });

    expect(mockProducer.send).toHaveBeenCalled();
  });
});