import type { ConfirmChannel } from "amqplib";
import { encode } from "@msgpack/msgpack";

export async function publishJSON<T>(
  ch: ConfirmChannel,
  exchange: string,
  routingKey: string,
  value: T,
): Promise<void> {
  const stringifiedValue = JSON.stringify(value);
  ch.publish(exchange, routingKey, Buffer.from(stringifiedValue), { contentType: "application/json" });
  console.log("Published:", stringifiedValue);
}

export async function publishMsgPack<T>(
  ch: ConfirmChannel, exchange: string, routingKey: string, value: T
): Promise<void> {
  const serializedMessage = encode(value);
  ch.publish(exchange, routingKey, Buffer.from(serializedMessage), { contentType: "application/x-msgpack" });
  console.log(`Published: ${value}`);
}
