import type { ConfirmChannel } from "amqplib";


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
