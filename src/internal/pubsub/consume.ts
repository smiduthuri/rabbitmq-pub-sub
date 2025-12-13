import type { Channel, ChannelModel, ConsumeMessage, Replies, Options } from "amqplib";
import { decode } from "@msgpack/msgpack";

export enum SimpleQueueType {
  DURABLE = 1,
  TRANSIENT,
};

export enum AckType {
  Ack = 1,
  NackRequeue,
  NackDiscard,
};


export async function declareAndBindQueue(
  conn: ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
): Promise<[Channel, Replies.AssertQueue]> {
  const channel = await conn.createChannel();
  const queueOptions: Options.AssertQueue = {
    durable: queueType === SimpleQueueType.DURABLE,
    autoDelete: queueType === SimpleQueueType.TRANSIENT,
    exclusive: queueType === SimpleQueueType.TRANSIENT,
    arguments: { "x-dead-letter-exchange": "peril_dlx" },
  };
  const newQueue: Replies.AssertQueue = await channel.assertQueue(queueName, queueOptions);
  await channel.bindQueue(newQueue.queue, exchange, key);
  return [channel, newQueue];
}


export async function subscribeJSON<T>(
  conn: ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => Promise<AckType> | AckType,
): Promise<void> {
  const response = await declareAndBindQueue(conn, exchange, queueName, key, queueType);
  const channel: Channel = response[0];

  await channel.consume(queueName, async (message: ConsumeMessage | null) => {
    if (message === null) {
      return;
    }
    // Get message and run message handler.
    const messageContent = JSON.parse(message.content.toString());
    const response = await handler(messageContent);

    // Remove message from queue with ack
    switch (response) {
      case AckType.Ack:
        channel.ack(message);
        // console.debug(`Acked ${message.content.toString()}`);
        break;
      case AckType.NackRequeue:
        channel.nack(message, false, true);
        // console.debug(`NackRequeued ${message.content.toString()}`);
        break;
      case AckType.NackDiscard:
        channel.nack(message, false, false);
        // console.debug(`NackDiscarded ${message.content.toString()}`);
        break;
      default:
        throw new Error(`Unknown handler response: ${response}`);
    }
    process.stdout.write("> ");
  });
}


export async function subscribeMsgPack<T>(
  conn: ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => Promise<void> | Promise<AckType> | AckType,
  unmarshaller: (data: Buffer) => T,
): Promise<void> {
  const response = await declareAndBindQueue(conn, exchange, queueName, key, queueType);
  const channel: Channel = response[0];

  await channel.consume(queueName, async (message: ConsumeMessage | null) => {
    if (message === null) {
      return;
    }
    // Get message and run message handler.
    const messageContent = unmarshaller(message.content);
    const response = await handler(messageContent);

    // Remove message from queue with ack
    switch (response) {
      case AckType.Ack:
        channel.ack(message);
        break;
      case AckType.NackRequeue:
        channel.nack(message, false, true);
        break;
      case AckType.NackDiscard:
        channel.nack(message, false, false);
        break;
      default:
        throw new Error(`Unknown handler response: ${response}`);
    }
    process.stdout.write("> ");
  });
}
