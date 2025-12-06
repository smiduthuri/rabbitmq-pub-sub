import type { Channel, ChannelModel, ConsumeMessage, Replies, Options } from "amqplib";

export enum SimpleQueueType {
  DURABLE = 1,
  TRANSIENT,
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
  handler: (data: T) => void,
): Promise<void> {
  const response = await declareAndBindQueue(conn, exchange, queueName, key, queueType);
  const channel: Channel = response[0];

  await channel.consume(queueName, (message: ConsumeMessage | null) => {
    if (message === null) {
      return;
    }
    // Get message and run message handler.
    const messageContent = JSON.parse(message.content.toString());
    handler(messageContent);

    // Remove message from queue with ack
    channel.ack(message);
  });
}
