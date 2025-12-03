import type { Channel, ChannelModel, Replies, Options } from "amqplib";

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
