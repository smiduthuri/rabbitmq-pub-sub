import amqp from "amqplib";
import { decode } from "@msgpack/msgpack";
import { writeLog, type GameLog } from "../internal/gamelogic/logs.js";
import { SimpleQueueType, subscribeMsgPack, AckType } from "../internal/pubsub/consume.js";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilDirect, ExchangePerilTopic, GameLogSlug, PauseKey } from "../internal/routing/routing.js";
import { getInput, printServerHelp } from "../internal/gamelogic/gamelogic.js";


function handlerLog(): (gameLog: GameLog) => Promise<AckType> {
  const handler = async (gameLog: GameLog): Promise<AckType> => {
    await writeLog(gameLog);
    return AckType.Ack;
  };
  return handler;
}


async function main() {
  console.log("Starting Peril server...");
  const rabbitMQConnection: string = "amqp://guest:guest@localhost:5672/";
  const connection = await amqp.connect(rabbitMQConnection);
  console.log("Connection to RabbitMQ was successful.");

  ["SIGINT", "SIGTERM"].forEach((signal) =>
    process.on(signal, async () => {
      try {
        await connection.close();
        console.log("RabbitMQ connection closed:", signal);
      } catch (err) {
        console.error("Error closing RabbitMQ connection:", err);
      } finally {
        process.exit(0);
      }
    }),
  );

  printServerHelp();

  const confirmChannel = await connection.createConfirmChannel();
  await subscribeMsgPack(
    connection,
    ExchangePerilTopic,
    GameLogSlug,
    `${GameLogSlug}.*`,
    SimpleQueueType.DURABLE,
    handlerLog(),
    (buffer) => decode(buffer) as GameLog,
  );

  let quit: boolean = false;

  while (!quit) {
    const inputWords: string[] = await getInput("> ");
    if (!inputWords.length) {
      continue;
    }
    try {
      switch (inputWords[0]) {
        case "help":
          printServerHelp();
          break;
        case "pause":
          try {
            await publishJSON(confirmChannel, ExchangePerilDirect, PauseKey, { isPaused: true });
          } catch (err) {
            console.error("Error publishing message:", err);
          }
          break;
        case "resume":
          try {
            await publishJSON(confirmChannel, ExchangePerilDirect, PauseKey, { isPaused: false });
          } catch (err) {
            console.error("Error publishing message:", err);
          }
          break;
        case "quit":
          console.log("Exiting game.");
          quit = true;
          break;
        default:
          console.log("Unknown command:", inputWords[0]);
          break;
      }
    } catch (err) {
      console.error(err);
    }
  }
  process.exit(0);
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
