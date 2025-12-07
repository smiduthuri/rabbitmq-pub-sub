import amqp from "amqplib";
import { type ArmyMove } from "../internal/gamelogic/gamedata.js";
import { clientWelcome, commandStatus, getInput, printClientHelp, printQuit } from "../internal/gamelogic/gamelogic.js";
import { GameState, type PlayingState } from "../internal/gamelogic/gamestate.js";
import { MoveOutcome, commandMove, handleMove } from "../internal/gamelogic/move.js";
import { handlePause } from "../internal/gamelogic/pause.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import { AckType, SimpleQueueType, declareAndBindQueue, subscribeJSON } from "../internal/pubsub/consume.js";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ArmyMovesPrefix, ExchangePerilDirect, ExchangePerilTopic, PauseKey } from "../internal/routing/routing.js";


function handlerPause(gs: GameState): (ps: PlayingState) => AckType {
  const handler = (ps: PlayingState) => {
    handlePause(gs, ps);
    process.stdout.write("> ");
    return AckType.Ack
  };
  return handler;
}


function handlerMove(gs: GameState): (move: ArmyMove) => AckType {
  const handler = (move: ArmyMove) => {
    let failed = false;
    let outcome: MoveOutcome = MoveOutcome.Safe;
    try {
      outcome = handleMove(gs, move);
    } catch (err) {
      console.error((err as Error).message);
      failed = true;
    } finally {
      process.stdout.write("> ");
    }

    if (!failed) {
      if (outcome === MoveOutcome.Safe || outcome === MoveOutcome.MakeWar) {
        return AckType.Ack;
      } else {
        return AckType.NackDiscard;
      }
    } else {
      return AckType.NackDiscard;
    }
  };
  return handler;
}


async function main() {
  console.log("Starting Peril client...");
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

  const username: string = await clientWelcome();
  await declareAndBindQueue(
    connection, ExchangePerilDirect, `${PauseKey}.${username}`, PauseKey, SimpleQueueType.TRANSIENT
  );

  const gameState = new GameState(username);
  await subscribeJSON(
    connection,
    ExchangePerilDirect,
    `${PauseKey}.${username}`,
    PauseKey,
    SimpleQueueType.TRANSIENT,
    handlerPause(gameState),
  );

  await subscribeJSON(
    connection,
    ExchangePerilTopic,
    `${ArmyMovesPrefix}.${username}`,
    `${ArmyMovesPrefix}.*`,
    SimpleQueueType.TRANSIENT,
    handlerMove(gameState),
  );

  const confirmChannel = await connection.createConfirmChannel();

  let quit: boolean = false;
  while (!quit) {
    const inputWords: string[] = await getInput("> ");
    if (!inputWords.length) {
      continue;
    }
    try {
      switch (inputWords[0]) {
        case "spawn":
          commandSpawn(gameState, inputWords);
          break;
        case "move":
          const move = commandMove(gameState, inputWords);
          await publishJSON(confirmChannel, ExchangePerilTopic, `${ArmyMovesPrefix}.${username}`, move);
          console.log("Published move");
          break;
        case "status":
          await commandStatus(gameState);
          break;
        case "help":
          printClientHelp();
          break;
        case "spam":
          console.log("Spamming not allowed yet!");
          break;
        case "quit":
          printQuit();
          quit = true;
          break;
        default:
          console.log("Unknown command:", inputWords[0]);
          break;
      }
    } catch (err) {
      console.error((err as Error).message);
    }
  }
  process.exit(0);
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
