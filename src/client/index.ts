import amqp, { type ConfirmChannel } from "amqplib";
import type { ArmyMove, RecognitionOfWar } from "../internal/gamelogic/gamedata.js";
import { clientWelcome, commandStatus, getInput, printClientHelp, printQuit } from "../internal/gamelogic/gamelogic.js";
import { GameState, type PlayingState } from "../internal/gamelogic/gamestate.js";
import { MoveOutcome, commandMove, handleMove } from "../internal/gamelogic/move.js";
import { handlePause } from "../internal/gamelogic/pause.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import { AckType, SimpleQueueType, declareAndBindQueue, subscribeJSON } from "../internal/pubsub/consume.js";
import { publishJSON } from "../internal/pubsub/publish.js";
import {
  ArmyMovesPrefix, ExchangePerilDirect, ExchangePerilTopic, PauseKey, WarRecognitionsPrefix
} from "../internal/routing/routing.js";
import { WarOutcome, handleWar } from "../internal/gamelogic/war.js";

function handlerPause(gs: GameState): (ps: PlayingState) => AckType {
  const handler = (ps: PlayingState) => {
    handlePause(gs, ps);
    process.stdout.write("> ");
    return AckType.Ack
  };
  return handler;
}


function handlerMove(channel: ConfirmChannel, gs: GameState, username: string): (move: ArmyMove) => Promise<AckType> {
  const handler = async (move: ArmyMove): Promise<AckType> => {
    const outcome = handleMove(gs, move);
    let ack = AckType.Ack;
    switch (outcome) {
      case MoveOutcome.MakeWar:
        try {
          await publishJSON(
            channel,
            ExchangePerilTopic,
            `${WarRecognitionsPrefix}.${username}`,
            { attacker: move.player, defender: gs.getPlayerSnap() } as RecognitionOfWar
          );
          ack = AckType.Ack;
        } catch (err) {
          console.error(`Failed to publish war recognition due to ${err as Error}`);
          ack = AckType.NackRequeue;
        }
        break;
      case MoveOutcome.Safe:
        ack = AckType.Ack;
        break;
      default:
        ack = AckType.NackDiscard;
        break;
    }
    process.stdout.write("> ");
    return ack;
  };
  return handler;
}


function handlerWar(gs: GameState): (rw: RecognitionOfWar) => Promise<AckType> {
  const handler = async (rw: RecognitionOfWar): Promise<AckType> => {
    const resolution = handleWar(gs, rw);
    let ack = AckType.Ack;
    switch (resolution.result) {
      case WarOutcome.NotInvolved:
        ack = AckType.NackRequeue;
        break;
      case WarOutcome.NoUnits:
        ack = AckType.NackDiscard;
        break;
      case WarOutcome.OpponentWon:
        ack = AckType.Ack;
        break;
      case WarOutcome.YouWon:
        ack = AckType.Ack;
        break;
      case WarOutcome.Draw:
        ack = AckType.Ack;
        break;
      default:
        console.error(`Unknown war resolution: ${resolution}`);
        ack = AckType.NackDiscard;
        break;
    }
    process.stdout.write("> ");
    return ack;
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

  const confirmChannel = await connection.createConfirmChannel();

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
    handlerMove(confirmChannel, gameState, username),
  );

  await subscribeJSON(
    connection,
    ExchangePerilTopic,
    WarRecognitionsPrefix,
    `${WarRecognitionsPrefix}.*`,
    SimpleQueueType.DURABLE,
    handlerWar(gameState),
  );

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
