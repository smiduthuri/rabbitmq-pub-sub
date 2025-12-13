# rabbitmq-pub-sub (Peril)

Based on Boot.dev's [Learn Pub/Sub](https://www.boot.dev/courses/learn-pub-sub-rabbitmq-typescript) course.

Building a TypeScript-based client/server setup that uses a [RabbitMQ](https://www.rabbitmq.com/tutorials/tutorial-one-javascript)
broker (or any AMQP-compliant message broker) for a simple pub-sub system.

## Description
This toy project is a simple backend implementation of the game "Peril" (a highly simplified form of Risk) between multiple
clients and one or more game servers.

The game is implemented as commands passed between clients, and between clients and servers, through a set of RabbitMQ
exchanges and queues, with various generic/specific routing keys depending on the types of actions performed by clients.

## How to use
These are instructions to simply run the game. To actually learn how the game was built, visit the course link above.

1. Install the NodeJS packages required:
    ```
    npm install
    ```
1. Start a RabbitMQ node as a docker container:
    ```
    npm run rabbit:start
    ```
    - Log into the Rabbit cluster at http://localhost:15672 (guest:guest)
    - Create new exchanges `peril_direct`, `peril_topic`, and `peril_dlx`.
    - Create a new queue `peril_dlq`, and bind it to the `peril_dlx` exchange.
1. Run the server:
    ```
    npm run server
    ```
1. To launch each client:
    ```
    npm run client
    ```
1. Give commands at each client to `spawn`, `move`, `status`, etc.
    - The `move` command creates messages and triggers actions that result in battles, war-declarations, etc.
