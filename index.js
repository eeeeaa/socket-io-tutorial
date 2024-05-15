const express = require("express");
const { createServer } = require("node:http");
const { join } = require("node:path");
const { Server } = require("socket.io");

const sqlite3 = require("sqlite3");
const { open } = require("sqlite");
const { availableParallelism } = require("node:os");
const cluster = require("node:cluster");
const { createAdapter, setupPrimary } = require("@socket.io/cluster-adapter");

if (cluster.isPrimary) {
  const numCPUs = availableParallelism();
  // create one worker per available core
  for (let i = 0; i < numCPUs; i++) {
    cluster.fork({
      PORT: 3000 + i,
    });
  }

  // set up the adapter on the primary thread
  return setupPrimary();
}

async function main() {
  // open the database file
  const db = await open({
    filename: "chat.db",
    driver: sqlite3.Database,
  });

  // create our 'messages' table (you can ignore the 'client_offset' column for now)
  await db.exec(`
    CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_offset TEXT UNIQUE,
        content TEXT
    );
  `);

  const app = express();
  const server = createServer(app);
  const io = new Server(server, {
    //store all data temporary in case of disconnection and attempt to recover after reconnection
    //disabled by default
    //(another way is for client to keep track of last data processed i.e. with database)
    connectionStateRecovery: {},
    // set up the adapter on each worker thread
    adapter: createAdapter(),
  });

  app.get("/", (req, res) => {
    res.sendFile(join(__dirname, "index.html"));
  });

  //Server side of socket
  io.on("connection", async (socket) => {
    console.log("a user connected");
    socket.on("chat message", async (msg) => {
      let result;
      try {
        // store the message in the database
        result = await db.run(
          "INSERT INTO messages (content, client_offset) VALUES (?, ?)",
          msg,
          clientOffset
        );
      } catch (e) {
        if (e.errno === 19 /* SQLITE_CONSTRAINT */) {
          // the message was already inserted, so we notify the client
          callback();
        } else {
          // nothing to do, just let the client retry
        }
        return;
      }
      //emit the message to everyone, including the sender.
      //if want to exclude the sender -> use something like socket.broadcast.emit('...'); instead
      // include id of last message for client to keep track of
      io.emit("chat message", msg, result.lastID);

      // acknowledge the event (to stop client from retrying)
      callback();
    });
    if (!socket.recovered) {
      // if the connection state recovery was not successful
      // fetch all the messeges not sent to the client
      try {
        await db.each(
          "SELECT id, content FROM messages WHERE id > ?",
          [socket.handshake.auth.serverOffset || 0],
          (_err, row) => {
            socket.emit("chat message", row.content, row.id);
          }
        );
      } catch (e) {
        // something went wrong
      }
    }
    socket.on("disconnect", () => {
      console.log("user disconnected");
    });
  });

  // each worker will listen on a distinct port
  const port = process.env.PORT;

  server.listen(port, () => {
    console.log(`server running at http://localhost:${port}`);
  });
}

main();
