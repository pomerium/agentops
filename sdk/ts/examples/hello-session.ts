import { createNodeClient } from "../src/node.js";
import { EndReason, SessionState, ToolCallStatus, subscribe } from "../src/index.js";

const client = createNodeClient({
  baseUrl: process.env.HARNESS_API_URL ?? "http://127.0.0.1:8099",
  tokenFile: process.env.HARNESS_TOKEN_FILE,
});

const { session } = await client.createSession({
  template: process.env.HARNESS_TEMPLATE ?? "runid",
  conversationRef: `hello-${Date.now()}`,
  approvalPrompt: "Run a hello-world session from the TypeScript example?",
  initialPrompt: "Say hello.",
});
const ref = { sessionId: session!.id };
console.log(`session ${ref.sessionId} is ${SessionState[session!.state]}`);

const feed = await subscribe(client, { ref });
for await (const event of feed) {
  switch (event.payload.case) {
    case "approvalRequired":
      console.log(`\n  approve at: ${event.payload.value.approvalUrl}\n`);
      break;
    case "agentMessage":
      console.log(event.payload.value.text);
      break;
    case "toolCall": {
      const call = event.payload.value;
      console.log(`  [${ToolCallStatus[call.status]}] ${call.title || call.id}`);
      break;
    }
    case "turnCompleted":
      console.log("\nturn finished");
      feed.close();
      break;
    case "sessionEnded":
      console.log(`session ended: ${EndReason[event.payload.value.reason]}`);
      break;
  }
}

await client.endSession({ ref });
