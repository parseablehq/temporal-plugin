import { Connection, Client } from '@temporalio/client';
import { loadClientConnectConfig } from '@temporalio/envconfig';
import { userEventExample } from './workflows';
import { nanoid } from 'nanoid';

async function run() {
  const config = loadClientConnectConfig();
  const connection = await Connection.connect(config.connectionOptions);
  const client = new Client({ connection });

  const handle = await client.workflow.start(userEventExample, {
    taskQueue: 'hello-world',
    args: ['Parseable'],
    workflowId: 'event-workflow-' + nanoid(),
  });
  console.log(`Started workflow ${handle.workflowId}`);
  console.log(await handle.result());
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
