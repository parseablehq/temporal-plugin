import { Connection, Client } from '@temporalio/client';
import { loadClientConnectConfig } from '@temporalio/envconfig';
import { failingExample } from './workflows';
import { nanoid } from 'nanoid';

async function run() {
  const config = loadClientConnectConfig();
  const connection = await Connection.connect(config.connectionOptions);
  const client = new Client({ connection });

  const handle = await client.workflow.start(failingExample, {
    taskQueue: 'hello-world',
    args: [42],
    workflowId: 'failing-workflow-' + nanoid(),
  });
  console.log(`Started workflow ${handle.workflowId}`);

  try {
    const result = await handle.result();
    console.log(`Unexpected success: ${result}`);
  } catch (err) {
    console.log(`Workflow failed as expected: ${err instanceof Error ? err.message : err}`);
  }
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
