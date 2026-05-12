import { NativeConnection, Worker } from '@temporalio/worker';
import { diag, DiagConsoleLogger, DiagLogLevel } from '@opentelemetry/api';
import * as activities from './activities';
import { ParseablePlugin } from '../src';

diag.setLogger(new DiagConsoleLogger(), DiagLogLevel.ERROR);

const PARSEABLE_URL = process.env.PARSEABLE_URL;
if (!PARSEABLE_URL) {
  console.error(
    'Set PARSEABLE_URL (and PARSEABLE_USERNAME / PARSEABLE_PASSWORD) before running the worker. ' +
      'Example: PARSEABLE_URL=http://localhost:8000 PARSEABLE_USERNAME=admin PARSEABLE_PASSWORD=admin npm run examples:worker',
  );
  process.exit(1);
}

async function run() {
  const connection = await NativeConnection.connect({ address: 'localhost:7233' });
  try {
    const worker = await Worker.create({
      connection,
      namespace: 'default',
      taskQueue: 'hello-world',
      workflowsPath: require.resolve('./workflows'),
      activities,
      plugins: [
        new ParseablePlugin({
          serviceName: 'temporal-worker',
          endpoint: PARSEABLE_URL,
          auth: {
            username: process.env.PARSEABLE_USERNAME ?? 'admin',
            password: process.env.PARSEABLE_PASSWORD ?? 'admin',
          },
        }),
      ],
    });

    await worker.run();
  } finally {
    await connection.close();
  }
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
