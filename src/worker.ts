import { NativeConnection, Worker } from '@temporalio/worker';
import { diag, DiagConsoleLogger, DiagLogLevel } from '@opentelemetry/api';
import * as activities from './activities';
import { ParseablePlugin } from './plugin';

diag.setLogger(new DiagConsoleLogger(), DiagLogLevel.ERROR);

async function run() {
  const connection = await NativeConnection.connect({
    address: 'localhost:7233',
  });
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
          endpoint: process.env.PARSEABLE_URL ?? 'http://anton:8010',
          auth: {
            username: process.env.PARSEABLE_USERNAME ?? 'admin',
            password: process.env.PARSEABLE_PASSWORD ?? 'admin',
          },
        }),
      ],
    });

    // Step 3: Start accepting tasks on the `hello-world` queue
    //
    // The worker runs until it encounters an unexpected error or the process receives a shutdown signal registered on
    // the SDK Runtime object.
    //
    // By default, worker logs are written via the Runtime logger to STDERR at INFO level.
    //
    // See https://typescript.temporal.io/api/classes/worker.Runtime#install to customize these defaults.
    await worker.run();
  } finally {
    // Close the connection once the worker has stopped
    await connection.close();
  }
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
