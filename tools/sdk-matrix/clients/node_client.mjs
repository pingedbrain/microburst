// AWS SDK for JavaScript v3 client for the SDK matrix.
// Prints one JSON line per scenario: {sdk, scenario, code, status, attempts}.
// attempts comes from $metadata.attempts (v3 records it on thrown errors).

import { DynamoDBClient, DescribeTableCommand } from "@aws-sdk/client-dynamodb";
import { SQSClient, GetQueueUrlCommand } from "@aws-sdk/client-sqs";
import { LambdaClient, GetFunctionCommand } from "@aws-sdk/client-lambda";
import { S3Client, HeadBucketCommand } from "@aws-sdk/client-s3";

const ENDPOINT = process.env.MB_ENDPOINT || "http://127.0.0.1:9999";
const base = {
  endpoint: ENDPOINT,
  region: "us-east-1",
  credentials: { accessKeyId: "matrix", secretAccessKey: "matrix" },
  maxAttempts: 3,
};

const SCENARIOS = {
  "dynamo-throttle": () => new DynamoDBClient(base)
    .send(new DescribeTableCommand({ TableName: "t" })),
  "lambda-notfound": () => new LambdaClient(base)
    .send(new GetFunctionCommand({ FunctionName: "f" })),
  "sqs-querycompat": () => new SQSClient(base)
    .send(new GetQueueUrlCommand({ QueueName: "q" })),
  "s3-slowdown": () => new S3Client({ ...base, forcePathStyle: true })
    .send(new HeadBucketCommand({ Bucket: "b" })),
};

const only = new Set(process.argv.slice(2));
for (const [name, call] of Object.entries(SCENARIOS)) {
  if (only.size && !only.has(name)) continue;
  let row = { sdk: "js-v3", scenario: name };
  try {
    await call();
    row = { ...row, code: null, status: 200, unexpected: "no error" };
  } catch (e) {
    row = {
      ...row,
      code: e.name ?? e.Code ?? e.code ?? null,
      status: e.$metadata?.httpStatusCode ?? e.$response?.statusCode ?? null,
      attempts: e.$metadata?.attempts ?? null,
    };
  }
  console.log(JSON.stringify(row));
}
