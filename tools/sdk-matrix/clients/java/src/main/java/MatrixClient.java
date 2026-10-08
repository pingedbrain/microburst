import java.net.URI;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.GetFunctionRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;

/**
 * AWS SDK for Java v2 client for the SDK matrix.
 * Prints one JSON line per scenario: {sdk, scenario, code, status, attempts}.
 * Attempts counted via an ExecutionInterceptor — beforeTransmission runs
 * per HTTP attempt.
 */
public final class MatrixClient {
    private static final String ENDPOINT =
        System.getenv().getOrDefault("MB_ENDPOINT", "http://127.0.0.1:9999");

    static final class AttemptCounter implements ExecutionInterceptor {
        int n = 0;

        @Override
        public void beforeTransmission(
                Context.BeforeTransmission context,
                ExecutionAttributes attrs) {
            n++;
        }
    }

    private static ClientOverrideConfiguration override(AttemptCounter c) {
        return ClientOverrideConfiguration.builder()
            .addExecutionInterceptor(c)
            .build();
    }

    private static StaticCredentialsProvider creds() {
        return StaticCredentialsProvider.create(
            AwsBasicCredentials.create("matrix", "matrix"));
    }

    private static String esc(String s) {
        return s == null ? "null" : "\"" + s.replace("\\", "\\\\")
            .replace("\"", "\\\"") + "\"";
    }

    private static void emit(String scenario, String code,
                             Integer status, int attempts, String error) {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("sdk", "\"java-v2\"");
        m.put("scenario", esc(scenario));
        m.put("code", esc(code));
        m.put("status", status == null ? "null" : status.toString());
        m.put("attempts", String.valueOf(attempts));
        StringBuilder sb = new StringBuilder("{");
        m.forEach((k, v) -> sb.append(esc(k)).append(":").append(v).append(","));
        if (error != null) {
            sb.append(esc("error")).append(":").append(esc(error)).append(",");
        }
        sb.setLength(sb.length() - 1);
        sb.append("}");
        System.out.println(sb);
    }

    private static void run(String scenario, Runnable call, AttemptCounter c) {
        try {
            call.run();
            emit(scenario, null, 200, c.n, "no error");
        } catch (SdkServiceException e) {
            emit(scenario, e.awsErrorDetails().errorCode(),
                 e.awsErrorDetails().sdkHttpResponse().statusCode(),
                 c.n, null);
        } catch (Exception e) {
            Integer status = null;
            emit(scenario, e.getClass().getSimpleName(), status, c.n,
                 String.valueOf(e.getMessage()).replace("\n", " "));
        }
    }

    public static void main(String[] args) {
        Set<String> only = new LinkedHashSet<>(Set.of(args));
        URI ep = URI.create(ENDPOINT);

        Map<String, Runnable> scenarios = new LinkedHashMap<>();
        scenarios.put("dynamo-throttle", () -> {
            AttemptCounter c = new AttemptCounter();
            var client = DynamoDbClient.builder()
                .endpointOverride(ep).region(Region.US_EAST_1)
                .credentialsProvider(creds())
                .overrideConfiguration(override(c)).build();
            run("dynamo-throttle",
                () -> client.describeTable(
                    DescribeTableRequest.builder().tableName("t").build()), c);
        });
        scenarios.put("lambda-notfound", () -> {
            AttemptCounter c = new AttemptCounter();
            var client = LambdaClient.builder()
                .endpointOverride(ep).region(Region.US_EAST_1)
                .credentialsProvider(creds())
                .overrideConfiguration(override(c)).build();
            run("lambda-notfound",
                () -> client.getFunction(
                    GetFunctionRequest.builder().functionName("f").build()), c);
        });
        scenarios.put("sqs-querycompat", () -> {
            AttemptCounter c = new AttemptCounter();
            var client = SqsClient.builder()
                .endpointOverride(ep).region(Region.US_EAST_1)
                .credentialsProvider(creds())
                .overrideConfiguration(override(c)).build();
            run("sqs-querycompat",
                () -> client.getQueueUrl(
                    GetQueueUrlRequest.builder().queueName("q").build()), c);
        });
        scenarios.put("s3-slowdown", () -> {
            AttemptCounter c = new AttemptCounter();
            var client = S3Client.builder()
                .endpointOverride(ep).region(Region.US_EAST_1)
                .credentialsProvider(creds())
                .serviceConfiguration(S3Configuration.builder()
                    .pathStyleEnabled(true).build())
                .overrideConfiguration(override(c)).build();
            run("s3-slowdown",
                () -> client.headBucket(
                    HeadBucketRequest.builder().bucket("b").build()), c);
        });

        scenarios.forEach((name, fn) -> {
            if (only.isEmpty() || only.contains(name)) {
                fn.run();
            }
        });
    }
}
