// AWS SDK for .NET (v4) client for the SDK matrix.
// Prints one JSON line per scenario: {sdk, scenario, code, status, attempts}.
// Attempts counted in a DelegatingHandler injected via HttpClientFactory —
// SendAsync runs per HTTP attempt, including retries (same seam as go-v2's
// finalize middleware).

using System.Text.Json;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Lambda;
using Amazon.Lambda.Model;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.SQS;
using Amazon.SQS.Model;

var endpoint = System.Environment.GetEnvironmentVariable("MB_ENDPOINT")
    ?? "http://127.0.0.1:9999";

AWSCredentials Creds() => new BasicAWSCredentials("matrix", "matrix");

void Emit(string scenario, string? code, int? status, int attempts,
    string? error = null)
{
    var row = new Dictionary<string, object?>
    {
        ["sdk"] = "dotnet",
        ["scenario"] = scenario,
        ["code"] = code,
        ["status"] = status,
        ["attempts"] = attempts,
    };
    if (error != null) row["error"] = error;
    Console.Out.WriteLine(JsonSerializer.Serialize(row));
}

async Task Run(string scenario, Func<Task> call, int[] n)
{
    try
    {
        await call();
        Emit(scenario, null, 200, n[0], "no error");
    }
    catch (AmazonServiceException e)
    {
        Emit(scenario, e.ErrorCode, (int)e.StatusCode, n[0]);
    }
    catch (Exception e)
    {
        Emit(scenario, e.GetType().Name, null, n[0],
            e.Message.Replace("\n", " "));
    }
}

var scenarios = new Dictionary<string, Func<Task>>
{
    ["dynamo-throttle"] = async () =>
    {
        var n = new int[1];
        var cfg = new AmazonDynamoDBConfig
        {
            ServiceURL = endpoint,
            AuthenticationRegion = "us-east-1",
            MaxErrorRetry = 2, // 3 attempts total
            HttpClientFactory = new CountingHttpClientFactory(n),
        };
        using var client = new AmazonDynamoDBClient(Creds(), cfg);
        await Run("dynamo-throttle",
            () => client.DescribeTableAsync(new DescribeTableRequest { TableName = "t" }), n);
    },
    ["lambda-notfound"] = async () =>
    {
        var n = new int[1];
        var cfg = new AmazonLambdaConfig
        {
            ServiceURL = endpoint,
            AuthenticationRegion = "us-east-1",
            MaxErrorRetry = 2,
            HttpClientFactory = new CountingHttpClientFactory(n),
        };
        using var client = new AmazonLambdaClient(Creds(), cfg);
        await Run("lambda-notfound",
            () => client.GetFunctionAsync(new GetFunctionRequest { FunctionName = "f" }), n);
    },
    ["sqs-querycompat"] = async () =>
    {
        var n = new int[1];
        var cfg = new AmazonSQSConfig
        {
            ServiceURL = endpoint,
            AuthenticationRegion = "us-east-1",
            MaxErrorRetry = 2,
            HttpClientFactory = new CountingHttpClientFactory(n),
        };
        using var client = new AmazonSQSClient(Creds(), cfg);
        await Run("sqs-querycompat",
            () => client.GetQueueUrlAsync(new GetQueueUrlRequest { QueueName = "q" }), n);
    },
    ["s3-slowdown"] = async () =>
    {
        var n = new int[1];
        var cfg = new AmazonS3Config
        {
            ServiceURL = endpoint,
            AuthenticationRegion = "us-east-1",
            MaxErrorRetry = 2,
            ForcePathStyle = true,
            HttpClientFactory = new CountingHttpClientFactory(n),
        };
        using var client = new AmazonS3Client(Creds(), cfg);
        await Run("s3-slowdown",
            () => client.HeadBucketAsync(new HeadBucketRequest { BucketName = "b" }), n);
    },
};

var only = new HashSet<string>(args);
foreach (var (name, fn) in scenarios)
{
    if (only.Count > 0 && !only.Contains(name)) continue;
    await fn();
}

/// Counts every SendAsync — i.e. every HTTP attempt on the wire.
sealed class CountingHandler : DelegatingHandler
{
    private readonly int[] _n;
    public CountingHandler(int[] n) { _n = n; }
    protected override Task<HttpResponseMessage> SendAsync(
        HttpRequestMessage request, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref _n[0]);
        return base.SendAsync(request, cancellationToken);
    }
}

/// Gives each service client an HttpClient wired to our counter.
sealed class CountingHttpClientFactory : HttpClientFactory
{
    private readonly int[] _n;
    public CountingHttpClientFactory(int[] n) { _n = n; }
    public override HttpClient CreateHttpClient(IClientConfig clientConfig) =>
        new(new CountingHandler(_n) { InnerHandler = new HttpClientHandler() });
    public override bool UseSDKHttpClientCaching(IClientConfig clientConfig) => false;
    public override bool DisposeHttpClientsAfterUse(IClientConfig clientConfig) => true;
}
