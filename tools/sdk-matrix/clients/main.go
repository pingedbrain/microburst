// AWS SDK for Go v2 client for the SDK matrix.
// Prints one JSON line per scenario: {sdk, scenario, code, status, attempts}.
// Attempts counted via a finalize-step middleware (runs per HTTP attempt).

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	smithy "github.com/aws/smithy-go"
	smithymw "github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

var endpoint = envOr("MB_ENDPOINT", "http://127.0.0.1:9999")

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

type counterMW struct{ n *int }

func (m counterMW) ID() string { return "AttemptCounter" }

func (m counterMW) HandleFinalize(
	ctx context.Context, in smithymw.FinalizeInput, next smithymw.FinalizeHandler,
) (smithymw.FinalizeOutput, smithymw.Metadata, error) {
	*m.n++
	return next.HandleFinalize(ctx, in)
}

func cfgWithCounter(n *int) aws.Config {
	creds := credentials.NewStaticCredentialsProvider("matrix", "matrix", "")
	countOpt := func(stack *smithymw.Stack) error {
		return stack.Finalize.Add(counterMW{n}, smithymw.After)
	}
	return aws.Config{
		Region:      "us-east-1",
		Credentials: aws.NewCredentialsCache(creds),
		APIOptions:  []func(*smithymw.Stack) error{countOpt},
	}
}

func describe(row map[string]any, err error, attempts int) {
	if err == nil {
		row["code"] = nil
		row["status"] = 200
		row["attempts"] = attempts
		row["unexpected"] = "no error"
		return
	}
	var ae smithy.APIError
	if errors.As(err, &ae) {
		row["code"] = ae.ErrorCode()
	} else {
		row["code"] = fmt.Sprintf("%T", err)
	}
	var re *smithyhttp.ResponseError
	if errors.As(err, &re) {
		row["status"] = re.HTTPStatusCode()
	}
	row["attempts"] = attempts
}

func emit(scenario string, row map[string]any) {
	row["sdk"] = "go-v2"
	row["scenario"] = scenario
	b, _ := json.Marshal(row)
	fmt.Println(string(b))
}

func runDynamo() {
	n := new(int)
	cfg := cfgWithCounter(n)
	c := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
	_, err := c.DescribeTable(context.Background(),
		&dynamodb.DescribeTableInput{TableName: aws.String("t")})
	row := map[string]any{}
	describe(row, err, *n)
	emit("dynamo-throttle", row)
}

func runLambda() {
	n := new(int)
	cfg := cfgWithCounter(n)
	c := lambda.NewFromConfig(cfg, func(o *lambda.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
	_, err := c.GetFunction(context.Background(),
		&lambda.GetFunctionInput{FunctionName: aws.String("f")})
	row := map[string]any{}
	describe(row, err, *n)
	emit("lambda-notfound", row)
}

func runSQS() {
	n := new(int)
	cfg := cfgWithCounter(n)
	c := sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
	_, err := c.GetQueueUrl(context.Background(),
		&sqs.GetQueueUrlInput{QueueName: aws.String("q")})
	row := map[string]any{}
	describe(row, err, *n)
	emit("sqs-querycompat", row)
}

func runS3() {
	n := new(int)
	cfg := cfgWithCounter(n)
	c := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
	_, err := c.HeadBucket(context.Background(),
		&s3.HeadBucketInput{Bucket: aws.String("b")})
	row := map[string]any{}
	describe(row, err, *n)
	emit("s3-slowdown", row)
}

func main() {
	only := map[string]bool{}
	for _, a := range os.Args[1:] {
		only[a] = true
	}
	scenarios := map[string]func(){
		"dynamo-throttle": runDynamo,
		"lambda-notfound": runLambda,
		"sqs-querycompat": runSQS,
		"s3-slowdown":     runS3,
	}
	for name, fn := range scenarios {
		if len(only) > 0 && !only[name] {
			continue
		}
		fn()
	}
}
