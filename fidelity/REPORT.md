# Live-AWS fidelity report

Captured 2026-10-08 14:49 UTC against real AWS (us-east-1). Each probe targets a nonexistent resource; captures are raw wire bytes via botocore's transport.

| service | operation | error code | AWS status | ours | AWS CT | ours | verdict |
|---|---|---|---|---|---|---|---|
| apigateway | get_rest_api | `NotFoundException` | 404 | 404 | `application/json` | `application/json` | ✅ |
| dynamodb | describe_table | `ResourceNotFoundException` | 400 | 400 | `application/x-amz-json-1.0` | `application/x-amz-json-1.0` | ✅ |
| ec2 | describe_instances | `InvalidInstanceID.Malformed` | 400 | 400 | `text/xml;charset=UTF-8` | `text/xml` | ✅ |
| iam | get_user | `NoSuchEntity` | 404 | 404 | `text/xml` | `text/xml` | ✅ |
| kms | describe_key | `NotFoundException` | 400 | 400 | `application/x-amz-json-1.1` | `application/x-amz-json-1.1` | ✅ |
| lambda | get_function | `ResourceNotFoundException` | 404 | 404 | `application/json` | `application/json` | ✅ |
| logs | get_log_events | `ResourceNotFoundException` | 400 | 400 | `application/x-amz-json-1.1` | `application/x-amz-json-1.1` | ✅ |
| route53 | get_hosted_zone | `NoSuchHostedZone` | 404 | 404 | `text/xml` | `text/xml` | ✅ |
| s3 | head_bucket | `404` | 404 | 404 | `application/xml` | `application/xml` | ✅ |
| s3 | head_object | `404` | 404 | 404 | `application/xml` | `application/xml` | ✅ |
| secretsmanager | describe_secret | `ResourceNotFoundException` | 400 | 400 | `application/x-amz-json-1.1` | `application/x-amz-json-1.1` | ✅ |
| sns | get_topic_attributes | `InvalidClientTokenId` | 403 | 403 | `text/xml` | `text/xml` | ✅ |
| sqs | get_queue_url | `AWS.SimpleQueueService.NonExistentQueue` | 400 | 400 | `application/x-amz-json-1.0` | `application/x-amz-json-1.0` | ✅ |

**13/13 probes: status + parsed Error.Code match.**
