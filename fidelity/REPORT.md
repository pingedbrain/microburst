# Live-AWS fidelity report

Captured 2026-10-09 15:39 UTC against real AWS (us-east-1). Each probe targets a nonexistent resource; captures are raw wire bytes via botocore's transport.

| service | operation | error code | AWS status | ours | AWS CT | ours | verdict |
|---|---|---|---|---|---|---|---|
| apigateway | get_rest_api | `NotFoundException` | 404 | 404 | `application/json` | `application/json` | ✅ |
| appsync | get_graphql_api | `NotFoundException` | 404 | 404 | `application/json` | `application/json` | ✅ |
| athena | get_work_group | `InvalidRequestException` | 400 | 400 | `application/x-amz-json-1.1` | `application/x-amz-json-1.1` | ✅ |
| cloudformation | describe_stacks | `ValidationError` | 400 | 400 | `text/xml` | `text/xml` | ✅ |
| cloudwatch | get_dashboard | `ResourceNotFound` | 404 | 404 | `application/x-amz-json-1.0` | `application/x-amz-json-1.0` | ✅ |
| cognito-idp | describe_user_pool | `ResourceNotFoundException` | 400 | 400 | `application/x-amz-json-1.1` | `application/x-amz-json-1.1` | ✅ |
| dynamodb | describe_table | `ResourceNotFoundException` | 400 | 400 | `application/x-amz-json-1.0` | `application/x-amz-json-1.0` | ✅ |
| ec2 | describe_instances | `InvalidInstanceID.Malformed` | 400 | 400 | `text/xml;charset=UTF-8` | `text/xml;charset=UTF-8` | ✅ |
| elbv2 | describe_load_balancers | `ValidationError` | 400 | 400 | `text/xml` | `text/xml` | ✅ |
| events | describe_event_bus | `ResourceNotFoundException` | 400 | 400 | `application/x-amz-json-1.1` | `application/x-amz-json-1.1` | ✅ |
| glacier | describe_vault | `ResourceNotFoundException` | 404 | 404 | `application/json` | `application/json` | ✅ |
| iam | get_user | `NoSuchEntity` | 404 | 404 | `text/xml` | `text/xml` | ✅ |
| kinesis | describe_stream | `ResourceNotFoundException` | 400 | 400 | `application/x-amz-json-1.1` | `application/x-amz-json-1.1` | ✅ |
| kms | describe_key | `NotFoundException` | 400 | 400 | `application/x-amz-json-1.1` | `application/x-amz-json-1.1` | ✅ |
| lambda | get_function | `ResourceNotFoundException` | 404 | 404 | `application/json` | `application/json` | ✅ |
| logs | get_log_events | `ResourceNotFoundException` | 400 | 400 | `application/x-amz-json-1.1` | `application/x-amz-json-1.1` | ✅ |
| pinpoint | get_app | `NotFoundException` | 404 | 404 | `application/json` | `application/json` | ✅ |
| rds | describe_db_instances | `DBInstanceNotFound` | 404 | 404 | `text/xml` | `text/xml` | ✅ |
| route53 | get_hosted_zone | `NoSuchHostedZone` | 404 | 404 | `text/xml` | `text/xml` | ✅ |
| route53resolver | get_resolver_endpoint | `ResourceNotFoundException` | 400 | 400 | `application/x-amz-json-1.1` | `application/x-amz-json-1.1` | ✅ |
| s3 | head_bucket | `404` | 404 | 404 | `application/xml` | `application/xml` | ✅ |
| s3 | head_object | `404` | 404 | 404 | `application/xml` | `application/xml` | ✅ |
| secretsmanager | describe_secret | `ResourceNotFoundException` | 400 | 400 | `application/x-amz-json-1.1` | `application/x-amz-json-1.1` | ✅ |
| sesv2 | get_email_identity | `NotFoundException` | 404 | 404 | `application/x-amz-json-1.1` | `application/x-amz-json-1.1` | ✅ |
| sns | get_topic_attributes | `InvalidClientTokenId` | 403 | 403 | `text/xml` | `text/xml` | ✅ |
| sqs | get_queue_url | `AWS.SimpleQueueService.NonExistentQueue` | 400 | 400 | `application/x-amz-json-1.0` | `application/x-amz-json-1.0` | ✅ |
| stepfunctions | describe_state_machine | `AccessDeniedException` | 400 | 400 | `application/x-amz-json-1.0` | `application/x-amz-json-1.0` | ✅ |
| wafv2 | get_web_acl | `ValidationException` | 400 | 400 | `application/x-amz-json-1.1` | `application/x-amz-json-1.1` | ✅ |

**28/28 probes: status + parsed Error.Code + Content-Type + envelope shape match.**
