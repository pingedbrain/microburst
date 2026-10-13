//! AWS SDK for Rust client for the SDK matrix.
//!
//! Prints one JSON line per scenario: {sdk, scenario, code, status, attempts}.
//! Attempts counted via a smithy Intercept — `read_before_attempt` fires per
//! HTTP attempt, including retries (same seam as go-v2's finalize middleware).

use std::collections::HashSet;
use std::env;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_runtime_api::client::interceptors::context::BeforeTransmitInterceptorContextRef;
use aws_smithy_runtime_api::client::interceptors::Intercept;
use aws_smithy_runtime_api::client::result::SdkError;
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
use aws_smithy_runtime_api::http::Response as HttpResponse;
use aws_smithy_types::config_bag::ConfigBag;
use aws_smithy_types::error::metadata::ProvideErrorMetadata;

const SDK: &str = "rust";

fn endpoint() -> String {
    env::var("MB_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:9999".into())
}

#[derive(Debug)]
struct AttemptCounter {
    n: Arc<AtomicUsize>,
}

impl Intercept for AttemptCounter {
    fn name(&self) -> &'static str {
        "AttemptCounter"
    }

    fn read_before_attempt(
        &self,
        _context: &BeforeTransmitInterceptorContextRef<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        self.n.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn creds() -> aws_sdk_dynamodb::config::SharedCredentialsProvider {
    use aws_sdk_dynamodb::config::{Credentials, SharedCredentialsProvider};
    SharedCredentialsProvider::new(Credentials::new(
        "matrix",
        "matrix",
        None,
        None,
        "matrix-client",
    ))
}

/// Name of the SdkError variant when it isn't a service (parsed) error —
/// emitted as `code`, like other clients emit exception type names.
fn sdk_err_kind<E>(e: &SdkError<E, HttpResponse>) -> &'static str {
    match e {
        SdkError::ConstructionFailure(_) => "ConstructionFailure",
        SdkError::TimeoutError(_) => "TimeoutError",
        SdkError::DispatchFailure(_) => "DispatchFailure",
        SdkError::ResponseError(_) => "ResponseError",
        SdkError::ServiceError(_) => "ServiceError",
        _ => "UnknownError",
    }
}

/// Describe an SDK call result as a matrix row.
fn row<O, E>(
    scenario: &str,
    result: &Result<O, SdkError<E, HttpResponse>>,
    attempts: usize,
) -> serde_json::Value
where
    E: ProvideErrorMetadata,
{
    match result {
        Ok(_) => serde_json::json!({
            "sdk": SDK, "scenario": scenario, "code": null,
            "status": 200, "attempts": attempts, "unexpected": "no error",
        }),
        Err(e) => {
            let status = e.raw_response().map(|r| r.status().as_u16() as i64);
            // codeless errors (e.g. HEAD 503) → null, like java-v2
            let code = match e.as_service_error() {
                Some(se) => se
                    .meta()
                    .code()
                    .map(|c| serde_json::Value::String(c.to_owned()))
                    .unwrap_or(serde_json::Value::Null),
                None => serde_json::Value::String(sdk_err_kind(e).to_string()),
            };
            serde_json::json!({
                "sdk": SDK, "scenario": scenario,
                "code": code, "status": status, "attempts": attempts,
            })
        }
    }
}

fn counter() -> (Arc<AtomicUsize>, AttemptCounter) {
    let n = Arc::new(AtomicUsize::new(0));
    (n.clone(), AttemptCounter { n })
}

async fn dynamo() -> serde_json::Value {
    let (n, ic) = counter();
    let conf = aws_sdk_dynamodb::Config::builder()
        .behavior_version(aws_sdk_dynamodb::config::BehaviorVersion::latest())
        .region(aws_sdk_dynamodb::config::Region::new("us-east-1"))
        .credentials_provider(creds())
        .endpoint_url(endpoint())
        .retry_config(aws_sdk_dynamodb::config::retry::RetryConfig::standard()
            .with_max_attempts(3))
        .interceptor(ic)
        .build();
    let res = aws_sdk_dynamodb::Client::from_conf(conf)
        .describe_table()
        .table_name("t")
        .send()
        .await;
    row("dynamo-throttle", &res, n.load(Ordering::SeqCst))
}

async fn lambda() -> serde_json::Value {
    let (n, ic) = counter();
    let conf = aws_sdk_lambda::Config::builder()
        .behavior_version(aws_sdk_lambda::config::BehaviorVersion::latest())
        .region(aws_sdk_lambda::config::Region::new("us-east-1"))
        .credentials_provider(creds())
        .endpoint_url(endpoint())
        .retry_config(aws_sdk_lambda::config::retry::RetryConfig::standard()
            .with_max_attempts(3))
        .interceptor(ic)
        .build();
    let res = aws_sdk_lambda::Client::from_conf(conf)
        .get_function()
        .function_name("f")
        .send()
        .await;
    row("lambda-notfound", &res, n.load(Ordering::SeqCst))
}

async fn sqs() -> serde_json::Value {
    let (n, ic) = counter();
    let conf = aws_sdk_sqs::Config::builder()
        .behavior_version(aws_sdk_sqs::config::BehaviorVersion::latest())
        .region(aws_sdk_sqs::config::Region::new("us-east-1"))
        .credentials_provider(creds())
        .endpoint_url(endpoint())
        .retry_config(aws_sdk_sqs::config::retry::RetryConfig::standard()
            .with_max_attempts(3))
        .interceptor(ic)
        .build();
    let res = aws_sdk_sqs::Client::from_conf(conf)
        .get_queue_url()
        .queue_name("q")
        .send()
        .await;
    row("sqs-querycompat", &res, n.load(Ordering::SeqCst))
}

async fn s3() -> serde_json::Value {
    let (n, ic) = counter();
    let conf = aws_sdk_s3::Config::builder()
        .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
        .region(aws_sdk_s3::config::Region::new("us-east-1"))
        .credentials_provider(creds())
        .endpoint_url(endpoint())
        .force_path_style(true)
        .retry_config(aws_sdk_s3::config::retry::RetryConfig::standard()
            .with_max_attempts(3))
        .interceptor(ic)
        .build();
    let res = aws_sdk_s3::Client::from_conf(conf)
        .head_bucket()
        .bucket("b")
        .send()
        .await;
    row("s3-slowdown", &res, n.load(Ordering::SeqCst))
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let only: HashSet<String> = env::args().skip(1).collect();
    let wanted = |name: &str| only.is_empty() || only.contains(name);
    if wanted("dynamo-throttle") {
        println!("{}", dynamo().await);
    }
    if wanted("lambda-notfound") {
        println!("{}", lambda().await);
    }
    if wanted("sqs-querycompat") {
        println!("{}", sqs().await);
    }
    if wanted("s3-slowdown") {
        println!("{}", s3().await);
    }
}
