"""Service model access via botocore.

Wraps botocore's loader to resolve AWS service models (protocol, operations,
error shapes) by the names requests actually carry: SigV4 credential scope and
endpoint prefixes. Everything is lazy and cached.
"""

from __future__ import annotations

from functools import cache
from typing import Any

import botocore.session

# SigV4 credential scopes that differ from the botocore service name.
# Derived empirically by resolving the signing name of a boto3 client for
# every modeled service. A scope that is itself a valid service name
# resolves directly and needs no entry; families that share one scope are
# noted — operation/targetPrefix detection disambiguates those further.
_SCOPE_ALIASES = {
    "IngestionService": "importexport",
    "IoTSecuredTunneling": "iotsecuretunneling",
    "access-analyzer": "accessanalyzer",
    "aco-automation": "compute-optimizer-automation",
    "aidevops": "devops-agent",
    "airflow": "mwaa",
    "airflow-serverless": "mwaa-serverless",
    "aoss": "opensearchserverless",
    "app-integrations": "appintegrations",
    "application-cost-profiler": "applicationcostprofiler",
    "applicationinsights": "application-insights",
    "aps": "amp",
    "aws-marketplace": "meteringmarketplace",  # marketplace family scope
    "awsssoportal": "sso",
    "backup-search": "backupsearch",
    "cassandra": "keyspaces",  # also keyspacesstreams
    "cases": "connectcases",
    "cleanrooms-ml": "cleanroomsml",
    "cloudcontrolapi": "cloudcontrol",
    "codeguru-profiler": "codeguruprofiler",
    "connect-campaigns": "connectcampaignsv2",  # also v1
    "elasticfilesystem": "efs",
    "elasticloadbalancing": "elbv2",  # also elb (classic)
    "elasticmapreduce": "emr",
    "elemental-inference": "elementalinference",
    "end-user-messaging": "endusermessaging",
    "execute-api": "apigatewaymanagementapi",  # also connectparticipant
    "finspace-api": "finspace-data",
    "geo": "location",
    "health-agent": "connecthealth",
    "iotdata": "iot-data",
    "iotmanagedintegrations": "iot-managed-integrations",
    "launchwizard": "launch-wizard",
    "lex": "lexv2-runtime",  # also lex-models/-runtime, lexv2-models
    "migrationhub-orchestrator": "migrationhuborchestrator",
    "migrationhub-strategy": "migrationhubstrategy",
    "mobiletargeting": "pinpoint",
    "monitoring": "cloudwatch",
    "mturk-requester": "mturk",
    "neptune-db": "neptunedata",
    "notifications-contacts": "notificationscontacts",
    "partnercentral": "partnercentral-revenue-measurement",
    "pricingplanmanager": "pricing-plan-manager",
    "profile": "customer-profiles",
    "refactor-spaces": "migration-hub-refactor-spaces",
    "s3-outposts": "s3outposts",
    "scn": "supplychain",
    "servicequotas": "service-quotas",
    "social-messaging": "socialmessaging",
    "sso-oauth": "sso-oidc",
    "states": "stepfunctions",
    "supportapp": "support-app",
    "tagging": "resourcegroupstaggingapi",
    "tax": "taxsettings",
    "thinclient": "workspaces-thin-client",
    "timestream": "timestream-write",  # also timestream-query
    "voiceid": "voice-id",
    # Defensive extras: endpoint hostnames other SDK generations have used
    # as signing names.
    "email": "ses",
    "iotdevice": "iot",
    "aws-migration-hub": "mgh",
}

# X-Amz-Target prefix / rpc-v2-cbor path segment -> service. Generated from
# botocore service models (regenerate by dumping metadata["targetPrefix"]
# per service). This resolves service *exactly* — including the scope
# collisions above (dynamodb vs dynamodbstreams, events vs eventbridgev2).
_TARGET_PREFIXES = {
    "CertificateManager": "acm",
    "ACMPrivateCA": "acm-pca",
    "AnyScaleFrontendService": "application-autoscaling",
    "EC2WindowsBarleyService": "application-insights",
    "AppRunner": "apprunner",
    "PhotonAdminProxyService": "appstream",
    "ArcRegionSwitch": "arc-region-switch",
    "AmazonAthena": "athena",
    "AnyScaleScalingPlannerFrontendService": "autoscaling-plans",
    "B2BI": "b2bi",
    "BackupOnPremises_v20210101": "backup-gateway",
    "AWSBCMDashboardsService": "bcm-dashboards",
    "AWSBillingAndCostManagementDataExports": "bcm-data-exports",
    "AWSBCMPricingCalculator": "bcm-pricing-calculator",
    "AWSBillingAndCostManagementRecommendedActions": "bcm-recommended-actions",
    "AmazonBedrockKeystoneRuntimeService": "bedrock-data-automation-runtime",
    "AWSBilling": "billing",
    "AWSBudgetServiceGateway": "budgets",
    "AWSInsightsIndexService": "ce",
    "AWSCloud9WorkspaceManagementService": "cloud9",
    "CloudApiService": "cloudcontrol",
    "CloudHsmFrontendService": "cloudhsm",
    "BaldrApiService": "cloudhsmv2",
    "com.amazonaws.cloudtrail.v20131101.CloudTrail_20131101": "cloudtrail",
    "GraniteServiceVersion20100801": "cloudwatch",
    "CloudWatchOmniFrontend": "cloudwatchomni",
    "CodeBuild_20161006": "codebuild",
    "CodeCommit_20150413": "codecommit",
    "com.amazonaws.codeconnections.CodeConnections_20231201": "codeconnections",
    "CodeDeploy_20141006": "codedeploy",
    "CodePipeline_20150709": "codepipeline",
    "com.amazonaws.codestar.connections.CodeStar_connections_20191201": "codestar-connections",
    "AWSCognitoIdentityService": "cognito-identity",
    "AWSCognitoIdentityProviderService": "cognito-idp",
    "Comprehend_20171127": "comprehend",
    "ComprehendMedical_20181030": "comprehendmedical",
    "ComputeOptimizerService": "compute-optimizer",
    "ComputeOptimizerAutomationService": "compute-optimizer-automation",
    "StarlingDoveService": "config",
    "CostOptimizationHubService": "cost-optimization-hub",
    "AWSOrigamiServiceGatewayService": "cur",
    "DataPipeline": "datapipeline",
    "FmrsService": "datasync",
    "AmazonDAXV3": "dax",
    "DeviceFarm_20150623": "devicefarm",
    "OvertureService": "directconnect",
    "AWSPoseidonService_V2015_11_01": "discovery",
    "AmazonDMSv20160101": "dms",
    "DirectoryService_20150416": "ds",
    "DynamoDB_20120810": "dynamodb",
    "DynamoDBStreams_20120810": "dynamodbstreams",
    "AWSEC2InstanceConnectService": "ec2-instance-connect",
    "AmazonEC2ContainerRegistry_V20150921": "ecr",
    "SpencerFrontendService": "ecr-public",
    "AmazonEC2ContainerServiceV20141113": "ecs",
    "ElasticMapReduce": "emr",
    "AWSEventsV2": "eventbridgev2",
    "AWSEvents": "events",
    "AmazonElasticVMwareService": "evs",
    "Firehose_20150804": "firehose",
    "AWSFMS_20180101": "fms",
    "AmazonForecast": "forecast",
    "AmazonForecastRuntime": "forecastquery",
    "AWSHawksNestServiceFacade": "frauddetector",
    "AWSFreeTierService": "freetier",
    "AWSSimbaAPIService_v20180301": "fsx",
    "GameLift": "gamelift",
    "GlobalAccelerator_V20180706": "globalaccelerator",
    "AWSGlue": "glue",
    "AWSHealth_20160804": "health",
    "HealthLake": "healthlake",
    "AWSIdentityStore": "identitystore",
    "InspectorService": "inspector",
    "Interconnect": "interconnect",
    "Invoicing": "invoicing",
    "IoTAutobahnControlPlane": "iotfleetwise",
    "IoTSecuredTunneling": "iotsecuretunneling",
    "IotThingsGraphFrontEndService": "iotthingsgraph",
    "AWSKendraFrontendService": "kendra",
    "AWSKendraRerankingFrontendService": "kendra-ranking",
    "KeyspacesService": "keyspaces",
    "KeyspacesStreams": "keyspacesstreams",
    "Kinesis_20131202": "kinesis",
    "KinesisAnalytics_20150814": "kinesisanalytics",
    "KinesisAnalytics_20180523": "kinesisanalyticsv2",
    "TrentService": "kms",
    "AWSLicenseManager": "license-manager",
    "Lightsail_20161128": "lightsail",
    "Logs_20140328": "logs",
    "AWSLookoutEquipmentFrontendService": "lookoutequipment",
    "AmazonML_20141212": "machinelearning",
    "MailManagerSvc": "mailmanager",
    "AWSMPCommerceService_v20200301": "marketplace-agreement",
    "AWSMPEntitlementService": "marketplace-entitlement",
    "MarketplaceCommerceAnalytics20150701": "marketplacecommerceanalytics",
    "MediaStore_20170901": "mediastore",
    "AmazonMemoryDB": "memorydb",
    "AWSMPMeteringService": "meteringmarketplace",
    "AWSMigrationHub": "mgh",
    "AWSMigrationHubMultiAccountService": "migrationhub-config",
    "MTurkRequesterServiceV20170117": "mturk",
    "AmazonMWAAServerless": "mwaa-serverless",
    "NetworkFirewall_20201112": "network-firewall",
    "Odb": "odb",
    "OpenSearchServerless": "opensearchserverless",
    "AWSOrganizationsV20161128": "organizations",
    "PartnerCentralAccount": "partnercentral-account",
    "PartnerCentralBenefitsService": "partnercentral-benefits",
    "PartnerCentralChannel": "partnercentral-channel",
    "PartnerCentralRevenueMeasurement": "partnercentral-revenue-measurement",
    "AWSPartnerCentralSelling": "partnercentral-selling",
    "PaymentCryptographyControlPlane": "payment-cryptography",
    "AWSParallelComputingService": "pcs",
    "AmazonPersonalize": "personalize",
    "PerformanceInsightsv20180227": "pi",
    "PinpointSMSVoiceV2": "pinpoint-sms-voice-v2",
    "AWSPriceListService": "pricing",
    "AwsProton20200720": "proton",
    "RedshiftData": "redshift-data",
    "RedshiftServerless": "redshift-serverless",
    "RekognitionService": "rekognition",
    "ResourceGroupsTaggingAPI_20170126": "resourcegroupstaggingapi",
    "ToggleCustomerAPI": "route53-recovery-cluster",
    "Route53Domains_v20140515": "route53domains",
    "Route53Resolver": "route53resolver",
    "SageMaker": "sagemaker",
    "secretsmanager": "secretsmanager",
    "ServiceQuotasV20190624": "service-quotas",
    "AWS242ServiceCatalogService": "servicecatalog",
    "Route53AutoNaming_v20170314": "servicediscovery",
    "AWSShield_20160616": "shield",
    "AWSIESnowballJobManagementService": "snowball",
    "AmazonSQS": "sqs",
    "AmazonSSM": "ssm",
    "SSMContacts": "ssm-contacts",
    "SWBExternalService": "sso-admin",
    "AWSStepFunctions": "stepfunctions",
    "StorageGateway_20130630": "storagegateway",
    "AWSSupport_20130415": "support",
    "SimpleWorkflowService": "swf",
    "Textract": "textract",
    "AmazonTimestreamInfluxDB": "timestream-influxdb",
    "Timestream_20181101": "timestream-query",  # shared with timestream-write
    "Transcribe": "transcribe",
    "TransferService": "transfer",
    "AWSShineFrontendService_20170701": "translate",
    "VerifiedPermissions": "verifiedpermissions",
    "VoiceID": "voice-id",
    "AWSWAF_20150824": "waf",
    "AWSWAF_Regional_20161128": "waf-regional",
    "AWSWAF_20190729": "wafv2",
    "WorkMailService": "workmail",
    "WorkspacesService": "workspaces",
    "EUCMIFrontendAPIService": "workspaces-instances",
}

_session = botocore.session.get_session()
_model_cache: dict[str, object] = {}


def service_for_scope(scope: str) -> str:
    """Map a SigV4 credential scope to a botocore service name."""
    return _SCOPE_ALIASES.get(scope, scope)


def service_for_target_prefix(prefix: str) -> str | None:
    """Resolve a service exactly by its X-Amz-Target/rpc-v2-cbor prefix."""
    return _TARGET_PREFIXES.get(prefix)


def get_service_model(service: str | None) -> Any | None:
    """Return the botocore ServiceModel for a service name, or None.

    ``Any`` deliberately — botocore models are a dynamic boundary
    (CachedProperty descriptors, partial stubs)."""
    if service is None:
        return None
    if service in _model_cache:
        return _model_cache[service]
    try:
        model = _session.get_service_model(service)
    except Exception:  # noqa: BLE001 — unknown service names / broken data loaders
        model = None
    _model_cache[service] = model
    return model


@cache
def service_metadata(service: str | None) -> dict:
    """Raw service-2 metadata dict (jsonVersion, targetPrefix, auth, ...)."""
    if service is None:
        return {}
    try:
        loader = _session.get_component("data_loader")
        model = loader.load_service_model(service, "service-2")
        meta = model.get("metadata")
        return meta if isinstance(meta, dict) else {}
    except Exception:  # noqa: BLE001 — service model may be absent/partial
        return {}


def get_protocol(service: str | None) -> str | None:
    model = get_service_model(service)
    if model is None:
        return None
    return model.protocol


def error_shape(service: str, code: str):
    """Find the shape for an error code/name in a service model, or None.

    Accepts the bare shape name or a Smithy-style ``prefix#Name``.
    """
    model = get_service_model(service)
    if model is None:
        return None
    name = code.rsplit("#", 1)[-1]
    shape = _shape_or_none(model, name)
    if shape is not None:
        return shape
    for candidate in model.shape_names:
        if candidate == name or candidate.rsplit("#", 1)[-1] == name:
            return _shape_or_none(model, candidate)
    return None


def _shape_or_none(model, name: str):
    try:
        return model.shape_for(name)
    except Exception:  # noqa: BLE001 — unmodeled codes raise assorted errors
        return None


# AWS's real status for common codes when the service model doesn't carry an
# ``httpStatusCode`` trait (most don't). Verified behavior that matters: the
# SDK's retry decision depends on the status — S3 SlowDown at 400 is treated
# as a terminal client error, at 503 it retries.
_KNOWN_STATUS = {
    # throttling family (botocore's retryable throttled codes)
    "Throttling": 400,
    "ThrottlingException": 400,
    "ThrottledException": 400,
    "RequestThrottledException": 400,
    "RequestThrottled": 400,
    "EC2ThrottledException": 400,
    "TooManyRequestsException": 429,
    "ProvisionedThroughputExceededException": 400,
    "TransactionInProgressException": 400,
    "RequestLimitExceeded": 503,
    "BandwidthLimitExceeded": 400,
    "LimitExceededException": 400,
    "PriorRequestNotComplete": 400,
    "SlowDown": 503,
    # transient server-side
    "InternalError": 500,
    "InternalServerError": 500,
    "InternalFailure": 500,
    "InternalServiceError": 500,
    "ServiceUnavailable": 503,
    "ServiceUnavailableException": 503,
    "ServiceUnavailableError": 503,
    "RequestTimeout": 408,
    "RequestTimeoutException": 408,
    # common terminal errors — keep these non-retryable
    "AccessDeniedException": 403,
    "AccessDenied": 403,
    "InvalidAccessKeyId": 403,
    "SignatureDoesNotMatch": 403,
    "ExpiredTokenException": 400,
    "UnauthorizedException": 401,
    "NotAuthorizedException": 400,
    "ValidationException": 400,
    "ValidationError": 400,
    "InvalidParameterValue": 400,
    "InvalidParameterException": 400,
    "InvalidParameterValueException": 400,
    "MissingParameter": 400,
    "NoSuchBucket": 404,
    "NoSuchKey": 404,
    "NotFound": 404,
    "NotFoundException": 404,
    "ResourceNotFoundException": 404,
    "ResourceNotFound": 404,
    "ResourceInUseException": 400,
    "ResourceAlreadyExistsException": 400,
    "ConditionalCheckFailedException": 400,
    "InvalidClientTokenId": 403,
}


@cache
def _modeled_status_map(service: str) -> dict[str, int]:
    """httpStatusCode per error shape, indexed by shape name AND wire code.

    The wire code is what AWS puts on the wire (``error.code`` trait) and
    what users copy from ``ClientError`` — it can differ from the shape
    name (e.g. autoscaling ``ResourceContentionFault`` → ``ResourceContention``).
    """
    try:
        loader = _session.get_component("data_loader")
        model = loader.load_service_model(service, "service-2")
    except Exception:  # noqa: BLE001 — service model may be absent/partial
        return {}
    out: dict[str, int] = {}
    for name, shape in model.get("shapes", {}).items():
        err = shape.get("error")
        if not isinstance(err, dict):
            continue
        status = err.get("httpStatusCode")
        if not isinstance(status, int):
            continue
        out.setdefault(name, status)
        out.setdefault(err.get("code") or name, status)
    return out


def _modeled_status(service: str, name: str) -> int | None:
    """httpStatusCode from the raw service model, when modeled."""
    return _modeled_status_map(service).get(name)


def error_http_status(
    service: str, code: str, default: int = 503, protocol: str | None = None
) -> int:
    """HTTP status for an error code.

    Order: modeled ``httpStatusCode`` (rare but authoritative when present) →
    curated AWS-observed map → ``default``. Live-AWS captures show json/cbor
    services serve unmodeled client errors at 400 per the awsJson spec even
    when the name looks like a 404 (``ResourceNotFoundException`` is 400 on
    DynamoDB); for those protocols the name map only contributes 5xx faults.
    """
    name = code.rsplit("#", 1)[-1]
    status = _modeled_status(service, name)
    if status is not None:
        return status
    if name.isdigit():
        return int(name)
    known = _KNOWN_STATUS.get(name)
    if protocol in ("json", "smithy-rpc-v2-cbor"):
        if known is not None and (known >= 500 or known == 429):
            return known
        return 400 if default < 500 else default
    return known if known is not None else default


def error_message_member(service: str, code: str) -> str:
    """Name of the error shape's message member (``message`` vs ``Message``).

    The casing is service-specific on the wire — DynamoDB emits lowercase
    ``message`` while SecretsManager emits ``Message``. Falls back to
    ``message`` when the shape is unmodeled or has no message member.
    """
    shape = error_shape(service, code)
    if shape is not None:
        members = getattr(shape, "members", None)
        if members:
            for name in members:
                if name.lower() == "message":
                    return name
    return "message"


def is_query_compat_service(service: str | None) -> bool:
    """Service model declares ``awsQueryCompatible`` (e.g. SQS on JSON wire)."""
    return "awsQueryCompatible" in service_metadata(service)


# Verified AWS query-compat namespaces for the ``x-amzn-query-error`` header —
# AWS emits ``AWS.{namespace}.{code};Sender``. Grows as live captures cover
# more migrated services; unknown services emit the bare code.
_QUERY_ERROR_NAMESPACE = {
    "sqs": "AWS.SimpleQueueService",
}


def query_error_namespace(service: str | None) -> str | None:
    if service is None:
        return None
    return _QUERY_ERROR_NAMESPACE.get(service)


def operation_error_names(service: str, operation: str) -> list[str]:
    """Error shape names modeled for an operation (for plausible sampling)."""
    model = get_service_model(service)
    if model is None:
        return []
    try:
        op = model.operation_model(operation)
    except Exception:  # noqa: BLE001 — OperationNotFoundError and friends
        return []
    return [shape.name for shape in op.error_shapes]
