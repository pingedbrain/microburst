# aws-chaos-proxy — Exploración profunda

**Fecha:** 2026-10-07
**Estado:** MVP implementado como `microburst` (este directorio). 26 tests verdes,
smoke test end-to-end contra MiniStack verificado (pass-through + throttle
con retry del SDK + fired log).
**TL;DR:** no existe ningún proxy standalone protocol-aware para inyectar fallas
en llamadas a la API de AWS. Todos los que lo hacen lo tienen *embebido en su
propio emulador* (y el más conocido, LocalStack, lo vende en tier Enterprise).
El hueco es real, técnico, y defendible.

---

## 1. El problema

Todo equipo que escribe código sobre AWS necesita responder: "¿qué pasa cuando
DynamoDB throttlea? ¿cuando SQS tiene 3s de latencia? ¿cuando Lambda devuelve
503?" Las opciones actuales son todas malas:

- **AWS FIS** — real AWS only, procedural (crear experiment template, correr,
  esperar minutos), nivel infraestructura, cuesta plata, no sirve para el loop
  dev ni para CI.
- **Toxiproxy / fault / saboteur / chaosproxy** — proxies TCP/HTTP genéricos.
  Pueden cortar conexiones y agregar latencia, pero **no pueden devolver un
  `ProvisionedThroughputExceededException` con la forma correcta** para que el
  SDK lo clasifique como throttling retriable.
- **DIY SDK middleware** — la respuesta oficial de AWS. En
  awslabs/aws-sdk-rust discussion #542, ante "¿cómo inyecto fallas para chaos
  testing?", la respuesta del equipo del SDK es literalmente "escribí tu propio
  connector o armate un proxy". Cada equipo reinventa esto.

## 2. Panorama competitivo (verificado 2026-10)

### Fault injection *embebido en emuladores* — todos

| Proyecto | Fault injection | Licencia/acceso | Problema |
|---|---|---|---|
| **LocalStack Chaos API** | `/_localstack/chaos/faults`: reglas por region/service/operation + probabilidad + error custom + latency via `/chaos/effects` | **Tier Enterprise** (verificado en docs) | Solo si pagás LocalStack Enterprise |
| **CloudMock** | `InjectFault(service, action, type)` — throttle, latency, timeout, blackhole, error + % | Source-available (NOASSERTION), no OSS | Embebido en su emulador; licencia no-OSI |
| **awsim** (QaidVoid) | Chaos engine con presets (`flaky-s3`, `ddb-throttle`, `kms-outage`...) | OSS | Embebido en su emulador |
| **local-web-services** | `lws chaos enable/set` — error-rate, latency, timeout, conn-reset; errores service-aware (JSON vs XML) | OSS | Embebido en su emulador |
| **MiniStack** | **No tiene** (verificado: 0 hits de chaos/fault-inject en el código) | MIT | Oportunidad de feature o proyecto hermano |

### Proxies genéricos — ninguno AWS-aware

| Proyecto | Nivel | Limitación |
|---|---|---|
| Toxiproxy (Shopify) | TCP | Sin noción de servicio/operación; errores no con forma AWS |
| `fault` (fault-project) | TCP/UDP/DNS | Idem; buen engine pero ciego al protocolo |
| saboteur | HTTP | Reglas por URL/method/headers; bodies genéricos |
| chaosproxy (josephwoodward) | HTTP | Match por regex de host/path; status codes genéricos |
| rodriguez | HTTP + mocks S3/SQS | Harness de test, no proxy protocol-aware |

### Por qué "error con forma correcta" es EL diferenciador

botocore (y todos los SDKs, que comparten la spec de retry) clasifican errores
**parseando el código de error del body**, no solo el status HTTP:

- Modo `standard` reintenta: códigos `Throttling`, `ThrottlingException`,
  `ProvisionedThroughputExceededException`, `SlowDown`, `RequestLimitExceeded`,
  `TransactionInProgressException`, ... + status 500/502/503/504 + errores de
  conexión.
- `ModeledRetryableChecker`: cada servicio puede marcar sus propias excepciones
  como retriables **en el service model** — o sea, el set correcto de errores
  retriables está definido por Smithy/botocore models.
- `x-amz-retry-after` (milisegundos) se incorpora al backoff; `Retry-After`
  estándar **se ignora** (boto3 lo descarta explícitamente).
- Un 429 genérico con body vacío **no** se clasifica como throttling — cae en
  el bucket genérico de status code. La diferencia cambia el backoff base
  (1s para throttling vs 50ms para transient) y el comportamiento de adaptive
  mode (client-side rate limiting solo reacciona a respuestas clasificadas
  como throttling).

Conclusión técnica: un proxy protocol-aware puede emitir fallos que el SDK trata
**exactamente como fallos reales de AWS**, ejercitando retry/backoff/circuit
breaker/adaptive rate limiting de verdad. Un proxy genérico no.

## 3. Cómo se detecta servicio + operación (evidencia: MiniStack router)

El mismo razonamiento que usa MiniStack's `core/router.py` aplica al proxy:

1. **Credential scope** del header `Authorization: AWS4-HMAC-SHA256
   Credential=AKIA.../20261007/us-east-1/dynamodb/aws4_request` → servicio +
   región firmados. Infalible para SDKs.
2. **`X-Amz-Target: DynamoDB_20120810.PutItem`** → servicio + operación exacta
   para todos los servicios JSON protocol (DynamoDB, Kinesis, SSM,
   SecretsManager, Logs, Step Functions, KMS, Cognito...).
3. **Query protocol** (SQS, SNS, IAM, STS, EC2, RDS...): `Action=` +
   `Version=` en body form-encoded o query string.
4. **REST-JSON / REST-XML** (API Gateway, Lambda control plane, S3): método +
   path pattern contra las rutas del service model.
5. **Host header** como fallback.

Los edge cases existen pero son conocidos y acotados (MiniStack ya los
catalogó: `streams.dynamodb` vs `dynamodb`, iot-jobs-data vs iot-data,
bedrock-runtime vs bedrock, etc.). El service→protocol mapping sale directo
de los service models de botocore (`metadata.protocol`: `json`, `query`,
`rest-xml`, `rest-json`, `ec2`).

## 4. Formatos de error por protocolo

| Protocol | Servicios ejemplo | Forma del error |
|---|---|---|
| `json` (1.0/1.1) | DynamoDB, Kinesis, SSM, Logs, States, KMS | `{"__type": "ProvisionedThroughputExceededException", "message": "..."}` + a veces `x-amzn-ErrorType` header |
| `query` | SQS, SNS, IAM, STS, EC2, RDS, CloudWatch | XML `<ErrorResponse><Error><Code>Throttling</Code><Message>...` |
| `rest-xml` | S3, CloudFront, Route53 | XML `<Error><Code>SlowDown</Code><Message>...` + `x-amz-request-id` + `x-amz-id-2` |
| `rest-json` | API Gateway v2, Lambda control, EKS | `{"message": "..."}` + `x-amzn-errortype: TooManyRequestsException` header |
| `ec2` | EC2 legacy | Variante query XML |

Detalles que importan para el realismo: `x-amzn-RequestId` /
`x-amz-request-id` con formato plausible, `x-amz-id-2` para S3,
`Content-Type` correcto (`application/x-amz-json-1.0`, `text/xml`), y para
JSON protocol el `__type` a veces lleva prefijo del shape
(`com.amazonaws.dynamodb.v20120810#ProvisionedThroughputExceededException`).

## 5. SigV4 y el problema del proxy

El SDK firma la request incluyendo el `host` en los SignedHeaders. Tres modos:

- **Modo emulator (primario):** `AWS_ENDPOINT_URL=http://localhost:9999` →
  proxy → upstream (MiniStack/moto/LocalStack). Los emuladores no validan
  la firma estrictamente (MiniStack solo parsea el credential scope). El
  proxy puede forwardear la firma intacta. **Cero fricción.**
- **Modo real AWS (re-signing):** el proxy termina la request y la re-firma
  con credenciales configuradas (las del usuario — es su propia cuenta para
  game days en staging). Es ~100 líneas (MiniStack `core/sigv4.py` ya tiene
  todos los primitivos: canonical request, derive key, calculate). La TLS la
  habla el proxy hacia arriba; el cliente habla HTTP plano al proxy. No hace
  falta MITM ni CA custom.
- **Modo passthrough MITM (opcional, v2+):** HTTPS_PROXY con CA instalada.
  Solo si se quiere inyectar sin cambiar endpoint config. No para MVP.

## 6. Catálogo de efectos (realistas de AWS)

Ordenados por valor/esfuerzo:

| Efecto | Qué simula | Notas |
|---|---|---|
| `error` con error code AWS real | ProvisionedThroughputExceeded, SlowDown, KMSInternalException... | Killer feature: lookup en service model de excepciones declaradas por operación |
| `throttle` | 429/400 throttling con forma correcta | Sub-caso de error; preset común |
| `latency` | ms fijos o rango uniforme con jitter | Simular región degradada |
| `timeout` | hold → 504 o colgar conexión | Ejercita timeouts del SDK |
| `connection-reset` | RST / half-close | Transient error del cliente HTTP |
| `partial-body` | Truncar body mid-stream / corromper | S3 GetObject truncado, checksum mismatch — detalles que rompen apps de formas raras |
| `slow-drip` | Response body a N bytes/seg | Simula throughput bajo |
| `stale-read` | Eventual consistency (put → get 404) | Difícil: requiere entender semántica del servicio. **Post-MVP.** |

Matchers por regla: service, operation, region, resource (table name, bucket,
queue URL — extraíble del body/path), probability, y **rate** (token bucket:
"throttlea todo lo que exceda 10 req/s" — más realista que probabilidad para
modelar límites de AWS).

Presets estilo awsim (buena idea copiar el patrón): `ddb-throttle`,
`flaky-s3`, `slow-lambda`, `kms-outage`, `regional-failover`, `sqs-backlog`.

## 7. Arquitectura propuesta

```
app (SDK cualquiera)
   │  AWS_ENDPOINT_URL=http://localhost:9999
   ▼
chaos-proxy :9999
   ├── protocol detector (credential scope → X-Amz-Target → Action= → host/path)
   ├── rule engine (first-match o all-match, probability, rate limit)
   ├── effect executor
   └── forwarder → upstream (ministack:4566 / moto:5000 / AWS real)

control plane:
   POST   /_chaos/rules          (append)
   GET    /_chaos/rules
   DELETE /_chaos/rules
   GET    /_chaos/fired          (qué reglas dispararon — clave para debug)
   POST   /_chaos/presets/{name}
   config YAML al arranque + hot-reload
```

Decisión de lenguaje — trade-off real:

- **Python + botocore:** los service models (incl. error shapes por operación)
  vienen gratis; toda la expertise de MiniStack es reutilizable; `uvx
  aws-chaos-proxy`. Contra: "single binary" requiere PyInstaller o uv.
- **Go:** binario real, mejor performance como proxy; pero hay que portar los
  service models o embeber un subset JSON de los de botocore.

Recomendación: **Python primero** (velocidad de iteración, modelos gratis,
ecosistema MiniStack). Reescribir en Go solo si la performance/distro lo
exige.

## 8. MVP (scope propuesto)

1. Proxy HTTP/1.1 con upstream configurable (`--upstream`).
2. Detector para ~10 servicios: dynamodb, sqs, s3, sns, lambda, kinesis,
   ssm, secretsmanager, sts, iam.
3. Efectos: `error` (protocol-correct, desde service model), `latency`,
   `timeout`, `connection-reset`.
4. Reglas vía REST + YAML; presets.
5. `/_chaos/fired` — log de qué regla matcheó cada request. *Esto es lo que
   convierte el proxy de "caos ciego" en herramienta de debugging.*
6. Demo: app Python contra MiniStack, inyectar 30% throttle en DynamoDB,
   mostrar retries del SDK.

Tests de validación (lo que prueba que funciona de verdad):
- boto3 clasifica el error inyectado igual que el real (throttling → backoff
  largo, retriable).
- Cross-SDK: aws-sdk-go-v2 y aws-sdk-js-v3 responden igual.
- Adaptive mode activa client-side rate limiting bajo throttling sostenido.

## 9. Riesgos

- **Servicios streaming/eventos:** Kinesis SubscribeToShard, S3 Select, SQS
  long-polling — payloads chunked/event-stream. MVP: passthrough sin
  inyección. Mediano plazo: inyectar a nivel frame.
- **S3 presigned URLs:** la firma va en query params; detección distinta.
  Acotable.
- **HTTP/2:** SDKs usan mayormente HTTP/1.1 para estos servicios; h2 en
  streaming APIs. MVP: HTTP/1.1.
- **Descubribilidad:** "chaos engineering" en búsquedas está saturado de
  herramientas de infra. Posicionar como *"test your AWS SDK error
  handling"* / *"failure injection for AWS API calls"*, no como chaos
  platform.
- **Absorción por emuladores:** MiniStack/LocalStack podrían copiarlo. Es
  riesgo aceptable — y si MiniStack lo integra, mejor para MiniStack (somos
  nosotros).
- **Espacio se mueve rápido:** CloudMock y awsim aparecieron hace meses. La
  ventana de "primero en standalone" existe pero no es eterna.

## 10. Diferenciación resumida

> Toxiproxy para AWS, pero que entiende el protocolo: matchea por
> servicio/operación/recurso, devuelve errores con la forma exacta que el SDK
> espera (desde los service models), y funciona contra cualquier backend —
> MiniStack, moto, LocalStack, o AWS real. LocalStack vende esto en
> Enterprise; CloudMock/atrición lo atan a su emulador. Este es el primero
> standalone y open source.

## 11. Nombres candidatos

`aws-chaos-proxy` (descriptivo), `faultline`, `stormfront`, `throttle-shop`,
`bad-weather`, `blip` (AWS tiene hiccups). Recomendación: nombre corto +
subtítulo descriptivo, ej. **`microburst`** — "AWS failure injection proxy".
Microburst = ráfaga de tormenta. Corto, disponible probablemente, on-theme.

## 12. Relación con MiniStack

Dos caminos no excluyentes:
1. Proyecto standalone primero — mayor alcance (funciona con todo el
   ecosistema), marca propia.
2. Después: MiniStack puede importarlo como middleware o re-implementar el
   faults endpoint compatible (`/_ministack/chaos/faults`) usando la misma
   rule engine. LocalStack cobra Enterprise por esto; MiniStack lo tendría
   gratis — argumento de marketing directo.
