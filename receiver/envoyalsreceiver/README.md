# Envoy ALS(access log service) receiver

<!-- status autogenerated section -->
| Status        |           |
| ------------- |-----------|
| Stability     | [alpha]: logs   |
| Distributions | [contrib] |
| Issues        | [![Open issues](https://img.shields.io/github/issues-search/open-telemetry/opentelemetry-collector-contrib?query=is%3Aissue%20is%3Aopen%20label%3Areceiver%2Fenvoyals%20&label=open&color=orange&logo=opentelemetry)](https://github.com/open-telemetry/opentelemetry-collector-contrib/issues?q=is%3Aopen+is%3Aissue+label%3Areceiver%2Fenvoyals) [![Closed issues](https://img.shields.io/github/issues-search/open-telemetry/opentelemetry-collector-contrib?query=is%3Aissue%20is%3Aclosed%20label%3Areceiver%2Fenvoyals%20&label=closed&color=blue&logo=opentelemetry)](https://github.com/open-telemetry/opentelemetry-collector-contrib/issues?q=is%3Aclosed+is%3Aissue+label%3Areceiver%2Fenvoyals) |
| Code coverage | [![codecov](https://codecov.io/github/open-telemetry/opentelemetry-collector-contrib/graph/main/badge.svg?component=receiver_envoyals)](https://app.codecov.io/gh/open-telemetry/opentelemetry-collector-contrib/tree/main/?components%5B0%5D=receiver_envoyals&displayType=list) |
| [Code Owners](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/CONTRIBUTING.md#becoming-a-code-owner)    | [@evan-bradley](https://www.github.com/evan-bradley), [@zirain](https://www.github.com/zirain) |

[alpha]: https://github.com/open-telemetry/opentelemetry-collector/blob/main/docs/component-stability.md#alpha
[contrib]: https://github.com/open-telemetry/opentelemetry-collector-releases/tree/main/distributions/otelcol-contrib
<!-- end autogenerated section -->

This is a receiver for the [Envoy gRPC ALS](https://www.envoyproxy.io/docs/envoy/latest/api-v3/extensions/access_loggers/grpc/v3/als.proto#envoy-v3-api-msg-extensions-access-loggers-grpc-v3-httpgrpcaccesslogconfig) sink.

Envoy ALS (Access Log Service) is a feature of Envoy Proxy that allows for the
centralized collection and management of access logs.

Instead of writing access logs to local files, Envoy can be configured to send these logs to a remote gRPC service.

This is particularly useful in distributed systems where centralized logging is required for monitoring, auditing, and debugging purposes.

[Istio](https://istio.io) and [Envoy Gateway](https://gateway.envoyproxy.io) support OTLP and gRPC ALS with first class API.

## Getting Started

The settings are:

- `endpoint` (required, default = localhost:19001 gRPC protocol): host:port to which the receiver is going to receive data. See our [security best practices doc](https://opentelemetry.io/docs/security/config-best-practices/#protect-against-denial-of-service-attacks) to understand how to set the endpoint in different environments.

Example:
```yaml
receivers:
  envoyals:
    endpoint: 0.0.0.0:3500
```

## Advanced Configuration

Other options can be configured to support more advanced use cases:

- [gRPC settings](https://github.com/open-telemetry/opentelemetry-collector/blob/main/config/configgrpc/README.md) including CORS
- [TLS and mTLS settings](https://github.com/open-telemetry/opentelemetry-collector/blob/main/config/configtls/README.md)

