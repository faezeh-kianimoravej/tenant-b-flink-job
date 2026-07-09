# Tenant B Flink Job

This repository contains the Apache Flink application source code owned by the Tenant B development team.

Tenant B demonstrates a product enrichment use case on the shared multi-tenant Flink platform. The job consumes orders and products, joins both streams by `productId`, and publishes enriched orders for downstream consumers.

This repository contains application source code only. Kubernetes deployment, environment configuration, and FlinkDeployment manifests are managed separately in the `flink-platform` GitOps repository.

## Use Case

Tenant B implements a second two-stream join use case that differs from Tenant A. Instead of reusing Tenant A's stream-processing scenario, this job enriches order events with product reference data.

Input topics:

- `tenant-b-orders`
- `tenant-b-products`

Output topic:

- `tenant-b-enriched-orders`

Join key:

- `productId`

The job uses a prototype-friendly keyed stream join. Product records are stored by `productId`; when an order arrives, the job emits an enriched order using the latest known product details. If product details are not available yet, the output uses a clear fallback product name and category.

## Example Messages

Order input:

```json
{
  "orderId": "order-1",
  "productId": "product-1",
  "quantity": 2
}
```

Product input:

```json
{
  "productId": "product-1",
  "productName": "Coffee",
  "category": "Beverages",
  "price": 4.50
}
```

Enriched output:

```json
{
  "orderId": "order-1",
  "productId": "product-1",
  "productName": "Coffee",
  "category": "Beverages",
  "price": 4.50,
  "quantity": 2,
  "tenant": "tenant-b",
  "processedBy": "tenant-b-flink-job"
}
```

## Runtime Configuration

Defaults:

```text
tenant=tenant-b
kafka.bootstrap.servers=kafka:9092
orders.input.topic=tenant-b-orders
products.input.topic=tenant-b-products
output.topic=tenant-b-enriched-orders
consumer.group.id=tenant-b-flink-job
platform.application.id=app-tenant-b-001
application.name=tenant-b-product-enrichment
owner.team=tenant-b
environment=dev
data.domain=orders
job.name=ProductEnrichmentJob
```

Each setting can be overridden with `--key=value` command-line arguments, for example:

```bash
java -jar target/tenant-b-flink-job-0.1.0-SNAPSHOT.jar \
  --kafka.bootstrap.servers=kafka:9092 \
  --orders.input.topic=tenant-b-orders \
  --products.input.topic=tenant-b-products \
  --output.topic=tenant-b-enriched-orders
```

## Repository Structure

```text
pom.xml
Dockerfile
src/main/java/com/example/tenantb/
  TenantBFlinkJobApplication.java
  config/FlinkJobConfig.java
  job/ProductEnrichmentJob.java
  kafka/KafkaSourceFactory.java
  kafka/KafkaSinkFactory.java
  mapper/ProductEnrichmentMapper.java
  model/Order.java
  model/Product.java
  model/EnrichedOrder.java
  model/JobMetadata.java
  serialization/JsonDeserializationSchema.java
  serialization/JsonSerializationSchema.java
src/test/java/com/example/tenantb/
```

## Prerequisites

- Java 21
- Maven 3.9 or newer
- Docker, for building the deployment image

This project currently uses Apache Flink 2.2.1 and the Flink Kafka connector 5.0.0-2.2.

## Continuous Integration

CI is provided by the reusable platform workflow in the `flink-platform` repository. This repository only supplies tenant-specific configuration, including the Tenant B image name.

The shared workflow builds, tests, and packages the application, then builds a Docker image and publishes it to GitHub Container Registry (GHCR).

Deployment is handled separately by the `flink-platform` GitOps repository using Argo CD and the Flink Kubernetes Operator.

## Build

Compile, test, and package the application:

```bash
mvn clean package
```

The Maven Shade Plugin runs during the `package` phase and creates the deployable fat JAR at:

```text
target/tenant-b-flink-job-0.1.0-SNAPSHOT.jar
```

## Docker Image

Build the Flink application image after creating the fat JAR:

```bash
docker build -t tenant-b-flink-job:0.1.0-SNAPSHOT .
```

The Dockerfile uses the official Apache Flink Java 21 image and copies the generated JAR into:

```text
/opt/flink/usrlib/tenant-b-flink-job.jar
```
