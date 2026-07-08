# Tenant B Flink Job

This repository is owned by the Tenant B development team.

It contains the Java source code for Tenant B's Apache Flink job and is set up
to build a Docker image for that job.

This repository does not contain Kubernetes deployment manifests. Deployment is
handled through the `flink-platform` GitOps repository.

## Project Structure

```text
src/main/java/com/example/tenantb/  Java Flink job source code
src/test/java/com/example/tenantb/  Java test source code
.github/workflows/                 CI workflows
docs/                              Project documentation
```

## Current Status

The initial repository structure is in place. Full Flink job logic has not been
implemented yet.
