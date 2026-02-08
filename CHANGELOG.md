# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

Initial release of Kafka Resilience library.

### Added

- **Non-blocking retries** using separate retry topic (Bulkhead pattern)
- **Strict per-key ordering** guarantees during retries via distributed locking
- **Distributed state coordination** using compacted Kafka topic for multi-instance deployments
- **Dead Letter Queue (DLQ)** routing after max retries exceeded
- **Three backoff strategies**: exponential, constant, and linear
- **Automatic topic creation** for retry, redirect, and DLQ topics
- **Sarama adapter** for IBM Sarama Kafka client
- **Library-agnostic core** with clean interface contracts for other Kafka clients

