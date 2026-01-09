# Development plan
*  Add possibility to set consumer offset or starting date
* Add unit tests with mock Kafka producers and brokers
* Finish error tracker

## Known Issues

### TestIntegration_PersistenceRestoration Failure
The integration test `TestIntegration_PersistenceRestoration` in `adapter/sarama/persistence_test.go` currently fails with a connection error (`dial tcp ... connection refused`) during Phase 3 (Restoration).

**Root Cause:**
This is an infrastructure test issue, not a library bug. The test uses `testcontainers-go` to restart the Kafka container to simulate a broker failure.
1.  Phase 1: Container starts on Port A (e.g., 55001). `KAFKA_ADVERTISED_LISTENERS` is set to `localhost:55001`.
2.  Phase 2: Container stops and restarts. Docker assigns a new Port B (e.g., 55002) on the host.
3.  Phase 3: The test correctly connects to Port B. However, the container (which was only stopped, not destroyed) preserves its environment variables. It still advertises `localhost:55001` to clients.
4.  Result: The Sarama client connects to Port B, gets metadata pointing to Port A, and fails to connect to Port A.

**Workaround:**
Ignore this failure for local development. To fix it properly, the test needs to be refactored to either:
- Use `FixedPort` (discouraged in CI).
- Destroy and recreate the container with the same volume mounts (to preserve data but update config).
- Use `kafka-go` module features that might handle dynamic port updates better.