# AsyncClient Documentation

The `AsyncClient` class inherits from `httpx.AsyncClient` and provides additional functionality to handle specific HTTP status codes gracefully.

---

## Key Feature: `ignore_on_status_codes`

A static method that decorates an asynchronous function to ignore specified HTTP status codes and return an `IgnoreResult` instead of raising exceptions.

