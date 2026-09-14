# Architecture Decision Records

**Audience**: Architects, technical leads, contributors.

| ADR | Status | Date | Decision |
|-----|--------|------|----------|
| [ADR-001](ADR-001-extract-from-edge-runtime-pilot.md) | Accepted | 2026-09-13 | Extract `spi`/`saf` from `edge-runtime`'s in-tree pilot rather than rebuild from scratch; initially `MessageBroker`-only, later amended to add the real in-memory backend and `TaskQueue` once both gaps were found to be incomplete migrations rather than deliberate scope boundaries — see architecture.md's own "Restoring the real in-memory backend" and "Scope boundary" sections for the full amendment history |

[← 3-design index](../README.md)
