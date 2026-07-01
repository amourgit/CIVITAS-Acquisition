# CIVITAS Acquisition Platform

> Infrastructure d'acquisition de connaissance pour la CIVITAS Knowledge Platform.

## Philosophie

La couche Acquisition a **une seule responsabilité** : récupérer des documents bruts depuis des sources externes et les déposer dans le Raw Document Repository.

Elle ne connaît pas les embeddings. Elle ne connaît pas LlamaIndex. Elle ne connaît pas les agents. Elle ne connaît pas les chunks. Elle sait uniquement acquérir.

## Architecture

```
sources externes
      │
      ▼
┌─────────────────────────────────────────┐
│          Acquisition Platform           │
│                                         │
│  Channels ──► Registry ──► Connectors   │
│       │                       │         │
│       └──────► Pipeline ◄─────┘         │
│                   │                     │
│           Resilience Layer              │
└─────────────────────────────────────────┘
      │
      ▼
Raw Document Repository
      │
      ▼
Transformation Platform (out of scope)
```

## Structure du monorepo

```
src/civitas_acquisition/
├── contracts/          # Interfaces pures — zéro dépendance externe
│   ├── models/         # Value objects immuables
│   ├── ports/          # ABC : ConnectorPort, ChannelPort, ...
│   └── errors/         # Hiérarchie d'exceptions typées
├── connectors/         # 22+ implémentations de connecteurs
├── channels/           # Polling, Webhook, Streaming, Queue, FileDropMonitoring Manual
├── registry/           # ConnectorRegistry, ConnectorFactory
├── scheduler/          # Adaptatif, Cron, Event-triggered
├── pipeline/           # Orchestrateur, Validator, Deduplicator, EnvelopeBuilder
├── security/vault/     # HashiCorp Vault, AWS Secrets Manager adapters
├── resilience/         # RetryEngine, CircuitBreaker, DeadLetterQueue
├── observability/      # Prometheus metrics, OpenTelemetry traces
└── events/             # InProcessEventBus
```

## Démarrage rapide

```bash
pip install -e ".[dev]"
pytest tests/ -v --cov=src
```

## Phases de développement

- [x] **Phase 1** — Contracts Layer (models, ports, errors)
- [ ] **Phase 2** — Registry & Factory
- [ ] **Phase 3** — Resilience (RetryEngine, CircuitBreaker, DLQ)
- [ ] **Phase 4** — Pipeline (Validator, Deduplicator, EnvelopeBuilder)
- [ ] **Phase 5** — Premier connecteur de référence (GitHub)
- [ ] **Phase 6** — Channels (Polling en premier)
- [ ] **Phase 7** — Connecteurs restants (22+)
- [ ] **Phase 8** — Observabilité
- [ ] **Phase 9** — Security (Vault adapters)

## Règle des 40%

Aucune technologie externe ne doit représenter plus de 40% de la surface de dépendance.
Tous les points d'intégration passent par un Port abstrait avec des Adapters échangeables.
