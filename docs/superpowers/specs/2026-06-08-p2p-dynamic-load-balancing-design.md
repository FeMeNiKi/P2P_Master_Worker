---
title: P2P Dynamic Load Balancing — Design
date: 2026-06-08
authors:
  - student: unknown
  - reviewer: copilot
---

# Resumo

Este documento descreve o design para o projeto "P2P com Balanceamento de Carga Dinâmico" (Sprints 1–3) especificado em `document.md`.
Escopo: Sprints 1 (Heartbeat), 2 (Ciclo de Tarefas) e 3 (Negociação Master-to-Master e redirecionamento de Workers).

## Objetivos do design
- Implementar nós `Master` e `Worker` comunicando via TCP com JSON delimitado por `\n`.
- Fornecer protocolo de negociação Master↔Master com `request_help` / `response_*` / `command_redirect` / `command_release`.
- Garantir interoperabilidade entre implementações distintas respeitando o esquema JSON e o framing.

**DoD (síntese)**: worker apresenta-se e faz heartbeat; master entrega tarefa ou responde `NO_TASK`; status retornado e ACK recebido; master saturado solicita ajuda com `request_help`, recebe `response_accepted` e workers são redirecionados e devolvidos corretamente.

---

## 1. Arquitetura (visão geral)

- Componentes principais:
  - `Master` (servidor TCP): aceita conexões de Workers e de peers Masters; mantém `task_queue`; detecta saturação e negocia com peers.
  - `Worker` (cliente/serviço): conecta-se ao Master, envia heartbeat, solicita tarefas, processa e reporta status; aceita redirecionamento.
  - `PeerManager`: gerencia conexões persistentes Master↔Master e correlaciona `request_id`.

- Concurrency model: Python com `threading` + `concurrent.futures.ThreadPoolExecutor` (Thread-pool bounded), filas com `queue.Queue`, e threads dedicadas para monitoração e dispatch.

---

## 2. Componentes e interfaces

- `MasterServer`:
  - Escuta em `ip:port` configurado.
  - Aceita conexões e submete `ConnectionHandler` ao `ThreadPoolExecutor`.
  - Exports: iniciar/parar servidor; endpoints locais para inspeção (opcional).

- `ConnectionHandler`:
  - Lê linhas (`\n` delimited), valida JSON, executa handlers por `type` (Master↔Master) ou por payload (Worker↔Master).
  - Responsabilidades: handshake Worker (`WORKER: ALIVE`), entregar `TASK`/`NO_TASK`, processar `STATUS` e enviar `ACK`.

- `TaskDispatcher`:
  - Puxa itens de `task_queue` e atribui a Workers disponíveis.
  - Registra logs de atribuição (local/emprestado).

- `SaturationMonitor`:
  - Observa `pending_requests` e compara com `thresholds` (saturação e liberação com histerese).
  - Ao saturar, invoca `PeerManager.request_help(neighbors, workers_needed)`.

- `PeerManager` / `NegotiationClient`:
  - Mantém conexões persistentes com peers listados em `config.yaml`.
  - Envia `request_help` com `request_id` UUID; aguarda `response_*` (timeout configurável).

- `WorkerClient`:
  - Loop de conexão: apresentar-se com `WORKER: ALIVE`, aguardar `TASK` ou `NO_TASK`, executar (simulação) e enviar `STATUS`.
  - Suporta mensagens `command_redirect` (abrir nova conexão com novo master) e `command_release` (encerrar e retornar ao original).

---

## 3. Fluxos de dados principais

1) Heartbeat (Sprint 1)
  - Worker → Master: `{ "SERVER_UUID": "Master_A", "TASK": "HEARTBEAT" }\n`
  - Master → Worker: `{ "SERVER_UUID": "Master_A", "TASK": "HEARTBEAT", "RESPONSE": "ALIVE" }\n`
  - Implementar loop do Worker com intervalo configurável (ex: 10s) e timeout de resposta 5s.

2) Ciclo de Tarefas (Sprint 2)
  - Apresentação: Worker → Master: `{ "WORKER": "ALIVE", "WORKER_UUID": "W-123", "SERVER_UUID": "Master_B"? }\n`
  - Master entrega: `TASK`/`NO_TASK` per schema.
  - Worker processa (simulação) e responde `{ "STATUS": "OK|NOK", "TASK": "QUERY", "WORKER_UUID": "..." }\n`
  - Master responde `{ "STATUS": "ACK", "WORKER_UUID": "..." }\n`

3) Negociação Master↔Master e redirecionamento (Sprint 3)
  - `request_help` inclui `request_id`, `master_id`, `current_load`, `capacity`, `workers_needed`.
  - `response_accepted` contém `workers_offered` e `worker_details` (id, address).
  - Master ofertante envia `command_redirect` a cada Worker ofertado.
  - Worker conecta-se ao novo Master e envia `register_temporary_worker` com `worker_id` e `original_master_address`.
  - Quando liberado, Master receptor envia `command_release` e notifica `notify_worker_returned` ao Master ofertante.

---

## 4. Mensagens, framing e validação

- Todas as mensagens Master↔Master seguem `{ "type": string, "request_id": uuid, "payload": { ... } }\n`.
- Worker↔Master usam os payloads apresentados em `document.md` (WORKER/ALIVE, TASK/NO_TASK, STATUS, ACK).
- Framing: `\n` terminator. Handlers acumulam bytes até `\n` e chamam `json.loads`.
- Validação: campos obrigatórios causam falha rápida (log + close) — implementar validação limpa e mensagens de erro no log para interoperabilidade.

---

## 5. Erros, timeouts e resiliência

- Timeouts importantes:
  - Worker aguardando resposta Master: 5s (por especificação).
  - Peer `request_help` timeout: configurável, sugerido 3s.

- Falhas de parsing: log + fechar conexão.
- Reconnect/backoff: Worker e PeerManager usam backoff exponencial simples (p. ex. 1s→2s→4s cap 30s).
- Histerese: `threshold_release = threshold_saturacao * 0.7` (configurável) para evitar oscilações.

---

## 6. Testes e validação

- Unit tests:
  - Validação de schemas JSON.
  - `SaturationMonitor` e decisões de `request_help`.
  - `TaskDispatcher` atribui e registra resultados.

- Integração (mínimo): script `tests/integration/two_masters_two_workers.py` que inicia dois Masters e dois Workers em portas temporárias e executa o fluxo: apresentação → atribuição → `request_help` → redirect → release.

- Test harness: usar `subprocess` para iniciar instâncias locais com `--config tests/config_*`.

---

## 7. Configuração e execução

- Arquivo de configuração: `config.yaml` (exemplo):

```yaml
master_id: Master_A
ip: 127.0.0.1
port: 6000
neighbors:
  - id: Master_B
    address: 127.0.0.1:6001
threshold_saturation: 100
threshold_release: 70
thread_pool_size: 20
```

- Comandos para rodar (exemplo):

```powershell
python -m master --config config_master_a.yaml
python -m worker --config config_worker_1.yaml
```

---

## 8. Critérios de aceitação (DoD detalhado)

1. Worker abre conexão TCP com Master e apresenta-se com `WORKER: ALIVE`.
2. Master parseia e responde `TASK` ou `NO_TASK` corretamente.
3. Worker executa tarefa simulada e envia `STATUS`; Master responde `ACK`.
4. Saturação no Master aciona `request_help` e completa negociação com `response_accepted`/`command_redirect`.
5. Worker ofertado se registra no Master receptor e é liberado posteriormente com `command_release` e `notify_worker_returned`.

---

## 9. Próximos passos

1. Auto-revisão do spec e correções.
2. Após sua revisão/aprovação, invocar `writing-plans` para criar o plano de implementação.
