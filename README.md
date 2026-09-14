# HydraCore

Сетевой рантайм VPN-стека Hydra: форк sing-box-extended (GPL-3.0) с
собственным транспортом `vk_parasite` и Android-рантаймом libbox. Это
движок, а не приложение и не сервис: клиент — HydraBox, управление VPS —
HYDRA-ULTIMATE.

```
HydraBox (Android-клиент)          HYDRA-ULTIMATE (VPS)
      │ libbox AAR                       │ sing-box binary
      └──────────── HydraCore ───────────┘
```

## Что своё, что унаследовано

От sing-box-extended — конфиг-пайплайн целиком: inbounds/outbounds,
routing, DNS, TLS, все штатные протоколы, CLI. Собственная часть HydraCore:

| Компонент | Где | Что делает |
| --- | --- | --- |
| Транспорт `vk_parasite` | `transport/call/vk-parasite/` (18 файлов) | QUIC поверх VK-звонков |
| Регистрация протокола | `protocol/call/` | inbound/outbound `call` в sing-box |
| Конфиг-опции | `option/call.go` | `"type": "call"` в конфиге sing-box |
| Контракт Hydra | `common/hydracore/` | минимальный VPS contract, типы health/failure, generation сети |
| Android-рантайм | `experimental/libbox/` (42 файла) | AAR через gomobile: команды рантайму, снимки, URL-test |
| Подписки | `contract/subscription/` | Hydra Subscription v2 |
| Сборочные теги | `include/call*.go` | `with_call_client` / `with_call_server` |

## Собственный транспорт

`vk_parasite` переносит QUIC через медиапуть звонка VK. Клиент создаёт четыре
независимых пути по `join_links`; worker'ы распределяются между ними и
переподключаются отдельно. Сервер принимает соединения на одном UDP-сокете.

Эта часть находится в `transport/call/vk-parasite/`; интеграция с sing-box —
в `protocol/call/` и `option/call.go`.

## Конфигурация

VPS inbound:

```json
{
  "type": "call",
  "tag": "call-vk-server",
  "platform": "vk",
  "mode": "vk_parasite",
  "listen": "0.0.0.0",
  "listen_port": 8443,
  "obfs_password": "outer-secret",
  "max_workers_per_session": 4,
  "users": [{"name": "tester-1", "password": "per-user-secret"}]
}
```

Клиент outbound:

```json
{
  "type": "call",
  "tag": "proxy-main",
  "platform": "vk",
  "mode": "vk_parasite",
  "server": "203.0.113.10",
  "server_port": 8443,
  "join_links": [
    "https://vk.com/call/join/call-0",
    "https://vk.com/call/join/call-1",
    "https://vk.com/call/join/call-2",
    "https://vk.com/call/join/call-3"
  ],
  "user": "tester-1",
  "password": "per-user-secret",
  "obfs_password": "outer-secret",
  "workers": 4
}
```

`workers` принимает только 4/8/12/16/20. `mode` — только `vk_parasite`.
Креды и join-ссылки — секреты, реальные значения в репозиторий не коммитить.

## Контракт рантайма

`sing-box hydra contract --json` сообщает ровно то, что Hydra Ultimate должна
проверить перед запуском серверного ядра:

```json
{
  "contract_version": 1,
  "core_id": "io.hydrabox.hydracore",
  "role": "vps",
  "calls_mode": "vk_parasite"
}
```

## Сборка

```bash
go build ./... && go test ./... && go vet ./...
bash release/verify_upstream_baseline.sh   # пины Go/NDK/JDK/gomobile
make lint
```

Версии инструментов и build tags libbox закреплены в
`release/UPSTREAM_BASELINE`, проверка — первый шаг каждой CI-джобы.
Артефакты релиза (AAR, архивы Linux, подписанный манифест) собирает только
CI (`.github/workflows/hydracore.yml`).

## Релизы

Устанавливать только артефакты из
[GitHub Releases](https://github.com/gr33nimax/hydracore/releases): AAR с
исходниками, три shared library для Android, архивы `amd64`/`arm64` и
подписанный манифест. Ветка `debug` публикует пре-релизы, `main` — стабильных
кандидатов. Артефакты клиента и VPS бери из одного релиза. Публичная
идентичность — `io.hydrabox.hydracore`.

## Документация

- [Hydra Subscription v2](contract/subscription/HYDRA_SUBSCRIPTION_V2.md)
- [Release notes](release/HYDRACORE_RELEASE_NOTES.md) · [CHANGELOG](CHANGELOG.md)
- [SECURITY](SECURITY.md) · [CONTRIBUTING](CONTRIBUTING.md)
