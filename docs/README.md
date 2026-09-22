# HydraCore documentation

HydraCore — сетевой рантайм VPN-стека Hydra: форк
[sing-box-extended](https://github.com/shtorm-7/sing-box-extended) (GPL-3.0) с собственным
транспортом `vk_parasite` и верифицируемой Android-сборкой libbox. Это **движок**, не
приложение и не сервис.

```text
HydraBox (Android-клиент)          HYDRA-ULTIMATE (VPS-оркестратор)
      │ libbox.aar                        │ sing-box binary
      └──────────────  HydraCore  ────────┘
                     io.hydrabox.hydracore
```

## С чего начать

| Ты хочешь | Читай |
| --- | --- |
| Общая картина, что своё / что от upstream, быстрый конфиг | [`../README.md`](../README.md) |
| **Как устроен `vk_parasite` вглубь** (QUIC-over-VK, lane'ы, wire, supervisor, TURN, MTU) | [architecture.md](architecture.md) |
| Полный справочник конфигурации `type: call` | [configuration.md](configuration.md) |
| Как ядро стыкуется с HydraBox и HYDRA-ULTIMATE | [ecosystem.md](ecosystem.md) |
| Сборка, версии, CI, тег-контракт, авто-обновление SBE | [build-and-release.md](build-and-release.md) |
| Формат подписок | [../contract/subscription/HYDRA_SUBSCRIPTION_V2.md](../contract/subscription/HYDRA_SUBSCRIPTION_V2.md) |
| Лицензии и происхождение | [../CREDITS.md](../CREDITS.md) · [../THIRD_PARTY_NOTICES.md](../THIRD_PARTY_NOTICES.md) |
| Безопасность, вклад | [../SECURITY.md](../SECURITY.md) · [../CONTRIBUTING.md](../CONTRIBUTING.md) |

## Что своё, а что унаследовано

От sing-box-extended — весь конфиг-пайплайн: inbounds/outbounds, routing, DNS, TLS,
штатные протоколы, CLI. Собственная часть HydraCore:

| Компонент | Каталог | Роль |
| --- | --- | --- |
| Транспорт `vk_parasite` | `transport/call/vk-parasite/` | QUIC поверх медиапути VK-звонка |
| Регистрация протокола | `protocol/call/` | inbound/outbound `call` в sing-box |
| Опции конфига | `option/call.go` | `"type": "call"` в документе sing-box |
| Контракт Hydra | `common/hydracore/` | product contract, health/failure, generation сети, TURN-edge |
| Android-рантайм | `experimental/libbox/` | AAR через gomobile: команды, снимки, URL-test, build-info |
| Подписки | `contract/subscription/` | Hydra Subscription v2 |
| Сборочные теги | `include/call*.go` | `with_call_client` / `with_call_server` |

Идентичность ядра для всех потребителей — `io.hydrabox.hydracore`
(`common/hydracore/contract.go`).

---
_Документация описывает ядро по коду. Каждый факт прослеживается к файлу в репозитории;
при расхождении кода и текста — код источник правды, а текст правится._
