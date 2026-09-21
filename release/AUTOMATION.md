# Канал поставок: автоматизация SBE → HydraCore → HydraBox

Этот файл описывает, какие шаги канала поставок идут сами, что для них нужно
настроить один раз, и где по-прежнему решает человек.

## Что автоматизировано

| Механизм | Файл | Что делает |
| --- | --- | --- |
| Обнаружение нового SBE | `.github/workflows/sbe-watch.yml` + `release/sbe_watch.sh` | Каждые 6 часов (cron `0 */6 * * *`) читает список тегов upstream `shtorm-7/sing-box-extended` через `git ls-remote` и сравнивает новейший тег с зафиксированным в `release/UPSTREAM_BASELINE`. |
| Подготовка апгрейда | `release/upgrade_sbe.sh` | Мержит найденный upstream-тег в текущую ветку, обновляет `UPSTREAM_BASELINE` (COMMIT/TAG/GO_VERSION) и поднимает `HYDRACORE_VERSION` до `hydracore-sbe-<sbe>-debug-1`. Коммитит результат. **Не пушит, не тегирует, не публикует.** |
| Триггер сборки клиента | `.github/workflows/hydracore.yml`, шаг «Notify HydraBox…» | После успешной публикации релиза ядра шлёт `repository_dispatch` (`event_type=core-released`) в `gr33nimax/hydrabox` с payload `{core_tag, channel, core_commit}`. |

Направление связи — строго `ядро → клиент`:

- ядро `debug` (пре-релизы `-debug-<n>` / `-rc-<n>`) → HydraBox канал `canary`;
- ядро `main` (`hydracore-sbe-<v>`) → HydraBox канал `stable`.

Клиент не выбирает канал сам: он берёт его из payload и не собирается из чужого канала.

## Как это выглядит целиком

```text
[cron 6h]                                  [publish ядра]
   │                                            │
   ▼                                            ▼
sbe-watch.yml                          hydracore.yml (workflow_dispatch)
   │ новый upstream?                            │ релиз опубликован (зелёно)
   │ да → upgrade_sbe.sh (merge)                │
   │ → ветка auto/sbe-<ver> + PR в debug        │ repository_dispatch (core-released)
   │ нет → выход без изменений                  ▼
   └─ конфликт merge → шаг падает, письмо   hydrabox: android-release.yml
                                                → сборка канала canary/stable
```

## Что нужно настроить один раз

### Секрет `HYDRABOX_DISPATCH_PAT`

Публикация ядра должна иметь право запустить workflow в **другом** репозитории, а
штатный `GITHUB_TOKEN` действует только внутри своего. Поэтому в репозитории
`gr33nimax/hydracore` заводится секрет Actions `HYDRABOX_DISPATCH_PAT`:

- значение — Personal Access Token, которому разрешён `repository_dispatch` в
  `gr33nimax/hydrabox`;
- classic PAT: scope `repo`;
- fine-grained PAT: доступ к репозиторию `gr33nimax/hydrabox`, право
  **Contents: read and write** (этого достаточно для `POST /repos/{owner}/{repo}/dispatches`).

Заводит секрет владелец (в настройках репозитория: Settings → Secrets and variables →
Actions → New repository secret).

### Поведение секрета

- **Секрета нет** — шаг «Notify HydraBox…» падает с ошибкой
  `HYDRABOX_DISPATCH_PAT is not set`. Релиз ядра к этому моменту уже опубликован (шаг
  идёт строго после него), поэтому падение не откатывает релиз, но прогон становится
  красным и владельцу приходит письмо GitHub — клиент просто не соберётся, пока секрет
  не заведён.
- **Токен протух/отозван** — тот же исход: шаг падает, приходит письмо. Нужно
  перевыпустить PAT и обновить секрет.
- Письма о падении Actions — штатные, GitHub: отдельный SMTP-шаг не заводится
  (решение владельца).

## Что остаётся ручным (осознанно)

- **Публикация ядра.** Тег-контракт и подпись не меняются; релиз по-прежнему выходит
  через `workflow_dispatch` с `publish=true` (guard `hydracore.yml`). Авто-канал только
  готовит merge-запрос и запускает сборку клиента после уже опубликованного релиза.
- **Слияние апгрейда в `release`.** `sbe-watch.yml` открывает PR в `debug`; слияние
  делает владелец. При конфликте merge скрипт ничего не коммитит и падает — разбор за
  владельцем.
- **Продвижение `debug → main`** (stable-cut) — ручное решение владельца.
- **Раскат на живые VPS/устройства** — ручная живая проверка.

## Как прогнать вручную

```bash
# только обнаружение (read-only, ничего не меняет)
bash release/sbe_watch.sh

# проверка правила выбора тега
bash release/sbe_watch_test.sh

# подготовка апгрейда на конкретный тег (мержит и коммитит, НЕ пушит)
bash release/upgrade_sbe.sh v1.14.1-extended-2.7.2
```

`SBE_WATCH_LISTING` подставляет список тегов вместо обращения к сети — так правило выбора
проверяется без upstream.
