# ДЗ-4. Apache ZooKeeper

Апрелков Михаил, ИИИ-3-ИнжДан-1-5.

Отчёт: [report/Aprelkov_HW4_report.pdf](report/Aprelkov_HW4_report.pdf)

- `zk/zk{1,2,3}/conf/zoo.cfg`, `data/zk{1,2,3}/myid` — конфигурации трёх серверов
- `compose.yaml` — тот же ансамбль в Docker
- `src/coordinator.py` — координатор расчёта из ДЗ-3 с выбором лидера, чекпоинтом и `FENCING`
- `scripts/` — сценарии этапов, `logs/` — их вывод и журналы координаторов

Запуск:

```bash
brew install openjdk@17
uv sync
./run_all.sh
```

ZooKeeper 3.9.6 распаковать в `zk/zk1`, `zk/zk2`, `zk/zk3`. Для данных нужен RustFS со стенда ДЗ-2 (`infra/stand.sh up`).
