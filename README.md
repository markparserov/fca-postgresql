# FCA In-Close (PostgreSQL)

Проект реализует перечисление формальных понятий (FCA) в PostgreSQL по мотивам In-Close5:

- хранение формального контекста: `fca.objects`, `fca.attributes`, `fca.context`;
- вертикальная материализация по атрибутам: `fca.attr_extent`;
- перечисление понятий через `fca.run_inclose(...)`;
- хранение результатов и метрик запуска;
- построение рёбер диаграммы Хассе.

## Файлы проекта

**Скрипты (по важности):**
- `scripts/inclose5_postgresql.sql` — ядро: схема, функции, вьюхи;
- `scripts/run_pairs_scenario.sql` — SQL-сценарий запуска на long-пары;
- `scripts/run_formal_context_csv.sh` — pipeline для wide CSV (конвертация + запуск);
- `scripts/wide_context_to_pairs.py` — конвертер wide → `obj_key,attr_key`;
- `scripts/autotest_pairs.sql` — автотесты для `data/pairs.csv` (падают с `ERROR` при нарушении корректности);
- `scripts/export_hasse_dot.sql` — экспорт в Graphviz DOT для визуализации.

**Данные (исходник → сгенерированные пары):**
- `data/pairs.csv` — мини-пример long-формата (`obj_key,attr_key`), используется сценарием и автотестами;
- `data/iris_binarized.csv` → `data/iris_binarized_pairs.csv` — wide-пример (iris, бинаризованные признаки);
- `data/animal_movement.csv` → `data/animal_movement_pairs.csv` — wide-пример (животные × способности: fly, hunt, run, swim).

Файлы `*_pairs.csv` создаются скриптом `run_formal_context_csv.sh`.

**Визуализация:**
- `hasse/` — артефакты (`.dot`, `.png`, `.svg`).

## Требования

- PostgreSQL 13+ (рекомендуется 14+);
- клиент `psql`;
- база, например `fca_db`.

## 1) Разовый запуск на `pairs.csv`

Запускается одним файлом:

```bash
cd FCA
psql -d fca_db -f "./scripts/run_pairs_scenario.sql"
```

Что делает сценарий:

1. Подключает `inclose5_postgresql.sql`;
2. Очищает таблицы `fca.*` и сбрасывает identity;
3. Загружает `pairs.csv`;
4. Строит `attr_extent`;
5. Выполняет `run_inclose(...)` с `p_min_extent => 0` и `p_store_extent => true`;
6. Строит рёбра Хассе;
7. Печатает `run_summary`, список понятий и рёбра.

## 2) Автотесты

Автотесты также запускаются одним файлом:

```bash
cd FCA
psql -d fca_db -f "./scripts/autotest_pairs.sql"
```

Проверки в `autotest_pairs.sql`:

- запуск завершился со статусом `finished`;
- число понятий равно `6` для текущего `pairs.csv`;
- число рёбер Хассе равно `7`;
- множество intent'ов совпадает с ожидаемым (без пропусков/лишних);
- проходит closure roundtrip: `intent -> extent -> intent`.

При любой ошибке тест завершится `ERROR` с причиной.

## 3) Автоматическая обработка wide formal context CSV

Если датасет уже в формате формального контекста (строки = объекты, колонки = атрибуты, ячейки = `True/False`), используй:

```bash
cd FCA
./scripts/run_formal_context_csv.sh ./data/iris_binarized.csv
./scripts/run_formal_context_csv.sh ./data/animal_movement.csv
```

Что делает скрипт:

1. Конвертирует wide CSV в `data/<имя_входного_файла>_pairs.csv` (`obj_key,attr_key`);
2. Загружает пары в `fca.*`;
3. Запускает `run_inclose(...)`;
4. Строит рёбра Хассе и печатает результаты.

Особенности входного CSV:

- первый столбец интерпретируется как `obj_key`;
- заголовок первого столбца может быть пустым или именованным;
- пустой `obj_key` в строке заменяется на `obj_<номер_строки>`;
- все остальные столбцы считаются атрибутами;
- как истина распознаются значения: `True/true`, `1`, `t`, `yes`, `x`, `+`, `да`.

Можно задать имя выходного файла пар:

```bash
./scripts/run_formal_context_csv.sh ./data/iris_binarized.csv ./data/iris_pairs.csv
```

Можно задать БД через переменную:

```bash
DB_NAME=fca_db ./scripts/run_formal_context_csv.sh ./data/iris_binarized.csv
```

## Ручной запуск

Если нужен пошаговый режим, можно запускать функции вручную:

```sql
SELECT fca.rebuild_attr_extent();
SELECT fca.run_inclose(
  p_min_extent => 1,
  p_min_intent => 0,
  p_max_concepts => 50000,
  p_max_runtime => interval '5 minutes',
  p_store_extent => false,
  p_rebuild_vertical => true
) AS run_id;
SELECT fca.build_hasse_edges(<run_id>);
```

Просмотр результата:

```sql
SELECT * FROM fca.v_run_summary ORDER BY run_id DESC LIMIT 1;
SELECT * FROM fca.v_concepts WHERE run_id = <run_id> ORDER BY concept_id;
SELECT * FROM fca.v_hasse_edges WHERE run_id = <run_id> ORDER BY parent_concept_id, child_concept_id;
```

## Визуализация (Graphviz)

Экспорт в DOT для конкретного запуска:

```bash
cd FCA
psql -qAt -d fca_db -v run_id=1 -f "./scripts/export_hasse_dot.sql" > "./hasse/hasse_run1_auto.dot"
```

Если `run_id` не передан, скрипт берёт последний запуск из `fca.inclose_runs`.

Рендер в картинку:

```bash
dot -Tpng "./hasse/hasse_run1_auto.dot" -o "./hasse/hasse_run1_auto.png"
dot -Tsvg "./hasse/hasse_run1_auto.dot" -o "./hasse/hasse_run1_auto.svg"
```