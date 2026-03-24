#!/usr/bin/env python3
"""Преобразует wide CSV формального контекста в пары obj_key,attr_key.

Вход:
  - первый столбец: ключ объекта (заголовок может быть пустым или именованным)
  - остальные столбцы: атрибуты
  - значения ячеек: бинарные (True/False, 1/0, t/f, yes/no, x/пусто и т.д.)

Выход:
  CSV с колонками obj_key,attr_key (только истинные пары).
"""

from __future__ import annotations

import csv
import pathlib
import sys


TRUE_VALUES = {"1", "true", "t", "yes", "y", "x", "+", "да", "истина"}


def is_true(value: str) -> bool:
    return (value or "").strip().lower() in TRUE_VALUES


def main() -> int:
    if len(sys.argv) != 3:
        print(
            "usage: wide_context_to_pairs.py <input_wide_csv> <output_pairs_csv>",
            file=sys.stderr,
        )
        return 2

    input_path = pathlib.Path(sys.argv[1]).expanduser().resolve()
    output_path = pathlib.Path(sys.argv[2]).expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not input_path.exists():
        print(f"input file not found: {input_path}", file=sys.stderr)
        return 2

    pair_count = 0
    object_count = 0
    attribute_count = 0

    with input_path.open("r", newline="", encoding="utf-8-sig") as src, output_path.open(
        "w", newline="", encoding="utf-8"
    ) as dst:
        reader = csv.reader(src)
        writer = csv.writer(dst)
        writer.writerow(["obj_key", "attr_key"])

        header = next(reader, None)
        if not header or len(header) < 2:
            print(
                "invalid context csv: expected at least 2 columns (object + attributes)",
                file=sys.stderr,
            )
            return 2

        attributes = [a.strip() for a in header[1:]]
        if any(a == "" for a in attributes):
            print("invalid context csv: empty attribute name in header", file=sys.stderr)
            return 2
        attribute_count = len(attributes)

        for row_idx, row in enumerate(reader, start=1):
            if not row:
                continue
            object_count += 1

            obj_key = (row[0] if len(row) > 0 else "").strip()
            if obj_key == "":
                obj_key = f"obj_{row_idx}"

            values = row[1:]
            if len(values) < attribute_count:
                values = values + [""] * (attribute_count - len(values))
            elif len(values) > attribute_count:
                values = values[:attribute_count]

            for attr, val in zip(attributes, values):
                if is_true(val):
                    writer.writerow([obj_key, attr])
                    pair_count += 1

    print(
        f"converted: objects={object_count}, attributes={attribute_count}, pairs={pair_count}",
        file=sys.stderr,
    )
    print(str(output_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
