#!/usr/bin/env bash
set -euo pipefail

# автоматическая обработка wide CSV формального контекста:
# 1) wide csv -> пары obj_key,attr_key
# 2) запуск FCA через scripts/run_pairs_scenario.sql
#
# usage:
#   ./scripts/run_formal_context_csv.sh ./data/iris_binarized.csv
#   ./scripts/run_formal_context_csv.sh ./data/iris_binarized.csv ./data/iris_pairs.csv
# env:
#   DB_NAME=fca_db

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "usage: $0 <input_wide_csv> [output_pairs_csv]" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
DB_NAME="${DB_NAME:-fca_db}"

INPUT_PATH="$1"
if [[ "${INPUT_PATH}" != /* ]]; then
  INPUT_PATH="${PWD}/${INPUT_PATH}"
fi

INPUT_BASENAME="$(basename "${INPUT_PATH}")"
INPUT_STEM="${INPUT_BASENAME%.*}"
DEFAULT_OUTPUT="${PROJECT_ROOT}/data/${INPUT_STEM}_pairs.csv"

OUTPUT_PATH="${2:-${DEFAULT_OUTPUT}}"
if [[ "${OUTPUT_PATH}" != /* ]]; then
  OUTPUT_PATH="${PWD}/${OUTPUT_PATH}"
fi

echo "convert wide context to pairs..." >&2
python3 "${SCRIPT_DIR}/wide_context_to_pairs.py" "${INPUT_PATH}" "${OUTPUT_PATH}" >/dev/null

echo "run FCA pipeline..." >&2
cd "${PROJECT_ROOT}"
TMP_SQL="$(mktemp "${PROJECT_ROOT}/scripts/.tmp_run_pairs_XXXXXX.sql")"
trap 'rm -f "${TMP_SQL}"' EXIT

python3 - "${SCRIPT_DIR}/run_pairs_scenario.sql" "${TMP_SQL}" "${OUTPUT_PATH}" <<'PY'
import pathlib
import sys

src = pathlib.Path(sys.argv[1])
dst = pathlib.Path(sys.argv[2])
pairs_path = pathlib.Path(sys.argv[3]).as_posix()
quoted_pairs_path = "'" + pairs_path.replace("'", "''") + "'"

content = src.read_text(encoding="utf-8")
content = content.replace("'./data/pairs.csv'", quoted_pairs_path)
dst.write_text(content, encoding="utf-8")
PY

psql -d "${DB_NAME}" -f "${TMP_SQL}"

echo "done. pairs file: ${OUTPUT_PATH}" >&2