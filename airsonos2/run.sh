#!/usr/bin/env bash
set -euo pipefail

OPTIONS_PATH="/data/options.json"
CONFIG_PATH="/data/config.toml"

echo "[airsonos2] Starting Home Assistant app"
echo "[airsonos2] Rendering ${CONFIG_PATH} from ${OPTIONS_PATH}"
airsonos2 render-ha-config --options "${OPTIONS_PATH}" --output "${CONFIG_PATH}"

run_doctor_on_start="false"
if [[ -r /usr/lib/bashio/bashio ]]; then
    # shellcheck source=/dev/null
    source /usr/lib/bashio/bashio
    if bashio::config.true "run_doctor_on_start"; then
        run_doctor_on_start="true"
    fi
elif command -v jq >/dev/null 2>&1; then
    run_doctor_on_start="$(jq -r '.run_doctor_on_start // false' "${OPTIONS_PATH}")"
elif grep -Eq '"run_doctor_on_start"[[:space:]]*:[[:space:]]*true' "${OPTIONS_PATH}"; then
    run_doctor_on_start="true"
fi

if [[ "${run_doctor_on_start}" == "true" ]]; then
    echo "[airsonos2] Running startup doctor"
    if ! airsonos2 doctor --config "${CONFIG_PATH}"; then
        echo "[airsonos2] Doctor reported issues; continuing startup"
    fi
fi

echo "[airsonos2] Starting bridge"
# Options regenerate the config on each start, so the web GUI must not edit it.
exec airsonos2 serve --config "${CONFIG_PATH}" --config-read-only
