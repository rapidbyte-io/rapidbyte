#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "rapidbyte rustc-wrapper: missing rustc invocation arguments" >&2
  exit 2
fi

warn_marker="/tmp/rapidbyte-sccache-disabled-${UID:-unknown}"

if [[ "${NO_SCCACHE:-0}" == "1" ]]; then
  exec "$@"
fi

is_probe_call=0
is_wasm_wasip2_target=0
for arg in "$@"; do
  if [[ "$arg" == "--print=file-names" ]]; then
    is_probe_call=1
  fi
  if [[ "$arg" == "--target=wasm32-wasip2" ]]; then
    is_wasm_wasip2_target=1
  fi
done

if [[ "$is_wasm_wasip2_target" -eq 0 ]]; then
  for ((i = 1; i < $#; i++)); do
    if [[ "${!i}" == "--target" ]]; then
      j=$((i + 1))
      if [[ $j -le $# ]] && [[ "${!j}" == "wasm32-wasip2" ]]; then
        is_wasm_wasip2_target=1
      fi
      break
    fi
  done
fi

# Guard wasm builds from user-global target-cpu=native flags.
if [[ "$is_wasm_wasip2_target" -eq 1 ]]; then
  sanitized=()
  skip_next=0
  args=("$@")
  for ((i = 0; i < ${#args[@]}; i++)); do
    arg="${args[$i]}"
    if [[ "$skip_next" -eq 1 ]]; then
      skip_next=0
      continue
    fi
    if [[ "$arg" == "-Ctarget-cpu=native" ]]; then
      continue
    fi
    if [[ "$arg" == "-C" ]] && [[ $((i + 1)) -lt ${#args[@]} ]]; then
      next="${args[$((i + 1))]}"
      if [[ "$next" == "target-cpu=native" ]]; then
        skip_next=1
        continue
      fi
    fi
    sanitized+=("$arg")
  done
  sanitized+=("-C" "target-cpu=generic")
  set -- "${sanitized[@]}"

  # Cargo may also forward flags through this encoded environment variable.
  if [[ -n "${CARGO_ENCODED_RUSTFLAGS:-}" ]]; then
    IFS=$'\x1f' read -r -a encoded_flags <<<"${CARGO_ENCODED_RUSTFLAGS}"
    sanitized_encoded=()
    skip_next=0
    for ((i = 0; i < ${#encoded_flags[@]}; i++)); do
      flag="${encoded_flags[$i]}"
      if [[ "$skip_next" -eq 1 ]]; then
        skip_next=0
        continue
      fi
      if [[ "$flag" == "-Ctarget-cpu=native" ]]; then
        continue
      fi
      if [[ "$flag" == "-C" ]] && [[ $((i + 1)) -lt ${#encoded_flags[@]} ]]; then
        next="${encoded_flags[$((i + 1))]}"
        if [[ "$next" == "target-cpu=native" ]]; then
          skip_next=1
          continue
        fi
      fi
      sanitized_encoded+=("$flag")
    done
    sanitized_encoded+=("-C" "target-cpu=generic")

    joined=""
    for flag in "${sanitized_encoded[@]}"; do
      if [[ -z "$joined" ]]; then
        joined="$flag"
      else
        joined+=$'\x1f'"$flag"
      fi
    done
    export CARGO_ENCODED_RUSTFLAGS="$joined"
  fi
fi

if [[ "$is_probe_call" -eq 1 ]]; then
  exec "$@"
fi

# Once sccache has failed in this session, skip future attempts to avoid
# repeated permission errors and startup overhead.
if [[ -f "$warn_marker" ]] && [[ "${FORCE_SCCACHE:-0}" != "1" ]]; then
  exec "$@"
fi

if ! command -v sccache >/dev/null 2>&1; then
  exec "$@"
fi

err_file="$(mktemp -t rapidbyte-sccache-wrapper.XXXXXX)"
cleanup() {
  rm -f "$err_file"
}
trap cleanup EXIT

if sccache "$@" 2>"$err_file"; then
  exit 0
fi

status=$?

# Fall back only for wrapper/cache infrastructure failures.
if grep -q "sccache: error:" "$err_file"; then
  if [[ ! -f "$warn_marker" ]]; then
    cat "$err_file" >&2
    {
      echo "rapidbyte: sccache unavailable; falling back to direct rustc."
      echo "rapidbyte: set NO_SCCACHE=1 to bypass sccache explicitly."
      echo "rapidbyte: set FORCE_SCCACHE=1 to retry sccache after fixing permissions."
    } >&2
    : >"$warn_marker" 2>/dev/null || true
  fi

  exec "$@"
fi

cat "$err_file" >&2
exit "$status"
