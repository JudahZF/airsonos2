#!/usr/bin/env bash
# Run in the pinned environment: nix develop -c bash scripts/check-reliability.sh
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

if [[ $# -gt 1 || ( $# -eq 1 && $1 != --packages ) ]]; then
  echo 'Usage: scripts/check-reliability.sh [--packages]' >&2
  exit 2
fi

cargo fmt --all -- --check
cargo clippy --workspace --all-features --all-targets --locked -- -D warnings
cargo test --workspace --all-features --locked
for features in '' ap2,resample video,hls; do
  cargo test --manifest-path vendor/shairplay/Cargo.toml --features "$features" --locked
done
cargo deny --locked check advisories bans licenses sources
cargo deny --manifest-path vendor/shairplay/Cargo.toml --locked check advisories bans licenses sources
cargo build --bin airsonos2 --locked
build_target_dir="$(cargo metadata --format-version 1 --no-deps --locked | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"
python3 scripts/test-process-lifecycle.py --binary "$build_target_dir/debug/airsonos2"
python3 scripts/test-release-version.py

# Local container checks use the Docker host architecture. Native CI runners
# cover both supported Linux architectures before publication.
if [[ ${1:-} == --packages ]]; then
  docker build -f packaging/docker/Dockerfile -t airsonos2-reliability:generic .
  docker build -f airsonos2/Dockerfile -t airsonos2-reliability:home-assistant .
  for image in airsonos2-reliability:generic airsonos2-reliability:home-assistant; do
    docker run --rm --entrypoint /usr/local/bin/airsonos2 "$image" --version
  done
fi
