# syntax=docker/dockerfile:1
FROM rust:1.88-bookworm AS builder
ARG TARGETARCH
WORKDIR /src

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY vendor ./vendor
RUN --mount=type=cache,id=airsonos2-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=airsonos2-target-${TARGETARCH},target=/src/target,sharing=locked \
    cargo build --release --locked --bin airsonos2 \
    && mkdir -p /out && cp target/release/airsonos2 /out/airsonos2

FROM ghcr.io/home-assistant/base-debian:bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates ffmpeg \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /out/airsonos2 /usr/local/bin/airsonos2
COPY airsonos2/run.sh /run.sh
RUN chmod a+x /run.sh

CMD ["/run.sh"]
