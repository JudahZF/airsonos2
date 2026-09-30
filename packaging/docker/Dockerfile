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

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates ffmpeg \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /out/airsonos2 /usr/local/bin/airsonos2
COPY docs/config.example.toml /etc/airsonos2/config.toml

VOLUME ["/var/lib/airsonos2"]
EXPOSE 5000-5010/tcp 7000/tcp 9100/tcp
ENTRYPOINT ["/usr/local/bin/airsonos2"]
CMD ["serve", "--config", "/etc/airsonos2/config.toml"]
