# ---------------------------------------------------
# Stage 1: Build Rust binary
# ---------------------------------------------------
FROM rust:1.85-slim-bookworm AS builder

# Install build dependencies for librdkafka, OpenSSL, and curl
RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake \
    build-essential \
    pkg-config \
    libssl-dev \
    zlib1g-dev \
    libcurl4-openssl-dev \
    libsasl2-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Cache dependency layer
COPY Cargo.toml ./
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release || true
RUN rm -rf src

# Copy real source code and compile release binary
COPY src ./src
RUN rm -rf target/release/data-faker-backend* target/release/deps/data_faker_backend* && \
    cargo build --release

# ---------------------------------------------------
# Stage 2: Minimal Runtime Image
# ---------------------------------------------------
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    zlib1g \
    libcurl4 \
    libsasl2-2 \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/data-faker-backend /app/data-faker-backend

ENV PORT=5000
ENV RUST_LOG=info
EXPOSE 5000

CMD ["/app/data-faker-backend"]
