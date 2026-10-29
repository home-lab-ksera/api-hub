FROM rust:1.99.1-alpine3.22 AS builder

WORKDIR /src

RUN apk add --no-cache build-base cmake perl ca-certificates

COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY migrations ./migrations
COPY openapi.json ./openapi.json

# x86_64 の旧CPUでも動く静的バイナリを生成する。
RUN if [ "$(uname -m)" = x86_64 ]; then \
      export RUSTFLAGS='-C target-cpu=x86-64 -C target-feature=-aes,-avx,-avx2'; \
    fi && \
    cargo build --release --locked --bin api-hub

# Runtime stage
FROM scratch

WORKDIR /

# Copy CA bundle for outbound TLS
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

# Copy the binary from the builder stage
COPY --from=builder /src/target/release/api-hub /api-hub

# Expose the port your application listens on
EXPOSE 3000

USER 65532:65532

# Run the application
CMD ["/api-hub"]
