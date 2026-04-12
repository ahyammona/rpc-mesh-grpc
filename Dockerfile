FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY target/release/rpc-mesh .
COPY config.toml .

CMD ["./rpc-mesh"]