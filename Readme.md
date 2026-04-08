# rpc-mesh

A gRPC-fronted smart routing layer for Solana JSON-RPC providers, plus a
Yellowstone Dragon's Mouth gRPC client for real-time account, transaction,
and slot streaming.

```
Your app  ──gRPC──►  rpc-mesh  ──HTTP──►  Helius (JSON-RPC)
                         │      ──HTTP──►  Triton (JSON-RPC)
                         │      ──HTTP──►  QuickNode (JSON-RPC)
                         └──────gRPC──►   yellowstone-geyser

geyser-client ──gRPC──►  Helius / Shyft / Triton (Yellowstone gRPC)
```

---

## Workspace layout

```
rpc-mesh-grpc/
  src/                  — mesh server (routing engine, providers, gRPC service)
  proto/
    mesh.proto          — rpcmesh.v1 service (used by rpc-client and the server)
    geyser.proto        — yellowstone.geyser.v1 service (used by geyser-client)
  rpc-client/           — test client for the rpc-mesh gRPC server
  geyser-client/        — test client for Yellowstone Dragon's Mouth gRPC
  config.toml           — server config (providers, listen address)
  Cargo.toml
```

---

## Providers: RPC and gRPC

Each entry in `[[providers]]` in the server `config.toml` can be either:

| `kind`   | What it connects to                           | Required field    |
|----------|-----------------------------------------------|-------------------|
| `"rpc"`  | Plain JSON-RPC HTTP endpoint                  | `url`             |
| `"grpc"` | Yellowstone geyser grpc   | `grpc_endpoint`   |

gRPC peers are probed via `WatchHealth` and routed through identically to
HTTP providers — the mesh doesn't distinguish at the routing layer.

---

## Quick start

### 1. Start the mesh server

```bash
# Edit config.toml with your RPC provider keys
cargo run --bin rpc-mesh
```

### 2. Test with the rpc-client

```bash
cd rpc-client

# Edit config.toml with your mesh server address
# Then run the smoke-test suite:
cargo run

# Stream live node health:
cargo run -- --watch-health

# Stream routing decisions:
cargo run -- --watch-routing

# Single method call:
cargo run -- --method getSlot
cargo run -- --method getBalance --account <PUBKEY>
```

### 3. Test with the geyser-client

```bash
cd geyser-client

# Edit config.toml — set your Yellowstone endpoint and x-token (API key)
# Then run the default slot smoke-test:
cargo run

# Print server version:
cargo run -- --get-version

# Stream slot updates:
cargo run -- --subscribe-slots

# Stream account changes (one or more pubkeys):
cargo run -- --subscribe-accounts <PUBKEY> [<PUBKEY> ...]

# Stream account changes by program owner:
cargo run -- --owner <PROGRAM_PUBKEY>

# Stream non-vote transactions touching an account:
cargo run -- --subscribe-transactions \
               --account-include <PUBKEY> \
               --no-votes

# Stream all transactions including failed ones:
cargo run -- --subscribe-transactions --include-failed

# Stream lightweight block headers:
cargo run -- --subscribe-blocks-meta

# Stream full blocks (high volume — use carefully):
cargo run -- --subscribe-blocks
```

---

## config.toml — server

```toml
listen_addr       = "[::1]:50051"
probe_interval_ms = 500

# JSON-RPC provider
[[providers]]
id   = "helius"
kind = "rpc"
url  = "https://mainnet.helius-rpc.com/?api-key=YOUR_KEY"

# gRPC peer (another rpc-mesh node)
[[providers]]
id            = "mesh-us-east"
kind          = "grpc"
grpc_endpoint = "http://us-east.example.com:50051"
```

## config.toml — rpc-client

Same format as the server config. The client reads `listen_addr` to know
where the mesh is running.

## config.toml — geyser-client

```toml
# Yellowstone gRPC endpoint
# Helius:  https://atlas-mainnet.helius-rpc.com
# Shyft:   https://rpc.shyft.to
# Triton:  https://YOUR_ENDPOINT.rpcpool.com
# Local:   http://127.0.0.1:10000
endpoint   = "https://atlas-mainnet.helius-rpc.com"
x_token    = "YOUR_HELIUS_API_KEY"
commitment = "confirmed"
```

---

## Architecture — mesh server

```
src/
  config.rs            — AppConfig / ProviderConfig (kind, url, grpc_endpoint)
  main.rs              — startup: load config, build providers, start prober + gRPC server
  provider/
    mod.rs             — Provider enum (Rpc | Grpc) — unified async interface
    rpc_client.rs      — JSON-RPC HTTP client (reqwest)
    grpc_client.rs     — gRPC client (tonic) — speaks rpcmesh.v1
  mesh/
    classifier.rs      — maps method → RoutingStrategy + RequestProfile
    router.rs          — picks best node from live metrics per strategy
    scorer.rs          — NodeMetrics with EMA latency / error_rate / slot_lag
    registry.rs        — background probe_loop_provider (works for both kinds)
    executor.rs        — send_to_node + multipath_send (transport-agnostic)
    server.rs          — tonic RpcMesh service implementation
proto/
  mesh.proto           — rpcmesh.v1 protobuf definitions
  geyser.proto         — yellowstone.geyser.v1 protobuf definitions
```

---

## gRPC services

### mesh.proto — rpcmesh.v1

| RPC               | Description                                                   |
|-------------------|---------------------------------------------------------------|
| `Send`            | Single routed call — set method flags in `RpcMethodSet`       |
| `SendTransaction` | Fires to all healthy nodes simultaneously (multipath)         |
| `WatchHealth`     | Server-streaming: live `NodeHealth` snapshots every 500ms     |
| `WatchRouting`    | Server-streaming: routing decisions for debugging / dashboards |

### geyser.proto — yellowstone.geyser.v1

| RPC          | Description                                                                |
|--------------|----------------------------------------------------------------------------|
| `Subscribe`  | Bidirectional stream — send filter updates, receive matching events        |
| `GetVersion` | Unary — returns the server's version string                                |

Subscription filter types: `accounts`, `slots`, `transactions`, `blocks`,
`blocks_meta`, `entry`.

Providers that expose the Yellowstone interface: **Helius**, **Shyft**,
**Triton** (RPC Pool), and any validator running the `yellowstone-grpc` plugin.
