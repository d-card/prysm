# Deployment Guide

Ref: https://www.offchainlabs.com/prysm/docs/install/install-with-script

The node tracks a validator.


Our strawman deployed validator:
> 0x99743c58a2de9946397bc92ddc12f108a71ddb82b61dde2c337d2489cd5d7901b2d045f7a1685b9a08bbfd07a0d12909

Other validators for testing in case ours is down:
> 0x8f186c70e253e77b8c77cc1e97e3744d35fc60fb7a354f40e781e40322622dac773c9593d883ba6c02be72b23b8bd09c

Trackers for the deployed validator:
- https://hoodi.beaconcha.in/validator/1131738
- https://v2-beta-hoodi.beaconcha.in/dashboard/MTEzMTczOA#summary

## Dependencies

### [Bazel](https://bazel.build/install) version 4.7.1

For linux:
```bash
apt install bazel
```

For mac:
```bash
export BAZEL_VERSION=7.4.1

curl -fLO "https://github.com/bazelbuild/bazel/releases/download/$BAZEL_VERSION/bazel-$BAZEL_VERSION-installer-darwin-x86_64.sh"
chmod +x "bazel-$BAZEL_VERSION-installer-darwin-x86_64.sh"

./bazel-$BAZEL_VERSION-installer-darwin-x86_64.sh --user

# Add to ~/.bashrc, ~/.zshrc, or ~/.profile
export PATH="$PATH:$HOME/bin"
```

### [Geth]()

https://geth.ethereum.org/docs/getting-started/installing-geth

## Deployment

Create a jwt secret.

```bash
./prysm.sh beacon-chain generate-auth-secret
```

### Execution node (geth)

```bash
geth --hoodi --http --http.api eth,net,engine,admin --authrpc.jwtsecret ./jwt.hex
```

### Consensus node (geth)

```bash
bazel build //cmd/beacon-chain

bazel-bin/cmd/beacon-chain/beacon-chain_/beacon-chain --execution-endpoint=http://localhost:8551 --hoodi --jwt-secret=jwt.hex  --checkpoint-sync-url=https://hoodi.beaconstate.info --genesis-beacon-api-url=https://hoodi.beaconstate.info
```

> [!TIP]
> If you haven't executed the node for a long time, use `--clear-db` to avoid syncing from the last saved state.

## Profile

For memory profiling:

```bash
bazel build //cmd/beacon-chain --copt=-g --copt=-O0

bazel-bin/cmd/beacon-chain/beacon-chain_/beacon-chain \
  --execution-endpoint=http://localhost:8551 \
  --hoodi \
  --jwt-secret=jwt.hex \
  --checkpoint-sync-url=https://hoodi.beaconstate.info \
  --genesis-beacon-api-url=https://hoodi.beaconstate.info \
  --pprof \
  --pprofaddr=127.0.0.1 \
  --pprofport=6060 \
  --memprofilerate=1
```

On parallel, run:
```bash
go tool pprof http://localhost:6060/debug/pprof/heap

# To visualize in web
go tool pprof -http=:8080 http://localhost:6060/debug/pprof/heap
```
