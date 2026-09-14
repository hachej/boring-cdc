# Contributing

Read [AGENTS.md](AGENTS.md) and [docs/AGENT_SYSTEM.md](docs/AGENT_SYSTEM.md) before changing the repository. Work from one claimed Bead and preserve the single-binary, fixed-topology scope.

Run the component quality gate before proposing a change:

```sh
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace --all-targets
scripts/validate/m0_scaffold.sh
scripts/validate/scaffold_license.sh
scripts/validate/scaffold_secrets.sh
```

Do not commit credentials, generated evidence, local databases, or `.secrets/`. Security reports follow [SECURITY.md](SECURITY.md).
