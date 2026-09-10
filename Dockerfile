# Required frontend: docker/dockerfile:1.12.0@sha256:db1ff77fb637a5955317c7a3a62540196396d565f3dd5742e76dddbb6d75c4c5
# // M0-PROVISIONAL: boring-cdc-d-compose
# // M0-PROVISIONAL: boring-cdc-d-compose
# linux/amd64 child sha256:c9ac3fa8945b61dede1e4500d25028aa8fd8a8fe46365fcf9c0422f8d999b9b0
FROM docker.io/library/rust:1.89.0-bookworm@sha256:948f9b08a66e7fe01b03a98ef1c7568292e07ec2e4fe90d88c07bb14563c84ff AS builder
WORKDIR /src
ENV RUSTUP_TOOLCHAIN=1.89.0-x86_64-unknown-linux-gnu
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY src ./src
RUN rustc --version | grep 'rustc 1.89.0' && cargo build --locked --release

# // M0-PROVISIONAL: boring-cdc-d-compose
# linux/amd64 child sha256:cea2634840f5a87503d8210e4df97b9f23a2acd67ff860a76c133d963032f866
FROM docker.io/library/debian:bookworm-20250811-slim@sha256:b1a741487078b369e78119849663d7f1a5341ef2768798f7b7406c4240f86aef
RUN useradd --system --uid 10001 --home /nonexistent --shell /usr/sbin/nologin boring-cdc \
    && install -d -m 0700 -o boring-cdc -g boring-cdc /var/lib/boring-cdc
COPY --from=builder --chown=boring-cdc:boring-cdc /src/target/release/boring-cdc /usr/local/bin/boring-cdc
USER 10001:10001
VOLUME ["/var/lib/boring-cdc"]
ENTRYPOINT ["boring-cdc"]
CMD ["serve"]
