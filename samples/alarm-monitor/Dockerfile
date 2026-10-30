# Build from the repository root: docker build -f samples/alarm-monitor/Dockerfile .
FROM --platform=linux/amd64 rust:1.96-alpine@sha256:a41f7740f8b45d45795624eec13a8b42263cc700f19f7e4e86e04d3dda08a479 AS build
RUN apk add --no-cache musl-dev
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
# Every workspace member: cargo needs all of them to resolve the workspace.
COPY examples ./examples
COPY samples ./samples
RUN --mount=type=cache,id=superset-module-sdk-rs-cargo-registry,sharing=locked,target=/usr/local/cargo/registry \
    --mount=type=cache,id=superset-module-sdk-rs-target,sharing=locked,target=/src/target \
    cargo build --release --locked -p alarm-monitor && cp target/release/alarm-monitor /module

# A Superset runs the module as uid 10001 on a read-only file system.
FROM --platform=linux/amd64 alpine:3.23@sha256:ff7f0c727dd796aa36787bfcd9a64bf35a6ce72a73451b3359fde849b6703d63
COPY --from=build /module /module
USER 10001
EXPOSE 8080
ENTRYPOINT ["/module"]
