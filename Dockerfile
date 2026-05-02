# syntax=docker/dockerfile:1.7
FROM rust:1.88-bookworm AS build
WORKDIR /src
# Cache deps layer
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
RUN mkdir -p src && echo 'fn main() {}' > src/main.rs && touch src/lib.rs && cargo build --release && rm -rf src
# Real build
COPY . .
RUN touch src/main.rs && cargo build --release && \
    strip target/release/withings-exporter

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /src/target/release/withings-exporter /usr/local/bin/withings-exporter
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/withings-exporter"]
CMD ["poll"]
