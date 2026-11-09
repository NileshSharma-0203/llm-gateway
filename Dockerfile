FROM rust:1.97-slim-trixie AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked --bin llm-gateway

FROM debian:trixie-slim
RUN useradd --uid 10001 --no-create-home gateway && mkdir /data && chown gateway /data
COPY --from=build /src/target/release/llm-gateway /usr/local/bin/llm-gateway
USER 10001
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/llm-gateway"]
CMD ["--config", "/etc/llm-gateway/config.json"]
