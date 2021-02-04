FROM rust:1.49 as builder
WORKDIR /usr/src
RUN USER=root cargo new --bin herdmq
WORKDIR /usr/src/herdmq
COPY ./Cargo.toml ./Cargo.toml
RUN cargo build --release
RUN rm src/*.rs
ADD . ./
RUN rm ./target/release/deps/herdmq*
RUN cargo build --release

FROM debian:buster-slim
ARG APP=/usr/src/app
ENV APP_USER=appuser
RUN groupadd $APP_USER \
    && useradd -g $APP_USER $APP_USER \
    && mkdir -p ${APP}
COPY --from=builder /usr/src/herdmq/target/release/herdmq ${APP}/herdmq
RUN chown -R $APP_USER:$APP_USER ${APP}
USER $APP_USER
WORKDIR ${APP}
EXPOSE 1883
CMD ["./herdmq"]