FROM golang:1.24.8-bookworm

WORKDIR /build

RUN git clone https://github.com/minio/minio.git .

RUN git checkout RELEASE.2025-10-15T17-29-55Z

RUN CGO_ENABLED=0 go build \
    -o /usr/local/bin/minio \
    -tags kqueue \
    .

FROM debian:bookworm-slim

COPY --from=0 /usr/local/bin/minio /usr/local/bin/minio

RUN mkdir -p /data

EXPOSE 9000 9001

ENTRYPOINT ["minio"]

CMD ["server", "/data", "--console-address", ":9001"]
