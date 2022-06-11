FROM golang:1.18.3-bullseye
RUN apt update && apt install -y build-essential wget
WORKDIR /root
RUN wget https://github.com/apple/foundationdb/releases/download/6.3.24/foundationdb-clients_6.3.24-1_amd64.deb && dpkg -i foundationdb-clients_6.3.24-1_amd64.deb
COPY . /app
WORKDIR /app
RUN go build

FROM debian:bullseye-slim
COPY --from=0 /root/foundationdb-clients_6.3.24-1_amd64.deb /root/
RUN dpkg -i /root/foundationdb-clients_6.3.24-1_amd64.deb
COPY --from=0 /app/fdb2kafka /
ENTRYPOINT ["/fdb2kafka"]
