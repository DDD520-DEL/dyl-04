FROM golang:1.27-bookworm
WORKDIR /app
COPY . .
ENV GOPROXY=off GOSUMDB=off
RUN go build -mod=vendor ./...
CMD ["bash"]
