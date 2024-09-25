FROM golang:1.23 AS builder

ARG GITHUB_NETRC

WORKDIR /app

# github setup
RUN echo $GITHUB_NETRC > ~/.netrc

# Copy and build the app
COPY go.mod go.sum ./
RUN go mod download && go mod verify

COPY . .
RUN go build -o ./bin/app ./cmd/app/main.go

RUN rm -f ~/.netrc

# --------------
FROM alpine:latest

COPY --from=builder /app/bin/app ./app
RUN chmod +x ./app

CMD [ "./app" ]

