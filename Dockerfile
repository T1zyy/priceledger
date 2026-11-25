FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/pricing ./cmd/pricing

FROM build AS tests
CMD ["go","test","-count=1","./..."]

FROM alpine:3.22
COPY --from=build /out/ /
USER 65532:65532
CMD ["/pricing"]
