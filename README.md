# sing

![test](https://github.com/sagernet/sing/actions/workflows/test.yml/badge.svg)
![lint](https://github.com/sagernet/sing/actions/workflows/lint.yml/badge.svg)

Do you hear the people sing?

## slimproxy fork

The `slimproxy-udpnat-capacity` branch (base upstream `03a6cce64bf744023af9e2691fb11afd405b9061`)
adds a bounded legacy `common/udpnat` cache, nonblocking drop-on-full queues, and generation-safe
`DeleteIf`/`UpdateLifetime` helpers. Its paired consumer is
[`bgtendtofree/sing-shadowsocks`](https://github.com/bgtendtofree/sing-shadowsocks) on the same branch.
The branch CI workflow runs `go vet ./common/udpnat ./common/cache`, `go test -race -count=1 ./...`, and repeats the udpnat regressions.
Downstream modules must keep their own `replace` for this fork; a dependency's `replace` is not inherited.