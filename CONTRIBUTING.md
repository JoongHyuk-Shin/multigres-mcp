# Contributing

Bug reports and small fixes are welcome as issues and pull requests. For a new tool or a larger change, open an issue first so the approach can be agreed before you write it.

## Before you start

Build the server as described in [Build](README.md#build) and run the tests as described in [Tests](README.md#tests).

## Pull requests

A pull request should:

- be formatted with `gofmt`;
- pass `go vet ./...` and `go vet -tags cluster ./...`;
- pass the unit tests (`go test ./...`);
- pass the cluster tests as well when it changes SQL the server sends through the gateway;
- add a test for any new behaviour.

## Changes to the read-only boundary

A change that lets restricted mode accept more statements or functions (`internal/sqlguard`) must show why the addition changes neither data nor session state, and must come with a test for it. This is the part of the server where a mistake reaches other clients of the same gateway.

## Security issues

Report a vulnerability privately through the repository's Security tab ("Report a vulnerability") instead of opening a public issue.

## License

Unless you state otherwise, a contribution you submit is under the Apache License 2.0, as section 5 of the [LICENSE](LICENSE) states. Only submit work you have the right to license this way; if you wrote it as part of your job, that may need your employer's permission.
