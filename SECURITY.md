# Security policy

Boring CDC is pre-release research software. Do not use it to protect production data.

Report vulnerabilities privately through the GitHub repository's security-advisory interface. Do not open a public issue containing credentials, DSNs, source identifiers, payloads, filesystem paths, or raw driver errors.

The scaffold exposes no network listener. Future status endpoints remain loopback-only by default and mutating operations remain on a peer-validated Unix socket until an approved contract says otherwise.
