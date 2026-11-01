# Security policy

## Supported versions

Security fixes are released for the latest minor version of pgdispatch.

## Reporting a vulnerability

Please **do not open a public issue** for security problems. Report them
privately through
[GitHub Security Advisories](https://github.com/AmirSherov/pgdispatch/security/advisories/new)
or by email to sher4mir@gmail.com.

Please include:

- the affected version and configuration;
- a description of the issue and its impact;
- steps to reproduce it or a proof of concept.

You will get an acknowledgement within three working days. You will also get an
assessment and a plan for a fix, and credit in the release notes unless you
prefer to stay anonymous.

pgdispatch sends HTTP requests to URLs supplied by third parties, so reports
about SSRF bypasses, signature verification and secret handling are especially
welcome. See [docs/security.md](docs/security.md) for the protections that are
already in place.
