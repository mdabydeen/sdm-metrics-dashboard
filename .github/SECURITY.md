# Security Policy

## Supported Versions

| Version | Supported |
| ------- | --------- |
| 0.1.x   | Yes       |

## Reporting a Vulnerability

Please **do not** open a public GitHub issue for security vulnerabilities.

Instead, report them privately via [GitHub's security advisory feature](../../security/advisories/new)
or by emailing the maintainer directly.

Include:
- Description of the vulnerability
- Steps to reproduce
- Potential impact
- Any suggested fix (optional)

You can expect an acknowledgement within 48 hours and a resolution timeline within 7 days for critical issues.

## Sensitive Configuration

This project uses API tokens (JIRA, GitHub, ServiceNow) stored in `.env`.
- Never commit `.env` — it is in `.gitignore`
- Use `.env.example` as the template; it must contain only placeholder values
- Rotate any token that is accidentally committed immediately
