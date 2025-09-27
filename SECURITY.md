# Security Policy

## Supported Versions

Currently supported versions of Mesh Master:

| Version | Supported          |
| ------- | ------------------ |
| main    | :white_check_mark: |
| feat/*  | :white_check_mark: |

## Reporting a Vulnerability

If you discover a security vulnerability in Mesh Master, please report it responsibly:

### 🔒 Private Reporting
- **DO NOT** create a public GitHub issue for security vulnerabilities
- Send an email to the repository owner through GitHub
- Include details about the vulnerability and how to reproduce it

### 📝 What to Include
- Description of the vulnerability
- Steps to reproduce the issue  
- Potential impact assessment
- Suggested fix (if you have one)

### ⏰ Response Timeline
- Initial response: Within 48 hours
- Status update: Within 1 week
- Fix timeline: Depends on severity

### 🏆 Recognition
- Security researchers will be credited in the fix announcement
- We appreciate responsible disclosure practices

### 🛡️ Security Considerations
Mesh Master handles:
- Local network communications
- AI API endpoints  
- Configuration files with potential credentials
- Serial device access

Please be mindful of these areas when reporting vulnerabilities.

## Security Best Practices

When using Mesh Master:
- Keep dependencies updated
- Use strong passwords/PINs for Home Assistant integration
- Monitor access logs
- Run with appropriate user permissions
- Keep Meshtastic firmware updated