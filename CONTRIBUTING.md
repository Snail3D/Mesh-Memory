# Contributing to Mesh Memory

Thank you for your interest in contributing to Mesh Memory! This project enhances MESH-AI with persistent memory, async processing, and enterprise-grade reliability.

## 🚀 Quick Start for Contributors

### Prerequisites
- Python 3.8+
- Meshtastic-compatible device (RAK4631, T-Beam, etc.)
- Git installed

### Development Setup
```bash
git clone https://github.com/Snail3D/Mesh-Memory.git
cd Mesh-Memory
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# or .venv\Scripts\activate  # Windows
pip install -r requirements.txt
```

## 🎯 Key Areas for Contribution

### 1. Memory Management
- Improve chat history persistence
- Optimize context window management
- Enhance memory cleanup algorithms

### 2. Async Processing
- Optimize message queue performance
- Add more sophisticated message prioritization
- Improve background worker efficiency

### 3. Reliability Features
- Enhance connection recovery logic
- Add more comprehensive error handling
- Improve single-instance enforcement

### 4. Performance Optimizations
- AI response speed improvements
- Memory usage optimizations
- Network communication efficiency

## 🐛 Bug Reports
- Use GitHub Issues with the "bug" label
- Include system info, logs, and reproduction steps
- Check existing issues first

## ✨ Feature Requests
- Use GitHub Issues with the "enhancement" label
- Explain the use case and expected behavior
- Consider backward compatibility

## 📝 Code Style
- Follow existing code patterns
- Add comprehensive error handling
- Include logging for debugging
- Update documentation for new features

## 🧪 Testing
- Test with actual Meshtastic hardware
- Verify memory persistence across restarts
- Test async processing under load
- Ensure single-instance enforcement works

## 📦 Pull Requests
1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 🏆 Recognition
Contributors will be recognized in the README and release notes!

## 📞 Questions?
- Open a GitHub Discussion
- Check existing documentation
- Review the FORK_CHANGES.md for technical details