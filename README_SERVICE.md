Service notes for MESH-AI

Enable and start the per-user systemd service (user session):

1. Reload user systemd units
   systemctl --user daemon-reload

2. Enable so it starts at login
   systemctl --user enable mesh-ai.service

3. Start now
   systemctl --user start mesh-ai.service

4. Check status and logs
   systemctl --user status mesh-ai.service
   journalctl --user -u mesh-ai.service -f

Notes
- The service runs the `start_mesh_ai.sh` script which activates the project's `.venv` and launches `mesh-ai.py`.
- If you want it to run even without a graphical login, enable user lingering:
   sudo loginctl enable-linger snailpi

