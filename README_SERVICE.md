Service notes for MESH-MASTER

Enable and start the per-user systemd service (user session):

1. Reload user systemd units
   systemctl --user daemon-reload

2. Enable so it starts at login
   systemctl --user enable mesh-master.service

3. Start now
   systemctl --user start mesh-master.service

4. Check status and logs
   systemctl --user status mesh-master.service
   journalctl --user -u mesh-master.service -f

Notes
- The service runs the `start_mesh_master.sh` script which activates the project's `.venv` and launches `mesh-master.py`.
- If you want it to run even without a graphical login, enable user lingering:
   sudo loginctl enable-linger snailpi

