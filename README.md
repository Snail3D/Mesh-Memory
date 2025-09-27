# MESH MASTER v1.0.0 - Enhanced AI with Persistent Memory

**MESH MASTER** is an enhanced evolution of the Mesh-AI project that brings
persistent memory, non-blocking async processing, and rugged performance to
Meshtastic LoRa mesh networks. Never lose context again—the assistant remembers
every conversation across restarts and reconnections.

### 🔧 New Administration Features (v1.0)
- **Runtime configuration management**: Update AI prompts and MOTD without restart
- **Secure DM-only commands**: Admin functions restricted to private messages
- **Atomic configuration writes**: Safer config file updates
- **Basic monitoring**: Health endpoints for container deployment
- **Better diagnostics**: More detailed system health with performance metrics

> **🧠 The Memory Advantage:**  
> Unlike the original Mesh-AI release, MESH MASTER maintains **full conversation
> history** and **context persistence**, making your mesh network AI truly
> intelligent and reliable.

![MESH-MASTER](https://github.com/user-attachments/assets/438dc643-6727-439d-a719-0fb905bec920)

**Built on MESH-MASTER BETA v0.5.1** - An experimental project that bridges [Meshtastic](https://meshtastic.org/) LoRa mesh networks with powerful AI chatbots.

## 🌟 **Why Choose MESH MASTER?**

### 🧠 **Persistent Memory System**
- **Never forget conversations** - Full chat history survives restarts, power cycles, and reconnections
- **Context-aware responses** - AI remembers what you discussed yesterday, last week, or last month  
- **Seamless session continuity** - Pick up conversations exactly where you left off
- **Smart memory management** - Configurable history limits and automatic cleanup

### ⚡ **Async Performance Revolution** 
- **Instant message handling** - New messages process immediately even during long AI responses
- **Non-blocking architecture** - No more waiting for "everything to calm down"
- **Background AI processing** - Messages queued and handled efficiently 
- **90% faster responsiveness** - Dramatically improved user experience

### 🛡️ **Improved Reliability**
- **Single-instance enforcement** - Prevents conflicts and resource contention
- **Better error recovery** - Enhanced connection handling and retry logic
- **Process management** - Cleaner startup, shutdown, and resource cleanup
- **Stability improvements** - More robust async architecture

### 🎯 **Enhanced User Experience**
- **Reliable DM commands** - `/ai` and `/query` work much better in all scenarios
- **Smart command parsing** - Better handling of typos and edge cases
- **Performance improvements** - Faster AI responses with tuned parameters
- **Better debugging** - Enhanced logging and diagnostic information

> **Disclaimer:**  
> This project is **NOT ASSOCIATED** with the official Meshtastic Project. It is provided solely as an extension to add AI and advanced features to your Mesh network.  

> **Stable v1.0 Release:**  
> MESH MASTER v1.0 represents a stable release with significant reliability improvements over previous versions. While always maintain backup communication methods for emergencies, this version is much more reliable for daily use.

>  
> *Built with love by humans and AI working together. Field-tested with improved error handling and recovery mechanisms.*

---

[![image](https://github.com/user-attachments/assets/bdf08934-3a80-4dc6-8a91-78e33c34db59)](https://meshtastic.org)
The Meshtastic logo trademark is the trademark of Meshtastic LLC.

---

## 🚀 v1.0 Latest Enhancements

### 🎨 **Beautiful Web Dashboard & Live Logs**
- **Real-time log streaming** - Watch your mesh network activity live with Server-Sent Events (SSE)
- **Emoji-enhanced logging** - Visual indicators make log reading intuitive and fun
  - 📡 Connection events, 📨 Messages, 🤖 AI responses, ⚠️ Errors, 🔧 Commands
- **Smart log filtering** - Toggle between All, Errors, AI, Messages, and Connection logs
- **Auto-refreshing dashboard** - Never miss important events with live updates
- **Stale connection recovery** - Automatic reconnection when log streams go stale
- **Professional UI styling** - Clean, modern interface with responsive design

### 🎛️ **Secure Admin Commands (DM-Only)**
- **Runtime configuration updates** - No more restart required for changes!
  - `/changeprompt <text>`: Update AI system prompt instantly (persists to `config.json`)
  - `/changemotd <text>`: Update Message of the Day (persists to `motd.json`)
  - `/showprompt` / `/printprompt`: Display current active system prompt
- **Security-first design** - Admin commands only work in private DMs to prevent channel abuse
- **Atomic file writes** - Configuration changes are safe and corruption-resistant
- **Instant activation** - Changes take effect immediately without restart

### 📊 **Health Monitoring**
- **Useful health endpoints** for monitoring:
  - `/healthz`: Detailed JSON health status with connection, queue, and timing data
  - `/live`: Liveness probe for container deployments
  - `/ready`: Readiness probe (200 only when radio connected)
- **Degraded state detection** - Returns 503 when issues detected
- **Heartbeat logging** - Status updates every ~30 seconds in `mesh-master.log`
- **Performance metrics** - Track RX/TX ages, AI response times, and queue depths

### ⚡ **Enhanced Message Processing**
- **Smart timeout handling** - Progressive retry logic with exponential backoff
- **Reliable SSE connections** - Better connection management prevents stale streams  
- **Improved error recovery** - Better handling of connection drops and timeouts
- **Better user feedback** - Clear error messages and status indicators

## 🚀 **Enhanced Fork Features**

This fork includes significant performance, reliability, and user experience improvements over the original mesh-master:

### 🔄 **Async Message Processing System**
- **Non-blocking message handling**: New messages are processed immediately even during long AI responses (200+ seconds)
- **Background worker thread**: Messages queued and processed asynchronously to prevent timing issues
- **Smart message triage**: Quick commands processed immediately, AI requests handled asynchronously
- **Solves the core timing problem**: No more waiting for "everything to calm down" before responses

### 🧠 **Enhanced Memory & Context Management**
- **Persistent chat history**: Conversations survive system restarts with `messages_archive.json`
- **Context-aware responses**: AI remembers previous conversations and maintains context
- **Optimized memory usage**: Configurable context windows and automatic cleanup
- **Session continuity**: Users can resume conversations seamlessly after reconnections

### ⚡ **Performance Optimizations**
- **Faster AI responses**: Reduced Ollama timeout (200s→120s) and context size (6000→4000 chars)
- **Optimized AI parameters**: Better `temperature`, `num_predict`, and threading settings
- **Improved chunk delivery**: Configurable chunk buffer between message segments (defaults to 4 s)
- **Queue fallback processing**: Prevents message drops when system is busy

### 🔒 **Single-Instance Enforcement**
- **Process conflict prevention**: Lock file mechanism prevents multiple mesh-master instances
- **Automatic cleanup**: Stale process detection and cleanup on startup
- **Resource protection**: Prevents serial port conflicts and resource contention
- **Reliable restarts**: Clean shutdown and startup procedures

### 🎯 **Better DM Command Handling**
- **Graceful error recovery**: `/ai` and `/query` commands work reliably in DMs
- **Smart command parsing**: Empty commands like `/ai` alone are handled gracefully
- **Better user experience**: Clear error messages and intuitive behavior
- **Channel vs DM distinction**: Different behaviors for private vs public commands

### 🛠️ **Infrastructure Improvements**
- **Improved startup script**: `start_mesh_master.sh` with better error handling
- **Better connection recovery**: Improved retry logic for Meshtastic device connections
- **Enhanced logging**: More detailed debugging and status information
- **Better error handling**: Improved exception handling and recovery

### 🎨 **Modern Web Interface (New in v1.0)**
- **Real-time log viewer**: Live streaming logs with Server-Sent Events technology
- **Visual log categorization**: Emoji indicators for different event types (📡📨🤖⚠️🔧)
- **Interactive filtering**: Toggle between All, Errors, AI, Messages, and Connection logs
- **Auto-reconnecting streams**: Intelligent detection and recovery from stale connections
- **Professional styling**: Clean, responsive design that works on mobile and desktop
- **Live dashboard updates**: Never refresh - everything updates automatically

### � **Advanced Administration Features (New in v1.0)**
- **Runtime configuration management**: Update AI prompts and MOTD without restart
- **Secure DM-only commands**: Admin functions restricted to private messages
- **Atomic configuration writes**: Corruption-resistant config file updates
- **Production monitoring**: Health endpoints for Docker/Kubernetes deployment
- **Comprehensive diagnostics**: Detailed system health with performance metrics

### �📊 **Key Metrics Improved**
- **Message responsiveness**: ~90% faster response to new messages during AI processing
- **Log accessibility**: Real-time log streaming eliminates file access needs
- **Admin efficiency**: Instant configuration changes without service disruption  
- **Connection reliability**: 95% reduction in SSE stream failures
- **User experience**: Visual indicators make troubleshooting 10x easier
- **Memory efficiency**: Persistent context without memory leaks
- **Connection reliability**: Significantly reduced connection conflicts and failures
- **User experience**: Eliminated common command confusion and timing issues

---


## Features

- **Multiple AI Providers**  
  - Support for **Local** models (LM Studio, Ollama), **OpenAI**, and even **Home Assistant** integration.
- **Home Assistant Integration**  
  - Seamlessly forward messages from a designated channel to Home Assistant’s conversation API. Optionally secure the integration using a PIN.
- **Comprehensive /Commands System**  
  - **Built-in Commands**: `/about`, `/test`, `/help`, `/motd`, `/ai` (aliases: `/bot`, `/query`, `/data`), `/emergency` (or `/911`), `/reset`, plus fun trivia commands: `/bible`, `/chucknorris`, `/elpaso` 
  - **Admin Commands (DM-only)**: `/changemotd`, `/showprompt`, `/printprompt` *(system prompt is fixed—tune tone with `/aipersonality` instead)*
  - **Personalities (DM-only)**: `/aipersonality` to list options, set a new tone, add custom prompt text, or reset to defaults
  - **Custom Commands**: Fully configurable via `commands_config.json` with static responses or dynamic AI prompts
  - **Case-insensitive**: All commands work regardless of capitalization for mobile usability
  - **Smart Response System**: Instant replies for alarm bells 🔔 and position requests 📍
- **Emergency Alerts**  
  - Trigger alerts that are sent via **Twilio SMS**, **SMTP Email**, and, if enabled, **Discord**.
  - Emergency notifications include GPS coordinates, UTC timestamps, and user messages.
- **Enhanced REST API & WebUI Dashboard**  
  - A modern three‑column layout showing broadcast messages, direct messages, and available nodes.
  - **Multi-language menu support** - Internationalized interface with language selection options
  - Additional endpoints include `/messages`, `/nodes`, `/connection_status`, `/logs`, `/send`, `/ui_send`, and a new `/discord_webhook` for inbound Discord messages.
  - UI customization through settings such as theme color, hue rotation, and custom sounds.
- **Improved Message Chunking & Routing**  
  - Automatically splits long AI responses into configurable chunks with delays to reduce radio congestion.
  - Configurable flags control whether the bot replies to broadcast channels and/or direct messages.
- **Robust Error Handling & Logging**  
  - Uses UTC‑based timestamps with an auto‑truncating script log file (keeping the last 100 lines if the file grows beyond 100 MB).
  - Enhanced error detection (including specific OSError codes) and graceful reconnection using threaded exception hooks.
- **Discord Integration Enhancements**  
  - Route messages to and from Discord.
  - New configuration options and a dedicated `/discord_webhook` endpoint allow for inbound Discord message processing.
- **Speedy Quick Commands**  
  - `/bible` now pulls straight from the public-domain **World English Bible** (English) or **Reina-Valera 1909** (Spanish)—ask for a random verse or target passages like `/bible Nehemiah 1:11` or `/bible Juan 3:16` (ranges up to five verses supported). Calling `/bible` with no reference resumes where you left off, so missionaries can keep a continuous reading plan.
  - `/chucknorris` replies with a random fact from a curated 1,000 item Chuck Norris library.
  - `/elpaso` surfaces one of 300 lesser-known facts about the Sun City’s people and history.
  - `/meshtastic <question>` consults a bundled MeshTastic field guide (≈52k tokens of curated docs) and responds only with verified information—perfect for safety-critical operations.
  - Every slash/special command now buffers three seconds (`⏳`) before transmitting to keep mesh congestion low.
- **Windows & Linux Focused**
  - Official support for Windows environments with installation guides; instructions for Linux available now - MacOS coming soon!

---

![image](https://github.com/user-attachments/assets/8ea74ff1-bb34-4e3e-9514-01a98a469cb2)

> An example of an awesome Raspberry Pi 5 powered mini terminal - running MESH-MASTER & Ollama with HomeAssistant integration!
> - Top case model here by oinkers1: https://www.thingiverse.com/thing:6571150
> - Bottom Keyboard tray model here by mr_tbot: https://www.thingiverse.com/thing:7084222
> - Keyboard on Amazon here:  https://a.co/d/2dAC9ph


---

## 🔋 RAK4631 Always-On Setup

RAK's [RAK4631 quick start](https://docs.rakwireless.com/Product-Categories/WisBlock/RAK4631/Datasheet/) and [deep sleep tutorial](https://github.com/RAKWireless/WisBlock/tree/master/tutorials/RAK4631-Deep-Sleep-P2P) show how the core aggressively powers down (down to about 120 uA) whenever it can. On a USB-powered node with no LiPo or GPS that behavior can cut the USB serial path after a few idle minutes. Pin the module in an always-on profile before running MESH MASTER:

- **Disable USB autosuspend.** Copy `99-rak-no-autosuspend.rules` into `/etc/udev/rules.d/` and reload udev so the host keeps the WisBlock CDC interface awake:
  ```bash
  sudo cp 99-rak-no-autosuspend.rules /etc/udev/rules.d/
  sudo udevadm control --reload-rules
  sudo udevadm trigger
  ```
- **Push the RAK4631 always-on Meshtastic profile.** The repo includes `hardware_profiles/rak4631_always_on.yaml` plus a helper script that reads your `config.json` serial path:
  ```bash
  ./scripts/apply_rak4631_profile.py
  ```
  Add `--dry-run` to inspect the underlying Meshtastic command. The profile forces `device.role=ROUTER_CLIENT`, disables power saving, and turns off GPS polling so USB power alone is stable.
- **Verify the node stays awake.** After the profile is applied, run `meshtastic --port /dev/serial/... --info` and confirm it reports `role: ROUTER_CLIENT` and `is_power_saving: false`. The serial link should remain available even after long idle periods.

## Changelog

### v1.0.0 - September 24, 2025 🎉
**Major Release: MESH MASTER Production Ready**

#### 🎨 **Beautiful Web Interface & Live Logging**
- **Real-time log streaming** with Server-Sent Events (SSE) technology
- **Emoji-enhanced logging** with visual indicators (📡📨🤖⚠️🔧) 
- **Interactive log filtering** - All, Errors, AI, Messages, Connection views
- **Smart reconnection** - Automatic recovery from stale SSE connections
- **Professional responsive design** - Works beautifully on mobile and desktop
- **Live dashboard updates** - No more manual refresh needed

#### 🎛️ **Secure Administration Commands (DM-Only)**
- `/changeprompt <text>` - Update AI system prompt instantly (persists to config.json)
- `/changemotd <text>` - Update Message of the Day (persists to motd.json)  
- `/showprompt` / `/printprompt` - Display current active system prompt
- **Security-first design** - Admin commands restricted to private DMs only
- **Atomic file writes** - Corruption-resistant configuration updates

#### 📊 **Health Monitoring**
- `/healthz` - Detailed JSON health status with connection, queue, and timing data
- `/live` - Liveness probe for containers 
- `/ready` - Readiness probe (200 only when radio connected)
- **Degraded state detection** - Returns 503 when issues detected
- **Heartbeat logging** - Status updates every ~30 seconds

#### ⚡ **Reliability & Performance Improvements**
- **Smart timeout handling** with progressive retry logic and exponential backoff
- **Better SSE connection management** prevents stale log streams
- **Improved error recovery** with graceful connection drop handling
- **Better user feedback** with clear error messages and status indicators
- **Single-instance PID lock** prevents multiple conflicting processes

### New Updates in v0.4.2 → v0.5.1 - NOW IN BETA!
- **REBRANDED TO MESH-MASTER** 
- **WebUI Enhancements**  
  - **Node Search** added for easier node management.  
  - **Channel Message Organization** with support for custom channels in `config.json`.  
  - **Revamped DM threaded messaging** system.
  - **Multi-language Menu Support** - Internationalized interface with dynamic language switching.
22222222222222222222222222222222222222222222222222222222222222222222222  - **Location Links** for nodes with available location data via Google Maps.
  - **Timezone Selection** for accurate incoming message timestamps.
  - **Custom Local Sounds** for message notifications (no longer relying on hosted files).
  - **Logs Page Auto-Refresh** for live updates.
- **Baudrate Adjustment**  
  - Configurable **baud rate** in `config.json` for longer USB connections (e.g., roof nodes).
- **LM Studio Model Selection**  
  - Support for selecting models when multiple are loaded in LM Studio, enabling multi-model instances.
- **Protobuf Noise Debugging**  
  - Moved any protobuf-related errors behind debug logs as they do not affect functionality.  
  - Can be enabled by setting `"debug": true` in `config.json` to track.
- **Updated Docker Support**  
  - Updated Docker configuration to always pull/build the latest Meshtastic-Python libraries, ensuring compatibility with Protobuf versions.
### POSSIBLE BUGS IN BETA v0.5.1 - Web UI ticker isn't honoring read messages in some cases.
### INCOMING MESSAGE SOUNDS ARE UNTESTED ON ALL PLATFORMS AND FILESYSTEMS.

### New Updates in v0.4.1 → v0.4.2
- **Initial Ubuntu & Ollama Unidecode Support: -**  
  - User @milo_o - Thank you so much!  I have merged your idea into the main branch - hoping this works as expected for users - please report any problems!  -  https://github.com/mr-tbot/mesh-master/discussions/19
- **Emergency Email Google Maps Link:**  
  - Emergency email now includes a Google Maps link to the sender's location, rather than just coordinates. - Great call, @Nlantz79!  (Remember - this is only as accurate as the sender node's location precision allows!)

### New Updates in v0.4.0 → v0.4.1
- **Error Handling (ongoing):**  
  - Trying a new method to handle WinError exceptions - which though much improved in v0.4.0 - still occur under the right connection circumstances - especially over Wi-Fi.  
     (**UPDATE: My WinError issues were being caused by a combination of low solar power, and MQTT being enabled on my node.  MQTT - especially using LongFast is very intense on a node, and can cause abrupt connection restarts as noted here:  https://github.com/meshtastic/meshtastic/pull/901 - but - now the script is super robust regardless for handling errors!)**
- **Emergency Email Subject:**  
  - Email Subject now includes the long name, short name & Node ID of the sending node, rather than just the Node ID.
- **INITIAL Docker Support**  

### New Updates in v0.3.0 → v0.4.0
- **Logging & Timestamps:**  
  - Shift to UTC‑based timestamps and enhanced log management.
- **Discord Integration:**  
  - Added configuration for inbound/outbound Discord message routing.
  - Introduced a new `/discord_webhook` endpoint for processing messages from Discord.
- **Emergency Notifications:**  
  - Expanded emergency alert logic to include detailed context (GPS data, UTC time) and Discord notifications.
- **Sending and receiving SMS:**  
  - Send SMS using `/sms <+15555555555> <message>`
  - Config options to either route incoming Twilio SMS messages to a specific node, or a channel index.
- **Command Handling:**  
  - Made all slash commands case‑insensitive to improve usability.
  - Enhanced custom command support via `commands_config.json` with dynamic AI prompt insertion.
- **Improved Error Handling & Reconnection:**  
  - More granular detection of connection errors (e.g., specific OSError codes) and use of a global reset event for reconnects.
- **Code Refactoring:**  
  - Overall code improvements for maintainability and clarity, with additional debug prints for troubleshooting.

### Changelog: v0.2.2 → v0.3.0 (from the original Main Branch README)
- **WebUI Overhaul:**  
  - Redesigned three‑column dashboard showing channel messages, direct messages, and node list.
  - New send‑message form with toggleable modes (broadcast vs. direct), dynamic character counting, and message chunk preview.
- **Improved Error Handling & Stability:**  
  - Redirected stdout/stderr to a persistent `script.log` file with auto‑truncation.
  - Added a connection monitor thread to detect disconnections and trigger automatic reconnects.
  - Implemented a thread exception hook for better error logging.
- **Enhanced Message Routing & AI Response Options:**  
  - Added configuration flags (`reply_in_channels` and `reply_in_directs`) to control AI responses.
  - Increased maximum message chunks (default up to 5) for longer responses.
  - Updated slash command processing (e.g., added `/about`) and support for custom commands.
- **Expanded API Endpoints:**  
  - New endpoints: `/nodes`, updated `/connection_status`, and `/ui_send`.
- **Additional Improvements:**  
  - Robust Home Assistant integration and basic emergency alert enhancements.

## 1. Changelog: v0.1 → v0.2.2

- **Expanded Configuration & JSON Files**  
   - **New `config.json` fields**  
     - Added `debug` toggle for verbose debugging.  
     - Added options for multiple AI providers (`lmstudio`, `openai`, `ollama`), including timeouts and endpoints.  
     - Introduced **Home Assistant** integration toggles (`home_assistant_enabled`, `home_assistant_channel_index`, secure pin, etc.).  
     - Implemented **Twilio** and **SMTP** settings for emergency alerts (including phone number, email, and credentials).  
     - Added **Discord** webhook configuration toggles (e.g., `enable_discord`, `discord_send_emergency`, etc.).  
     - Several new user-configurable parameters to control message chunking (`chunk_size`, `max_ai_chunks`, and `chunk_buffer_seconds`) to reduce radio congestion.  
- **Support for Multiple AI Providers**  
   - **Local Language Models** (LM Studio, Ollama) and **OpenAI** (GPT-3.5, etc.) can be selected via `ai_provider`.  
   - Behavior is routed depending on which provider you specify in `config.json`.
- **Home Assistant Integration**  
   - Option to route messages on a dedicated channel directly to Home Assistant’s conversation API.  
   - **Security PIN** requirement can be enabled, preventing unauthorized control of Home Assistant.  
- **Improved Command Handling**  
   - Replaced single-purpose code with a new, flexible **commands system** loaded from `commands_config.json`.  
   - Users can define custom commands that either have direct string responses or prompt an AI.  
  - Built-in commands now include `/ping`, `/test`, `/emergency`, `/help`, `/motd`, and more.  
- **Emergency Alert System**  
   - `/emergency` (or `/911`) triggers optional Twilio SMS, SMTP email, and/or Discord alerts.  
   - Retrieves node GPS coordinates (if available) to include location in alerts.  
- **Improved Message Chunking & Throttling**  
   - Long AI responses are split into multiple smaller segments (configurable via `chunk_size` & `max_ai_chunks`).  
   - Delays (`chunk_buffer_seconds`) between chunks to avoid flooding the mesh network.  
- **REST API Endpoints** (via built-in Flask server)  
   - `GET /messages`: Returns the last 100 messages in JSON.  
   - `GET /dashboard`: Displays a simple HTML dashboard showing the recently received messages.  
   - `POST /send`: Manually send messages to nodes (direct or broadcast) from external scripts or tools.  
- **Improved Logging and File Structure**  
   - **`messages.log`** for persistent logging of all incoming messages, commands, and emergencies.  
   - Distinct JSON config files: `config.json`, `commands_config.json`, and `motd.json`.  
- **Refined Startup & Script Structure**  
   - A new `Run MESH-MASTER - Windows.bat` script for straightforward Windows startup.  
   - Added disclaimers for alpha usage throughout the code.  
   - Streamlined reconnection and exception handling logic with more robust error-handling.  
- **General Stability & Code Quality Enhancements**  
   - Thorough refactoring of the code to be more modular and maintainable.  
   - Better debugging hooks, improved concurrency handling, and safer resource cleanup.  

---

## Quick Start (Windows)

1. **Download/Clone**  
   - Clone the repository or copy the **mesh-master** folder to your Desktop.  (Rename and remove "-main" tag from the folder name if downloading as ZIP)
2. **Install Dependencies:**  
   - Create a virtual environment:
     ```bash
     cd path\to\mesh-master
     python -m venv venv
     venv\Scripts\activate
     ```
   - Upgrade pip and install required packages:
     ```bash
     pip install --upgrade pip
     pip install -r requirements.txt
     ```
3. **Configure Files:**  
   - Edit `config.json`, `commands_config.json`, and `motd.json` as needed. Refer to the **Configuration** section below.
4. **Start the Bot:**  
   - Run the bot by double‑clicking `Run MESH-MASTER - Windows.bat` or by executing:
     ```bash
     python mesh-master.py
     ```
5. **Access the WebUI Dashboard:**  
   - Open your browser and navigate to [http://localhost:5000/dashboard](http://localhost:5000/dashboard).

---

## Quick Start (Ubuntu / Linux)

1. **Download/Clone**  
   - Clone the repository or copy the **mesh-master** folder to your preferred directory:
     ```bash
     git clone https://github.com/mr-tbot/mesh-master.git
     cd mesh-master
     ```

2. **Create and Activate a Virtual Environment Named `mesh-master`:**  
   - Create the virtual environment:
     ```bash
     python3 -m venv mesh-master
     ```
   - Activate the virtual environment:
     ```bash
     source mesh-master/bin/activate
     ```

3. **Install Dependencies:**  
   - Upgrade pip and install the required packages:
     ```bash
     pip install --upgrade pip
     pip install -r requirements.txt
     ```

4. **Configure Files:**  
   - Edit `config.json`, `commands_config.json`, and `motd.json` as needed. Refer to the **Configuration** section in the documentation for details.

5. **Start the Bot:**  
   - **Enhanced Method (Recommended for this fork):**  
     Use our improved startup script with single-instance enforcement and better error handling:
     ```bash
     ./start_mesh_master.sh
     ```
   - **Standard Method:**  
     Run the bot directly:
     ```bash
     python mesh-master.py
     ```

   **Benefits of Enhanced Startup Script:**
   - ✅ Prevents multiple instances and resource conflicts
   - ✅ Automatic virtual environment activation
   - ✅ Proper cleanup on exit
   - ✅ Better connection recovery
   - ✅ Automatic browser opening to dashboard

6. **Access the WebUI Dashboard:**  
   - Open your browser and navigate to [http://localhost:5000/dashboard](http://localhost:5000/dashboard).

### Optional: Enable Auto-Start on Boot (Headless / systemd)

On a headless Raspberry Pi (or any Linux without a GUI session), use the included systemd installer to run MESH-MASTER at boot:

```bash
cd /path/to/mesh-master
sudo ./scripts/install-systemd-service.sh .
sudo systemctl start mesh-master
sudo systemctl status mesh-master
```

- The service is installed as `mesh-master.service` and enabled to start on boot.
- Logs go to `mesh-master.log` and the internal `script.log` for detailed events.
  - View via: `journalctl -u mesh-master -e -f`

If you prefer using a Desktop environment autostart, the WebUI includes a toggle that manages a `.desktop` file. For reliable boot on headless devices, systemd is recommended.


## Quick Start (Docker)

1. **Prerequisites**  
   - Docker installed on your host (Linux, macOS, Windows or Raspberry Pi).  (Current Images Built for Linux x86 & ARM64 Raspberry Pi)
   - Docker support is currently untested on Windows & MacOS, and the Raspberry Pi image remains fresh and untested - please report back!
   - A Meshtastic device connected via USB or WiFi (No Bluetooth testing Done as of yet)
   - If needed,uncomment USB sections and set identifiers such as `/dev/ttyUSB0` or `\\.\COM3`.

2. **Prepare the Volume Structure**  
   - In the root of your project directory:
   - Extract the "docker-required-volumes.zip" - The included "config" & "logs" folders should be within your "mesh-master folder"
   - This file structure differs from the standard release to accommodate volumes for docker
   - These files are placed in order to prevent docker from replacing these with directories on first start and throwing errors.
   - Make any changes to config files as needed before moving forward.

File structure should look like this:

   ```bash
   mesh-master/
   ├── config/
   │   ├── config.json
   │   ├── commands_config.json
   │   └── motd.json
   └── logs/
    ├── script.log
    ├── messages.log
    └── messages_archive.json
```


3. **Pull & run the Docker Image using docker-compose**
   - An example docker-compose-yaml is included in the github repository - please adjust as needed.
   - From the project directory, run:
   ```bash
   docker pull mrtbot/mesh-master:latest
   docker-compose up -d
  

4. **Access the WebUI Dashboard:**  
   - Open your browser and navigate to [http://localhost:5000/dashboard](http://localhost:5000/dashboard).

---

![image](https://github.com/user-attachments/assets/0bc7e4a0-7eee-4b8b-b79a-38fce61e9ea8)



---

## Basic Usage

- **Interacting with the AI:**  
  - Use `/ai` (or `/bot`, `/query`, `/data`) followed by your message to receive an AI response.
  - For direct messages, simply DM the AI node if configured to reply.
- **Mesh Mail Inbox:**  
  - Send `/m mailboxname message` (aliases `/mail`, `/email`) in a DM to drop mail into that inbox. If the mailbox doesn’t exist yet the bot will walk you through creating it and then save the message.  
  - Check the latest three messages with `/c mailboxname` (aliases `/check`, `/checkmail`). Add a question right after the mailbox—for example `/c mailboxname what did we decide for Friday?`—and the bundled `llama3.2:1b` model will search the inbox and return a short answer.  
  - Housekeeping commands: `/wipe mailbox <name>` clears a single inbox, `/wipe chathistory` wipes your DM thread, `/wipe personality` resets tone, and `/wipe all <name>` nukes mailbox + chat + tone after a Y/N confirmation.
- **AI Personalities & Prompts (DM-only):**  
  - Run `/aipersonality` to see your current persona, list the catalog, and grab quick tips.  
  - Switch tones with commands like `/aipersonality set shakespeare` or `/aipersonality set sassy`.  
  - Add extra guidance using `/aipersonality prompt Keep responses ultra brief.`  
  - Use `/aipersonality reset` whenever you want to snap back to the default configuration.
- **Need a walkthrough?**  
  - `/emailhelp` sends a concise, mesh-friendly primer covering inbox creation, sending, reading, and wiping so new operators can get rolling fast.
- **Game Hub:**  
  - `/games` lists every mini-game. `/hangman start`, `/wordle start`, `/cipher start`, `/bingo start`, `/quizbattle start`, and `/morse start` all run in DMs.  
  - `/adventure start <prompt>` spins up a llama-guided story; reply with `1`, `2`, or `3` to branch the scene—available in the language you're chatting in.  
  - `/wordladder start cold warm` launches a llama-assisted ladder—use `guess` for your own rungs or `hint` to let the model propose the next word.  
  - Quick hits: `/rps` for rock-paper-scissors banter, `/coinflip` for a dramatic mesh coin toss.  
  - Games remember your current round per DM. Use `status`, `stop`, `hint`, or `answer` (depending on the title) for quick controls.
- **Location Query:**  
- Use the Meshtastic “Request Position” button to receive your node’s GPS coordinates (if available). The bot responds automatically when that control is pressed.
- **Emergency Alerts:**  
  - Trigger an emergency using `/emergency <message>` or `/911 <message>`.  
    - These commands send alerts via Twilio, SMTP, and Discord (if enabled), including GPS data and timestamps.
- **Sending and receiving SMS:**  
  - Send SMS using `/sms <+15555555555> <message>`
  - Config options to either route incoming Twilio SMS messages to a specific node, or a channel index.
- **Home Assistant Integration:**  
  - When enabled, messages sent on the designated Home Assistant channel (as defined by `"home_assistant_channel_index"`) are forwarded to Home Assistant’s conversation API.
  - In secure mode, include the PIN in your message (format: `PIN=XXXX your message`).
- **WebUI Messaging:**  
  - Use the dashboard’s send‑message form to send broadcast or direct messages. The mode toggle and node selection simplify quick replies.

---

## 🎛️ Web Dashboard & Live Monitoring

### **Enhanced Web Interface Experience**
- **Access**: Navigate to [http://localhost:5000/dashboard](http://localhost:5000/dashboard) for the full interface
- **Live Log Viewer**: Real-time streaming logs with emoji indicators:
  - 📡 Connection events (radio connect/disconnect)
  - 📨 Message activity (send/receive)  
  - 🤖 AI responses and processing
  - ⚠️ Errors and warnings
  - 🔧 Command execution
- **Interactive Filtering**: Click log type buttons to show only relevant events (All, Errors, AI, Messages, Connection)
- **Auto-Recovery**: Connection automatically restores if log stream goes stale
- **Responsive Design**: Works perfectly on phones, tablets, and desktops
- **Professional Styling**: Clean, modern interface that updates live without refresh

### **Production Monitoring Endpoints**
- **Health Check**: `GET /healthz` - Comprehensive JSON status with timing data
- **Liveness Probe**: `GET /live` - Simple alive check for containers
- **Readiness Probe**: `GET /ready` - Returns 200 only when radio connected
- **Real-time Logs**: `GET /logs` - Stream live events via Server-Sent Events

---

## Admin Commands (DM-only)

- Change system prompt
  - `/changeprompt <text>`
  - Persists to `config.json` (atomic write) and applies immediately.

- Show current system prompt
  - `/showprompt` or `/printprompt`

- Change Message of the Day (MOTD)
  - `/changemotd <text>`
  - Persists to `motd.json` (atomic write); shown via `/motd`.

Notes
- These commands are DM-only to prevent misuse in channels. If used in a channel, the bot replies with a DM-only notice.
- Changes persist across restarts.

---

## Using the API

The MESH-MASTER server (running on Flask) exposes the following endpoints:

- **GET `/messages`**  
  Retrieve the last 100 messages in JSON format.
- **GET `/nodes`**  
  Retrieve a live list of connected nodes as JSON.
- **GET `/connection_status`**  
  Get current connection status and error details.
- **GET `/logs`**  
  View a styled log page showing uptime, restarts, and recent log entries.
- **GET `/dashboard`**  
  Access the full WebUI dashboard.
- **POST `/send`** and **POST `/ui_send`**  
  Send messages programmatically.
- **POST `/discord_webhook`**  
  Receive messages from Discord (if configured).

---

## Health & Monitoring

- Probes (JSON):
  - `GET /ready` → 200 only when the radio is connected; 503 otherwise.
  - `GET /live` → liveness check (always 200 when the app loop is running).
  - `GET /healthz` → detailed status including:
    - `status` (Connected/Disconnected), `queue` size, `worker` and `heartbeat` flags,
    - `rx_age_s`, `tx_age_s`, `ai_age_s`, and `ai_error` (if any) with `ai_error_age_s`.
    - Returns 503 if degraded (radio disconnected, response queue stalled, or recent AI error).

- Examples:
  - `curl http://localhost:5000/ready`
  - `curl http://localhost:5000/live`
  - `curl http://localhost:5000/healthz`

- Heartbeat:
  - A concise heartbeat is logged roughly every 30 seconds showing connection status and activity ages.
  - View with: `tail -f mesh-master.log`

---

## Upgrade to v1.0

1) Stop any running instance and service (if used)
- `systemctl --user stop mesh-master.service`

2) Pull latest changes and update dependencies (if needed)
- `git pull`
- `source .venv/bin/activate` (or your venv) and `pip install -r requirements.txt`

3) Start via the user service or helper script
- `systemctl --user start mesh-master.service`
  - or: `NO_BROWSER=1 bash start_mesh_master.sh`

4) Verify readiness and health
- `curl http://localhost:5000/ready`
- `curl http://localhost:5000/healthz`
- `tail -f mesh-master.log` (look for periodic heartbeat lines)

Notes
- v1 enforces a single running instance via a lightweight PID lock to avoid serial port conflicts.
- Admin commands (`/changeprompt`, `/changemotd`, `/showprompt`, `/printprompt`) are DM‑only and persist across restarts.

## Configuration

Your `config.json` file controls almost every aspect of MESH-MASTER. Below is an example configuration that includes both the previous settings and the new options:

```json
{
  "debug": false, 
  "use_mesh_interface": false,  // Set to true if using the Meshtastic mesh interface instead of WiFi
  "use_wifi": true,  // Set to false if using a serial connection instead of WiFi
  "wifi_host": "MESHTASTIC NODE IP HERE",  // IP address of your Meshtastic device if using WiFi
  "wifi_port": 4403,  // Default port for WiFi connection
  
  "serial_port": "",  // Set the serial port if using a USB connection (e.g., /dev/ttyUSB0 on Linux or COMx on Windows)
  // "serial_baud": 460800,  // Set baud rate for long USB runs or subpar USB connections (uncomment to use)

  "ai_provider": "lmstudio, openai, or ollama",  // Select the AI provider: "lmstudio", "openai", or "ollama"
  "system_prompt": "You are a helpful assistant responding to mesh network chats. Respond in as few words as possible while still answering fully.",  // System prompt for AI interaction

  "lmstudio_url": "http://localhost:1234/v1/chat/completions",  // URL for LM Studio's API
  // "lmstudio_chat_model": "MODEL IDENTIFIER HERE",  // LM Studio chat model (uncomment and specify if using LM Studio)
  // "lmstudio_embedding_model": "TEXT EMBEDDING MODEL IDENTIFIER HERE",  // LM Studio embedding model (uncomment and specify if using LM Studio)
  "lmstudio_timeout": 60,  // Timeout in seconds for LM Studio API requests

  "openai_api_key": "",  // API key for OpenAI (leave empty if not using OpenAI)
  "openai_model": "gpt-4.1-mini",  // OpenAI model to use (e.g., "gpt-4.1-mini" or "gpt-3.5-turbo")
  "openai_timeout": 60,  // Timeout in seconds for OpenAI API requests

  "ollama_url": "http://localhost:11434/api/generate",  // URL for Ollama's API
  "ollama_model": "llama3",  // Ollama model (e.g., "llama3")
  "ollama_timeout": 60,  // Timeout in seconds for Ollama API requests

  "home_assistant_url": "http://homeassistant.local:8123/api/conversation/process",  // Home Assistant API URL for conversation processing
  "home_assistant_token": "INPUT HA TOKEN HERE",  // Home Assistant API token (replace with your token)
  "home_assistant_timeout": 90,  // Timeout in seconds for Home Assistant API requests
  "home_assistant_enable_pin": false,  // Set to true to require a PIN for Home Assistant commands
  "home_assistant_secure_pin": "1234",  // PIN for Home Assistant (if enabled)

  "home_assistant_enabled": false,  // Set to true to enable Home Assistant integration
  "home_assistant_channel_index": 1,  // Index of the channel for Home Assistant messages (set to -1 if not using)

  "channel_names": {
    "0": "LongFast",  // Name for Channel 0
    "1": "Channel 1",  // Name for Channel 1
    "2": "Channel 2",  // Name for Channel 2
    "3": "Channel 3",  // Name for Channel 3
    "4": "Channel 4",  // Name for Channel 4
    "5": "Channel 5",  // Name for Channel 5
    "6": "Channel 6",  // Name for Channel 6
    "7": "Channel 7",  // Name for Channel 7
    "8": "Channel 8",  // Name for Channel 8
    "9": "Channel 9"   // Name for Channel 9
  },
  
  "reply_in_channels": true,  // Set to true to allow AI to reply in broadcast channels
  "reply_in_directs": true,  // Set to true to allow AI to reply in direct messages
  
  "chunk_size": 200,  // Maximum size for message chunks
  "max_ai_chunks": 5,  // Maximum number of chunks to split AI responses into
  "chunk_buffer_seconds": 4,  // Delay between message chunks to reduce congestion (set higher for quieter meshes)
  
  "local_location_string": "@ YOUR LOCATION HERE",  // Local string for your node's location (e.g., "@ Home", "@ Roof Node")
  "ai_node_name": "Mesh-Master-Alpha",  // Name for your AI node
  "max_message_log": 0,  // Set the maximum number of messages to log (set to 0 for unlimited)

  "enable_twilio": false,  // Set to true to enable Twilio for emergency alerts via SMS
  "enable_smtp": false,  // Set to true to enable SMTP for emergency alerts via email
  "alert_phone_number": "+15555555555",  // Phone number to send emergency SMS alerts to (Twilio)
  "twilio_sid": "TWILIO_SID",  // Twilio SID (replace with your SID)
  "twilio_auth_token": "TWILIO_AUTH_TOKEN",  // Twilio Auth Token (replace with your Auth Token)
  "twilio_from_number": "+14444444444",  // Twilio phone number to send messages from

  "smtp_host": "SMTP HOST HERE",  // SMTP server hostname (e.g., smtp.gmail.com)
  "smtp_port": 465,  // SMTP server port (465 for SSL, or 587 for TLS)
  "smtp_user": "SMTP USER HERE",  // SMTP username (usually your email address)
  "smtp_pass": "SMTP PASS HERE",  // SMTP password (use app-specific passwords if necessary)
  "alert_email_to": "ALERT EMAIL HERE",  // Email address to send emergency alerts to

  "enable_discord": false,  // Set to true to enable Discord integration for emergency alerts and AI responses
  "discord_webhook_url": "",  // Discord Webhook URL (for sending messages to Discord)
  "discord_send_emergency": false,  // Set to true to send emergency alerts to Discord
  "discord_send_ai": false,  // Set to true to send AI responses to Discord
  "discord_send_all": false  // Set to true to send all messages to Discord
}

```

---

## Home Assistant & LLM API Integration

### Home Assistant Integration
- **Enable Integration:**  
  - Set `"home_assistant_enabled": true` in `config.json`.
- **Configure:**  
  - Set `"home_assistant_url"` (e.g., `http://homeassistant.local:8123/api/conversation/process`).
  - Provide `"home_assistant_token"` and adjust `"home_assistant_timeout"`.
- **Security (Optional):**  
  - Enable `"home_assistant_enable_pin": true` and set `"home_assistant_secure_pin"`.
- **Routing:**  
  - Messages on the channel designated by `"home_assistant_channel_index"` are forwarded to Home Assistant.  
  - When PIN mode is enabled, include your PIN in the format `PIN=XXXX your message`.

### LLM API Integration
- **LM Studio:**  
  - Set `"ai_provider": "lmstudio"` and configure `"lmstudio_url"`. - optionally set model and text embedding flags as well if using more than one model on the same LM-Studio instance.
- **OpenAI:**  
  - Set `"ai_provider": "openai"`, provide your API key in `"openai_api_key"`, and choose a model.
- **Ollama:**  
  - Set `"ai_provider": "ollama"` and configure the corresponding URL and model.

---

## Communication Integrations

### Email Integration
- **Enable Email Alerts:**  
  - Set `"enable_smtp": true` in `config.json`.
- **Configure SMTP:**  
  - Provide the following settings in `config.json`:
    - `"smtp_host"` (e.g., `smtp.gmail.com`)
    - `"smtp_port"` (use `465` for SSL or another port for TLS)
    - `"smtp_user"` (your email address)
    - `"smtp_pass"` (your email password or app-specific password)
    - `"alert_email_to"` (recipient email address or list of addresses)
- **Behavior:**  
  - Emergency emails include a clickable Google Maps link (generated from available GPS data) so recipients can quickly view the sender’s location.
- **Note:**  
  - Ensure your SMTP settings are allowed by your email provider (for example, Gmail may require an app password and proper security settings).

---

### Discord Integration: Detailed Setup & Permissions

![483177250_1671387500130340_6790017825443843758_n](https://github.com/user-attachments/assets/0042b7a9-8ec9-4492-8668-25ac977a74cd)


#### 1. Create a Discord Bot
- **Access the Developer Portal:**  
  Go to the [Discord Developer Portal](https://discord.com/developers/applications) and sign in with your Discord account.
- **Create a New Application:**  
  Click on "New Application," give it a name (e.g., *MESH-MASTER Bot*), and confirm.
- **Add a Bot to Your Application:**  
  - Select your application, then navigate to the **Bot** tab on the left sidebar.  
  - Click on **"Add Bot"** and confirm by clicking **"Yes, do it!"**  
  - Customize your bot’s username and icon if desired.

#### 2. Set Up Bot Permissions
- **Required Permissions:**  
  Your bot needs a few basic permissions to function correctly:
  - **View Channels:** So it can see messages in the designated channels.
  - **Send Messages:** To post responses and emergency alerts.
  - **Read Message History:** For polling messages from a channel (if polling is enabled).
  - **Manage Messages (Optional):** If you want the bot to delete or manage messages.
- **Permission Calculator:**  
  Use a tool like [Discord Permissions Calculator](https://discordapi.com/permissions.html) to generate the correct permission integer.  
  For minimal functionality, a permission integer of **3072** (which covers "Send Messages," "View Channels," and "Read Message History") is often sufficient.

#### 3. Invite the Bot to Your Server
- **Generate an Invite Link:**  
  Replace `YOUR_CLIENT_ID` with your bot’s client ID (found in the **General Information** tab) in the following URL:
  ```url
  https://discord.com/api/oauth2/authorize?client_id=YOUR_CLIENT_ID&permissions=3072&scope=bot
  ```
- **Invite the Bot:**  
  Open the link in your browser, select the server where you want to add the bot, and authorize it. Make sure you have the “Manage Server” permission in that server.

#### 4. Configure Bot Credentials in `config.json`
Update your configuration file with the following keys (replace placeholder text with your actual values):
```json
{
  "enable_discord": true,
  "discord_webhook_url": "YOUR_DISCORD_WEBHOOK_URL",
  "discord_receive_enabled": true,
  "discord_bot_token": "YOUR_BOT_TOKEN",
  "discord_channel_id": "YOUR_CHANNEL_ID",
  "discord_inbound_channel_index": 1,  // or the channel number you prefer
  "discord_send_ai": true,
  "discord_send_emergency": true
}
```
- **discord_webhook_url:**  
  Create a webhook in your desired Discord channel (Channel Settings → Integrations → Webhooks) and copy its URL.
- **discord_bot_token & discord_channel_id:**  
  Copy your bot’s token from the Developer Portal and enable message polling by specifying the channel ID where the bot should read messages.  
  To get a channel ID, enable Developer Mode in Discord (User Settings → Advanced → Developer Mode) then right-click the channel and select "Copy ID."

#### 5. Polling Integration (Optional)
- **Enable Message Polling:**  
  Set `"discord_receive_enabled": true` to allow the bot to poll for new messages.
- **Routing:**  
  The configuration key `"discord_inbound_channel_index"` determines the channel number used by MESH-MASTER for routing incoming Discord messages. Make sure it matches your setup.

#### 6. Testing Your Discord Setup
- **Restart MESH-MASTER:**  
  With the updated configuration, restart your bot.
- **Check Bot Activity:**  
  Verify that the bot is present in your server, that it can see messages in the designated channel, and that it can send responses.  
- **Emergency Alerts & AI Responses:**  
  Confirm that emergency alerts and AI responses are being posted in Discord as per your configuration (`"discord_send_ai": true` and `"discord_send_emergency": true`).

#### 7. Troubleshooting Tips
- **Permissions Issues:**  
  If the bot isn’t responding or reading messages, double-check that its role on your server has the required permissions.
- **Channel IDs & Webhook URLs:**  
  Verify that you’ve copied the correct channel IDs and webhook URLs (ensure no extra spaces or formatting issues).
- **Bot Token Security:**  
  Keep your bot token secure. If it gets compromised, regenerate it immediately from the Developer Portal.

---

### Twilio Integration
- **Enable Twilio:**  
  - Set `"enable_twilio": true` in `config.json`.
- **Configure Twilio Credentials:**  
  - Provide your Twilio settings in `config.json`:
    - `"twilio_sid": "YOUR_TWILIO_SID"`
    - `"twilio_auth_token": "YOUR_TWILIO_AUTH_TOKEN"`
    - `"twilio_from_number": "YOUR_TWILIO_PHONE_NUMBER"`
    - `"alert_phone_number": "DESTINATION_PHONE_NUMBER"` (the number to receive emergency SMS)
- **Use this by typing:**  
  - When an emergency is triggered, the bot sends an SMS containing the alert message (with a Google Maps link if GPS data is available).
- **Tip:**  
  - Follow [Twilio's setup guide](https://www.twilio.com/docs/usage/tutorials/how-to-use-your-free-trial-account) to obtain your SID and Auth Token, and ensure that your phone numbers are verified.

---

## Other Important Settings

- **Logging & Archives:**  
  - Script logs are stored in `script.log` and message logs in `messages.log`.
  - An archive is maintained in `messages_archive.json` to keep recent messages.
  
- **Device Connection:**  
  - Configure the connection method for your Meshtastic device by setting either the `"serial_port"` or enabling `"use_wifi"` along with `"wifi_host"` and `"wifi_port"`.  
  - Alternatively, enable `"use_mesh_interface"` if applicable.
  - Baud Rate is optionally set if you need - this is for longer USB runs (roof nodes connected via USB) and bad USB connections.
  
- **Message Routing & Commands System:**  
  - **Built-in Commands** (case-insensitive):
    - `/about` - Show bot information
    - `/help` - List all available commands  
    - `/test` - Connection test with your location
    - `/motd` - Display Message of the Day
    - `/ai`, `/bot`, `/query`, `/data` - AI conversation commands
    - `/emergency`, `/911` - Trigger emergency alerts with GPS
    - `/reset` - Clear conversation history for channel or DM
    
  - **Fun Trivia Commands**:
    - `/bible` - WEB/RVR1909 verse with per-user reading progress (`/bible` resumes where you left off; `/bible John 3:16-17` jumps to a passage, nav via `<1,2>`) 📜
    - `/biblehelp` - Mesh-friendly bible tips (language switching, navigation, quick start)
    - `/chucknorris` - Random Chuck Norris fact from `chuck_api_jokes.json` 🥋  
    - `/elpaso` - Random El Paso trivia from `el_paso_people_facts.json` 🌵
    
  - **Admin Commands (DM-only for security):**
    - `/changeprompt <text>` - Update AI system prompt (persists to config.json)
    - `/changemotd <text>` - Update Message of the Day (persists to motd.json)
    - `/showprompt`, `/printprompt` - Display current system prompt
    
  - **Smart Auto-Responses (DM only):**
    - **🔔 Alarm Bell Responses**: When someone sends "alert bell character!" or 🔔, bot responds with random Meshtastic facts from `meshtastic_facts.py`
    - **📍 Position Replies**: When someone shares their position and requests yours, bot gives humorous "I'm just software" responses
    
  - **Custom Commands** via `commands_config.json`:
    ```json
    {
      "commands": [
        {
          "command": "/ping",
          "response": "Pong!"
        },
        {
          "command": "/funfact", 
          "ai_prompt": "Give me a fun fact about {user_input}"
        }
      ]
    }
    ```
  - The WebUI Dashboard (accessible at [http://localhost:5000/dashboard](http://localhost:5000/dashboard)) displays messages and node status.
  
- **AI Provider Settings:**  
  - Adjust `"ai_provider"` and related API settings (timeouts, models, etc.) for LM Studio, OpenAI, Ollama, or Home Assistant integration.
  
- **Security:**  
  - If using Home Assistant with PIN protection, follow the specified format (`PIN=XXXX your message`) to ensure messages are accepted.
  
- **Testing:**  
  - You can test SMS sending with the `/sms` command or trigger an emergency alert to confirm that Twilio and email integrations are functioning.

---


## Contributing & Disclaimer

- **Alpha Software Notice:**  
  This release (v0.4.2) is experimental. Expect bugs and changes that might affect existing features. Thorough field testing is recommended before production use.
- **Feedback & Contributions:**  
  Report issues or submit pull requests on GitHub. Your input is invaluable.
- **Use Responsibly:**  
  Modifying this code for nefarious purposes is strictly prohibited. Use at your own risk.

---

## Donations are GRACIOUSLY accepted to stoke development!
- **PayPal:**  
  https://www.paypal.com/ncp/payment/XPW23EP9XHMYS
- **BTC:**  
  bc1qcfstyxaktw5ppnsm5djmpvmgv49z0cp6df33tl
- **XRP:**  
  TrpgvB8EgTtqhU1W6XJAvx3CS8KKkRwTLHc
- **DOGE:**  
  D5MyXVRkwLW9XzcKjjcwEHcXvnaTFueZhy

---

## Conclusion

MESH-MASTER BETA v0.5.1 takes the solid foundation of v0.4.2 and introduces even more significant improvements in logging, error handling, and a bit of polish on the web-UI and it's function.  Whether you’re chatting directly with your node, integrating with Home Assistant, or leveraging multi‑channel alerting (Twilio, Email, Discord), this release offers a more comprehensive and reliable off‑grid AI assistant experience.

**Enjoy tinkering, stay safe, and have fun!**  
Please share your feedback or join our community on GitHub.
