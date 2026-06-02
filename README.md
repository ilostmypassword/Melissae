# Melissae Honeypot Framework

<p align="center">
  <img width="320" height="320" alt="Melissae Logo" src="https://github.com/user-attachments/assets/6aeb5230-8f2e-427d-aa38-4d23519ede2e" />
</p>

<p align="center">
  <em>A distributed honeypot framework that lures attackers, scores them, and tells you the story of the attack.</em>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/version-2.6-brightgreen?style=flat-square" alt="Version" />
  <img src="https://img.shields.io/badge/React-19-61DAFB?style=flat-square&logo=react&logoColor=white" alt="React" />
  <img src="https://img.shields.io/badge/Vite-6-646CFF?style=flat-square&logo=vite&logoColor=white" alt="Vite" />
  <img src="https://img.shields.io/badge/Tailwind-3.4-06B6D4?style=flat-square&logo=tailwindcss&logoColor=white" alt="Tailwind" />
  <img src="https://img.shields.io/badge/Flask-3-000000?style=flat-square&logo=flask&logoColor=white" alt="Flask" />
  <img src="https://img.shields.io/badge/MongoDB-4.4-47A248?style=flat-square&logo=mongodb&logoColor=white" alt="MongoDB" />
  <img src="https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker&logoColor=white" alt="Docker" />
  <img src="https://img.shields.io/badge/Nginx-mTLS-009639?style=flat-square&logo=nginx&logoColor=white" alt="Nginx mTLS" />
  <img src="https://img.shields.io/badge/Python-3-3776AB?style=flat-square&logo=python&logoColor=white" alt="Python" />
  <img src="https://img.shields.io/badge/mTLS-ECDSA%20P--384-FF6F00?style=flat-square&logo=letsencrypt&logoColor=white" alt="mTLS" />
  <img src="https://img.shields.io/badge/AWS%20Bedrock-AI-FF9900?style=flat-square&logo=amazonaws&logoColor=white" alt="AWS Bedrock" />
  <img src="https://img.shields.io/badge/LangChain-Agent-1C3C3C?style=flat-square&logo=langchain&logoColor=white" alt="LangChain" />
</p>

<p align="center">
  <a href="https://melissae-documentation.readthedocs.io"><strong>📖 Documentation</strong></a> &nbsp;·&nbsp;
  <a href="https://discord.gg/RXWn85cnYm"><strong>💬 Discord</strong></a> &nbsp;·&nbsp;
  <a href="#quick-start"><strong>🚀 Quick Start</strong></a>
</p>

---

## Why Melissae

Most honeypots give you a wall of logs. **Melissae gives you a fleet of decoys, a verdict on every attacker, and an AI analyst that explains what just happened.**

- 🕸️ **Manager / agent fleet** : Deploy lightweight agents anywhere, manage them from a single hive.
- 🔐 **Zero-trust by default** : Every agent talks to the manager over mTLS (ECDSA P-384), enrolled with one-time tokens.
- 🎯 **Rule-based scoring** : YAML detection rules rate each IP from 0 to 100 (benign · suspicious · malicious).
- 🐝 **Inspektor AI** : Built-in threat analyst on AWS Bedrock + LangChain; chat with your hive, export PDF briefings.
- 📡 **Real-time dashboard** : Live topology, GeoIP map, kill-chains, log search with logical operators, STIX 2.1 export.
- 🧩 **7 honeypot modules** : Web (Nginx/Apache), SSH, FTP, Telnet, Modbus/ICS, MQTT, plus CVE-specific decoys.

<br>

<details open>
<summary><strong>📊 Dashboard Overview</strong></summary>
<br>
<p align="center">
  <img width="1919" alt="overview-dashboard" src="https://github.com/user-attachments/assets/d86012db-b845-437f-bda4-e47e9baa7d8d" />
</p>
</details>

<details>
<summary><strong>🚨 Rule-Based Alerting</strong></summary>
<br>
<p align="center">
  <img width="1919" alt="detection-alerts" src="https://github.com/user-attachments/assets/d6015f73-dd34-4e36-93d3-762c3b464fc6" />
</p>
</details>

<details>
<summary><strong>📈 Statistics & Charts</strong></summary>
<br>
<p align="center">
  <img width="1919" alt="statistics-traffic-1" src="https://github.com/user-attachments/assets/ce5fd426-5be3-4275-9003-d11ad493b620" />
</p>
<p align="center">
  <img width="1919" alt="statistics-traffic-2" src="https://github.com/user-attachments/assets/f298eea0-f853-4467-891d-64938b5a9f29" />
</p>
<p align="center">
  <img width="1919" alt="statistics-threats-1" src="https://github.com/user-attachments/assets/8da4b56f-ff6e-4714-a3a4-36ca61a053ba" />
</p>
</details>

<details>
<summary><strong>🛰️ Agents Management</strong></summary>
<br>
<p align="center">
  <img width="1919" alt="overview-agents" src="https://github.com/user-attachments/assets/c2a9105d-a9fb-4357-8df6-0a39fb3afaba" />
</p>
</details>

<details>
<summary><strong>🌍 GeoIP Attack Map</strong></summary>
<br>
<p align="center">
  <img width="1919" alt="explore-geo-map-1" src="https://github.com/user-attachments/assets/6741957f-98ba-4252-b371-ee8d1fd74d35" />
</p>
<p align="center">
  <img width="1919" alt="explore-geo-map-2" src="https://github.com/user-attachments/assets/815f9905-33ab-47ca-be41-23f093749dc6" />
</p>
</details>

<details>
<summary><strong>🔎 Search Engine</strong></summary>
<br>
<p align="center">
  <img width="1919" alt="explore-search" src="https://github.com/user-attachments/assets/88de9da8-9e79-45a6-bb71-fc0cf6265433" />
</p>
</details>

<details>
<summary><strong>🧠 Threat Intelligence</strong></summary>
<br>
<p align="center">
  <img width="1919" alt="intelligence-threat-intelligence-1" src="https://github.com/user-attachments/assets/9d276fbd-1a70-49e6-b860-eb3ee1585fbb" />
</p>
<p align="center">
  <img width="1919" alt="intelligence-threat-intelligence-2" src="https://github.com/user-attachments/assets/94e9cd5a-e14f-4f34-88c1-a0d81d0201f4" />
</p>
</details>

<details>
<summary><strong>🐝 Inspektor AI</strong></summary>
<br>

</details>

---

## Quick Start

### 1. Manager

```bash
git clone https://github.com/ilostmypassword/Melissae.git
cd Melissae/manager/ && chmod +x melissae-manager.sh
./melissae-manager.sh
```
```text
manager [0 active] > install
manager [0 active] > start
manager [3 active] > enroll my-agent <agent-ip>
```

> After `install`, add your user to the `docker` group: `sudo usermod -aG docker <username> && newgrp docker`

### 2. Agent

> [!IMPORTANT]
> Deploy agents on dedicated servers, properly isolated from your production infrastructure.

```bash
git clone https://github.com/ilostmypassword/Melissae.git
cd Melissae/agent/ && chmod +x melissae-agent.sh
./melissae-agent.sh
```
```text
agent:? [0 active] > install https://<manager-ip>:8443 <token>
agent:my-agent [0 active] > enable <module>
agent:my-agent [0 active] > start
```

### 3. Open the dashboard

```
https://<manager-ip>
```

📚 Need more? See the [**full installation guide**](https://melissae-documentation.readthedocs.io/en/latest/getting-started.html).

---

## Documentation

| | Section | |
|:---:|---|---|
| 📋 | [Overview](https://melissae-documentation.readthedocs.io/en/latest/overview.html) | Features, capabilities, screenshots |
| 🏗️ | [Architecture](https://melissae-documentation.readthedocs.io/en/latest/architecture.html) | Manager/agent model, mTLS, PKI, workflow |
| 📦 | [Modules](https://melissae-documentation.readthedocs.io/en/latest/modules.html) | Honeypot modules, log formats, configuration |
| 📊 | [Dashboard](https://melissae-documentation.readthedocs.io/en/latest/dashboard.html) | Pages, search engine, threat intelligence |
| 🎯 | [Scoring](https://melissae-documentation.readthedocs.io/en/latest/scoring.html) | Detection rules, signals, verdicts |
| 🐝 | [Inspektor](https://melissae-documentation.readthedocs.io/en/latest/inspektor.html) | AI threat analyst |
| 🚀 | [Getting Started](https://melissae-documentation.readthedocs.io/en/latest/getting-started.html) | Installation, enrollment, configuration |
| ⌨️ | [CLI Reference](https://melissae-documentation.readthedocs.io/en/latest/cli-reference.html) | Manager and agent commands |
| 🤝 | [Contributing](https://melissae-documentation.readthedocs.io/en/latest/contributing.html) | Roadmap and how to contribute |

---

## Contributing

Contributions are welcome — see [CONTRIBUTING.md](CONTRIBUTING.md) and join the hive on Discord:

<p>
  <a href="https://discord.gg/RXWn85cnYm"><img src="https://img.shields.io/badge/Discord-Join%20the%20hive-5865F2?style=flat-square&logo=discord&logoColor=white" alt="Discord" /></a>
</p>

## Credits

- [summoningshells](https://github.com/summoningshells)
- [Mlh4040](https://github.com/Mlh4040)
