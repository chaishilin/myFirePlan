# 📈 My FIRE Dashboard (个人资产管理系统)

一个专为 FIRE (Financial Independence, Retire Early) 追求者打造的、运行在树莓派上的**极简个人资产管理系统**。

它不记流水，只记快照。它不追求复杂的会计平衡，只关注净值的长期增长。数据完全私有，部署在本地。

[English](https://www.google.com/search?q=README_EN.md) | [中文](README.md) | [日本語](https://www.google.com/search?q=README_JA.md)

---

## ✨ 核心理念 (Philosophy)

* **快照式记账 (Snapshot over Ledger)**: 放弃繁琐的每一笔收支记录，只在每月/每周记录一次资产余额。
* **隐私至上 (Privacy First)**: 数据存储在本地 SQLite 数据库中，不上传任何云端，完全由你掌控。
* **极低维护 (Low Maintenance)**: 优化的录入体验，每月仅需 5 分钟即可完成资产盘点。
* **长期主义 (Long-termism)**: 关注 10 年、20 年的复利增长，而不是今天的涨跌。

## 🚀 功能特性 (Features)

* **📊 深度资产透视**:
* 双轴趋势图：同时监控总资产、持有收益和收益率。
* 结构化分析：支持按“标签组”（如：风险、市场、流动性）透视资产分布。
* 每日快照：饼图展示持仓占比与收益贡献。


* **📝 投资笔记 (Journal)**:
* 时间轴形式记录投资决策与心路历程。
* 支持原地编辑与Markdown格式。


* **📅 定投计划与现金流**:
* 管理定投任务，自动推演未来 30 天的资金需求。


* **🏷️ 灵活的标签系统**:
* 支持多维度标签（如：A股/美股，高风险/低风险）。
* 支持批量资产打标与级联筛选。


* **🔥 FIRE 展望**:
* 基于复利模型的 50 年财富推演。
* 自动计算“本金收益交叉点”与财务自由里程碑。


* **🛡️ 安全与备份**:
* 支持 **自动邮件备份**：每次操作自动将数据库发送至指定邮箱。
* 本地备份管理与一键恢复。


* **🌍 多语言支持**:
* 内置 中文 / English / 日本語 切换。



## 🛠️ 技术栈 (Tech Stack)

* **Frontend & Backend**: [Streamlit](https://streamlit.io/) (纯 Python 快速构建)
* **Database**: SQLite (单文件数据库，易于迁移与备份)
* **Visualization**: [Plotly](https://plotly.com/) (交互式图表)
* **Data Manipulation**: Pandas

## 📥 安装与运行 (Installation)

### 1. 环境准备

确保你的环境中安装了 Python 3.9+。

```bash
git clone https://github.com/yourusername/fire-dashboard.git
cd fire-dashboard
pip install -r requirements.txt

```

### 2. 初始化数据库

首次运行前，需要初始化数据库结构：

```bash
# 依次运行初始化脚本
python init_db.py
python add_session_table.py
python add_notes_table.py
python add_settings_table.py

```

### 3. 启动应用

```bash
streamlit run app.py

```

访问浏览器 `http://localhost:8501` 即可使用。默认初始账号需在登录页注册。

## 🍓 树莓派部署 (Raspberry Pi Deployment)

本项目非常适合部署在树莓派上，作为家中的 24h 资产看板。

1. **设置开机自启**:
创建服务文件 `/etc/systemd/system/fireplan.service`:
```ini
[Unit]
Description=FIRE Dashboard
After=network.target

[Service]
User=pi
WorkingDirectory=/home/pi/fire-dashboard
ExecStart=/home/pi/.local/bin/streamlit run app.py --server.port 8501 --server.address 0.0.0.0
Restart=always

[Install]
WantedBy=multi-user.target

```


2. **启动服务**:
```bash
sudo systemctl enable fireplan
sudo systemctl start fireplan

```


3. **局域网访问**:
在同一 WiFi 下的手机或电脑访问 `http://树莓派IP:8501`。

## ⚙️ 配置邮件备份 (Email Backup)

为了防止硬件损坏导致数据丢失，强烈建议在 **"设置与备份"** 页面配置 SMTP：

* **SMTP 服务器**: 例如 `smtp.qq.com`
* **端口**: `465` (SSL)
* **账号**: 你的邮箱
* **密码**: 邮箱授权码 (非登录密码)

配置后，系统会在后台自动检测并定期将数据库文件发送到你的邮箱。

## 📸 截图 (Screenshots)

*(此处建议放几张截图，例如：1. 总览看板图 2. FIRE推演图 3. 手机端录入界面)*

## 🤝 贡献 (Contributing)

欢迎提交 Issue 和 Pull Request！如果你有更好的想法，或者想翻译成其他语言，请随时贡献。

## 📜 免责声明 (Disclaimer)

本项目仅作为个人资产记录与辅助分析工具，不构成任何投资建议。开发者不对因使用本软件产生的数据丢失或投资损失负责。请务必定期检查邮件备份。

## 📄 License

MIT License
