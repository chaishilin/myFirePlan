import sqlite3
import os

DB_FILE = 'asset_tracker.db'

def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    print("🚀 正在初始化数据库 (整合 V4 最新结构)...")

    # 1. 用户表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        personal_notice TEXT DEFAULT '',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # 2. 会话表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS user_sessions (
        session_id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        token TEXT UNIQUE NOT NULL,
        expires_at DATETIME NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users (user_id)
    )
    ''')

    # 3. 资产表 (Assets)
    # 整合了: currency, remarks, auto_update(v2), last_shares(v2), unit_cost(v3)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS assets (
        asset_id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        name TEXT NOT NULL,
        code TEXT,
        type TEXT NOT NULL,
        currency TEXT DEFAULT 'CNY',     -- 币种
        remarks TEXT,                    -- 备注
        auto_update INTEGER DEFAULT 0,   -- v2: 是否自动更新 (0=否, 1=是)
        last_shares REAL DEFAULT 0.0,    -- v2补丁: 当前持仓份额
        unit_cost REAL DEFAULT 0.0,      -- v3: 单位成本
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users (user_id)
    )
    ''')

    # 4. 标签表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS tags (
        tag_id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        tag_group TEXT NOT NULL,
        tag_name TEXT NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(user_id, tag_group, tag_name),
        FOREIGN KEY (user_id) REFERENCES users (user_id)
    )
    ''')

    # 5. 资产-标签关联表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS asset_tag_map (
        map_id INTEGER PRIMARY KEY AUTOINCREMENT,
        asset_id INTEGER NOT NULL,
        tag_id INTEGER NOT NULL,
        UNIQUE(asset_id, tag_id),
        FOREIGN KEY (asset_id) REFERENCES assets (asset_id),
        FOREIGN KEY (tag_id) REFERENCES tags (tag_id)
    )
    ''')

    # 6. 快照表 (Snapshots)
    # 整合了: is_cleared, shares(v3), unit_nav(v3)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS snapshots (
        snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
        asset_id INTEGER NOT NULL,
        date TEXT NOT NULL,
        amount REAL NOT NULL,          -- 总市值
        profit REAL NOT NULL,          -- 持有收益
        cost REAL NOT NULL,            -- 总成本
        yield_rate REAL,               -- 收益率
        shares REAL DEFAULT 0.0,       -- v3: 持有份额
        unit_nav REAL DEFAULT 0.0,     -- v3: 当日单位净值
        is_cleared INTEGER DEFAULT 0,  -- 是否清仓 (0=否, 1=是)
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(asset_id, date),
        FOREIGN KEY (asset_id) REFERENCES assets (asset_id)
    )
    ''')
    
    # 7. 投资笔记表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS investment_notes (
        note_id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        title TEXT NOT NULL,
        content TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users (user_id)
    )
    ''')

    # 8. 系统设置表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS system_settings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        backup_frequency TEXT DEFAULT '关闭',
        last_backup_at TEXT,
        email_host TEXT,
        email_port INTEGER,
        email_user TEXT,
        email_password TEXT,
        email_to TEXT
    )
    ''')
    # 初始化默认设置
    cursor.execute('INSERT OR IGNORE INTO system_settings (id, backup_frequency) VALUES (1, "关闭")')

    # 9. 定投计划表 (Investment Plans)
    # 整合了: source_asset_id(v2)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS investment_plans (
        plan_id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        asset_id INTEGER NOT NULL,
        amount REAL NOT NULL,
        frequency TEXT NOT NULL,
        execution_day INTEGER NOT NULL,
        source_asset_id INTEGER,       -- v2: 扣款来源资产ID (现金账户)
        is_active INTEGER DEFAULT 1,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users (user_id),
        FOREIGN KEY (asset_id) REFERENCES assets (asset_id)
    )
    ''')

    # 10. 汇率表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS exchange_rates (
        date TEXT,
        currency TEXT,
        rate REAL,
        PRIMARY KEY (date, currency)
    )
    ''')

    # 11. 再平衡目标表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS rebalance_targets (
        target_id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        tag_group TEXT NOT NULL,
        tag_name TEXT NOT NULL,
        target_percentage REAL NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(user_id, tag_group, tag_name),
        FOREIGN KEY (user_id) REFERENCES users (user_id)
    )
    ''')

    # 12. 现金流表 (Cashflows)
    # 整合了: operator(v4)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS cashflows (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        date TEXT,
        type TEXT,                     -- '收入', '支出'
        amount REAL,
        category TEXT,                 -- '工资', '信用卡' 等
        note TEXT,
        operator TEXT DEFAULT '我',    -- v4: 操作人
        created_at TEXT
    )
    ''')

    # 13. 月度收益明细表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS monthly_profits (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        month TEXT NOT NULL,       -- 格式 '2025-01'
        tag_group TEXT NOT NULL,   -- 核心隔离字段
        tag_name TEXT NOT NULL,    -- 标签名
        amount REAL NOT NULL,      -- 收益金额
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(user_id, month, tag_group, tag_name)
    )
    ''')

    # 14. 月度复盘笔记表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS monthly_reviews (
        review_id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        month TEXT NOT NULL,       -- 格式 '2025-01'
        tag_group TEXT NOT NULL,   -- 核心隔离字段
        content TEXT,              -- 复盘文字
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(user_id, month, tag_group)
    )
    ''')

    # 15. 个人基金净值历史表 (My Fund History)
    # 使用 v4 版本的完整定义 (含回撤、本金)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS my_fund_history (
        date TEXT PRIMARY KEY,
        unit_nav REAL NOT NULL,         -- 单位净值
        total_shares REAL NOT NULL,     -- 总份额
        total_assets REAL NOT NULL,     -- 总资产
        daily_return REAL,              -- 日涨跌幅
        drawdown REAL,                  -- v4: 当前回撤
        max_nav_so_far REAL,            -- v4: 历史最高净值
        accumulated_profit REAL,        -- v4: 累计持有收益
        principal REAL                  -- v4: 当前总本金
    )
    ''')

    conn.commit()
    conn.close()
    print("✅ 数据库全量初始化完成！包含所有升级字段 (v1-v4)。")

if __name__ == '__main__':
    init_db()