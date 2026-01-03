import sqlite3
import os

DB_FILE = 'asset_tracker.db'

def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # 1. 用户表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
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

    # 3. 资产表 (已更新: 增加 currency 和 remarks)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS assets (
        asset_id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        name TEXT NOT NULL,
        code TEXT,
        type TEXT NOT NULL,
        currency TEXT DEFAULT 'CNY',  -- 新增：币种
        remarks TEXT,                 -- 新增：备注
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

    # 6. 快照表 (已更新: 增加 is_cleared)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS snapshots (
        snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
        asset_id INTEGER NOT NULL,
        date TEXT NOT NULL,
        amount REAL NOT NULL,
        profit REAL NOT NULL,
        cost REAL NOT NULL,
        yield_rate REAL,
        is_cleared INTEGER DEFAULT 0, -- 新增：是否清仓 (0=否, 1=是)
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

    # 9. 定投计划表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS investment_plans (
        plan_id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        asset_id INTEGER NOT NULL,
        amount REAL NOT NULL,
        frequency TEXT NOT NULL,
        execution_day INTEGER NOT NULL,
        is_active INTEGER DEFAULT 1,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users (user_id),
        FOREIGN KEY (asset_id) REFERENCES assets (asset_id)
    )
    ''')

    # 10. 汇率表 (新增)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS exchange_rates (
        date TEXT,
        currency TEXT,
        rate REAL,
        PRIMARY KEY (date, currency)
    )
    ''')

    # 11. 再平衡目标表 (新增)
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

    # 创建 cashflows 表
    conn.execute('''
            CREATE TABLE IF NOT EXISTS cashflows (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                date TEXT,
                type TEXT,      -- '收入', '支出'
                amount REAL,
                category TEXT,  -- '工资', '信用卡', '大额转账' 等
                note TEXT,
                created_at TEXT
    )
    ''')

    # 12. 月度收益明细表 (支持按标签组隔离)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS monthly_profits (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        month TEXT NOT NULL,       -- 格式 '2025-01'
        tag_group TEXT NOT NULL,   -- 核心隔离字段 (如 '资金渠道')
        tag_name TEXT NOT NULL,    -- 标签名 (如 '支付宝')
        amount REAL NOT NULL,      -- 收益金额
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(user_id, month, tag_group, tag_name)
    )
    ''')

    # 13. 月度复盘笔记表 (支持按标签组隔离)
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
    
    # 打印提示，方便确认
    print("✅ 已更新数据库结构：增加 monthly_profits 和 monthly_reviews 表")

    conn.commit()
    conn.close()
    print("✅ 数据库结构初始化完成 (含最新字段：is_cleared, currency, remarks 及汇率表)")

if __name__ == '__main__':
    init_db()