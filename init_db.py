import sqlite3
import os

DB_FILE = 'asset_tracker.db'

def get_db_connection():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    print(f"🚀 正在初始化数据库: {DB_FILE} ...")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys = ON;")
    
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
        token TEXT PRIMARY KEY,
        user_id INTEGER NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        expires_at DATETIME NOT NULL,
        FOREIGN KEY (user_id) REFERENCES users (user_id)
    )
    ''')
    
    # 3. 资产表 (已新增 remarks 字段)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS assets (
        asset_id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        name TEXT NOT NULL,
        code TEXT,
        type TEXT,
        remarks TEXT,  -- 新增备注字段
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
    
    # 5. 映射表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS asset_tag_map (
        asset_id INTEGER NOT NULL,
        tag_id INTEGER NOT NULL,
        PRIMARY KEY (asset_id, tag_id),
        FOREIGN KEY (asset_id) REFERENCES assets (asset_id),
        FOREIGN KEY (tag_id) REFERENCES tags (tag_id)
    )
    ''')
    
    # 6. 快照表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS snapshots (
        snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
        asset_id INTEGER NOT NULL,
        date TEXT NOT NULL,
        amount REAL NOT NULL,
        profit REAL NOT NULL,
        cost REAL NOT NULL,
        yield_rate REAL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(asset_id, date),
        FOREIGN KEY (asset_id) REFERENCES assets (asset_id)
    )
    ''')
    
    # 7. 再平衡表
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

    # 8. 定投计划表 (新增)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS investment_plans (
        plan_id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        asset_id INTEGER NOT NULL,
        amount REAL NOT NULL,
        frequency TEXT NOT NULL, 
        execution_day INTEGER,
        is_active BOOLEAN DEFAULT 1,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users (user_id),
        FOREIGN KEY (asset_id) REFERENCES assets (asset_id)
    )
    ''')
    print("✅ 定投计划表 (investment_plans) 就绪")

    # 9. 投资笔记表 (新增)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS investment_notes (
        note_id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        title TEXT NOT NULL,
        content TEXT NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users (user_id)
    )
    ''')
    print("✅ 投资笔记表 (investment_notes) 就绪")
    
    conn.commit()
    conn.close()
    print("🎉 数据库初始化完成 (含备注字段)！")

if __name__ == "__main__":
    init_db()