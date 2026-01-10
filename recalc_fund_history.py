# recalc_fund_history.py
import sqlite3
import pandas as pd
import numpy as np

DB_FILE = 'asset_tracker.db'

def get_db_connection():
    return sqlite3.connect(DB_FILE)

def ensure_table_schema():
    """
    自动检查并修复表结构，确保支持多用户 (user_id)
    """
    conn = get_db_connection()
    try:
        # 检查是否已有 user_id 列
        cursor = conn.execute("PRAGMA table_info(my_fund_history)")
        columns = [row[1] for row in cursor.fetchall()]
        
        if 'user_id' not in columns:
            print("⚠️ 检测到旧版表结构，正在重建 my_fund_history 以支持多用户...")
            conn.execute("DROP TABLE IF EXISTS my_fund_history")
            conn.execute('''
            CREATE TABLE my_fund_history (
                user_id INTEGER NOT NULL,       -- 🔥 新增：用户ID
                date TEXT NOT NULL,
                unit_nav REAL NOT NULL,
                total_shares REAL NOT NULL,
                total_assets REAL NOT NULL,
                daily_return REAL,
                drawdown REAL,
                max_nav_so_far REAL,
                accumulated_profit REAL,
                principal REAL,
                PRIMARY KEY (user_id, date)     -- 🔥 联合主键
            )
            ''')
            conn.commit()
            print("✅ 表结构升级完成。")
    finally:
        conn.close()

def recalculate_user_history(user_id):
    """
    为指定用户重算历史净值
    """
    ensure_table_schema() # 运行前先检查表结构
    
    print(f"🔄 正在为用户 {user_id} 重算基金净值...")
    conn = get_db_connection()
    
    try:
        # 1. 获取资产快照 (含汇率折算)
        # ---------------------------------------------------
        df_snaps = pd.read_sql('''
            SELECT s.date, s.amount, a.currency
            FROM snapshots s
            JOIN assets a ON s.asset_id = a.asset_id
            WHERE a.user_id = ?
        ''', conn, params=(user_id,))
        
        if df_snaps.empty:
            print(f"用户 {user_id} 无资产快照，跳过计算。")
            return False, "无资产数据"

        df_rates = pd.read_sql('SELECT date, currency, rate FROM exchange_rates', conn)
        
        df_snaps['date'] = pd.to_datetime(df_snaps['date'])
        df_rates['date'] = pd.to_datetime(df_rates['date'])
        
        # 合并汇率
        df_merged = pd.merge(df_snaps, df_rates, on=['date', 'currency'], how='left')
        df_merged.loc[df_merged['currency'] == 'CNY', 'rate'] = 1.0
        df_merged = df_merged.sort_values(['currency', 'date'])
        df_merged['rate'] = df_merged.groupby('currency')['rate'].ffill().fillna(1.0)
        
        df_merged['amount_cny'] = df_merged['amount'] * df_merged['rate']
        
        # 按日期聚合总资产
        df_assets = df_merged.groupby('date')['amount_cny'].sum().reset_index()
        df_assets.rename(columns={'amount_cny': 'total_assets'}, inplace=True)
        df_assets['date'] = df_assets['date'].dt.strftime('%Y-%m-%d')
        
        # 2. 获取外部现金流 (剔除内部转账)
        # ---------------------------------------------------
        df_flows = pd.read_sql('''
            SELECT date, type, amount 
            FROM cashflows 
            WHERE user_id = ? 
            AND category NOT IN ('内部调仓', '定投扣款') 
        ''', conn, params=(user_id,))
        
        flow_map = {}
        if not df_flows.empty:
            for _, row in df_flows.iterrows():
                d = row['date']
                amt = row['amount']
                if row['type'] == '支出': amt = -amt
                flow_map[d] = flow_map.get(d, 0.0) + amt

        # 3. 核心计算循环
        # ---------------------------------------------------
        history_data = []
        prev_assets = 0.0
        prev_nav = 1.0
        prev_shares = 0.0
        max_nav = 1.0 
        current_principal = 0.0 
        
        for idx, row in df_assets.iterrows():
            curr_date = row['date']
            curr_total_assets = float(row['total_assets'])
            net_flow = flow_map.get(curr_date, 0.0)
            
            # 维护本金
            current_principal += net_flow
            
            if idx == 0:
                # 建仓日
                unit_nav = 1.0
                daily_return = 0.0
                total_shares = curr_total_assets
                # 第一天特例：所有钱都视为本金
                current_principal = curr_total_assets
            else:
                # 运营日
                # 涨跌幅 = (今日资产 - 净投入 - 昨日资产) / 昨日资产
                if prev_assets == 0:
                    daily_return = 0.0
                else:
                    daily_return = (curr_total_assets - net_flow - prev_assets) / prev_assets
                
                # 更新净值
                unit_nav = prev_nav * (1 + daily_return)
                
                # 更新份额 (按最新净值折算)
                if unit_nav > 0:
                    new_shares = net_flow / unit_nav
                else:
                    new_shares = 0
                
                total_shares = prev_shares + new_shares
            
            # 累计收益
            accumulated_profit = curr_total_assets - current_principal
            
            # 回撤
            if unit_nav > max_nav: max_nav = unit_nav
            drawdown = (unit_nav - max_nav) / max_nav if max_nav > 0 else 0.0
            
            history_data.append({
                'user_id': user_id,
                'date': curr_date,
                'unit_nav': unit_nav,
                'total_shares': total_shares,
                'total_assets': curr_total_assets,
                'daily_return': daily_return,
                'drawdown': drawdown,
                'max_nav_so_far': max_nav,
                'accumulated_profit': accumulated_profit,
                'principal': current_principal
            })
            
            prev_assets = curr_total_assets
            prev_nav = unit_nav
            prev_shares = total_shares

        # 4. 写入数据库 (先删旧数据)
        conn.execute("DELETE FROM my_fund_history WHERE user_id = ?", (user_id,))
        conn.executemany('''
            INSERT INTO my_fund_history 
            (user_id, date, unit_nav, total_shares, total_assets, daily_return, drawdown, max_nav_so_far, accumulated_profit, principal)
            VALUES (:user_id, :date, :unit_nav, :total_shares, :total_assets, :daily_return, :drawdown, :max_nav_so_far, :accumulated_profit, :principal)
        ''', history_data)
        
        conn.commit()
        return True, f"重算完成，生成 {len(history_data)} 条净值记录"
        
    except Exception as e:
        return False, f"重算失败: {str(e)}"
    finally:
        conn.close()

if __name__ == '__main__':
    # 本地测试用，假设 user_id=1
    recalculate_user_history(1)