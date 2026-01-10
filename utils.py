import streamlit as st
import sqlite3
import pandas as pd
import os
import shutil
import smtplib
import hashlib
import time
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

# 引入你的业务逻辑模块
# 注意：确保这些文件在根目录下
import recalc_fund_history
from data_provider import DataProvider

# ==========================================
# 1. 基础配置与常量
# ==========================================

# 🍓 树莓派/环境判断逻辑
if os.path.exists('/share'):
    DB_FILE = '/share/asset_tracker.db'
    IS_RASPBERRY_PI = True
    # 硬盘缓存模式
    CACHE_PARAMS = {
        "persist": "disk", 
        "ttl": None, 
        "show_spinner": "正在从硬盘读取历史数据 (树莓派模式)..."
    }
else:
    DB_FILE = 'asset_tracker.db'
    IS_RASPBERRY_PI = False
    # 开发模式：不缓存
    CACHE_PARAMS = {
        "persist": None, 
        "ttl": 0, 
        "show_spinner": "正在实时计算 (PC开发模式)..."
    }

# ==========================================
# 2. 数据库基础操作
# ==========================================

def get_db_connection():
    """获取数据库连接 (Row Factory)"""
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """初始化数据库 (包含 V4 所有最新表结构)"""
    if not os.path.exists(DB_FILE):
        # 如果文件不存在，或者你希望每次启动检查表结构，可以在这里执行建表 SQL
        # 为节省篇幅，这里假设你已经运行了 init_db.py
        # 如果需要完全自动初始化，可以将生成的 init_db.py 内容粘贴到这里
        pass

# ==========================================
# 3. 用户与侧边栏逻辑 (核心重构)
# ==========================================

def get_all_usernames():
    """获取所有用户名列表"""
    conn = get_db_connection()
    try:
        users = conn.execute('SELECT username FROM users').fetchall()
        return [u['username'] for u in users]
    except Exception:
        return []
    finally:
        conn.close()

def get_or_create_user_by_name(username):
    """获取或创建用户"""
    conn = get_db_connection()
    try:
        user = conn.execute('SELECT * FROM users WHERE username = ?', (username,)).fetchone()
        if user:
            return dict(user)
        else:
            dummy_hash = hashlib.sha256("123456".encode()).hexdigest() 
            cursor = conn.execute('INSERT INTO users (username, password_hash) VALUES (?, ?)', 
                                 (username, dummy_hash))
            user_id = cursor.lastrowid
            conn.commit()
            return {'user_id': user_id, 'username': username}
    except Exception as e:
        st.error(f"用户获取失败: {e}")
        return None
    finally:
        conn.close()

def delete_user_fully(target_user_id):
    """彻底级联删除用户"""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # 1. 删关联
        assets = conn.execute('SELECT asset_id FROM assets WHERE user_id = ?', (target_user_id,)).fetchall()
        asset_ids = [str(row['asset_id']) for row in assets]
        
        if asset_ids:
            placeholders = ','.join(['?'] * len(asset_ids))
            cursor.execute(f'DELETE FROM snapshots WHERE asset_id IN ({placeholders})', asset_ids)
            cursor.execute(f'DELETE FROM asset_tag_map WHERE asset_id IN ({placeholders})', asset_ids)

        # 2. 删各表数据
        tables_with_userid = [
            'assets', 'tags', 'cashflows', 'investment_plans', 
            'investment_notes', 'monthly_profits', 'monthly_reviews', 
            'rebalance_targets', 'user_sessions', 'my_fund_history'
        ]
        for table in tables_with_userid:
            try:
                cursor.execute(f'DELETE FROM {table} WHERE user_id = ?', (target_user_id,))
            except sqlite3.OperationalError:
                pass # 防止表不存在报错

        # 3. 删用户
        cursor.execute('DELETE FROM users WHERE user_id = ?', (target_user_id,))
        conn.commit()
        return True, "删除成功"
    except Exception as e:
        conn.rollback()
        return False, f"删除失败: {str(e)}"
    finally:
        conn.close()

def show_sidebar_user_picker():
    """
    📌 公共侧边栏组件：负责用户切换、Demo 提示、树莓派刷新
    在每个 Page 文件的开头调用此函数
    """
    with st.sidebar:
        
        # 1. 获取用户列表
        existing_users = get_all_usernames()
        menu_options = existing_users + ["➕ 新增成员..."]
        
        # 2. 确定默认选中项
        default_index = 0
        if 'user' in st.session_state and st.session_state.user:
            current_name = st.session_state.user['username']
            if current_name in existing_users:
                default_index = existing_users.index(current_name)
        
        # 3. 下拉框
        selected_option = st.selectbox(
            "当前成员", 
            menu_options, 
            index=default_index,
            key="user_selector_global"
        )

        # 4. 逻辑处理
        if selected_option == "➕ 新增成员...":
            st.info("👋 欢迎新成员加入！")
            new_username = st.text_input("请输入昵称", placeholder="例如：奶奶")
            if st.button("确认创建"):
                if new_username.strip() and new_username not in existing_users:
                    new_user = get_or_create_user_by_name(new_username)
                    st.session_state.user = new_user
                    st.success(f"欢迎 {new_username}！")
                    st.rerun()
                elif new_username in existing_users:
                    st.error("名字已存在")
            st.stop() # 暂停后续页面渲染
            
        else:
            # 切换用户 Session
            if 'user' not in st.session_state or st.session_state.user is None or st.session_state.user['username'] != selected_option:
                user_obj = get_or_create_user_by_name(selected_option)
                st.session_state.user = user_obj
                st.toast(f"已切换到账户: {selected_option}", icon="👋")
                st.rerun()

        st.divider()
        
        # Demo 提示
        if 'user' in st.session_state and st.session_state.user and st.session_state.user['username'] == 'demo':
            st.warning("⚠️ 演示模式", icon="🤖")

        # 树莓派强制刷新
        if IS_RASPBERRY_PI:
            if st.button("🔄 强制刷新缓存"):
                st.cache_data.clear()
                st.toast("缓存已清除", icon="🚀")
                st.rerun()
                
        # 显示当前用户信息
        if 'user' in st.session_state and st.session_state.user:
             st.caption(f"当前用户 ID: {st.session_state.user['user_id']}")

# ==========================================
# 4. 数据编辑与同步 (Data Editor Utils)
# ==========================================

def save_changes_to_db(edited_df, original_df, table_name, id_col, user_id, fixed_cols=None):
    """处理 DataEditor 的增删改"""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # 1. 删除处理
        if not original_df.empty and not edited_df.empty:
            orig_ids = set(original_df[id_col].dropna().astype(int))
            new_ids = set(edited_df[id_col].dropna().astype(int))
            deleted_ids = orig_ids - new_ids
        elif not original_df.empty and edited_df.empty:
            deleted_ids = set(original_df[id_col].dropna().astype(int))
        else:
            deleted_ids = set()

        for del_id in deleted_ids:
            # 级联删除资产相关子表
            if table_name == 'assets':
                cursor.execute('DELETE FROM snapshots WHERE asset_id = ?', (del_id,))
                cursor.execute('DELETE FROM asset_tag_map WHERE asset_id = ?', (del_id,))
            elif table_name == 'tags':
                cursor.execute('DELETE FROM asset_tag_map WHERE tag_id = ?', (del_id,))
            
            cursor.execute(f'DELETE FROM {table_name} WHERE {id_col} = ? AND user_id = ?', (del_id, user_id))

        # 2. 新增与修改
        for index, row in edited_df.iterrows():
            data = row.to_dict()
            if fixed_cols: data.update(fixed_cols)
            
            # 新增 (ID为空或0)
            if pd.isna(row[id_col]) or row[id_col] == 0:
                cols = [k for k in data.keys() if k != id_col and k != 'created_at']
                placeholders = ', '.join(['?'] * len(cols))
                col_names = ', '.join(cols)
                values = [data[c] for c in cols]
                cursor.execute(f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})", values)
            
            # 修改
            elif row[id_col] in (original_df[id_col].values if not original_df.empty else []):
                cols = [k for k in data.keys() if k != id_col and k != 'created_at']
                set_clause = ', '.join([f"{c} = ?" for c in cols])
                values = [data[c] for c in cols]
                values.append(row[id_col])
                values.append(user_id)
                cursor.execute(f"UPDATE {table_name} SET {set_clause} WHERE {id_col} = ? AND user_id = ?", values)

        conn.commit()
        st.success("数据同步成功！")
        return True
    except Exception as e:
        conn.rollback()
        st.error(f"保存失败: {str(e)}")
        return False
    finally:
        conn.close()

# ==========================================
# 5. 核心计算与业务逻辑
# ==========================================

def execute_daily_plans_safe(user_id, target_date_str):
    """执行定投计划"""
    conn = get_db_connection()
    logs = []
    try:
        plans = conn.execute('''
            SELECT p.*, a.code, a.type, a.name as asset_name
            FROM investment_plans p
            JOIN assets a ON p.asset_id = a.asset_id
            WHERE p.user_id = ? AND p.is_active = 1
        ''', (user_id,)).fetchall()
        
        target_date = datetime.strptime(target_date_str, '%Y-%m-%d').date()
        
        for plan in plans:
            # 1. 日期检查
            freq = plan['frequency']
            exec_day = int(plan['execution_day'])
            is_due = False
            if freq == '每天': is_due = True
            elif freq == '每周' and target_date.weekday() == exec_day: is_due = True
            elif freq == '每月' and target_date.day == exec_day: is_due = True
            
            if not is_due: continue

            # 2. 幂等检查
            check_note_pattern = f"自动定投: {plan['asset_name']}"
            exist_flow = conn.execute('''
                SELECT id FROM cashflows 
                WHERE user_id = ? AND date = ? AND category = '定投扣款' AND note LIKE ?
            ''', (user_id, target_date_str, f"%{check_note_pattern}%")).fetchone()
            if exist_flow:
                logs.append(f"⏭️ 跳过: {plan['asset_name']} (今日已执行)")
                continue

            # 3. 执行逻辑
            amount = plan['amount']
            asset_id = plan['asset_id']
            code = plan['code']
            
            nav = 1.0
            if plan['type'] in ['基金', '股票'] and code:
                nav = DataProvider.get_fund_nav(code) or 1.0
            
            shares_to_add = amount / nav
            
            # 更新成本 (移动加权)
            curr = conn.execute('SELECT last_shares, unit_cost FROM assets WHERE asset_id=?', (asset_id,)).fetchone()
            old_shares = curr['last_shares'] or 0.0
            old_cost = curr['unit_cost'] or 0.0
            new_shares = old_shares + shares_to_add
            new_unit_cost = ((old_shares * old_cost) + amount) / new_shares if new_shares > 0 else 0
            
            conn.execute('UPDATE assets SET last_shares=?, unit_cost=? WHERE asset_id=?', (new_shares, new_unit_cost, asset_id))
            
            # 扣减来源
            if plan['source_asset_id']:
                src = conn.execute('SELECT last_shares FROM assets WHERE asset_id=?', (plan['source_asset_id'],)).fetchone()
                src_shares = (src['last_shares'] or 0.0) - amount
                conn.execute('UPDATE assets SET last_shares=? WHERE asset_id=?', (src_shares, plan['source_asset_id']))
                # 记录快照
                conn.execute('''
                    INSERT INTO snapshots (asset_id, date, amount, profit, cost, shares, unit_nav, is_cleared)
                    VALUES (?, ?, ?, 0, ?, ?, 1.0, 0)
                    ON CONFLICT(asset_id, date) DO UPDATE SET amount=excluded.amount, shares=excluded.shares
                ''', (plan['source_asset_id'], target_date_str, src_shares, src_shares, src_shares))

            # 记流水
            note = f"{check_note_pattern} (份额+{shares_to_add:.2f}, 新成本{new_unit_cost:.4f})"
            conn.execute("INSERT INTO cashflows (user_id, date, type, amount, category, note, created_at) VALUES (?, ?, '支出', ?, '定投扣款', ?, datetime('now'))",
                         (user_id, target_date_str, amount, note))
            logs.append(f"✅ 执行: {plan['asset_name']} {amount}元")

        conn.commit()
        return True, logs
    except Exception as e:
        return False, [f"错误: {e}"]
    finally:
        conn.close()

def recalculate_daily_nav(user_id, target_date_str, progress_bar=None, status_text=None, limit_asset_ids=None):
    """一键更新行情净值"""
    conn = get_db_connection()
    results = {"success": [], "fail": []}
    try:
        sql = 'SELECT asset_id, name, code, type, last_shares, unit_cost FROM assets WHERE user_id = ? AND auto_update = 1 AND code IS NOT NULL'
        params = [user_id]
        if limit_asset_ids:
            placeholders = ','.join(['?'] * len(limit_asset_ids))
            sql += f" AND asset_id IN ({placeholders})"
            params.extend(limit_asset_ids)
        else:
            return True, {"success": [], "fail": ["未选中资产"]}

        targets = conn.execute(sql, params).fetchall()
        total = len(targets)
        if total == 0: return True, {"success":[], "fail":["无自动更新资产"]}

        for idx, asset in enumerate(targets):
            if progress_bar: progress_bar.progress(idx / total)
            if status_text: status_text.caption(f"更新: {asset['name']}...")
            
            try:
                nav = 1.0
                if '基金' in asset['type']:
                    nav = DataProvider.get_fund_nav(asset['code'], end_date=target_date_str)
                elif '股票' in asset['type']:
                    nav = DataProvider.get_stock_price(asset['code'])
                
                # 获取该日快照或最新份额
                snap = conn.execute('SELECT shares FROM snapshots WHERE asset_id=? AND date=?', (asset['asset_id'], target_date_str)).fetchone()
                shares = snap['shares'] if (snap and snap['shares'] > 0) else asset['last_shares']
                
                cost = asset['unit_cost'] or 0.0
                amt = shares * nav
                profit = amt - (shares * cost)
                
                conn.execute('''
                    INSERT INTO snapshots (asset_id, date, amount, profit, cost, shares, unit_nav, is_cleared)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 0)
                    ON CONFLICT(asset_id, date) DO UPDATE SET
                    amount=excluded.amount, profit=excluded.profit, unit_nav=excluded.unit_nav
                ''', (asset['asset_id'], target_date_str, amt, profit, shares * cost, shares, nav))
                
                results['success'].append(f"{asset['name']}: {nav}")
            except Exception as e:
                results['fail'].append(f"{asset['name']}: {e}")

        conn.commit()
        if progress_bar: progress_bar.progress(1.0)
        return True, results
    finally:
        conn.close()

def get_latest_rates(conn):
    """获取最新汇率字典"""
    df = pd.read_sql("SELECT currency, rate, date FROM exchange_rates ORDER BY date DESC", conn)
    if df.empty: return {}
    return df.drop_duplicates(subset=['currency']).set_index('currency')['rate'].to_dict()

# ==========================================
# 6. 分析与报表 (Analytics)
# ==========================================

@st.cache_data(**CACHE_PARAMS)
def get_cached_analytics_data(user_id):
    """获取带缓存的资产分析数据"""
    local_conn = sqlite3.connect(DB_FILE)
    try:
        # 1. 基础快照
        df_raw = pd.read_sql('''
            SELECT s.date, s.asset_id, s.amount, s.profit, s.cost, s.yield_rate, a.name, a.currency, a.type
            FROM snapshots s JOIN assets a ON s.asset_id = a.asset_id
            WHERE a.user_id = ?
        ''', local_conn, params=(user_id,))
        if df_raw.empty: return None, None
        df_raw['date'] = pd.to_datetime(df_raw['date'])

        # 2. 汇率换算
        df_rates = pd.read_sql("SELECT date, currency, rate FROM exchange_rates", local_conn)
        df_rates['date'] = pd.to_datetime(df_rates['date'])
        df_merged = pd.merge(df_raw, df_rates, on=['date', 'currency'], how='left')
        df_merged['rate'] = df_merged.apply(lambda r: 1.0 if r['currency'] == 'CNY' else r['rate'], axis=1).fillna(1.0)
        
        for col in ['amount', 'profit', 'cost']:
            df_merged[col] = df_merged[col] * df_merged['rate']

        # 3. 标签聚合
        df_tags = pd.read_sql('SELECT t.tag_group, t.tag_name, atm.asset_id FROM tags t JOIN asset_tag_map atm ON t.tag_id=atm.tag_id WHERE t.user_id=?', local_conn, params=(user_id,))
        
        df_tags_agg = pd.DataFrame()
        if not df_tags.empty:
            merged = pd.merge(df_merged, df_tags, on='asset_id', how='inner')
            tag_analytics = []
            for name, group in merged.groupby(['date', 'tag_group', 'tag_name']):
                d, tg, tn = name
                tag_analytics.append({
                    'date': d, 'tag_group': tg, 'tag_name': tn,
                    'amount': group['amount'].sum(),
                    'profit': group['profit'].sum(),
                    'cost': group['cost'].sum(),
                    'yield_rate': (group['profit'].sum()/group['cost'].sum()*100) if group['cost'].sum()!=0 else 0
                })
            df_tags_agg = pd.DataFrame(tag_analytics)

        return df_merged, df_tags_agg
    finally:
        local_conn.close()

@st.cache_data(ttl=3600*12)
def get_market_index_data_cached(index_name, start_date_str, end_date_str):
    """缓存指数数据"""
    return DataProvider.get_market_index_data(index_name, start_date_str, end_date_str)

# ==========================================
# 7. AI 与 备份系统
# ==========================================

def generate_and_send_ai_prompt(user_id, start_date_str, end_date_str, dimension_group):
    """
    生成 AI 顾问提示词 (专业版：包含每日净值CSV + 结构对比 + 核心持仓)
    :param dimension_group: "按具体资产" 或 具体的标签组名称 (如 "资产大类")
    """
    import pandas as pd
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    
    conn = get_db_connection()
    
    # --- 1. 获取系统邮箱设置 ---
    settings = conn.execute('SELECT * FROM system_settings WHERE id = 1').fetchone()
    if not settings['email_host']:
        conn.close()
        return False, "未配置邮箱 SMTP，无法发送。"

    try:
        # --- 2. 准备每日趋势数据 (Daily Trend) ---
        # 直接从 fund_history 取，因为那里有计算好的净值 (NAV)
        sql_trend = '''
            SELECT date, total_assets, accumulated_profit, unit_nav 
            FROM my_fund_history 
            WHERE user_id = ? AND date BETWEEN ? AND ?
            ORDER BY date ASC
        '''
        df_trend = pd.read_sql(sql_trend, conn, params=(user_id, start_date_str, end_date_str))
        
        if df_trend.empty:
            return False, f"该时间段 ({start_date_str} ~ {end_date_str}) 内没有生成过净值历史数据，请先确保已进行过数据录入和重算。"
        
        # 手动构建 Markdown 表格 (日期 | 总资产 | 累计收益 | 单位净值)
        trend_lines = ["| 日期 | 总资产 | 累计收益 | 单位净值 |"]
        trend_lines.append("|---|---|---|---|")
        for _, r in df_trend.iterrows():
            trend_lines.append(f"| {r['date']} | {r['total_assets']:.2f} | {r['accumulated_profit']:.2f} | {r['unit_nav']:.4f} |")
        markdown_trend_str = "\n".join(trend_lines)
        
        # --- 3. 准备期初 vs 期末 结构对比 (Structure Comparison) ---
        # 根据用户选择的维度 (dimension_group) 获取数据
        # 我们利用 get_cached_analytics_data 获取快照数据
        df_assets_all, df_tags_all = get_cached_analytics_data(user_id)
        
        # 筛选日期
        start_date = pd.to_datetime(start_date_str)
        end_date = pd.to_datetime(end_date_str)
        
        target_df = pd.DataFrame()
        group_col = ""
        
        if dimension_group == "按具体资产":
            # 使用 df_assets_all
            mask = df_assets_all['date'].isin([start_date, end_date])
            target_df = df_assets_all[mask].copy()
            group_col = "name"
        else:
            # 使用 df_tags_all
            mask = (df_tags_all['date'].isin([start_date, end_date])) & (df_tags_all['tag_group'] == dimension_group)
            target_df = df_tags_all[mask].copy()
            group_col = "tag_name"

        structure_str = ""
        if target_df.empty:
            structure_str = "(该维度下暂无数据)"
        else:
            # 透视表：Index=名称, Column=日期, Value=金额
            pivot = target_df.pivot_table(index=group_col, columns='date', values='amount', aggfunc='sum').fillna(0)
            
            # 确保列名存在（防止某一期完全没数据）
            if start_date not in pivot.columns: pivot[start_date] = 0.0
            if end_date not in pivot.columns: pivot[end_date] = 0.0
            
            # 计算总额用于算占比
            total_start = pivot[start_date].sum()
            total_end = pivot[end_date].sum()
            
            # 格式化输出
            lines = []
            # 按期末金额降序排
            pivot = pivot.sort_values(by=end_date, ascending=False)
            
            lines.append(f"| {group_col} | 期初金额 | 期初占比 | 期末金额 | 期末占比 | 变动额 |")
            lines.append(f"|---|---|---|---|---|---|")
            
            for name, row in pivot.iterrows():
                s_amt = row[start_date]
                e_amt = row[end_date]
                # 忽略太小的杂项，减少 token
                if s_amt < 100 and e_amt < 100: continue
                
                s_pct = (s_amt / total_start * 100) if total_start > 0 else 0
                e_pct = (e_amt / total_end * 100) if total_end > 0 else 0
                diff = e_amt - s_amt
                
                lines.append(f"| {name} | {s_amt:.0f} | {s_pct:.1f}% | {e_amt:.0f} | {e_pct:.1f}% | {diff:+.0f} |")
            
            structure_str = "\n".join(lines)

        # --- 4. 核心持仓分析 (>0.5%) ---
        # 仅针对 Period End Date
        top_holdings_str = ""
        mask_end = df_assets_all['date'] == end_date
        if not mask_end.any():
            top_holdings_str = "(期末无资产数据)"
        else:
            df_end_assets = df_assets_all[mask_end].copy()
            total_end_val = df_end_assets['amount'].sum()
            df_end_assets['ratio'] = df_end_assets['amount'] / total_end_val
            key_assets = df_end_assets[df_end_assets['ratio'] > 0.005].sort_values('amount', ascending=False)
            
            # Markdown 表头
            lines = [f"当前总资产: **{total_end_val:,.2f}**\n"]
            lines.append("| 资产名称 | 币种 | 金额 | 占比 | 状态 |")
            lines.append("|---|---|---|---|---|")
            
            for _, row in key_assets.iterrows():
                curr = row['currency'] if row['currency'] != 'CNY' else ""
                profit_txt = f"浮盈 {row['profit']:,.0f}" if row['profit'] > 0 else f"浮亏 {row['profit']:,.0f}"
                lines.append(f"| **{row['name']}** | {curr} | {row['amount']:,.0f} | {row['ratio']*100:.2f}% | {profit_txt} |")
            
            top_holdings_str = "\n".join(lines)

        # --- 5. 组装 Prompt (Prompt Engineering) ---
        prompt_content = f"""
# Role / 角色设定
**你是一位拥有 20 年经验的专业基金投资顾问 (CIO 级别)。**
你的专长是基于详实的数据，对个人投资者的投资组合进行**归因分析**、**风险评估**和**策略建议**。
你即关注宏观周期的影响，也关注微观持仓的结构健康度。你的分析风格客观、理性，且善于发现数据背后的隐患或机会。

# Context / 分析背景
- **分析周期**: {start_date_str} 至 {end_date_str}
- **统计维度**: {dimension_group}

# Data Section / 投资组合数据

## 1. 每日净值与收益趋势 (Daily Trend)
个人投资者的全部资产已经净值化
{markdown_trend_str}

## 2. 结构变化对比 (Structure Change)

*维度: {dimension_group} | 对比: 期初 vs 期末*
{structure_str}

## 3. 期末核心持仓 (Key Holdings > 0.5%)

{top_holdings_str}

---

# Action Required / 你的任务

请基于上述数据，为我生成一份专业的**《投资组合复盘报告》**。请包含以下章节：

### 第一部分：周期表现综述

1. **收益归因**：结合 Daily Trend 数据，分析这段时间净值波动的主要原因。是在哪几天发生了大幅回撤或上涨？这可能与当时的什么市场大事件有关？（请结合你的互联网知识检索该时间段的市场新闻）
2. **风险指标**：基于净值数据，估算这段时间的最大回撤 (Max Drawdown) 和波动情况。

### 第二部分：结构与仓位分析

1. **调仓评价**：基于 Structure Change 表格，分析我在这段时间的主要资金流向。我加仓了什么？减仓了什么？这种结构调整是否让组合变得更抗跌或更激进？
2. **持仓集中度**：基于 Key Holdings 列表，点评我的持仓集中度风险。是否存在单一资产占比过高的问题？

### 第三部分：未来建议

1. 基于当前的宏观环境和我的持仓结构，给出 1-3 条具体的调整建议（如：是否需要增加债券对冲？是否需要止盈某类资产？）。

"""
        # --- 6. 发送邮件 ---
        msg = MIMEMultipart()
        msg['Subject'] = f'🤖 AI 深度投顾 Prompt ({start_date_str} ~ {end_date_str})'
        msg['From'] = settings['email_user']
        msg['To'] = settings['email_to'] if settings['email_to'] else settings['email_user']
        
        body = "这是为您生成的 AI 投顾提示词，包含了每日净值数据和详细持仓结构。\n请将下方内容完整复制给 AI 模型。\n\n" + prompt_content
        msg.attach(MIMEText(body, 'plain'))

        server = smtplib.SMTP_SSL(settings['email_host'], settings['email_port'])
        server.login(settings['email_user'], settings['email_password'])
        server.send_message(msg)
        server.quit()
        
        return True, "Prompt 已发送至邮箱！",prompt_content

    except Exception as e:
        import traceback
        traceback.print_exc()
        return False, f"生成失败: {str(e)}"
    finally:
        conn.close()
 
# ==========================================
# 8. 用户公告板 (新增)
# ==========================================

def get_user_notice(user_id):
    """获取用户的个人公告"""
    conn = get_db_connection()
    try:
        row = conn.execute('SELECT personal_notice FROM users WHERE user_id = ?', (user_id,)).fetchone()
        return row['personal_notice'] if row and row['personal_notice'] else ""
    except Exception:
        return ""
    finally:
        conn.close()

def update_user_notice(user_id, new_notice):
    """更新用户的个人公告"""
    conn = get_db_connection()
    try:
        conn.execute('UPDATE users SET personal_notice = ? WHERE user_id = ?', (new_notice, user_id))
        conn.commit()
        return True
    except Exception as e:
        return False
    finally:
        conn.close()
                
def send_email_backup(filepath, settings):
    """发送数据库备份邮件"""
    try:
        msg = MIMEMultipart()
        msg['Subject'] = f'备份 {datetime.now().strftime("%Y-%m-%d")}'
        msg['From'] = settings['email_user']
        msg['To'] = settings['email_to'] or settings['email_user']
        
        with open(filepath, "rb") as f:
            part = MIMEApplication(f.read(), Name=os.path.basename(filepath))
            part['Content-Disposition'] = f'attachment; filename="{os.path.basename(filepath)}"'
            msg.attach(part)

        server = smtplib.SMTP_SSL(settings['email_host'], settings['email_port'])
        server.login(settings['email_user'], settings['email_password'])
        server.send_message(msg)
        try: server.quit() 
        except: pass
        return True, "发送成功"
    except Exception as e:
        return False, str(e)

def perform_backup(manual=False):
    """执行备份"""
    conn = get_db_connection()
    settings = conn.execute('SELECT * FROM system_settings WHERE id = 1').fetchone()
    
    backup_dir = "backups"
    if not os.path.exists(backup_dir): os.makedirs(backup_dir)
    filename = f"asset_tracker_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
    path = os.path.join(backup_dir, filename)
    
    try:
        shutil.copy2(DB_FILE, path)
        status = "本地成功"
        if settings['email_host']:
            ok, msg = send_email_backup(path, settings)
            status += f" | 邮件: {msg}"
        
        conn.execute('UPDATE system_settings SET last_backup_at = ? WHERE id = 1', 
                    (datetime.now().strftime('%Y-%m-%d %H:%M:%S'),))
        conn.commit()
        return True, status
    except Exception as e:
        return False, str(e)
    finally:
        conn.close()

def auto_backup_check():
    """自动备份检查 (可在 app.py 入口调用)"""
    conn = get_db_connection()
    try:
        row = conn.execute('SELECT backup_frequency, last_backup_at FROM system_settings WHERE id = 1').fetchone()
        if not row or row['backup_frequency'] == '关闭': return

        last = datetime.strptime(row['last_backup_at'], '%Y-%m-%d %H:%M:%S') if row['last_backup_at'] else datetime.min
        delta = (datetime.now() - last).days
        freq_map = {'每天':1, '每周':7, '每月':30}
        
        if delta >= freq_map.get(row['backup_frequency'], 999):
            st.toast("正在自动备份...", icon="⏳")
            perform_backup()
    except Exception:
        pass
    finally:
        conn.close()