import streamlit as st
import sqlite3
from datetime import datetime
import hashlib
import os
import shutil
import recalc_fund_history  # 🔥 引入计算引擎
from pathlib import Path
import re
import calendar # 用于处理月份天数
from streamlit import cache_data  # 如果之前没引
from datetime import timedelta
from data_provider import DataProvider

import time
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

# 🔥 修改这里：智能判断数据库路径
# 如果系统里有 /share 这个文件夹，说明是在 HA 里，就把数据库存那里
if os.path.exists('/share'):
    DB_FILE = '/share/asset_tracker.db'
else:
    # 否则（在电脑开发时）存当前目录
    DB_FILE = 'asset_tracker.db'
    
# --- 兼容性修复 ---
# 某些旧版库可能还在找 np.bool8，这里做一个简单的映射防止报错
#if not hasattr(np, 'bool8'):
#    np.bool8 = np.bool_

# --- 配置 ---
st.set_page_config(
    page_title="个人资产管理系统",
    page_icon="💼", # 直接写死 Emoji，不要加载图片了
    layout="wide"
)

DB_FILE = 'asset_tracker.db'

# --- 数据库工具函数 ---
def get_db_connection():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

def get_all_usernames():
    """获取数据库中所有已注册的用户名列表"""
    conn = get_db_connection()
    try:
        users = conn.execute('SELECT username FROM users').fetchall()
        # 将结果转换为纯字符串列表 ['爸爸', '妈妈', '孩子']
        return [u['username'] for u in users]
    except Exception:
        return []
    finally:
        conn.close()

def init_db():
    """确保数据库表存在，如果不存在则创建"""
    # 这里直接复用你提供的 init_db.py 的逻辑，为节省篇幅，仅做检查
    if not os.path.exists(DB_FILE):
        # 如果文件不存在，建议先运行 init_db.py 或在这里写完整的建表逻辑
        st.error("数据库文件未找到，请先运行 init_db.py 初始化数据库！")
        st.stop()

# --- 核心逻辑：智能表格同步 ---
def save_changes_to_db(edited_df, original_df, table_name, id_col, user_id, fixed_cols=None):
    """
    对比编辑前后的数据，自动处理新增、修改、删除
    :param edited_df: 编辑后的 DataFrame
    :param original_df: 原始从数据库读出的 DataFrame
    :param table_name: 数据库表名
    :param id_col: 主键列名 (如 'asset_id')
    :param user_id: 当前用户ID
    :param fixed_cols: 需要在插入/更新时强制固定的列 (如 {'user_id': 1})
    """
    import pandas as pd  # 👈 加上这句
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # 1. 处理删除
        # 原表中存在，但编辑后表中不存在的 ID，就是被删除的
        if not original_df.empty and not edited_df.empty:
            orig_ids = set(original_df[id_col].dropna().astype(int))
            new_ids = set(edited_df[id_col].dropna().astype(int))
            deleted_ids = orig_ids - new_ids
        elif not original_df.empty and edited_df.empty:
            deleted_ids = set(original_df[id_col].dropna().astype(int))
        else:
            deleted_ids = set()

        for del_id in deleted_ids:
            # 级联删除处理（简单粗暴版）
            if table_name == 'assets':
                cursor.execute('DELETE FROM snapshots WHERE asset_id = ?', (del_id,))
                cursor.execute('DELETE FROM asset_tag_map WHERE asset_id = ?', (del_id,))
            elif table_name == 'tags':
                cursor.execute('DELETE FROM asset_tag_map WHERE tag_id = ?', (del_id,))
            
            cursor.execute(f'DELETE FROM {table_name} WHERE {id_col} = ? AND user_id = ?', (del_id, user_id))

        # 2. 处理新增和修改
        for index, row in edited_df.iterrows():
            # 准备数据字典
            data = row.to_dict()
            if fixed_cols:
                data.update(fixed_cols)
            
            # 如果 ID 为空或 NaN，视为新增
            if pd.isna(row[id_col]) or row[id_col] == 0:
                # 构建 INSERT 语句
                cols = [k for k in data.keys() if k != id_col and k != 'created_at'] # 排除自增ID和时间
                placeholders = ', '.join(['?'] * len(cols))
                col_names = ', '.join(cols)
                values = [data[c] for c in cols]
                
                query = f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})"
                cursor.execute(query, values)
            
            # 如果 ID 存在且在原始 ID 集合中，视为修改
            elif row[id_col] in (original_df[id_col].values if not original_df.empty else []):
                # 检查数据是否真的变了（简化起见，这里直接 Update，性能损耗可忽略）
                cols = [k for k in data.keys() if k != id_col and k != 'created_at']
                set_clause = ', '.join([f"{c} = ?" for c in cols])
                values = [data[c] for c in cols]
                values.append(row[id_col]) # Where clause value
                values.append(user_id)     # Where clause user_id
                
                query = f"UPDATE {table_name} SET {set_clause} WHERE {id_col} = ? AND user_id = ?"
                cursor.execute(query, values)

        conn.commit()
        st.success("数据已成功同步！")
        return True
        
    except Exception as e:
        conn.rollback()
        st.error(f"保存失败: {str(e)}")
        return False
    finally:
        conn.close()

# --- 核心逻辑：级联删除用户所有数据 ---
def delete_user_fully(target_user_id):
    """
    彻底删除一个用户及其名下所有数据。
    顺序很重要：先删子表（快照、关联），再删主表（资产），最后删用户。
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # 1. 获取该用户所有的 asset_id，以便删除快照和标签关联
        assets = conn.execute('SELECT asset_id FROM assets WHERE user_id = ?', (target_user_id,)).fetchall()
        asset_ids = [str(row['asset_id']) for row in assets]
        
        if asset_ids:
            # SQL IN 语法需要占位符
            placeholders = ','.join(['?'] * len(asset_ids))
            
            # 删除 snapshots (关联 asset_id)
            cursor.execute(f'DELETE FROM snapshots WHERE asset_id IN ({placeholders})', asset_ids)
            
            # 删除 asset_tag_map (关联 asset_id)
            cursor.execute(f'DELETE FROM asset_tag_map WHERE asset_id IN ({placeholders})', asset_ids)

        # 2. 删除属于该用户的直接数据表
        tables_with_userid = [
            'assets',           # 资产表
            'tags',             # 标签表
            'cashflows',        # 现金流
            'investment_plans', # 定投计划
            'investment_notes', # 笔记
            'monthly_profits',  # 月度收益
            'monthly_reviews',  # 月度复盘
            'rebalance_targets',# 再平衡目标
            'user_sessions'     # 会话记录
        ]
        
        for table in tables_with_userid:
            cursor.execute(f'DELETE FROM {table} WHERE user_id = ?', (target_user_id,))

        # 3. 最后删除用户本身
        cursor.execute('DELETE FROM users WHERE user_id = ?', (target_user_id,))
        
        conn.commit()
        return True, "删除成功"
    except Exception as e:
        conn.rollback()
        return False, f"删除失败: {str(e)}"
    finally:
        conn.close()

# --- 页面模块 ---
# --- 简化版用户管理 (无密码模式) ---
def get_or_create_user_by_name(username):
    """
    根据名字直接获取用户，如果不存在则自动创建。
    不再校验密码，主打一个家庭内部互信。
    """
    conn = get_db_connection()
    try:
        # 1. 尝试查找用户
        user = conn.execute('SELECT * FROM users WHERE username = ?', (username,)).fetchone()
        
        if user:
            return dict(user)
        else:
            # 2. 如果不存在，自动注册一个 (密码留空即可，反正不查了)
            # 注意：这里给一个默认后的 dummy 密码哈希，防止数据库非空约束报错
            dummy_hash = hashlib.sha256("123456".encode()).hexdigest() 
            cursor = conn.execute('INSERT INTO users (username, password_hash) VALUES (?, ?)', 
                                 (username, dummy_hash))
            user_id = cursor.lastrowid
            conn.commit()
            
            # 返回新创建的用户
            return {'user_id': user_id, 'username': username}
    except Exception as e:
        st.error(f"用户获取失败: {e}")
        return None
    finally:
        conn.close()

# ❌ 删除或注释掉原来的: hash_password, verify_user, create_user, create_session, get_user_from_token
# ❌ 删除或注释掉原来的: page_login 函数

def page_assets_tags():
    import pandas as pd  # 👈 加上这句
    st.header("资产与标签管理")
    user_id = st.session_state.user['user_id']
    conn = get_db_connection()
    
    # --- 公共筛选逻辑 (封装在这里以便复用) ---
    def apply_advanced_filters(df, context_key):
        """
        df: 必须包含 asset_id, name, code 列
        返回: 筛选后的 df
        """
        with st.expander("🔍 高级筛选 (支持查找未分类资产)", expanded=False):
            c1, c2, c3 = st.columns([2, 1, 2])
            
            # 1. 关键字搜索
            with c1:
                kw = st.text_input("1. 关键字搜索", placeholder="资产名或代码...", key=f"kw_{context_key}")
            
            # 2. 标签组选择
            # 获取所有标签组
            all_groups = pd.read_sql("SELECT DISTINCT tag_group FROM tags WHERE user_id = ?", conn, params=(user_id,))
            groups_list = ["(不筛选)"] + all_groups['tag_group'].tolist()
            
            with c2:
                sel_group = st.selectbox("2. 选择标签组", groups_list, key=f"grp_{context_key}")
            
            # 3. 标签名选择 (根据组动态变化)
            selected_tag_names = []
            if sel_group != "(不筛选)":
                # 获取该组下的所有标签
                tags_in_group = pd.read_sql("SELECT tag_name FROM tags WHERE user_id = ? AND tag_group = ?", 
                                          conn, params=(user_id, sel_group))
                # ★★★ 核心功能：添加【无标签】选项 ★★★
                options = ["【无此标签】"] + tags_in_group['tag_name'].tolist()
                
                with c3:
                    selected_tag_names = st.multiselect(
                        f"3. 筛选 '{sel_group}' 下的状态", 
                        options=options,
                        key=f"tag_{context_key}",
                        placeholder="留空则显示全部"
                    )
        
        # --- 开始执行筛选 ---
        filtered_ids = df['asset_id'].tolist()
        
        # A. 关键字过滤
        if kw:
            df = df[df['name'].str.contains(kw, case=False) | df['code'].str.contains(kw, case=False, na=False)]
            filtered_ids = df['asset_id'].tolist()
            
        # B. 标签过滤 (核心逻辑)
        if sel_group != "(不筛选)" and selected_tag_names:
            # 1. 找出在该组下，拥有特定标签的资产ID
            # 先查出所有打过该组标签的映射关系
            sql_labeled = '''
                SELECT atm.asset_id, t.tag_name 
                FROM asset_tag_map atm
                JOIN tags t ON atm.tag_id = t.tag_id
                WHERE t.user_id = ? AND t.tag_group = ?
            '''
            df_labeled = pd.read_sql(sql_labeled, conn, params=(user_id, sel_group))
            
            target_ids = set()
            
            # 情况1: 用户选了 【无此标签】
            if "【无此标签】" in selected_tag_names:
                # 在该组下有记录的资产ID
                ids_with_tags = set(df_labeled['asset_id'].unique())
                # 当前上下文所有资产ID
                all_current_ids = set(df['asset_id'].unique())
                # 差集 = 没有该组标签的资产
                ids_without_tags = all_current_ids - ids_with_tags
                target_ids.update(ids_without_tags)
            
            # 情况2: 用户选了具体的标签 (如 "高风险")
            real_tags = [t for t in selected_tag_names if t != "【无此标签】"]
            if real_tags:
                ids_with_specific_tags = set(df_labeled[df_labeled['tag_name'].isin(real_tags)]['asset_id'])
                target_ids.update(ids_with_specific_tags)
            
            # 取交集：既满足关键字，又满足标签条件
            df = df[df['asset_id'].isin(target_ids)]
            
        return df

    tab1, tab2, tab3 = st.tabs(["1. 资产列表", "2. 标签定义", "3. 关联打标"])
    
    # 1. 资产管理
    with tab1:
        # --- 修改点1: SQL 增加 auto_update ---
        assets_df = pd.read_sql(
            'SELECT asset_id, name, code, type, currency, remarks, auto_update FROM assets WHERE user_id = ?', 
            conn, params=(user_id,)
        )
        
        # 应用筛选 (保持不变)
        assets_df = apply_advanced_filters(assets_df, "tab1")
        
        st.caption(f"共显示 {len(assets_df)} 条资产")
        
        # --- 修改点2: 配置 auto_update 列 ---
        edited_assets = st.data_editor(
            assets_df,
            num_rows="dynamic",
            column_config={
                "asset_id": st.column_config.NumberColumn("ID", disabled=True),
                "name": st.column_config.TextColumn("资产名称", required=True),
                "code": "代码",
                "type": st.column_config.SelectboxColumn("大类", options=["基金", "股票", "债券", "现金", "其他"]),
                "currency": st.column_config.SelectboxColumn("币种", options=["CNY", "USD", "HKD", "JPY", "EUR", "GBP", "BTC"], required=True, default="CNY", width="small"),
                # 🔥 新增配置
                "auto_update": st.column_config.CheckboxColumn("自动更新?", help="勾选后，'一键更新'功能会自动拉取该资产净值", default=False),
                "remarks": st.column_config.TextColumn("备注", width="medium")
            },
            key="editor_assets",
            use_container_width=True
        )
        
        if st.button("💾 保存资产变动", type="primary"):
            if save_changes_to_db(edited_assets, assets_df, 'assets', 'asset_id', user_id, fixed_cols={'user_id': user_id}):
                st.rerun()

    # 2. 标签管理 (不需要筛选，逻辑不变)
    with tab2:
        tags_df = pd.read_sql('SELECT tag_id, tag_group, tag_name FROM tags WHERE user_id = ?', conn, params=(user_id,))
        edited_tags = st.data_editor(
            tags_df,
            num_rows="dynamic",
            column_config={
                "tag_id": st.column_config.NumberColumn("ID", disabled=True),
                "tag_group": st.column_config.TextColumn("标签组", required=True),
                "tag_name": st.column_config.TextColumn("标签名", required=True)
            },
            key="editor_tags",
            use_container_width=True
        )
        if st.button("💾 保存标签变动", type="primary"):
            if save_changes_to_db(edited_tags, tags_df, 'tags', 'tag_id', user_id, fixed_cols={'user_id': user_id}):
                st.rerun()

    # 3. 关联打标 (级联筛选 + 全选支持)
    with tab3:
        st.write("### 🏷️ 批量资产打标")
        
        # --- A. 准备资产列表数据 ---
        df_assets_tags = pd.read_sql('''
            SELECT 
                a.asset_id, 
                a.name, 
                a.code, 
                GROUP_CONCAT(t.tag_group || '-' || t.tag_name, ', ') as "当前标签"
            FROM assets a
            LEFT JOIN asset_tag_map atm ON a.asset_id = atm.asset_id
            LEFT JOIN tags t ON atm.tag_id = t.tag_id
            WHERE a.user_id = ?
            GROUP BY a.asset_id
        ''', conn, params=(user_id,))
        
        # 初始化选择列（默认为 False，后续可能会被全选按钮覆盖）
        df_assets_tags.insert(0, "选择", False)

        # --- B. 应用高级筛选 ---
        df_filtered = apply_advanced_filters(df_assets_tags, "tab3")
        
        # --- C. 全选/反选 控制区 (新增) ---
        # 引入 session state 来控制 data_editor 的重置
        if 'tag_batch_version' not in st.session_state:
            st.session_state.tag_batch_version = 0
        if 'tag_batch_default_val' not in st.session_state:
            st.session_state.tag_batch_default_val = False

        c_info, c_btn1, c_btn2 = st.columns([3, 1, 1])
        with c_info:
             st.caption(f"当前筛选结果: {len(df_filtered)} 个资产")
        
        with c_btn1:
            if st.button("✅ 全选当前", key="btn_sel_all", help="选中当前列表中的所有资产", use_container_width=True):
                st.session_state.tag_batch_default_val = True
                st.session_state.tag_batch_version += 1 # 强制更新 key，触发表格重绘
                st.rerun()
        
        with c_btn2:
            if st.button("⬜ 取消全选", key="btn_sel_none", help="取消所有勾选", use_container_width=True):
                st.session_state.tag_batch_default_val = False
                st.session_state.tag_batch_version += 1
                st.rerun()

        # 根据按钮状态，强制设置某一列的值
        df_filtered["选择"] = st.session_state.tag_batch_default_val

        # --- D. 表格显示 ---
        # 关键点：key 包含了 version。一旦 version 变了，Streamlit 会认为这是一个全新的表格，
        # 从而丢弃之前的编辑状态，重新加载 df_filtered (也就是我们刚设为 True 的那些数据)
        unique_key = f"tag_editor_{len(df_filtered)}_{st.session_state.tag_batch_version}"
        
        edited_df = st.data_editor(
            df_filtered,
            column_config={
                "选择": st.column_config.CheckboxColumn("✅", default=False),
                "asset_id": st.column_config.NumberColumn("ID", disabled=True, width="small"),
                "name": st.column_config.TextColumn("资产名称", disabled=True),
                "code": st.column_config.TextColumn("代码", disabled=True),
                "当前标签": st.column_config.TextColumn("当前标签", disabled=True, width="large"),
            },
            hide_index=True,
            use_container_width=True,
            key=unique_key
        )
        
        # --- E. 操作区域 (级联标签选择) ---
        st.divider()
        st.markdown("##### 🛠️ 批量操作")
        
        col_actions, col_submit = st.columns([3, 1])
        
        with col_actions:
            all_tags_data = conn.execute('SELECT tag_id, tag_group, tag_name FROM tags WHERE user_id = ? ORDER BY tag_group, tag_name', (user_id,)).fetchall()
            
            if not all_tags_data:
                st.warning("暂无标签，请先去【标签定义】页签添加。")
                selected_tags_to_apply = []
                action_mode = "➕ 添加"
            else:
                all_groups = sorted(list(set([t['tag_group'] for t in all_tags_data])))
                
                # [第一级] 标签组
                filter_groups = st.multiselect(
                    "1. 先筛选标签组", 
                    options=all_groups,
                    placeholder="留空则显示全部标签...",
                    key="tag_action_group_filter"
                )
                
                # [第二级] 具体标签
                if filter_groups:
                    available_tags = [t for t in all_tags_data if t['tag_group'] in filter_groups]
                else:
                    available_tags = all_tags_data
                
                tag_options = {t['tag_id']: f"【{t['tag_group']}】{t['tag_name']}" for t in available_tags}
                
                selected_tags_to_apply = st.multiselect(
                    f"2. 选择要应用的标签 (可选 {len(available_tags)} 个)", 
                    options=tag_options.keys(), 
                    format_func=lambda x: tag_options[x],
                    placeholder="可多选...",
                    key="tag_action_final_select"
                )
                
                action_mode = st.radio("3. 操作模式", ["➕ 添加 (保留已有)", "🔄 覆盖 (清除旧标)", "➖ 移除 (仅删选中)"], horizontal=True)

        with col_submit:
            st.write(" ")
            st.write(" ")
            st.write(" ")
            if st.button("🚀 执行更新", type="primary", use_container_width=True):
                # 统计有多少行被选中了
                selected_assets = edited_df[edited_df["选择"] == True]["asset_id"].tolist()
                
                if not selected_assets:
                    st.warning("请在表格左侧勾选至少一个资产！")
                elif not selected_tags_to_apply:
                    st.warning("请选择要操作的标签！")
                else:
                    try:
                        cursor = conn.cursor()
                        if "覆盖" in action_mode:
                            placeholders = ','.join(['?'] * len(selected_assets))
                            cursor.execute(f'DELETE FROM asset_tag_map WHERE asset_id IN ({placeholders})', selected_assets)
                            for aid in selected_assets:
                                for tid in selected_tags_to_apply:
                                    cursor.execute('INSERT INTO asset_tag_map (asset_id, tag_id) VALUES (?, ?)', (aid, tid))
                                    
                        elif "添加" in action_mode:
                            for aid in selected_assets:
                                for tid in selected_tags_to_apply:
                                    cursor.execute('INSERT OR IGNORE INTO asset_tag_map (asset_id, tag_id) VALUES (?, ?)', (aid, tid))
                                    
                        elif "移除" in action_mode:
                            for aid in selected_assets:
                                for tid in selected_tags_to_apply:
                                    cursor.execute('DELETE FROM asset_tag_map WHERE asset_id = ? AND tag_id = ?', (aid, tid))
                                    
                        conn.commit()
                        st.success(f"✅ 成功更新 {len(selected_assets)} 个资产！")
                        
                        # 更新后，我们重置一下全选状态，防止误操作
                        st.session_state.tag_batch_default_val = False
                        st.session_state.tag_batch_version += 1
                        
                        import time
                        time.sleep(0.5)
                        st.rerun()
                    except Exception as e:
                        conn.rollback()
                        st.error(str(e))
    conn.close()

# --- 放在 page_data_entry 之前 ---
def execute_daily_plans_safe(user_id, target_date_str):
    """
    安全执行当日定投计划 (含成本加权平均计算)
    """
    from datetime import datetime
    import pandas as pd
    
    conn = get_db_connection()
    logs = []
    
    try:
        # 1. 获取今日需执行的计划
        plans = conn.execute('''
            SELECT p.*, a.code, a.type, a.name as asset_name
            FROM investment_plans p
            JOIN assets a ON p.asset_id = a.asset_id
            WHERE p.user_id = ? AND p.is_active = 1
        ''', (user_id,)).fetchall()
        
        target_date = datetime.strptime(target_date_str, '%Y-%m-%d').date()
        
        executed_count = 0
        
        for plan in plans:
            # --- A. 检查日期 (保持不变) ---
            freq = plan['frequency']
            exec_day = int(plan['execution_day'])
            is_due = False
            if freq == '每天': is_due = True
            elif freq == '每周' and target_date.weekday() == exec_day: is_due = True
            elif freq == '每月' and target_date.day == exec_day: is_due = True
            
            if not is_due: continue

            # --- B. 幂等性检查 (保持不变) ---
            check_note_pattern = f"自动定投: {plan['asset_name']}"
            exist_flow = conn.execute('''
                SELECT id FROM cashflows 
                WHERE user_id = ? AND date = ? AND category = '定投扣款' AND note LIKE ?
            ''', (user_id, target_date_str, f"%{check_note_pattern}%")).fetchone()
            if exist_flow:
                logs.append(f"⏭️ 跳过: {plan['asset_name']} (今日已执行)")
                continue

            # --- C. 执行逻辑 ---
            asset_id = plan['asset_id']
            amount = plan['amount']
            source_id = plan['source_asset_id']
            code = plan['code']
            
            # === Part 1: 买入目标资产 (更新份额 & 摊薄成本) ===
            nav = 1.0
            if plan['type'] in ['基金', '股票'] and code:
                nav = DataProvider.get_fund_nav(code) or 1.0
            
            shares_to_add = amount / nav
            
            # 查当前状态
            curr_target = conn.execute('SELECT last_shares, unit_cost FROM assets WHERE asset_id = ?', (asset_id,)).fetchone()
            old_shares = curr_target['last_shares'] if curr_target and curr_target['last_shares'] else 0.0
            old_cost = curr_target['unit_cost'] if curr_target and curr_target['unit_cost'] else 0.0
            
            # 🔥 核心：移动加权平均算成本
            new_shares = old_shares + shares_to_add
            if new_shares > 0:
                # (旧市值 + 新投入) / 总份额
                new_unit_cost = ((old_shares * old_cost) + amount) / new_shares
            else:
                new_unit_cost = 0.0 # 理论上不会走到这
            
            # 更新 Assets 表
            conn.execute('UPDATE assets SET last_shares = ?, unit_cost = ? WHERE asset_id = ?', (new_shares, new_unit_cost, asset_id))
            
            # === Part 2: 扣减来源资产 (保持不变) ===
            if source_id:
                curr_source = conn.execute('SELECT last_shares FROM assets WHERE asset_id = ?', (source_id,)).fetchone()
                curr_source_shares = curr_source['last_shares'] if curr_source and curr_source['last_shares'] else 0.0
                
                # 现金：净值1，金额即份额
                new_source_shares = curr_source_shares - amount
                conn.execute('UPDATE assets SET last_shares = ? WHERE asset_id = ?', (new_source_shares, source_id))
                
                # 写入 Snapshots
                new_source_amt = new_source_shares * 1.0
                conn.execute('''
                    INSERT INTO snapshots (asset_id, date, amount, profit, cost, yield_rate, shares, unit_nav, is_cleared)
                    VALUES (?, ?, ?, 0, ?, 0, ?, 1.0, 0)
                    ON CONFLICT(asset_id, date) DO UPDATE SET
                    amount=excluded.amount, cost=excluded.cost, shares=excluded.shares, unit_nav=1.0
                ''', (source_id, target_date_str, new_source_amt, new_source_amt, new_source_shares))

            # === Part 3: 写入流水 (保持不变) ===
            note = f"{check_note_pattern} (份额+{shares_to_add:.2f}, 新成本{new_unit_cost:.4f})"
            conn.execute('''
                INSERT INTO cashflows (user_id, date, type, amount, category, note, created_at)
                VALUES (?, ?, '支出', ?, '定投扣款', ?, datetime('now'))
            ''', (user_id, target_date_str, amount, note))
            
            executed_count += 1
            logs.append(f"✅ 买入 {plan['asset_name']}: {amount}元, 成本更新 {old_cost:.3f}->{new_unit_cost:.3f}")
            
        conn.commit()
        return True, logs
        
    except Exception as e:
        return False, [f"执行出错: {str(e)}"]
    finally:
        conn.close()

def recalculate_daily_nav(user_id, target_date_str, progress_bar=None, status_text=None, limit_asset_ids=None):
    """
    一键更新功能：拉取行情 -> 更新快照 -> 重算市值/收益
    (支持进度条、错误收集、以及指定资产范围)
    :param limit_asset_ids: list/tuple, 仅更新这些 ID 的资产。如果为 None 或空，则不更新任何资产。
    """
    from data_provider import DataProvider
    import time
    
    conn = get_db_connection()
    results = {"success": [], "fail": []} 
    
    try:
        # 1. 构建查询 SQL
        # 基础条件：属于该用户 AND 开启自动更新 AND 代码不为空
        sql = '''
            SELECT asset_id, name, code, type, last_shares, unit_cost 
            FROM assets 
            WHERE user_id = ? AND auto_update = 1 AND code IS NOT NULL
        '''
        params = [user_id]
        
        # 🔥 核心修改：增加 ID 筛选限制
        if limit_asset_ids:
            # 动态构建 IN (?,?,?)
            placeholders = ','.join(['?'] * len(limit_asset_ids))
            sql += f" AND asset_id IN ({placeholders})"
            params.extend(limit_asset_ids)
        else:
            # 如果没有指定 ID (列表为空)，按照你的需求，直接返回，不进行全量更新
            return True, {"success": [], "fail": ["未选中任何需要更新的资产"]}

        targets = conn.execute(sql, params).fetchall()
        
        total_tasks = len(targets)
        if total_tasks == 0:
            return True, {"success": [], "fail": ["当前筛选列表内没有开启'自动更新'的资产"]}

        updated_count = 0
        
        for idx, asset in enumerate(targets):
            asset_id = asset['asset_id']
            name = asset['name']
            code = asset['code']
            a_type = asset['type']
            
            # --- 更新 UI 进度 ---
            if progress_bar:
                progress_bar.progress((idx) / total_tasks)
            if status_text:
                status_text.caption(f"🔄 [{idx+1}/{total_tasks}] 正在更新: {name} ({code})...")

            try:
                # A. 获取最新净值/价格 (带超时)
                nav = 1.0
                if '基金' in a_type:
                    nav = DataProvider.get_fund_nav(code, end_date=target_date_str)
                elif '股票' in a_type:
                    nav = DataProvider.get_stock_price(code)
                
                # B. 查出当天的快照
                snap = conn.execute('SELECT shares, cost FROM snapshots WHERE asset_id=? AND date=?', (asset_id, target_date_str)).fetchone()
                current_shares = snap['shares'] if (snap and snap['shares'] > 0) else asset['last_shares']

                # C. 计算数值
                unit_cost = asset['unit_cost'] if asset['unit_cost'] else 0.0
                
                new_amount = current_shares * nav
                new_cost = current_shares * unit_cost
                new_profit = new_amount - new_cost
                new_yield = (new_profit / new_cost * 100) if new_cost != 0 else 0.0
                
                # D. 更新数据库
                conn.execute('''
                    INSERT INTO snapshots (asset_id, date, amount, profit, cost, yield_rate, shares, unit_nav, is_cleared)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)
                    ON CONFLICT(asset_id, date) DO UPDATE SET
                    amount=excluded.amount,
                    profit=excluded.profit,
                    cost=excluded.cost,
                    yield_rate=excluded.yield_rate,
                    shares=excluded.shares,
                    unit_nav=excluded.unit_nav
                ''', (asset_id, target_date_str, new_amount, new_profit, new_cost, new_yield, current_shares, nav))
                
                updated_count += 1
                results['success'].append(f"{name}: {nav}")
                
            except TimeoutError:
                results['fail'].append(f"{name}: ❌ 网络超时")
            except Exception as e:
                results['fail'].append(f"{name}: ❌ {str(e)}")
            
        conn.commit()
        if progress_bar: progress_bar.progress(1.0)
        return True, results

    except Exception as e:
        return False, f"系统错误: {e}"
    finally:
        conn.close()

def page_data_entry():
    import pandas as pd
    st.header("📝 每日资产快照录入 (余额法)")
    user_id = st.session_state.user['user_id']
    conn = get_db_connection()
    
    # --- 1. 日期选择 ---
    col_date, _ = st.columns([1, 2])
    with col_date:
        date_input = st.date_input("选择快照日期", datetime.now())
        str_date = date_input.strftime('%Y-%m-%d')

    # 准备基础资产数据
    assets = pd.read_sql('SELECT asset_id, name, code, type, currency, last_shares, auto_update FROM assets WHERE user_id = ?', conn, params=(user_id,))
    
    if assets.empty:
        st.warning("暂无资产，请先去【资产与标签管理】添加资产。")
        conn.close()
        return

    # --- 2. 汇率录入区 (自动化升级版) ---
    if 'currency' in assets.columns:
        unique_currencies = assets['currency'].unique().tolist()
        foreign_currencies = [c for c in unique_currencies if c and c != 'CNY']
    else:
        foreign_currencies = []
    
    if foreign_currencies:
        with st.expander(f"💱 设置当日汇率 ({str_date})", expanded=False):
            # 布局：左边提示，右边放个自动拉取按钮
            h1, h2 = st.columns([3, 1])
            with h1:
                st.caption("检测到您持有外币资产，请确认当日汇率（对人民币）：")
            with h2:
                if st.button("🔄 自动拉取汇率(中行汇买价)", help="调用 AkShare 获取中国银行当日中间价", key="btn_auto_rate"):
                    from data_provider import DataProvider
                    with st.spinner("正在连接中国银行接口..."):
                        fetched_count = 0
                        for curr in foreign_currencies:
                            # 调用数据接口
                            r = DataProvider.get_exchange_rate(curr, str_date)
                            if r:
                                # 🔥 关键：更新 session_state 以刷新 number_input 的值
                                k = f"rate_{curr}_{str_date}"
                                st.session_state[k] = r
                                fetched_count += 1
                        
                        if fetched_count > 0:
                            st.toast(f"成功拉取 {fetched_count} 个币种汇率", icon="✅")
                            import time; time.sleep(0.5); st.rerun() # 刷新界面显示数值
                        else:
                            st.error("未能获取汇率，请检查日期是否为交易日，或手动输入。")

            # 读取数据库已存的，或者 Session State 里的(刚拉取的)
            saved_rates = pd.read_sql("SELECT currency, rate FROM exchange_rates WHERE date = ?", conn, params=(str_date,))
            saved_rate_map = dict(zip(saved_rates['currency'], saved_rates['rate']))
            
            cols = st.columns(len(foreign_currencies) + 1)
            rates_to_save = {}
            
            for i, curr in enumerate(foreign_currencies):
                # 优先级：SessionState (刚拉取的) > Database (已存的) > 1.0 (默认)
                input_key = f"rate_{curr}_{str_date}"
                
                # 如果 session_state 里没有，才去数据库取默认值
                if input_key not in st.session_state:
                    default_val = saved_rate_map.get(curr, 1.0)
                else:
                    default_val = st.session_state[input_key] # 这一步其实是多余的，st.number_input会自动取key的值，但为了逻辑清晰写出来
                
                with cols[i]:
                    # 注意：st.number_input 如果 key 对应的值存在，会自动使用该值
                    r = st.number_input(
                        f"{curr} ➡️ CNY", 
                        value=float(default_val) if input_key not in st.session_state else None, # 如果key存在，value参数会被忽略
                        format="%.4f", 
                        key=input_key
                    )
                    rates_to_save[curr] = r
            
            with cols[-1]:
                st.write(""); st.write("") 
                if st.button("💾 保存汇率", type="secondary"):
                    try:
                        for curr, rate in rates_to_save.items():
                            conn.execute("INSERT OR REPLACE INTO exchange_rates (date, currency, rate) VALUES (?, ?, ?)", (str_date, curr, rate))
                        conn.commit()
                        st.toast("汇率已更新", icon="💱")
                    except Exception as e: st.error(f"汇率保存失败: {e}")


    # --- 3. 筛选与排序工具 (简化版) ---
    with st.expander("🔍 筛选资产", expanded=True):
        c1, c2, c3 = st.columns([2, 1, 2])
        with c1:
            kw = st.text_input("关键字搜索", placeholder="名称/代码")
        with c2:
            hide_cleared = st.checkbox("🙈 隐藏已清仓", value=True)
        with c3:
            all_groups = pd.read_sql("SELECT DISTINCT tag_group FROM tags WHERE user_id = ?", conn, params=(user_id,))
            grp_list = ["(不筛选)"] + all_groups['tag_group'].tolist()
            sel_group = st.selectbox("标签组", grp_list)
        
        # 标签二级筛选逻辑
        sel_tags = []
        if sel_group != "(不筛选)":
            t_df = pd.read_sql("SELECT tag_name FROM tags WHERE user_id=? AND tag_group=?", conn, params=(user_id, sel_group))
            opts = ["【无此标签】"] + t_df['tag_name'].tolist()
            sel_tags = st.multiselect("标签名", opts)
            
            # 排序逻辑
            sort_option = st.radio("排序", ["默认", "金额(高→低)", "收益(高→低)"], horizontal=True, label_visibility="collapsed")
        else:
            sort_option = "默认"

    # --- 4. 数据准备与合并 ---
    # 获取清仓状态
    all_asset_ids = tuple(assets['asset_id'].tolist())
    if not all_asset_ids: return

    if len(all_asset_ids) == 1: str_ids = f"({all_asset_ids[0]})"
    else: str_ids = str(all_asset_ids)
    
    last_status_df = pd.read_sql(f'SELECT asset_id, is_cleared FROM snapshots WHERE asset_id IN {str_ids} ORDER BY date DESC', conn)
    last_status_df = last_status_df.drop_duplicates(subset=['asset_id'])
    assets = pd.merge(assets, last_status_df, on='asset_id', how='left')
    assets['is_cleared'] = assets['is_cleared'].fillna(0).astype(bool)

    # 筛选
    filtered_df = assets.copy()
    if hide_cleared: filtered_df = filtered_df[filtered_df['is_cleared'] == False]
    if kw: filtered_df = filtered_df[filtered_df['name'].str.contains(kw, case=False) | filtered_df['code'].str.contains(kw, case=False, na=False)]
    if sel_group != "(不筛选)" and sel_tags:
        # ... (标签筛选逻辑保持不变，为节省篇幅省略，此处直接引用上一版逻辑) ...
        sql_labeled = '''SELECT atm.asset_id, t.tag_name FROM asset_tag_map atm JOIN tags t ON atm.tag_id = t.tag_id WHERE t.user_id = ? AND t.tag_group = ?'''
        df_labeled = pd.read_sql(sql_labeled, conn, params=(user_id, sel_group))
        target_ids = set()
        current_ids = set(filtered_df['asset_id'])
        if "【无此标签】" in sel_tags: target_ids.update(current_ids - set(df_labeled['asset_id']))
        real_tags = [t for t in sel_tags if t != "【无此标签】"]
        if real_tags: target_ids.update(set(df_labeled[df_labeled['tag_name'].isin(real_tags)]['asset_id']))
        filtered_df = filtered_df[filtered_df['asset_id'].isin(target_ids)]

    # --- 5. 准备 DataEditor 数据 ---
    if filtered_df.empty:
        st.info("没有符合条件的资产。")
    else:
        final_ids = tuple(filtered_df['asset_id'].tolist())
        q_ids = str(final_ids) if len(final_ids) > 1 else f"({final_ids[0]})"
        
        # 查今日快照 (如果今天还没填，就取最近一次的 amount, profit, unit_nav 用于预填充)
        # 注意：这里我们做个优化，如果今天没填， amount/profit 取"昨天"的值作为默认值，方便用户只改变动的部分
        
        # 1. 先查今天的
        snap_today = pd.read_sql(f'SELECT * FROM snapshots WHERE date = ? AND asset_id IN {q_ids}', conn, params=(str_date,))
        
        # 2. 再查最近一次的 (作为默认值兜底)
        snap_last = pd.read_sql(f'''
            SELECT asset_id, amount, profit, unit_nav 
            FROM snapshots 
            WHERE asset_id IN {q_ids} AND date < ? 
            ORDER BY date DESC
        ''', conn, params=(str_date,))
        snap_last = snap_last.drop_duplicates(subset=['asset_id'])
        
        merged = pd.merge(filtered_df, snap_today, on='asset_id', how='left', suffixes=('', '_today'))
        merged = pd.merge(merged, snap_last, on='asset_id', how='left', suffixes=('', '_last'))
        
        # --- 填充逻辑 (核心) ---
        # 优先用今天的；如果没有，用上次的；还没有，用0
        merged['amount'] = merged['amount'].fillna(merged['amount_last']).fillna(0.0)
        merged['profit'] = merged['profit'].fillna(merged['profit_last']).fillna(0.0)
        
        # 净值优先用今天的；如果没有，用上次的；再没有，用1.0
        # 注意：现金类强制 1.0 (虽然 DataEditor 会显示，但我们可以通过 Column config 提示)
        merged['unit_nav'] = merged['unit_nav'].fillna(merged['unit_nav_last']).fillna(1.0)
        
        # 现金类特殊处理：净值默认为1
        if 'type' in merged.columns:
            merged.loc[merged['type'] == '现金', 'unit_nav'] = 1.0

        # 反推逻辑演示 (仅用于显示，不存库，真正计算在保存时)
        # shares = amount / nav
        # cost = amount - profit
        # unit_cost = cost / shares
        # 这些字段我们展示在表格里供参考，但设为 disabled
        merged['shares_est'] = merged.apply(lambda r: r['amount'] / r['unit_nav'] if r['unit_nav'] else 0, axis=1)
        merged['cost_est'] = merged['amount'] - merged['profit']
        merged['unit_cost_est'] = merged.apply(lambda r: r['cost_est'] / r['shares_est'] if r['shares_est'] else 0, axis=1)
        merged['yield_est'] = merged.apply(lambda r: (r['profit'] / r['cost_est'] * 100) if r['cost_est'] else 0, axis=1)

        # 排序
        if "金额" in sort_option: merged = merged.sort_values(by='amount', ascending=False)
        elif "收益" in sort_option: merged = merged.sort_values(by='profit', ascending=False)

        # --- 6. 按钮区 ---
        col_act, _ = st.columns([1, 4])
        with col_act:
            # 🔥 仅保留“刷新净值”功能，移除“定投”和“调仓”
            # 这里的刷新只是为了获取最新的 NAV，方便反推份额
            visible_ids = merged['asset_id'].tolist()
            if st.button("🔄 刷新当前列表净值", help="从网络拉取最新净值，填入表格（不改变市值，只影响反推的份额）"):
                progress_bar = st.progress(0.0)
                status_text = st.empty()
                success, res = recalculate_daily_nav(user_id, str_date, progress_bar, status_text, limit_asset_ids=visible_ids)
                status_text.empty(); progress_bar.empty()
                if success:
                    st.toast("净值已更新，请检查数据", icon="✅")
                    import time; time.sleep(1); st.rerun()
                else:
                    st.error(f"更新失败: {res}")

        # --- 7. DataEditor (余额法核心) ---
        st.caption("💡 **余额法操作指南**：直接对照理财APP，修改【市值】和【持有收益】即可。系统会自动反推份额和成本。")
        
        col_cfg = {
            "asset_id": st.column_config.NumberColumn("ID", disabled=True, width="small"),
            "name": st.column_config.TextColumn("名称", disabled=True),
            "code": st.column_config.TextColumn("代码", disabled=True),
            
            # 🔥 核心输入列：允许编辑
            "amount": st.column_config.NumberColumn("💰 总市值 (查APP)", required=True, format="%.2f", help="对照支付宝/券商APP填写当前总金额"),
            "profit": st.column_config.NumberColumn("🎉 持有收益 (查APP)", required=True, format="%.2f", help="对照APP填写显示的持有收益"),
            "unit_nav": st.column_config.NumberColumn("📈 当日净值", required=True, format="%.4f", help="可手动修改，或点刷新按钮自动拉取"),
            
            # 🔥 衍生列：禁止编辑 (由反推得到)
            "shares_est": st.column_config.NumberColumn("份额 (反推)", disabled=True, format="%.2f"),
            "unit_cost_est": st.column_config.NumberColumn("成本价 (反推)", disabled=True, format="%.4f"),
            "yield_est": st.column_config.NumberColumn("收益率", disabled=True, format="%.2f%%"),
            
            "is_cleared": st.column_config.CheckboxColumn("🏁 清仓?", help="勾选后表示该资产已清仓"),
        }
        if 'currency' in merged.columns:
            col_cfg["currency"] = st.column_config.TextColumn("币", disabled=True, width="small")

        # 只要这几列
        cols_show = ['asset_id','name','code','currency','amount','profit','unit_nav','shares_est','unit_cost_est','yield_est','is_cleared']
        cols_show = [c for c in cols_show if c in merged.columns]

        edited_snapshot = st.data_editor(
            merged[cols_show],
            column_config=col_cfg,
            hide_index=True,
            use_container_width=True,
            key=f"entry_v3_{str_date}"
        )

        # --- 8. 保存逻辑 (反推并存库) ---
        if st.button("💾 保存快照 (自动反推份额)", type="primary"):
            try:
                c = 0
                for _, row in edited_snapshot.iterrows():
                    asset_id = row['asset_id']
                    
                    # 1. 获取用户填写的核心数据
                    amount = float(row['amount'])
                    profit = float(row['profit'])
                    nav = float(row['unit_nav'])
                    is_clr = 1 if row['is_cleared'] else 0
                    
                    # 2. 执行反推 (Reverse Calculation)
                    # 份额 = 市值 / 净值
                    shares = 0.0
                    if nav > 0:
                        shares = amount / nav
                    
                    # 本金 = 市值 - 收益
                    cost = amount - profit
                    
                    # 单位成本 = 本金 / 份额
                    unit_cost = 0.0
                    if shares > 0:
                        unit_cost = cost / shares
                    
                    # 收益率
                    y_rate = 0.0
                    if cost != 0:
                        y_rate = (profit / cost) * 100
                    
                    # 3. 存入 snapshots 表
                    conn.execute('''
                        INSERT INTO snapshots (asset_id, date, amount, profit, cost, yield_rate, shares, unit_nav, is_cleared) 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(asset_id, date) DO UPDATE SET 
                        amount=excluded.amount, profit=excluded.profit, 
                        cost=excluded.cost, yield_rate=excluded.yield_rate,
                        shares=excluded.shares, unit_nav=excluded.unit_nav,
                        is_cleared=excluded.is_cleared
                    ''', (asset_id, str_date, amount, profit, cost, y_rate, shares, nav, is_clr))
                    
                    # 4. 同步更新 assets 表 (为了下次打开能继承，以及 Tab 1 显示正确)
                    # 只更新份额和单位成本，这俩是资产属性
                    conn.execute('UPDATE assets SET last_shares=?, unit_cost=? WHERE asset_id=?', (shares, unit_cost, asset_id))
                    
                    c += 1
                
                conn.commit()

                # 🔥 触发个人基金净值重算
                with st.spinner("正在重新计算个人基金历史净值..."):
                    success, msg = recalc_fund_history.recalculate_user_history(user_id)
                    if not success:
                        st.error(msg)
                    else:
                        st.toast(msg, icon="📈")
                        
                st.cache_data.clear()
                st.success(f"已保存 {c} 条记录！份额已反推，净值曲线已更新。")
                import time; time.sleep(1); st.rerun()
                
            except Exception as e:
                st.error(f"保存失败: {e}")

        # --- 9. 删除/重置 (保留) ---
        st.write(""); st.write(""); st.divider()
        exist_count = conn.execute('SELECT COUNT(*) FROM snapshots s JOIN assets a ON s.asset_id = a.asset_id WHERE s.date = ? AND a.user_id = ?', (str_date, user_id)).fetchone()[0]

        if exist_count > 0:
            with st.expander(f"🗑️ 删除/重置 【{str_date}】 的数据", expanded=False):
                if st.button("🧨 确认彻底删除", type="primary", key="btn_del_daily"):
                    conn.execute('DELETE FROM snapshots WHERE date = ? AND asset_id IN (SELECT asset_id FROM assets WHERE user_id = ?)', (str_date, user_id))
                    conn.commit()
                    st.success(f"已删除 {str_date} 记录！"); import time; time.sleep(1); st.rerun()
    
    conn.close()

def page_cashflow():
    import pandas as pd
    import plotly.express as px
    import time  # <--- 🔥 加上这一行，问题解决！
    st.header("💰 现金流与本金归集")
    st.caption("“模糊记账法”核心：只记大额进出 (外部收支)，倒推本金投入。")
    
    user_id = st.session_state.user['user_id']
    username = st.session_state.user['username'] # 获取当前用户名作为默认操作人
    conn = get_db_connection()

    # --- 1. 顶部：极简录入区 ---
    with st.container(border=True):
        st.subheader("➕ 新增记录")
        
        # 第一行：基础信息
        c1, c2, c3, c4 = st.columns([2, 2, 2, 2])
        with c1:
            record_date = st.date_input("日期", datetime.now(), key="cf_date")
        with c2:
            flow_type = st.selectbox("类型", ["📥 收入 (投入本金)", "📤 支出 (消耗本金)"], key="cf_type")
        with c3:
            amount = st.number_input("金额", min_value=0.0, step=1000.0, format="%.2f", key="cf_amt")
        with c4:
            # 🔥 新增：操作人 (默认填自己，可改)
            operator = st.text_input("操作人", value=username, key="cf_operator")

        # 第二行：类别与提交
        c5, c6 = st.columns([3, 1])
        with c5:
            if "收入" in flow_type:
                options = ["工资/奖金", "理财赎回", "其他收入"]
            else:
                options = ["信用卡/花呗账单", "房贷/房租", "大额转账", "其他大额支出"]
            category = st.selectbox("类别 (可编辑)", options, key="cf_cat") 
            
        with c6:
            st.write("")
            st.write("")
            if st.button("💾 记一笔", type="primary", use_container_width=True):
                if amount > 0:
                    real_type = "收入" if "收入" in flow_type else "支出"
                    # 🔥 修改：插入 operator
                    conn.execute('''
                        INSERT INTO cashflows (user_id, date, type, amount, category, operator, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
                    ''', (user_id, record_date.strftime('%Y-%m-%d'), real_type, amount, category, operator))
                    
                    conn.commit()
                    st.success("已记录")
                    import time
                    time.sleep(0.5)
                    st.rerun()
                else:
                    st.warning("金额需大于0")

    # --- 2. 中部：年度统计卡片 ---
    current_year = datetime.now().year
    
    # 这里的查询仅用于统计总数，简单查即可
    df_stat = pd.read_sql('''
        SELECT type, amount, date 
        FROM cashflows 
        WHERE user_id = ? 
        AND category NOT IN ('定投扣款', '内部调仓') -- 排除内部交易
    ''', conn, params=(user_id,))
    
    if not df_stat.empty:
        df_stat['date'] = pd.to_datetime(df_stat['date'])
        df_stat['year'] = df_stat['date'].dt.year
        
        df_this_year = df_stat[df_stat['year'] == current_year]
        income_year = df_this_year[df_this_year['type'] == '收入']['amount'].sum()
        expense_year = df_this_year[df_this_year['type'] == '支出']['amount'].sum()
        net_input = income_year - expense_year
        
        st.divider()
        st.markdown(f"### 📅 {current_year} 年度本金投入概览")
        k1, k2, k3 = st.columns(3)
        k1.metric("📥 本年累计大额收入", f"¥{income_year:,.2f}")
        k2.metric("📤 本年累计大额支出", f"¥{expense_year:,.2f}")
        k3.metric("🌱 本年净投入本金", f"¥{net_input:,.2f}", 
                 delta="这是你的努力存下的钱" if net_input > 0 else "本金正在流出",
                 delta_color="normal" if net_input > 0 else "inverse")

    # --- 3. 底部：数据管理 (升级版：含份额计算) ---
    st.divider()
    st.subheader("📋 历史流水明细")
    
    # 🔥 核心查询升级：关联 my_fund_history 获取当时的净值
    # 左连接 (Left Join)，因为可能有些日子还没生成净值(比如今天刚记的，还没点重算)
    df_display = pd.read_sql('''
        SELECT 
            c.id, c.date, c.type, c.amount, c.category, c.operator, c.note,
            h.unit_nav as nav_at_date
        FROM cashflows c
        LEFT JOIN my_fund_history h ON c.user_id = h.user_id AND c.date = h.date
        WHERE c.user_id = ? 
        AND c.category NOT IN ('定投扣款', '内部调仓')
        ORDER BY c.date DESC
    ''', conn, params=(user_id,))
    
    if not df_display.empty:
        df_display['date_obj'] = pd.to_datetime(df_display['date'])
        df_display['date'] = df_display['date_obj'].dt.date
        
        # 🔥 计算份额逻辑
        # 如果当天还没有净值(NaN)，暂时按 1.0 显示，或者显示空
        # 我们可以填充一个默认值 1.0，但为了严谨，最好让用户去点一下"重算净值"
        # 这里为了展示美观，若无净值则填 1.0 (IPO价格)
        df_display['nav_at_date'] = df_display['nav_at_date'].fillna(1.0)
        
        # 计算份额 = 金额 / 当日净值
        df_display['shares_calc'] = df_display['amount'] / df_display['nav_at_date']
        
        edited_df = st.data_editor(
            df_display,
            column_config={
                "id": st.column_config.NumberColumn("ID", disabled=True, width="small"),
                "date": st.column_config.DateColumn("日期", format="YYYY-MM-DD"),
                "type": st.column_config.SelectboxColumn("类型", options=["收入", "支出"], required=True, width="small"),
                "amount": st.column_config.NumberColumn("金额", format="%.2f", min_value=0),
                "operator": st.column_config.TextColumn("操作人", width="small"), # 🔥 新增列
                
                # 🔥 新增展示列 (只读，用于给用户即时反馈)
                "nav_at_date": st.column_config.NumberColumn("当日净值", format="%.4f", disabled=True, help="该日期对应的个人基金净值"),
                "shares_calc": st.column_config.NumberColumn("对应份额", format="%.2f", disabled=True, help="金额 ÷ 净值 = 买入/卖出的份额数"),
                
                "category": st.column_config.TextColumn("类别"),
                "note": st.column_config.TextColumn("备注"),
            },
            # 隐藏不需要显示的辅助列
            column_order=["date", "type", "amount", "operator", "nav_at_date", "shares_calc", "category", "note"],
            use_container_width=True,
            num_rows="dynamic",
            key="cf_editor_v2"
        )
        
        if st.button("💾 保存修改 (支持删除)", type="secondary"):
            try:
                # 1. 找出被删除的
                orig_ids = set(df_display['id'].tolist())
                new_ids = set(edited_df['id'].dropna().tolist())
                del_ids = orig_ids - new_ids
                
                for did in del_ids:
                    conn.execute("DELETE FROM cashflows WHERE id = ?", (did,))
                
                # 2. 更新/新增
                for index, row in edited_df.iterrows():
                    # 注意：nav_at_date 和 shares_calc 是计算列，不需要保存回 cashflows
                    if pd.isna(row['id']): # 新增
                         conn.execute("INSERT INTO cashflows (user_id, date, type, amount, category, operator, note) VALUES (?,?,?,?,?,?,?)",
                                      (user_id, row['date'], row['type'], row['amount'], row['category'], row['operator'], row['note']))
                    elif row['id'] in new_ids: # 修改
                         conn.execute("UPDATE cashflows SET date=?, type=?, amount=?, category=?, operator=?, note=? WHERE id=?",
                                      (row['date'], row['type'], row['amount'], row['category'], row['operator'], row['note'], row['id']))
                
                conn.commit()
                
                # 🔥 关键联动：修改现金流后，历史净值肯定变了，建议自动触发重算
                # 这里引入 recalc 模块
                import recalc_fund_history
                with st.spinner("正在因流水变动重算历史净值..."):
                    recalc_fund_history.recalculate_user_history(user_id)
                
                st.success("更新成功！历史净值已同步修正。")
                time.sleep(1)
                st.rerun()
            except Exception as e:
                st.error(f"保存失败: {e}")
    else:
        st.info("暂无记录，请在上方添加。")

    conn.close()


def get_latest_rates(conn):
    import pandas as pd  # 👈 加上这句
    """获取系统中每种货币最新的汇率 (对CNY)"""
    # 按日期降序排，去重取第一个
    df = pd.read_sql("SELECT currency, rate, date FROM exchange_rates ORDER BY date DESC", conn)
    if df.empty:
        return {}
    # drop_duplicates 默认保留第一个，也就是最新的
    return df.drop_duplicates(subset=['currency']).set_index('currency')['rate'].to_dict()


# ==============================================================================
# 🚀 核心优化：智能缓存分析函数 (PC实时算 / 树莓派存硬盘)
# ==============================================================================

# 1. 定义环境与策略
IS_RASPBERRY_PI = os.path.exists('/share') # 复用你之前的判断逻辑

if IS_RASPBERRY_PI:
    # 🍓 树莓派模式：硬盘持久化，永不过期 (除非手动点刷新)
    # 这样重启 Streamlit 后依然秒开
    CACHE_PARAMS = {
        "persist": "disk", 
        "ttl": None, 
        "show_spinner": "正在从硬盘读取历史数据 (树莓派模式)..."
    }
else:
    # 💻 PC 开发模式：ttl=0 等于不缓存/立即过期
    # 每次刷新都重新计算，方便你调试代码或数据
    CACHE_PARAMS = {
        "persist": None, 
        "ttl": 0, 
        "show_spinner": "正在实时计算 (PC开发模式)..."
    }

# 2. 应用动态参数
@st.cache_data(**CACHE_PARAMS)
def get_cached_analytics_data(user_id):
    """
    替代原来的 process_analytics_data，增加了智能缓存机制
    """
    # 延迟加载重型库
    import pandas as pd
    import sqlite3
    
    # 函数内部建立连接 (因为连接对象不能被缓存)
    local_conn = sqlite3.connect(DB_FILE)
    
    try:
        # --- 原有逻辑开始 ---
        # 1. 获取基础数据
        df_raw = pd.read_sql('''
            SELECT s.date, s.asset_id, s.amount, s.profit, s.cost, s.yield_rate, a.name, a.currency, a.type
            FROM snapshots s
            JOIN assets a ON s.asset_id = a.asset_id
            WHERE a.user_id = ?
        ''', local_conn, params=(user_id,))

        if df_raw.empty:
            return None, None

        df_raw['date'] = pd.to_datetime(df_raw['date'])
        
        # 2. 获取汇率表
        df_rates = pd.read_sql("SELECT date, currency, rate FROM exchange_rates", local_conn)
        df_rates['date'] = pd.to_datetime(df_rates['date'])
        
        # 3. 汇率匹配与折算
        df_merged = pd.merge(df_raw, df_rates, on=['date', 'currency'], how='left')
        
        df_merged['rate'] = df_merged.apply(
            lambda row: 1.0 if row['currency'] == 'CNY' else row['rate'], axis=1
        )
        df_merged['rate'] = df_merged['rate'].fillna(1.0)
        
        df_merged['amount_cny'] = df_merged['amount'] * df_merged['rate']
        df_merged['profit_cny'] = df_merged['profit'] * df_merged['rate']
        df_merged['cost_cny'] = df_merged['cost'] * df_merged['rate']
        
        # 4. 获取标签 (🔥 恢复全量查询，不在这里剔除，以免影响其他图表)
        df_tags = pd.read_sql('''
            SELECT t.tag_group, t.tag_name, atm.asset_id
            FROM tags t
            JOIN asset_tag_map atm ON t.tag_id = atm.tag_id
            WHERE t.user_id = ?
        ''', local_conn, params=(user_id,))

        # --- 🔥 准备工作：获取“已清仓”资产 ID 集合 ---
        # 仅用于下方的完整性校验逻辑
        cleared_assets_set = set()
        status_df = pd.read_sql('SELECT asset_id, is_cleared FROM snapshots ORDER BY date DESC', local_conn)
        if not status_df.empty:
            # 这里的 drop_duplicates 会保留每个 asset_id 的最新一条记录
            latest_status = status_df.drop_duplicates(subset=['asset_id'])
            # 拿到所有最新状态为 1 (已清仓) 的 ID
            cleared_assets_set = set(latest_status[latest_status['is_cleared'] == 1]['asset_id'].tolist())

        # 5. 标签聚合计算
        tag_analytics = []
        if not df_tags.empty:
            merged_tags = pd.merge(df_merged, df_tags, on='asset_id', how='inner')
            
            # 🔥 核心修改：预先计算每个标签组下【理论上应该有哪些资产 ID】
            # 变成字典：{ ('资产大类', '基金'): {1, 2, 3}, ... }
            tag_expected_ids_map = df_tags.groupby(['tag_group', 'tag_name'])['asset_id'].apply(set).to_dict()
            
            grouped = merged_tags.groupby(['date', 'tag_group', 'tag_name'])
            
            for name, group in grouped:
                date, tag_group, tag_name = name
                total_amount = group['amount_cny'].sum()
                total_profit = group['profit_cny'].sum()
                total_cost = group['cost_cny'].sum()
                weighted_yield = (total_profit / total_cost * 100) if total_cost != 0 else 0.0
                
                # --- 🔥 微调后的校验逻辑 ---
                # 1. 理论应有的资产 ID 集合
                expected_ids = tag_expected_ids_map.get((tag_group, tag_name), set())
                # 2. 实际当日录入的资产 ID 集合
                current_ids = set(group['asset_id'])
                
                # 3. 计算缺失的 ID
                missing_ids = expected_ids - current_ids
                
                # 4. 关键一步：从缺失名单中，剔除掉那些“已清仓”的
                # 如果缺失的资产本来就是已清仓的，那就不算缺失
                real_missing_ids = missing_ids - cleared_assets_set
                
                tag_analytics.append({
                    'date': date, 'tag_group': tag_group, 'tag_name': tag_name,
                    'amount': total_amount, 'profit': total_profit, 'cost': total_cost,
                    'yield_rate': weighted_yield, 
                    # 只有当【真正】缺失的数量为 0 时，才算完整
                    'is_complete': len(real_missing_ids) == 0,
                    'missing_count': len(real_missing_ids)
                })
                
        df_tags_agg = pd.DataFrame(tag_analytics)
        
        # 构造返回
        df_final_assets = df_merged.copy()
        df_final_assets['amount'] = df_final_assets['amount_cny']
        df_final_assets['profit'] = df_final_assets['profit_cny']
        df_final_assets['cost'] = df_final_assets['cost_cny']
        
        return df_final_assets, df_tags_agg
        
    finally:
        local_conn.close()

# --- 包装函数：带缓存的指数获取 ---
@st.cache_data(ttl=3600*12)  # 缓存控制依然在 UI 层
def get_market_index_data_cached(index_name, start_date_str, end_date_str):
    # 调用 DataProvider 的纯逻辑方法
    return DataProvider.get_market_index_data(index_name, start_date_str, end_date_str)

# --- 新版看板页面 ---
def page_dashboard():
    import pandas as pd
    import plotly.express as px
    import plotly.graph_objects as go
    import numpy as np
    from datetime import datetime, timedelta
    
    st.header("📊 个人基金驾驶舱(长流基金)")
    user_id = st.session_state.user['user_id']
    conn = get_db_connection()

    # ==========================================
    # 1. 顶部核心指标
    # ==========================================
    df_fund = pd.read_sql('SELECT * FROM my_fund_history WHERE user_id = ? ORDER BY date ASC', conn, params=(user_id,))
    
    if not df_fund.empty:
        df_fund['date'] = pd.to_datetime(df_fund['date'])
        latest = df_fund.iloc[-1]
        
        # 计算累计收益率 (百分比)
        total_ret_pct = (latest['unit_nav'] - 1.0) * 100
        
        # 布局改为 5 列
        k1, k2, k3, k4, k5 = st.columns(5)
        
        with k1:
            st.metric("当前总资产", f"¥{latest['total_assets']/10000:,.2f}万", 
                      help="当前个人基金的总市值")
        with k2:
            st.metric("持有收益", f"¥{latest['accumulated_profit']:,.2f}")
        with k3:
            st.metric("最新净值", f"{latest['unit_nav']:.4f}", 
                      delta=f"{latest['daily_return']*100:.2f}% (最新)", 
                      delta_color="normal")
        with k4:
            st.metric("累计收益率", f"{total_ret_pct:.2f}%", 
                      help="单位净值相对于 1.0 的涨幅")
        with k5:
            st.metric("历史最大回撤", f"{df_fund['drawdown'].min()*100:.2f}%", 
                      delta_color="inverse")
        
        st.divider()
    else:
        st.info("⏳ 暂无基金净值数据，请先去【数据录入】保存一次快照。")
        conn.close()
        return
    
    # ==========================================
    # 2. 准备详细资产数据
    # ==========================================
    df_assets, df_tags = get_cached_analytics_data(user_id)
    
    # ==========================================
    # 3. 功能标签页
    # ==========================================
    tab1, tab2, tab3 = st.tabs(["🚀 净值与回撤", "📈 结构对比", "🍰 每日透视"])
    
   
    # --- Tab 1: 基金净值与回撤 (全图表优化版) ---
    with tab1:
        if not df_fund.empty:
            # 1. 顶部通用时间筛选
            st.write("⏱️ **统计周期选择**")
            period_map = {
                "近1月": 30, "近3月": 90, "近6月": 180, 
                "近1年": 365, "近3年": 365*3, "近5年": 365*5, "成立以来": 99999
            }
            sel_period = st.radio("统计周期", list(period_map.keys()), index=3, horizontal=True, label_visibility="collapsed", key="dash_period_sel")
            
            # 2. 数据切片
            days = period_map[sel_period]
            end_date = df_fund['date'].max()
            start_date_limit = end_date - timedelta(days=days)
            
            if sel_period == "成立以来":
                df_slice = df_fund.copy()
            else:
                df_slice = df_fund[df_fund['date'] >= start_date_limit].copy()

            if df_slice.empty:
                st.warning("所选周期内无数据")
            else:
                # 定义通用的 X 轴样式配置 (复用代码)
                common_xaxis_config = dict(
                    title="日期",
                    tickformat="%Y年%m月%d日", 
                    tickmode='array',
                    tickvals=[df_slice['date'].min(), df_slice['date'].max()], # 仅显示首尾
                    tickangle=0,
                    ticklabelmode='period',
                    range=[
                        df_slice['date'].min() - pd.Timedelta(days=1), 
                        df_slice['date'].max() + pd.Timedelta(days=3)
                    ]
                )

                # === A. 第一排：总资产 & 持有收益 ===
                c_top1, c_top2 = st.columns(2)
                
                with c_top1:
                    st.subheader("总资产变化")
                    fig_asset = go.Figure()
                    fig_asset.add_trace(go.Scatter(
                        x=df_slice['date'], 
                        y=df_slice['total_assets'] / 10000, 
                        mode='lines', name='总资产',
                        line=dict(width=2, color='#2980B9'),
                        fill='tozeroy',
                        fillcolor='rgba(41, 128, 185, 0.2)',
                        hovertemplate='日期: %{x|%Y年%m月%d日}<br>总资产: %{y:.2f} 万元<extra></extra>'
                    ))
                    fig_asset.update_layout(
                        hovermode="x unified", height=350, margin=dict(t=10),
                        yaxis=dict(title="金额 (万元)", tickformat=",.2f"),
                        xaxis=common_xaxis_config # 应用通用配置
                    )
                    st.plotly_chart(fig_asset, use_container_width=True)

                with c_top2:
                    st.subheader("持有收益变化")
                    fig_profit = go.Figure()
                    fig_profit.add_trace(go.Scatter(
                        x=df_slice['date'], y=df_slice['accumulated_profit'],
                        mode='lines', name='持有收益',
                        line=dict(width=2, color='#E74C3C'), # 红色
                        fill='tozeroy', # 增加填充，风格统一
                        fillcolor='rgba(231, 76, 60, 0.2)', # 淡红色背景
                        hovertemplate='日期: %{x|%Y年%m月%d日}<br>持有收益: %{y:,.2f} 元<extra></extra>'
                    ))
                    fig_profit.update_layout(
                        hovermode="x unified", height=350, margin=dict(t=10),
                        yaxis=dict(title="金额 (元)", tickformat=",.2f"),
                        xaxis=common_xaxis_config # 应用通用配置
                    )
                    st.plotly_chart(fig_profit, use_container_width=True)

                st.divider()

                # === B. 第二排：业绩走势 & 回撤修复 ===
                
                nav_start = df_slice.iloc[0]['unit_nav']
                nav_end = df_slice.iloc[-1]['unit_nav']
                period_return = (nav_end - nav_start) / nav_start
                return_color = "red" if period_return >= 0 else "green"
                return_sign = "+" if period_return >= 0 else ""

                c_chart1, c_chart2 = st.columns(2)
                
                with c_chart1:
                    # 标题栏：左边标题，右边放个小的下拉框
                    h_col1, h_col2 = st.columns([2, 1])
                    with h_col1:
                        st.subheader("业绩走势")
                    with h_col2:
                        benchmark_name = st.selectbox(
                            "🆚 对比基准", 
                            ["(无)", "沪深300", "纳斯达克100", "标普500"], 
                            index=3,
                            label_visibility="collapsed",
                            key="bench_sel"
                        )

                    # 显示涨跌幅文本
                    st.markdown(f"区间涨跌: <span style='color:{return_color}; font-weight:bold; font-size:1.1em'>{return_sign}{period_return*100:.2f}%</span>", unsafe_allow_html=True)
                    
                    fig_nav = px.line(df_slice, x='date', y='unit_nav', title=None)
                    
                    # 1. 个人基金曲线 (实线，红色)
                    fig_nav.update_traces(
                        showlegend=True,
                        line_color="#0E44E5", line_width=2.5, name='我的净值',
                        hovertemplate='净值: %{y:.4f}<extra></extra>'
                    )
                    
                    # 2. 对比指数曲线
                    if benchmark_name != "(无)":
                        # 获取指数数据 (时间范围稍微放宽一点，防止时区差异导致取不到起点)
                        s_str = df_slice['date'].min().strftime('%Y-%m-%d')
                        e_str = df_slice['date'].max().strftime('%Y-%m-%d')
                        
                        df_bench = get_market_index_data_cached(benchmark_name, s_str, e_str)
                        
                        if not df_bench.empty and len(df_bench) > 1:
                            # --- 核心：归一化处理 (Rebase) ---
                            # 逻辑：让指数的起点，跟我的基金起点对齐
                            my_start_nav = df_slice.iloc[0]['unit_nav'] # 我的起点净值 (e.g. 1.2)
                            bench_start_val = df_bench.iloc[0]['close'] # 指数起点点位 (e.g. 4000)
                            
                            if bench_start_val > 0:
                                # 计算归一化后的净值曲线
                                df_bench['rebased_nav'] = (df_bench['close'] / bench_start_val) * my_start_nav
                                
                                # 计算指数涨跌幅用于图例显示
                                bench_ret = (df_bench.iloc[-1]['close'] - bench_start_val) / bench_start_val
                                b_sign = "+" if bench_ret >= 0 else ""
                                
                                fig_nav.add_trace(go.Scatter(
                                    x=df_bench['date'], 
                                    y=df_bench['rebased_nav'],
                                    mode='lines',
                                    name=f'{benchmark_name} ({b_sign}{bench_ret*100:.1f}%)',
                                    line_color="#29BEF0", line_width=2.5, opacity=0.2,
                                    hovertemplate=f'{benchmark_name}: %{{y:.4f}}<extra></extra>'
                                ))
                        else:
                            st.caption(f"⚠️ 暂未获取到 {benchmark_name} 数据 (可能是网络问题或非交易日)")

                    # 基准线 1.0
                    fig_nav.add_hline(y=1.0, line_dash="solid", line_color="#ECF0F1", line_width=1)
                    
                    fig_nav.update_layout(
                        hovermode="x unified", 
                        yaxis_title="单位净值", 
                        height=380, 
                        margin=dict(t=10),
                        legend=dict(
                            orientation="h",  # 保持水平排列（两个图例并排）
                            yanchor="top", y=0.1,    # 垂直位置：图表内侧顶部
                            xanchor="right", x=0.98,   # 水平位置：图表内侧右侧
                            bgcolor="rgba(0,0,0,0)", # 白色半透明背景，遮挡下方曲线更清晰
                            bordercolor="rgba(0,0,0,0)"     # 无边框，更美观
                        ),
                        xaxis=common_xaxis_config # 复用之前的通用配置
                    )
                    st.plotly_chart(fig_nav, use_container_width=True)

                with c_chart2:
                    st.subheader("回撤修复")
                    
                    # 回撤算法
                    df_slice['rolling_max'] = df_slice['unit_nav'].cummax()
                    df_slice['period_dd'] = (df_slice['unit_nav'] - df_slice['rolling_max']) / df_slice['rolling_max']
                    
                    min_dd_val = df_slice['period_dd'].min()
                    trough_idx = df_slice['period_dd'].idxmin()
                    trough_date = df_slice.loc[trough_idx]['date']
                    trough_nav = df_slice.loc[trough_idx]['unit_nav']
                    
                    peak_val = df_slice.loc[trough_idx]['rolling_max']
                    peak_date = df_slice[(df_slice['date'] <= trough_date) & (df_slice['unit_nav'] >= peak_val)].iloc[-1]['date']
                    
                    recover_df = df_slice[(df_slice['date'] > trough_date) & (df_slice['unit_nav'] >= peak_val)]
                    repair_status = "未修复"
                    end_shade_date = df_slice['date'].max()
                    
                    if not recover_df.empty:
                        recover_date = recover_df.iloc[0]['date']
                        days_used = (recover_date - peak_date).days
                        repair_status = f"{days_used}天修复"
                        end_shade_date = recover_date
                    else:
                        repair_status = "修复中..."

                    st.markdown(f"区间最大回撤: **{min_dd_val*100:.2f}%** | 状态: **{repair_status}**")

                    fig_repair = go.Figure()
                    fig_repair.add_trace(go.Scatter(
                        x=df_slice['date'], y=df_slice['unit_nav'], 
                        mode='lines', name='净值', 
                        line=dict(color='#2980B9', width=2),
                        hovertemplate='日期: %{x|%Y年%m月%d日}<br>单位净值: %{y:.4f}<extra></extra>'
                    ))
                    
                    if abs(min_dd_val) > 0.001:
                        fig_repair.add_vrect(
                            x0=peak_date, x1=end_shade_date,
                            fillcolor="rgba(231, 76, 60, 0.2)", layer="below", line_width=0
                        )
                        fig_repair.add_trace(go.Scatter(
                            x=[trough_date], y=[trough_nav],
                            mode='markers+text',
                            text=[f"最大回撤\n{min_dd_val*100:.2f}%"],
                            textposition="bottom center",
                            marker=dict(color='red', size=8), showlegend=False,
                            hovertemplate='日期: %{x|%Y年%m月%d日}<br>最大回撤点: %{y:.4f}<extra></extra>'
                        ))
                        fig_repair.add_trace(go.Scatter(x=[peak_date], y=[peak_val], mode='markers', marker=dict(color='green', size=6), showlegend=False, hoverinfo='skip'))

                    fig_repair.update_layout(
                        showlegend=False,
                        hovermode="x unified", yaxis_title="单位净值", height=380, margin=dict(t=10),
                        xaxis=common_xaxis_config # 应用通用配置
                    )
                    st.plotly_chart(fig_repair, use_container_width=True)

    # --- Tab 2: 结构对比 (完整找回版) ---
    with tab2:
        st.subheader("📊 结构化趋势分析")
        
        # 1. 筛选与绘图控制
        c1, c2, c3 = st.columns([1, 1, 2])
        with c1:
            view_mode = st.radio("分析维度", ["按具体资产", "按标签组"], horizontal=True, key="trend_view")
        with c2:
            metric_type = st.selectbox("画图指标 (Y轴)", ["总金额 (Amount)", "持有收益 (Profit)", "收益率 (Yield %)", "占比 (Share %)"], key="trend_metric")
        with c3:
            tooltip_extras = st.multiselect("🖱️ 悬停显示额外指标", ["总金额", "持有收益", "本金", "收益率", "占比"], default=["占比", "持有收益", "收益率"], key="trend_tooltip")

        plot_df = None
        color_col = ""
        
        # 1.1 数据筛选逻辑
        if view_mode == "按具体资产":
            plot_df = df_assets.copy()
            color_col = "name"
            
            with st.expander("🔍 资产精准筛选", expanded=False):
                f_col1, f_col2, f_col3 = st.columns([2, 2, 2])
                with f_col1:
                    filter_kw = st.text_input("1. 关键字 (名称/代码)", placeholder="搜股票、基金...", key="trend_kw")
                
                # 临时查一下标签映射
                conn_temp = get_db_connection()
                df_tag_map = pd.read_sql('''
                    SELECT t.tag_group, t.tag_name, atm.asset_id 
                    FROM tags t JOIN asset_tag_map atm ON t.tag_id = atm.tag_id
                    WHERE t.user_id = ?
                ''', conn_temp, params=(user_id,))
                conn_temp.close()

                with f_col2:
                    if not df_tag_map.empty:
                        all_groups = sorted(df_tag_map['tag_group'].unique().tolist())
                        sel_filter_group = st.selectbox("2. 筛选标签组", ["(全部)"] + all_groups, key="trend_f_group")
                    else:
                        sel_filter_group = "(全部)"
                        st.selectbox("2. 筛选标签组", ["(无标签数据)"], disabled=True)
                        
                with f_col3:
                    if sel_filter_group != "(全部)" and not df_tag_map.empty:
                        available_tags = sorted(df_tag_map[df_tag_map['tag_group'] == sel_filter_group]['tag_name'].unique().tolist())
                        sel_filter_tag = st.selectbox("3. 筛选标签名", ["(全部)"] + available_tags, key="trend_f_tag")
                    else:
                        sel_filter_tag = "(全部)"
                        st.selectbox("3. 筛选标签名", ["(先选标签组)"], disabled=True)

                # 执行筛选
                valid_asset_ids = set(plot_df['asset_id'].unique())
                if sel_filter_group != "(全部)" and not df_tag_map.empty:
                    target_map = df_tag_map[df_tag_map['tag_group'] == sel_filter_group]
                    if sel_filter_tag != "(全部)":
                        target_map = target_map[target_map['tag_name'] == sel_filter_tag]
                    valid_asset_ids = valid_asset_ids.intersection(set(target_map['asset_id']))
                
                if filter_kw:
                    # 检查 name 列是否存在，防止极端情况报错
                    if 'name' in plot_df.columns:
                        kw_matched = plot_df[plot_df['name'].str.contains(filter_kw, case=False, na=False)]
                        valid_asset_ids = valid_asset_ids.intersection(set(kw_matched['asset_id']))
                
                # 最终选择框
                asset_meta = plot_df[['asset_id', 'name']].drop_duplicates()
                asset_meta = asset_meta[asset_meta['asset_id'].isin(valid_asset_ids)]
                available_names = sorted(asset_meta['name'].unique().tolist())
                
                selected_assets = st.multiselect(
                    f"4. 勾选要对比的资产 (筛选后可选 {len(available_names)} 个)",
                    options=available_names,
                    placeholder="留空则显示筛选出的【所有】资产...",
                    key="trend_final_select"
                )
                
                if selected_assets:
                    plot_df = plot_df[plot_df['name'].isin(selected_assets)]
                else:
                    plot_df = plot_df[plot_df['asset_id'].isin(valid_asset_ids)]
                
        else: # 按标签组
            if df_tags is None or df_tags.empty:
                st.warning("暂无标签数据。")
            else:
                groups = df_tags['tag_group'].unique()
                selected_group = st.selectbox("选择标签分组", groups, key="trend_group")
                plot_df = df_tags[df_tags['tag_group'] == selected_group].copy()
                color_col = "tag_name"

        # 1.2 绘制折线图
        if plot_df is not None and not plot_df.empty:
            plot_df['amt_w'] = plot_df['amount'] / 10000
            plot_df['prof_w'] = plot_df['profit'] / 10000
            plot_df['cost_w'] = plot_df['cost'] / 10000
            daily_sums = plot_df.groupby('date')['amount'].transform('sum')
            plot_df['share'] = (plot_df['amount'] / daily_sums * 100).fillna(0)

            y_col, y_unit, y_title = "amt_w", "w", "金额 (万)"
            if metric_type.startswith("持有收益"): y_col, y_unit, y_title = "prof_w", "w", "收益 (万)"
            elif metric_type.startswith("收益率"): y_col, y_unit, y_title = "yield_rate", "%", "收益率 (%)"
            elif metric_type.startswith("占比"): y_col, y_unit, y_title = "share", "%", "占比 (%)"

            custom_data_cols = ['amt_w', 'prof_w', 'cost_w', 'yield_rate', 'share']
            fig = px.line(plot_df, x='date', y=y_col, color=color_col, markers=True, custom_data=custom_data_cols)
            
            hover_html = f"<b>%{{fullData.name}}</b>: <b>{metric_type.split(' ')[0]}:%{{y:.2f}}{y_unit}</b>"
            extra_info = []
            if "总金额" in tooltip_extras: extra_info.append("💰%{customdata[0]:.2f}w")
            if "持有收益" in tooltip_extras: extra_info.append("📈%{customdata[1]:.2f}w")
            if "本金" in tooltip_extras: extra_info.append("🌱%{customdata[2]:.2f}w")
            if "收益率" in tooltip_extras: extra_info.append("🚀%{customdata[3]:.1f}%")
            if "占比" in tooltip_extras: extra_info.append("🍰%{customdata[4]:.1f}%")
            if extra_info: hover_html += "<br>" + "   ".join(extra_info)
            hover_html += "<extra></extra>"
            
            fig.update_traces(hovertemplate=hover_html)
            fig.update_layout(hovermode="x unified", yaxis_title=y_title, legend_title_text="")
            st.plotly_chart(fig, use_container_width=True)

            # 1.3 两期数据横向比对
            st.divider()
            st.subheader("两期数据横向比对")
            
            # === 🔥 核心修改开始：智能计算默认对比日期 ===
            # 1. 提取当前筛选范围内所有的有效日期 (去重并排序)
            available_dates = sorted(plot_df['date'].dt.date.unique())
            
            if not available_dates:
                st.warning("当前筛选条件下无数据。")
            else:
                # 2. 设定默认值
                # 默认 B (新) = 最新的一天
                default_d2 = available_dates[-1]
                
                # 默认 A (旧) = 最新日期的“上一条有效记录”
                # 逻辑：如果有 >=2 天的数据，取倒数第2个；否则取第1个
                if len(available_dates) > 1:
                    default_d1 = available_dates[-2]
                else:
                    default_d1 = available_dates[0]

                # 3. 设定日期选择器的范围
                valid_min = available_dates[0]
                valid_max = available_dates[-1]
                
                dc1, dc2, dc3 = st.columns([2, 2, 3])
                with dc1:
                    d1_input = st.date_input(
                        "📅 日期 A (旧)", 
                        value=default_d1, 
                        min_value=valid_min, 
                        max_value=valid_max, 
                        key="diff_d1",
                        help="默认选中最新日期的上一条有效记录"
                    )
                with dc2:
                    d2_input = st.date_input(
                        "📅 日期 B (新)", 
                        value=default_d2, 
                        min_value=valid_min, 
                        max_value=valid_max, 
                        key="diff_d2"
                    )
                with dc3:
                    diff_metric = st.radio("对比指标", ["总金额 (Amount)", "持有收益 (Profit)", "收益率 (Yield %)", "占比 (Share %)"], horizontal=True, key="diff_m")

                d1_ts = pd.Timestamp(d1_input)
                d2_ts = pd.Timestamp(d2_input)
                
                # 再次校验用户选的日期是否真的有数据 (防止用户手动选了中间的空档日)
                has_d1 = not plot_df[plot_df['date'] == d1_ts].empty
                has_d2 = not plot_df[plot_df['date'] == d2_ts].empty

                if d1_ts == d2_ts:
                    st.info("请选择两个不同的日期进行对比。")
                elif not has_d1 or not has_d2:
                    st.warning(f"所选日期无数据。请确保选中的日期 ({d1_input} 或 {d2_input}) 有资产快照记录。")
                else:
                    if "总金额" in diff_metric: val_col = "amount"; unit_suffix = "元"
                    elif "持有收益" in diff_metric: val_col = "profit"; unit_suffix = "元"
                    elif "收益率" in diff_metric: val_col = "yield_rate"; unit_suffix = "%"
                    elif "占比" in diff_metric: val_col = "share"; unit_suffix = "%"

                    df_d1 = plot_df[plot_df['date'] == d1_ts].copy(); df_d1['Period'] = d1_ts.strftime('%Y-%m-%d')
                    df_d2 = plot_df[plot_df['date'] == d2_ts].copy(); df_d2['Period'] = d2_ts.strftime('%Y-%m-%d')
                    df_viz = pd.concat([df_d1, df_d2], ignore_index=True)
                    
                    # 排序优化
                    rank_order = df_d2.sort_values(val_col, ascending=False)[color_col].tolist()
                    
                    fig_compare = px.bar(
                        df_viz, x=color_col, y=val_col, color='Period', barmode='group', 
                        category_orders={color_col: rank_order}, text_auto='.2s' if unit_suffix == "元" else '.2f'
                    )
                    
                    metric_label = diff_metric.split(' ')[0]
                    if unit_suffix == "元":
                        hover_template = f"<b>%{{x}}</b><br>📅 %{{fullData.name}}<br>{metric_label}: <b>¥%{{y:,.2f}}</b><extra></extra>"
                    else:
                        hover_template = f"<b>%{{x}}</b><br>📅 %{{fullData.name}}<br>{metric_label}: <b>%{{y:.2f}}%</b><extra></extra>"
                    fig_compare.update_traces(hovertemplate=hover_template)
                    fig_compare.update_layout(yaxis_title=diff_metric, xaxis_title="", legend_title_text="", hovermode="x unified")
                    st.plotly_chart(fig_compare, use_container_width=True)

                    # 🔥🔥 找回的列表：数据透视表 🔥🔥
                    with st.expander(f"📋 查看 {metric_label} 具体变动明细表", expanded=True):
                        df_pivot = df_viz.pivot(index=color_col, columns='Period', values=val_col).reset_index()
                        d1_str = d1_ts.strftime('%Y-%m-%d')
                        d2_str = d2_ts.strftime('%Y-%m-%d')
                        
                        df_pivot = df_pivot.fillna(0)
                        df_pivot['变动量'] = df_pivot[d2_str] - df_pivot[d1_str]
                        df_pivot = df_pivot.sort_values(d2_str, ascending=False)
                        
                        # 格式化配置
                        col_config = {
                            color_col: "名称",
                            d1_str: st.column_config.NumberColumn(f"{d1_str} (旧)", format="%.2f"),
                            d2_str: st.column_config.NumberColumn(f"{d2_str} (新)", format="%.2f"),
                            "变动量": st.column_config.NumberColumn("差值 (新-旧)", format="%.2f")
                        }
                        if unit_suffix == "%":
                            col_config[d1_str] = st.column_config.NumberColumn(f"{d1_str} (旧)", format="%.2f%%")
                            col_config[d2_str] = st.column_config.NumberColumn(f"{d2_str} (新)", format="%.2f%%")
                            col_config["变动量"] = st.column_config.NumberColumn("差值", format="%.2f%%")

                        st.dataframe(df_pivot, column_config=col_config, hide_index=True, use_container_width=True)

    # --- Tab 3: 每日透视 (保留老逻辑) ---
    with tab3:
        st.subheader("🍰 每日资产快照分析")
        control_c1, control_c2 = st.columns(2)
        with control_c1:
            default_date = df_assets['date'].max().date()
            min_date = df_assets['date'].min().date()
            selected_date_input = st.date_input("📅 选择要查看的日期", value=default_date, min_value=min_date, max_value=default_date)
            selected_date = pd.Timestamp(selected_date_input)
        
        with control_c2:
            tag_groups = list(df_tags['tag_group'].unique()) if (df_tags is not None and not df_tags.empty) else []
            dim_options = ["按具体资产"] + tag_groups
            selected_dim = st.selectbox("🔍 分析维度 (筛选标签组)", dim_options)

        st.divider()

        if selected_dim == "按具体资产":
            day_data = df_assets[df_assets['date'] == selected_date].copy()
            name_col = 'name'
        else:
            if df_tags is None: day_data = pd.DataFrame()
            else:
                day_data = df_tags[(df_tags['date'] == selected_date) & (df_tags['tag_group'] == selected_dim)].copy()
                name_col = 'tag_name'

        if day_data.empty:
            st.warning(f"📅 {selected_date_input} 当天没有录入数据。")
        else:
            day_data['amount_w'] = day_data['amount'] / 10000
            day_data['profit_w'] = day_data['profit'] / 10000
            
            day_total_amt = day_data['amount'].sum()
            day_total_profit = day_data['profit'].sum()
            total_cost = day_total_amt - day_total_profit
            
            m1, m2, m3 = st.columns(3)
            m1.metric("当日总资产", f"¥{day_total_amt/10000:,.2f}万")
            m2.metric("当日持有收益", f"¥{day_total_profit/10000:,.2f}万", delta_color="normal" if day_total_profit >= 0 else "inverse")
            m3.metric("当日综合收益率", f"{(day_total_profit/total_cost*100 if total_cost!=0 else 0):.2f}%")

            chart_c1, chart_c2 = st.columns(2)
            with chart_c1:
                fig_pie_amt = px.pie(day_data, values='amount', names=name_col, title=f"【总金额】占比 ({selected_dim})", hole=0.4, custom_data=['amount_w'])
                fig_pie_amt.update_traces(textposition='inside', textinfo='percent+label', hovertemplate='<b>%{label}</b>: 💰%{customdata[0]:.2f}万 (🍰%{percent})<extra></extra>')
                st.plotly_chart(fig_pie_amt, use_container_width=True)
            
            with chart_c2:
                if (day_data['profit'] > 0).any():
                    pos_profit_data = day_data[day_data['profit'] > 0]
                    fig_pie_prof = px.pie(pos_profit_data, values='profit', names=name_col, title=f"【正收益】贡献占比 ({selected_dim})", hole=0.4, custom_data=['profit_w'])
                    fig_pie_prof.update_traces(textposition='inside', textinfo='percent+label', hovertemplate='<b>%{label}</b>: 📈%{customdata[0]:.2f}万 (🍰%{percent})<extra></extra>')
                    st.plotly_chart(fig_pie_prof, use_container_width=True)
                else:
                    st.info("当日无正收益资产，不展示贡献图。")

            st.dataframe(day_data[[name_col, 'amount', 'profit', 'yield_rate']].sort_values('amount', ascending=False), use_container_width=True, hide_index=True)

    conn.close()

def page_investment_plans():
    import pandas as pd
    import plotly.express as px
    st.header("📅 定投计划与未来现金流")
    user_id = st.session_state.user['user_id']
    conn = get_db_connection()

    tab1, tab2 = st.tabs(["⚙️ 计划管理", "🔮 未来现金流看板"])

    # === 准备工作：获取现金类资产列表 (用于扣款来源) ===
    # 只有【现金】类型的资产才能作为扣款来源
    cash_assets = pd.read_sql(
        "SELECT asset_id, name FROM assets WHERE user_id = ? AND type = '现金'", 
        conn, params=(user_id,)
    )
    # 制作字典方便后续转换: Name -> ID
    cash_map_name_to_id = dict(zip(cash_assets['name'], cash_assets['asset_id']))
    # 制作字典: ID -> Name
    cash_map_id_to_name = dict(zip(cash_assets['asset_id'], cash_assets['name']))
    
    # 下拉框选项 (加一个空的选项表示不自动扣款)
    source_options = ["(不自动扣款)"] + cash_assets['name'].tolist()

    # === TAB 1: 计划管理 (CRUD) ===
    with tab1:
        st.caption("在这里管理你的自动定投计划。")
        
        # 1. 新增计划表单
        with st.expander("➕ 新增定投计划", expanded=True):
            
            # --- A. 准备基础数据 ---
            all_assets = pd.read_sql('SELECT asset_id, name, code, currency FROM assets WHERE user_id = ?', conn, params=(user_id,))
            
            if all_assets.empty:
                st.warning("⚠️ 请先去【资产与标签管理】页面添加至少一个资产。")
            else:
                # --- B. 筛选工具栏 ---
                st.markdown("##### 🔍 第一步：筛选目标资产")
                f_col1, f_col2, f_col3 = st.columns([2, 1, 2])
                with f_col1:
                    filter_kw = st.text_input("关键字搜索", placeholder="名称/代码...", key="plan_filter_kw")
                with f_col2:
                    all_groups = pd.read_sql("SELECT DISTINCT tag_group FROM tags WHERE user_id = ?", conn, params=(user_id,))
                    grp_list = ["(不筛选)"] + all_groups['tag_group'].tolist()
                    sel_group = st.selectbox("标签组", grp_list, key="plan_filter_group")
                with f_col3:
                    sel_tags = []
                    if sel_group != "(不筛选)":
                        t_df = pd.read_sql("SELECT tag_name FROM tags WHERE user_id=? AND tag_group=?", conn, params=(user_id, sel_group))
                        opts = ["【无此标签】"] + t_df['tag_name'].tolist()
                        sel_tags = st.multiselect("标签状态", opts, key="plan_filter_tags")

                # 筛选逻辑 (同原版)
                filtered_ids = set(all_assets['asset_id'].tolist())
                if filter_kw:
                    matched = all_assets[all_assets['name'].str.contains(filter_kw, case=False) | all_assets['code'].str.contains(filter_kw, case=False, na=False)]
                    filtered_ids = filtered_ids.intersection(set(matched['asset_id']))
                if sel_group != "(不筛选)" and sel_tags:
                    sql_labeled = '''
                        SELECT atm.asset_id, t.tag_name 
                        FROM asset_tag_map atm JOIN tags t ON atm.tag_id = t.tag_id 
                        WHERE t.user_id = ? AND t.tag_group = ?
                    '''
                    df_labeled = pd.read_sql(sql_labeled, conn, params=(user_id, sel_group))
                    target_group_ids = set()
                    if "【无此标签】" in sel_tags: target_group_ids.update(filtered_ids - set(df_labeled['asset_id']))
                    real_tags = [t for t in sel_tags if t != "【无此标签】"]
                    if real_tags: target_group_ids.update(set(df_labeled[df_labeled['tag_name'].isin(real_tags)]['asset_id']))
                    filtered_ids = filtered_ids.intersection(target_group_ids)
                
                final_assets = all_assets[all_assets['asset_id'].isin(filtered_ids)].copy()
                
                st.divider()
                st.markdown("##### 📝 第二步：设置定投参数")
                
                if final_assets.empty:
                    st.info("没有符合筛选条件的资产。")
                else:
                    c1, c2 = st.columns(2)
                    with c1:
                        sel_asset = st.selectbox(
                            f"选择定投目标 (筛选出 {len(final_assets)} 个)", 
                            options=final_assets['asset_id'], 
                            format_func=lambda x: f"{final_assets[final_assets['asset_id']==x]['name'].values[0]} ({final_assets[final_assets['asset_id']==x]['currency'].values[0]})",
                            key="plan_new_asset"
                        )
                        curr_symbol = final_assets[final_assets['asset_id']==sel_asset]['currency'].values[0]
                        amount = st.number_input(f"每次定投金额 (单位: {curr_symbol})", min_value=0.0, step=100.0, key="plan_new_amount")
                    
                    with c2:
                        freq = st.selectbox("频率", ["每周", "每月", "每天"], key="plan_new_freq")
                        exec_day = 0
                        if freq == "每周":
                            weekdays = {0:"周一", 1:"周二", 2:"周三", 3:"周四", 4:"周五", 5:"周六", 6:"周日"}
                            exec_day = st.selectbox("选择周几", options=list(weekdays.keys()), format_func=lambda x: weekdays[x], key="plan_new_day_week")
                        elif freq == "每月":
                            exec_day = st.number_input("选择每月几号", min_value=1, max_value=28, value=1, key="plan_new_day_month")

                    # 🔥 新增：选择扣款来源
                    st.write("")
                    st.markdown("##### 💳 资金来源设置")
                    col_src, _ = st.columns([1, 1])
                    with col_src:
                        sel_source_name = st.selectbox(
                            "每次定投从哪个现金账户扣款?", 
                            options=source_options,
                            help="如果选择了一个现金账户，系统会在每次定投日自动减少该账户余额，并增加目标资产持仓。",
                            key="plan_new_source"
                        )
                        # 解析 ID
                        sel_source_id = None
                        if sel_source_name != "(不自动扣款)":
                            sel_source_id = cash_map_name_to_id.get(sel_source_name)

                    st.write("") 
                    
                    if st.button("💾 保存定投计划", type="primary", key="btn_save_plan"):
                        if amount <= 0:
                            st.error("定投金额必须大于 0")
                        else:
                            try:
                                conn.execute('''
                                    INSERT INTO investment_plans (user_id, asset_id, amount, frequency, execution_day, source_asset_id)
                                    VALUES (?, ?, ?, ?, ?, ?)
                                ''', (user_id, sel_asset, amount, freq, exec_day, sel_source_id))
                                conn.commit()
                                st.success(f"✅ 已添加定投计划！")
                                st.rerun()
                            except Exception as e:
                                st.error(f"保存失败: {e}")

        # 2. 现有计划列表
        st.subheader("📋 正在运行的计划")
        
        # 🔥 修改查询：多查 source_asset_id
        plans_df = pd.read_sql('''
            SELECT p.plan_id, a.name, a.currency, p.amount, p.frequency, p.execution_day, p.is_active, p.source_asset_id
            FROM investment_plans p
            JOIN assets a ON p.asset_id = a.asset_id
            WHERE p.user_id = ?
        ''', conn, params=(user_id,))

        if not plans_df.empty:
            def format_freq(row):
                if row['frequency'] == '每天': return "每天"
                if row['frequency'] == '每周': 
                    ws = ["周一","周二","周三","周四","周五","周六","周日"]
                    return f"每周 {ws[int(row['execution_day'])]}"
                if row['frequency'] == '每月': return f"每月 {int(row['execution_day'])} 号"
                return ""

            plans_df['描述'] = plans_df.apply(format_freq, axis=1)
            
            # 🔥 核心转换：把 source_asset_id (数字) 转成 source_name (文本) 方便编辑
            # 如果 ID 找不到(比如已删除)或为空，显示 "(不自动扣款)"
            plans_df['source_name'] = plans_df['source_asset_id'].map(cash_map_id_to_name).fillna("(不自动扣款)")

            edited_plans = st.data_editor(
                plans_df,
                column_config={
                    "plan_id": st.column_config.NumberColumn("ID", disabled=True, width="small"),
                    "name": st.column_config.TextColumn("目标资产", disabled=True),
                    "currency": st.column_config.TextColumn("币种", disabled=True, width="small"),
                    "amount": st.column_config.NumberColumn("定投金额", format="%.2f"),
                    "frequency": st.column_config.TextColumn("频率", disabled=True),
                    "source_name": st.column_config.SelectboxColumn(
                        "💳 扣款来源", 
                        options=source_options,
                        width="medium",
                        required=True,
                        help="选择关联的现金账户"
                    ),
                    "is_active": st.column_config.CheckboxColumn("启用"),
                    # 隐藏不想显示的列
                    "execution_day": None, 
                    "source_asset_id": None
                },
                hide_index=True,
                use_container_width=True,
                num_rows="dynamic",
                key="plans_editor"
            )
            
            if st.button("💾 保存计划变更"):
                # --- 保存前的逆向转换 ---
                # 1. 把 source_name 变回 source_asset_id
                # 这一步很关键：用户在表格里改的是文字，我们存回数据库要是 ID
                def map_back_id(row):
                    val = row['source_name']
                    if val == "(不自动扣款)": return None
                    return cash_map_name_to_id.get(val, None) # 找不到返回 None

                edited_plans['source_asset_id'] = edited_plans.apply(map_back_id, axis=1)
                
                # 2. 剔除纯展示用的列
                # 'name', 'currency', '描述' 是展示用的
                # 'source_name' 是我们刚才辅助编辑用的，也要剔除
                cols_to_drop = ['name', 'currency', '描述', 'source_name']
                
                df_to_save = edited_plans.drop(columns=[c for c in cols_to_drop if c in edited_plans.columns])
                
                # 3. 提交保存
                if save_changes_to_db(df_to_save, plans_df, 'investment_plans', 'plan_id', user_id, fixed_cols={'user_id':user_id}):
                    st.rerun()
        else:
            st.info("暂无定投计划。")

    # === TAB 2: 现金流看板 (保持不变) ===
    with tab2:
        # 1. 计算未来现金流逻辑
        st.subheader("🗓️ 未来 30 天资金需求推演 (折合人民币)")
        
        # 获取最新汇率表
        rates_map = get_latest_rates(conn)
        
        # 获取所有启用的计划
        active_plans = pd.read_sql('''
            SELECT p.asset_id, a.name, a.currency, p.amount, p.frequency, p.execution_day
            FROM investment_plans p
            JOIN assets a ON p.asset_id = a.asset_id
            WHERE p.user_id = ? AND p.is_active = 1
        ''', conn, params=(user_id,))
        
        asset_tags = pd.read_sql('''
            SELECT atm.asset_id, t.tag_group, t.tag_name
            FROM asset_tag_map atm
            JOIN tags t ON atm.tag_id = t.tag_id
            WHERE t.user_id = ?
        ''', conn, params=(user_id,))

        if active_plans.empty:
            st.info("请先启用至少一个定投计划。")
        else:
            from datetime import datetime, timedelta
            today = datetime.now().date()
            future_days = 30
            projection_data = []

            for i in range(future_days):
                current_date = today + timedelta(days=i)
                current_weekday = current_date.weekday()
                current_day = current_date.day
                
                for _, plan in active_plans.iterrows():
                    hit = False
                    if plan['frequency'] == '每天': hit = True
                    elif plan['frequency'] == '每周' and int(plan['execution_day']) == current_weekday: hit = True
                    elif plan['frequency'] == '每月' and int(plan['execution_day']) == current_day: hit = True
                    
                    if hit:
                        raw_amt = plan['amount']
                        curr = plan['currency']
                        rate = 1.0 if curr == 'CNY' else rates_map.get(curr, 1.0)
                        cny_amt = raw_amt * rate
                        
                        projection_data.append({
                            "date": current_date,
                            "asset_id": plan['asset_id'],
                            "asset_name": plan['name'],
                            "amount_cny": cny_amt,
                            "raw_info": f"{raw_amt} {curr}"
                        })

            if not projection_data:
                st.warning("未来30天内没有匹配的定投日。")
            else:
                df_proj = pd.DataFrame(projection_data)
                
                total_needed = df_proj['amount_cny'].sum()
                col1, col2 = st.columns(2)
                col1.metric("未来 30 天总定投 (CNY)", f"¥{total_needed:,.2f}")
                col2.metric("平均每日流出 (CNY)", f"¥{total_needed/30:,.2f}")

                st.divider()

                all_groups = asset_tags['tag_group'].unique().tolist() if not asset_tags.empty else []
                dim_options = ["按具体资产"] + all_groups
                selected_dim = st.selectbox("选择分析维度 (堆叠方式)", dim_options)
                
                df_viz = df_proj.copy()
                
                if selected_dim == "按具体资产":
                    df_viz['category'] = df_viz['asset_name']
                else:
                    tags_in_group = asset_tags[asset_tags['tag_group'] == selected_dim]
                    df_viz = pd.merge(df_viz, tags_in_group, on='asset_id', how='left')
                    df_viz['tag_name'] = df_viz['tag_name'].fillna('未分类')
                    df_viz['category'] = df_viz['tag_name']

                df_agg = df_viz.groupby(['date', 'category'])['amount_cny'].sum().reset_index()
                daily_totals = df_agg.groupby('date')['amount_cny'].transform('sum')
                df_agg['share'] = (df_agg['amount_cny'] / daily_totals) * 100

                fig = px.bar(
                    df_agg, 
                    x='date', 
                    y='amount_cny', 
                    color='category',
                    title=f"未来 30 天每日定投分布 ({selected_dim}) - 折合人民币",
                    labels={'amount_cny': '金额 (CNY)', 'date': '日期', 'category': '类别'},
                    custom_data=['share'] 
                )
                fig.update_traces(hovertemplate='<b>%{fullData.name}</b>: ¥%{y:,.0f} (%{customdata[0]:.1f}%)<extra></extra>')
                fig.update_layout(hovermode="x unified", legend_title_text="")
                
                st.plotly_chart(fig, use_container_width=True)

    conn.close()

def page_rebalance():
    import pandas as pd            # 👈 加上这句
    import plotly.graph_objects as go  # 👈 加上这句
    st.header("⚖️ 投资组合再平衡助手")
    st.caption("设定你的理想资产配比，系统将计算如何调整仓位以维持风险平衡。")
    
    user_id = st.session_state.user['user_id']
    conn = get_db_connection()

    # --- 1. 选择要进行再平衡的维度 ---
    # 通常我们只对大的维度做再平衡，比如 "资产大类" (股/债/金) 或 "风险等级"
    all_groups = pd.read_sql("SELECT DISTINCT tag_group FROM tags WHERE user_id = ?", conn, params=(user_id,))
    
    if all_groups.empty:
        st.warning("请先去设置标签。")
        conn.close()
        return

    col1, col2 = st.columns([1, 2])
    with col1:
        # 默认尝试选中 "资产大类" 或 "风险等级"，如果没有就选第一个
        default_idx = 0
        groups_list = all_groups['tag_group'].tolist()
        if "资产大类" in groups_list: default_idx = groups_list.index("资产大类")
        elif "风险等级" in groups_list: default_idx = groups_list.index("风险等级")
        
        selected_group = st.selectbox("选择配置维度", groups_list, index=default_idx)

    # --- 2. 获取当前持仓数据 (Real) ---
    _, df_tags = get_cached_analytics_data(user_id)
    
    if df_tags is None or df_tags.empty:
        st.info("暂无资产数据。")
        conn.close()
        return

    # 过滤出当前维度的最新数据
    latest_date = df_tags['date'].max()
    current_portfolio = df_tags[
        (df_tags['date'] == latest_date) & 
        (df_tags['tag_group'] == selected_group)
    ].copy()
    
    total_asset_val = current_portfolio['amount'].sum() # 总资产 (CNY)

    # --- 3. 获取/设置目标配置 (Target) ---
    # 读取已保存的目标
    saved_targets = pd.read_sql(
        "SELECT tag_name, target_percentage FROM rebalance_targets WHERE user_id = ? AND tag_group = ?",
        conn, params=(user_id, selected_group)
    )
    
    # 构造编辑表格数据
    # 拿到该组下所有的标签名
    all_tags_in_group = pd.read_sql(
        "SELECT tag_name FROM tags WHERE user_id = ? AND tag_group = ?", 
        conn, params=(user_id, selected_group)
    )
    
    # 合并：标签名 + 现有目标 + 当前持仓
    # 这样即使用户还没持有某个标签的资产，也能给它设目标（准备买入）
    df_editor = pd.merge(all_tags_in_group, saved_targets, on='tag_name', how='left')
    df_editor['target_percentage'] = df_editor['target_percentage'].fillna(0.0)
    
    # 关联当前实际持仓占比，方便参考
    current_portfolio['actual_percentage'] = (current_portfolio['amount'] / total_asset_val * 100)
    df_editor = pd.merge(df_editor, current_portfolio[['tag_name', 'actual_percentage']], on='tag_name', how='left')
    df_editor['actual_percentage'] = df_editor['actual_percentage'].fillna(0.0)
    
    st.divider()
    
    c_edit, c_chart = st.columns([2, 3])
    
    with c_edit:
        st.subheader("🎯 设定目标比例")
        st.caption("请直接在表格中修改【目标占比】，总和应为 100%。")
        
        edited_df = st.data_editor(
            df_editor[['tag_name', 'target_percentage', 'actual_percentage']],
            column_config={
                "tag_name": st.column_config.TextColumn("类别", disabled=True),
                "target_percentage": st.column_config.NumberColumn("目标占比 (%)", min_value=0, max_value=100, step=1.0, required=True),
                "actual_percentage": st.column_config.NumberColumn("当前占比 (%)", disabled=True, format="%.2f%%"),
            },
            hide_index=True,
            use_container_width=True,
            key=f"rebalance_editor_{selected_group}"
        )
        
        current_sum = edited_df['target_percentage'].sum()
        if abs(current_sum - 100) > 0.01:
            st.warning(f"⚠️ 当前目标总和为 {current_sum:.2f}%，请调整至 100%。")
        else:
            if st.button("💾 保存配置", type="primary"):
                # 保存逻辑
                try:
                    conn.execute("DELETE FROM rebalance_targets WHERE user_id = ? AND tag_group = ?", (user_id, selected_group))
                    for _, row in edited_df.iterrows():
                        if row['target_percentage'] > 0:
                            conn.execute(
                                "INSERT INTO rebalance_targets (user_id, tag_group, tag_name, target_percentage) VALUES (?, ?, ?, ?)",
                                (user_id, selected_group, row['tag_name'], row['target_percentage'])
                            )
                    conn.commit()
                    st.success("配置已保存！")
                    st.rerun()
                except Exception as e:
                    st.error(f"保存失败: {e}")

    # --- 4. 计算与展示再平衡建议 ---
    if abs(current_sum - 100) <= 0.01:
        with c_chart:
            st.subheader("📊 偏差分析")
            
            # 准备绘图数据
            # 比较 Target vs Actual
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=edited_df['tag_name'],
                y=edited_df['actual_percentage'],
                name='当前实际',
                marker_color='#95A5A6'
            ))
            fig.add_trace(go.Bar(
                x=edited_df['tag_name'],
                y=edited_df['target_percentage'],
                name='理想目标',
                marker_color='#3498DB'
            ))
            fig.update_layout(barmode='group', title=f"实际 vs 目标 ({selected_group})", hovermode="x unified")
            st.plotly_chart(fig, use_container_width=True)

        st.divider()
        st.subheader("💊 再平衡操作建议")
        st.caption(f"基于当前总资产折合人民币：¥{total_asset_val:,.2f}")

        # 计算具体买卖金额
        # 逻辑：理想金额 = 总资产 * 目标% - 实际持有的金额
        # 注意：这里我们假设总资产不变（即通过卖出多的买入少的，或者用新增资金去填补）
        
        # 重新merge一下确保数据最新
        # 需要把 edited_df 里的 target_percentage 和 current_portfolio 里的 amount 结合
        # current_portfolio 可能缺某些 tag（如果还没买），所以要以 edited_df 为主
        
        # 构造一个完整的计算表
        df_calc = pd.merge(
            edited_df[['tag_name', 'target_percentage']], 
            current_portfolio[['tag_name', 'amount']], 
            on='tag_name', 
            how='left'
        )
        df_calc['amount'] = df_calc['amount'].fillna(0.0)
        
        # 核心计算
        df_calc['target_amount'] = total_asset_val * (df_calc['target_percentage'] / 100.0)
        df_calc['diff_amount'] = df_calc['target_amount'] - df_calc['amount']
        
        # 分类建议
        to_buy = df_calc[df_calc['diff_amount'] > 100].sort_values('diff_amount', ascending=False) # 忽略小额噪音
        to_sell = df_calc[df_calc['diff_amount'] < -100].sort_values('diff_amount', ascending=True)
        
        col_buy, col_sell = st.columns(2)
        
        with col_buy:
            if not to_buy.empty:
                st.success("🔵 建议买入 / 加仓")
                for _, row in to_buy.iterrows():
                    st.markdown(f"**{row['tag_name']}**: 需买入 **¥{row['diff_amount']:,.0f}**")
                    st.progress(min(1.0, row['amount'] / row['target_amount']) if row['target_amount']>0 else 0)
            else:
                st.write("✅ 无需买入")

        with col_sell:
            if not to_sell.empty:
                st.error("🔴 建议卖出 / 减仓")
                for _, row in to_sell.iterrows():
                    sell_val = abs(row['diff_amount'])
                    st.markdown(f"**{row['tag_name']}**: 需卖出 **¥{sell_val:,.0f}**")
                    # 进度条展示超配程度
                    over_ratio = (row['amount'] - row['target_amount']) / row['target_amount'] if row['target_amount']>0 else 1
                    st.progress(min(1.0, over_ratio))
            else:
                st.write("✅ 无需卖出")

    conn.close()

def page_investment_notes():
    import pandas as pd  # 👈 加上这句
    st.header("📒 投资笔记与复盘")
    st.caption("记录每一次决策的思考，构建自己的投资体系。")
    
    user_id = st.session_state.user['user_id']
    conn = get_db_connection()

    # --- 状态管理 ---
    # 我们只需要记录当前正在编辑哪一个 note_id
    if 'editing_note_id' not in st.session_state:
        st.session_state.editing_note_id = None

    # --- A. 顶部：仅用于新建笔记 ---
    # 使用 expander 收纳，显得页面更干净，想写的时候再点开
    with st.expander("✍️ 写一篇新笔记", expanded=False):
        new_title = st.text_input("标题", placeholder="例如：美股大跌，加仓机会？", key="new_note_title")
        new_content = st.text_area("正文", height=100, placeholder="记录你的分析、情绪和操作计划...", key="new_note_content")
        
        if st.button("🚀 发布笔记", type="primary"):
            if not new_title.strip():
                st.warning("标题不能为空")
            else:
                try:
                    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    conn.execute('''
                        INSERT INTO investment_notes (user_id, title, content, created_at, updated_at) 
                        VALUES (?, ?, ?, ?, ?)
                    ''', (user_id, new_title, new_content, timestamp, timestamp))
                    conn.commit()
                    st.success("发布成功！")
                    st.rerun()
                except Exception as e:
                    st.error(f"发布失败: {e}")

    st.divider()

    # --- B. 时间轴展示区 (含原地编辑逻辑) ---
    st.subheader("⏳ 笔记时间轴")
    
    notes = pd.read_sql('''
        SELECT note_id, title, content, created_at, updated_at 
        FROM investment_notes 
        WHERE user_id = ? 
        ORDER BY created_at DESC
    ''', conn, params=(user_id,))

    if notes.empty:
        st.info("还没有笔记，快去写第一篇吧！")
    else:
        for index, note in notes.iterrows():
            note_id = note['note_id']
            
            # 使用 container 模拟卡片
            with st.container(border=True):
                
                # === 判断：当前是否处于编辑模式 ===
                if st.session_state.editing_note_id == note_id:
                    # >>>>> 编辑模式界面 >>>>>
                    st.markdown("##### 📝 编辑中...")
                    
                    # 输入框 (默认值为当前笔记内容)
                    edit_title = st.text_input("标题", value=note['title'], key=f"edit_title_{note_id}")
                    edit_content = st.text_area("正文", value=note['content'], height=150, key=f"edit_content_{note_id}")
                    
                    col_save, col_cancel = st.columns([1, 5])
                    with col_save:
                        if st.button("💾 保存", type="primary", key=f"save_{note_id}"):
                            if not edit_title.strip():
                                st.warning("标题不能为空")
                            else:
                                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                conn.execute('''
                                    UPDATE investment_notes 
                                    SET title = ?, content = ?, updated_at = ? 
                                    WHERE note_id = ?
                                ''', (edit_title, edit_content, timestamp, note_id))
                                conn.commit()
                                # 退出编辑模式
                                st.session_state.editing_note_id = None
                                st.rerun()
                    
                    with col_cancel:
                        if st.button("❌ 取消", key=f"cancel_{note_id}"):
                            # 退出编辑模式，不保存
                            st.session_state.editing_note_id = None
                            st.rerun()
                    # <<<<< 编辑模式结束 <<<<<

                else:
                    # >>>>> 浏览模式界面 >>>>>
                    # 1. 标题行
                    col_title, col_time = st.columns([3, 1])
                    with col_title:
                        st.markdown(f"**{note['title']}**")
                    with col_time:
                        t_str = pd.to_datetime(note['created_at']).strftime('%Y-%m-%d %H:%M')
                        st.caption(f"📅 {t_str}")

                    # 2. 正文 (已修复换行显示问题)
                    st.markdown(note['content'].replace('\n', '  \n'))
                    
                    # 3. 底部操作栏
                    st.divider()
                    f_c1, f_c2, f_c3 = st.columns([4, 1, 1])
                    
                    with f_c1:
                        # 显示最后修改时间
                        if note['updated_at'] != note['created_at']:
                            up_str = pd.to_datetime(note['updated_at']).strftime('%Y-%m-%d %H:%M')
                            st.caption(f"📝 修改于: {up_str}")
                    
                    with f_c2:
                        # 点击编辑，更新 state，触发 rerun，下一次渲染就会进入上面的 if 分支
                        if st.button("✏️ 编辑", key=f"btn_edit_{note_id}"):
                            st.session_state.editing_note_id = note_id
                            st.rerun()
                    
                    with f_c3:
                        if st.button("🗑️ 删除", key=f"btn_del_{note_id}"):
                            conn.execute('DELETE FROM investment_notes WHERE note_id = ?', (note_id,))
                            conn.commit()
                            st.success("已删除")
                            st.rerun()
                    # <<<<< 浏览模式结束 <<<<<
    
    conn.close()

def page_fire_projection():
    import pandas as pd            # 👈 加上这句
    import plotly.graph_objects as go  # 👈 加上这句
    st.header("🔥 FIRE 财富自由展望 2.0")
    st.caption("引入通胀调节与风险区间，还原最真实的财富自由之路。")
    
    user_id = st.session_state.user['user_id']
    conn = get_db_connection()
    
    # --- 1. 获取当前总资产 (起点) ---
    rates_map = get_latest_rates(conn)
    latest_date_row = conn.execute('SELECT MAX(date) as d FROM snapshots JOIN assets ON snapshots.asset_id = assets.asset_id WHERE assets.user_id = ?', (user_id,)).fetchone()
    
    current_total_assets_cny = 0.0
    start_year = datetime.now().year
    
    if latest_date_row and latest_date_row['d']:
        latest_date = latest_date_row['d']
        rows = conn.execute('''
            SELECT s.amount, a.currency
            FROM snapshots s
            JOIN assets a ON s.asset_id = a.asset_id
            WHERE a.user_id = ? AND s.date = ?
        ''', (user_id, latest_date)).fetchall()
        
        for row in rows:
            amt = row['amount']
            curr = row['currency']
            rate = 1.0 if curr == 'CNY' else rates_map.get(curr, 1.0)
            current_total_assets_cny += amt * rate
            
    conn.close()

    # --- 2. 参数设置区域 ---
    with st.expander("🛠️ 核心参数设定", expanded=True):
        c1, c2, c3 = st.columns(3)
        with c1:
            base_amount_wan = st.number_input("当前总资产 (万 CNY)", value=float(current_total_assets_cny) / 10000.0, step=1.0, format="%.2f")
            base_amount = base_amount_wan * 10000
            
            annual_addition_wan = st.number_input("每年定投/追加 (万)", value=20.0, step=1.0)
            annual_addition = annual_addition_wan * 10000

        with c2:
            current_age = st.number_input("当前年龄", value=28, step=1)
            
            annual_rate = st.number_input("预期年化收益率 (%)", value=8.0, step=0.5, help="长期来看，标普500约 8-10%")
            
        with c3:
            inflation_rate = st.number_input("预估通胀率 (%)", value=3.0, step=0.1)
            target_monthly_expense = st.number_input("理想月生活费 (元)", value=10000, step=1000)

    st.divider()

    # --- 3. 4% 法则仪表盘 ---
    safe_withdrawal_rate = 0.04
    monthly_passive_income = (base_amount * safe_withdrawal_rate) / 12
    coverage_ratio = (monthly_passive_income / target_monthly_expense) * 100
    fire_number = (target_monthly_expense * 12) / safe_withdrawal_rate
    
    kpi1, kpi2, kpi3 = st.columns(3)
    with kpi1:
        st.metric("当前每月被动收入 (4%)", f"¥{monthly_passive_income:,.0f}", help="按4%法则提取的月安全收入")
    with kpi2:
        st.metric("生活费覆盖率", f"{coverage_ratio:.1f}%", delta=f"差 {100-coverage_ratio:.1f}%" if coverage_ratio < 100 else "已达成！", delta_color="normal" if coverage_ratio < 100 else "inverse")
        st.progress(min(1.0, coverage_ratio / 100))
    with kpi3:
        st.metric("FIRE 目标金额", f"¥{fire_number/10000:.0f}万", delta=f"当前: {base_amount/10000:.0f}万")

    st.divider()

    # --- 4. 复利与风险推演计算 ---
    years_to_project = 40
    projection_data = []
    
    curr_bal = base_amount
    curr_principal = base_amount
    
    # 初始年份数据
    projection_data.append({
        "year": start_year, "age": current_age,
        "balance": curr_bal, "balance_real": curr_bal,
        "principal": curr_principal
    })

    for i in range(1, years_to_project + 1):
        # 核心复利公式
        curr_bal = curr_bal * (1 + annual_rate / 100.0) + annual_addition
        curr_principal += annual_addition
        
        # 真实购买力 (剔除通胀)
        real_purchasing_power = curr_bal / ((1 + inflation_rate / 100.0) ** i)
        
        projection_data.append({
            "year": start_year + i, "age": current_age + i,
            "balance": curr_bal, "balance_real": real_purchasing_power,
            "principal": curr_principal
        })

    df_proj = pd.DataFrame(projection_data)
    # 单位换算为“万”
    cols_to_convert = ['balance', 'balance_real', 'principal']
    for c in cols_to_convert: df_proj[f'{c}_w'] = df_proj[c] / 10000

    # --- 5. 绘图 (Plotly) ---
    st.subheader("📈 资产推演：名义 vs 真实")
    
    fig = go.Figure()

    # A. 名义总资产
    fig.add_trace(go.Scatter(
        x=df_proj['age'], y=df_proj['balance_w'],
        mode='lines',
        name='名义预期',
        line=dict(color='#2E86C1', width=3),
        customdata=df_proj['year'],
        hovertemplate='<b>⚖️ 名义预期</b><br>年份: %{customdata}<br>资产: <b>%{y:.0f}万</b><extra></extra>'
    ))

    # B. 真实购买力
    fig.add_trace(go.Scatter(
        x=df_proj['age'], y=df_proj['balance_real_w'],
        mode='lines',
        name='真实购买力 (剔除通胀)',
        line=dict(color='#E74C3C', width=3, dash='dash'),
        customdata=df_proj['year'],
        hovertemplate='<b>🍔 真实购买力</b><br>年份: %{customdata}<br>折合现值: <b>%{y:.0f}万</b><extra></extra>'
    ))

    # C. 投入本金
    fig.add_trace(go.Scatter(
        x=df_proj['age'], y=df_proj['principal_w'],
        mode='lines',
        name='投入本金',
        line=dict(color='#95A5A6', width=2, dash='dot'),
        customdata=df_proj['year'],
        hovertemplate='🌱 累计本金: %{y:.0f}万<extra></extra>'
    ))

    fig.update_layout(
        xaxis_title="年龄", yaxis_title="金额 (万)",
        hovermode="x unified",
        legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01),
        height=500
    )
    st.plotly_chart(fig, use_container_width=True)

    # --- 6. 关键数据解读 ---
    # 找20年后的数据
    target_year_20 = df_proj.iloc[20]

    st.info(f"""
    **💡 深度解读 (20年后 / {int(target_year_20['year'])}年)：**
    
    * **账面富贵**：按照预期，20年后你的账户里会有 **{target_year_20['balance_w']:.0f}万**。
    * **真实缩水**：但在 {inflation_rate}% 的通胀下，这笔钱的购买力只相当于今天的 **{target_year_20['balance_real_w']:.0f}万**。
    * **对抗通胀**：只要【名义预期】那条蓝线跑赢了【真实购买力】红虚线，就说明你的财富在增值。
    """, icon="🧐")

    # --- 7. 数据表 ---
    with st.expander("查看详细推演数据"):
        st.dataframe(
            df_proj[['age', 'year', 'balance_w', 'balance_real_w', 'principal_w']],
            column_config={
                "age": "年龄",
                "year": "年份",
                "balance_w": st.column_config.NumberColumn("名义资产 (万)", format="%.0f"),
                "balance_real_w": st.column_config.NumberColumn("真实购买力 (万)", format="%.0f"),
                "principal_w": st.column_config.NumberColumn("累计本金 (万)", format="%.0f"),
            },
            hide_index=True,
            use_container_width=True
        )
  
# --- 备份核心逻辑 ---
def send_email_backup(filepath, settings):
    """发送带有数据库附件的邮件 (修复 SSL 关闭报错版)"""
    if not settings['email_host'] or not settings['email_user'] or not settings['email_password']:
        return False, "邮箱配置不完整"

    try:
        msg = MIMEMultipart()
        msg['Subject'] = f'【自动备份】资产数据备份 - {datetime.now().strftime("%Y-%m-%d")}'
        msg['From'] = settings['email_user']
        msg['To'] = settings['email_to'] if settings['email_to'] else settings['email_user']
        
        # 正文
        body = "这是您的个人资产管理系统数据库自动备份，请妥善保管。\n\n"
        body += f"备份时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        msg.attach(MIMEText(body, 'plain'))

        # 附件
        filename = os.path.basename(filepath)
        with open(filepath, "rb") as f:
            part = MIMEApplication(f.read(), Name=filename)
            part['Content-Disposition'] = f'attachment; filename="{filename}"'
            msg.attach(part)

        # --- 核心修改开始：手动管理连接，忽略退出错误 ---
        server = smtplib.SMTP_SSL(settings['email_host'], settings['email_port'])
        try:
            server.login(settings['email_user'], settings['email_password'])
            server.send_message(msg)
            
            # 邮件已发送成功，尝试礼貌退出，但如果报错则忽略
            try:
                server.quit()
            except Exception:
                pass  # 忽略 (-1, b'\x00\x00\x00') 这种退出错误
            
            return True, "邮件发送成功"
            
        except Exception as e:
            # 只有发送过程中的错误才是真正的失败
            return False, f"发送中断: {str(e)}"
        finally:
            # 确保连接关闭
            try:
                server.close()
            except Exception:
                pass
        # --- 核心修改结束 ---

    except Exception as e:
        return False, f"邮件准备失败: {str(e)}"

def page_ai_advisor():
    import pandas as pd
    from datetime import datetime, timedelta
    
    st.header("🤖 AI 智能投顾助理")
    st.caption("生成包含每日净值、结构变化、核心持仓的深度 Prompt，发送给 ChatGPT/Claude 进行专业诊断。")
    
    user_id = st.session_state.user['user_id']
    conn = get_db_connection()
    
    # --- 1. 获取所有有数据的日期 (用于智能推断默认时间) ---
    # 我们查 my_fund_history 表，因为这是生成报告的数据源
    df_dates = pd.read_sql('SELECT DISTINCT date FROM my_fund_history WHERE user_id = ? ORDER BY date', conn, params=(user_id,))
    
    if df_dates.empty:
        st.warning("⚠️ 暂无基金净值数据。请先去【数据录入】页保存至少两天的快照。")
        conn.close()
        return

    # 转换为 date 对象列表
    valid_dates = pd.to_datetime(df_dates['date']).dt.date.tolist()
    latest_date = valid_dates[-1] # 列表中最后一个就是最近的日期
    
    # === 🔥 核心修改：智能计算默认开始日期 ===
    # 目标：找 7 天前的那个日期
    target_date = latest_date - timedelta(days=7)
    
    default_start = target_date # 先给个初始值，下面修正
    
    # 逻辑：
    # 1. 尝试找 <= target_date 的日期中，离 target_date 最近的一个 (往前找)
    candidates_past = [d for d in valid_dates if d <= target_date]
    
    if candidates_past:
        # 如果有，取最后一个 (即最接近 target_date 的过去日期)
        default_start = candidates_past[-1]
    else:
        # 2. 如果往前找不到 (说明用户可能才用了不到7天)，那就往后找
        # 找 > target_date 且 < latest_date 的日期
        candidates_future = [d for d in valid_dates if d > target_date and d < latest_date]
        
        if candidates_future:
            # 取第一个 (即最接近 target_date 的未来日期)
            default_start = candidates_future[0]
        else:
            # 3. 如果还是找不到 (说明一共就只有 latest_date 这一天数据，或者数据非常稀疏)
            if len(valid_dates) > 1:
                # 至少取最早的那一天
                default_start = valid_dates[0]
            else:
                # 真就只有一天数据，那就没办法了
                default_start = latest_date

    # ==========================================

    # 2. 设置区域
    with st.container(border=True):
        st.subheader("🛠️ 生成配置")
        
        c1, c2 = st.columns(2)
        
        with c1:
            date_range = st.date_input(
                "1. 选择复盘时间段",
                value=(default_start, latest_date), # 使用计算出的智能日期
                max_value=latest_date,
                help="默认选中最近一次快照的一周前（自动修正为有效日期）"
            )
        
        with c2:
            # 获取所有标签组
            all_groups = pd.read_sql("SELECT DISTINCT tag_group FROM tags WHERE user_id = ?", conn, params=(user_id,))
            group_opts = ["按具体资产"] + all_groups['tag_group'].tolist()
            
            selected_dim = st.selectbox(
                "2. 选择结构分析维度", 
                group_opts,
                index=0,
                help="AI 将对比期初和期末，该维度下各分类的资金占比变化。"
            )

        st.info("💡 **提示**：系统将提取选中时间段内的**每日净值走势**、**期初/期末持仓结构对比**以及**期末核心持仓明细**，组合成专业的 Prompt 发送到你的邮箱。")
        
        if st.button("🚀 生成并发送 AI Prompt 到邮箱", type="primary"):
            # 校验日期
            if isinstance(date_range, tuple) and len(date_range) == 2:
                start_d, end_d = date_range
                if start_d >= end_d:
                    st.error("开始日期必须早于结束日期。")
                elif start_d not in valid_dates or end_d not in valid_dates:
                    # 虽然 date_input 限制了 max_value，但用户选中间空档期可能会导致查询为空
                    # 这里做一个软提醒，其实 generate 函数里也有判空处理
                    st.warning("注意：所选日期如果没有对应的快照数据，AI 可能无法分析准确。")
                    with st.spinner("正在提取每日数据、计算结构变化、组装 Prompt..."):
                        s_str = start_d.strftime('%Y-%m-%d')
                        e_str = end_d.strftime('%Y-%m-%d')
                        success, msg = generate_and_send_ai_prompt(user_id, s_str, e_str, selected_dim)
                        if success:
                            st.success(f"✅ {msg}")
                            st.balloons()
                        else:
                            st.error(f"❌ {msg}")
                else:
                    with st.spinner("正在提取每日数据、计算结构变化、组装 Prompt..."):
                        s_str = start_d.strftime('%Y-%m-%d')
                        e_str = end_d.strftime('%Y-%m-%d')
                        success, msg = generate_and_send_ai_prompt(user_id, s_str, e_str, selected_dim)
                        if success:
                            st.success(f"✅ {msg}")
                            st.balloons()
                        else:
                            st.error(f"❌ {msg}")
            else:
                st.error("请选择完整的开始和结束日期。")

    conn.close()

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
        
        # 将每日数据转换为 CSV 格式字符串，方便 AI 读取
        # 为了节省 token，保留 2 位小数
        csv_trend_str = df_trend.to_csv(index=False, float_format='%.4f')

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
            
            # 计算占比
            df_end_assets['ratio'] = df_end_assets['amount'] / total_end_val
            # 筛选 > 0.5%
            key_assets = df_end_assets[df_end_assets['ratio'] > 0.005].sort_values('amount', ascending=False)
            
            lines = []
            lines.append(f"当前总资产: {total_end_val:,.2f}")
            lines.append("占比超过 0.5% 的核心资产列表：")
            for _, row in key_assets.iterrows():
                curr_txt = f"({row['currency']})" if row['currency'] != 'CNY' else ""
                profit_txt = f"浮盈 {row['profit']:,.0f}" if row['profit'] > 0 else f"浮亏 {row['profit']:,.0f}"
                lines.append(f"- **{row['name']}**{curr_txt}: ¥{row['amount']:,.0f} (占比 {row['ratio']*100:.2f}%) | {profit_txt}")
            
            top_holdings_str = "\n".join(lines)

        # --- 5. 组装 Prompt (Prompt Engineering) ---
        prompt_content = f"""
===== AI 投资顾问提示词 (请复制以下内容发送给 ChatGPT/Claude) =====

# Role / 角色设定
**你是一位拥有 20 年经验的专业基金投资顾问 (CIO 级别)。**
你的专长是基于详实的数据，对个人投资者的投资组合进行**归因分析**、**风险评估**和**策略建议**。
你即关注宏观周期的影响，也关注微观持仓的结构健康度。你的分析风格客观、理性，且善于发现数据背后的隐患或机会。

# Context / 分析背景
- **分析周期**: {start_date_str} 至 {end_date_str}
- **统计维度**: {dimension_group}

# Data Section / 投资组合数据

## 1. 每日净值与收益趋势 (Daily Trend CSV)
*数据列说明: Date(日期), TotalAssets(总资产), AccumulatedProfit(累计持有收益), UnitNav(单位净值)*
```csv
{csv_trend_str}

```

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

================================
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
        
        return True, "Prompt 已发送至邮箱！"

    except Exception as e:
        import traceback
        traceback.print_exc()
        return False, f"生成失败: {str(e)}"
    finally:
        conn.close()

def perform_backup(manual=False):
    """执行备份：1.本地复制 2.发送邮件 3.更新时间"""
    conn = get_db_connection()
    settings = conn.execute('SELECT * FROM system_settings WHERE id = 1').fetchone()
    
    # 1. 准备目录
    backup_dir = "backups"
    if not os.path.exists(backup_dir):
        os.makedirs(backup_dir)
        
    # 2. 生成本地备份文件
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"asset_tracker_{timestamp}.db"
    backup_path = os.path.join(backup_dir, filename)
    
    try:
        # 为了防止复制时数据库正在写入，虽然 sqlite 允许读时复制，但稳妥起见我们用 connection 的 backup API 或者简单 copy
        # 简单 copy 对于单用户系统通常足够
        shutil.copy2(DB_FILE, backup_path)
        
        log_msg = f"本地备份已保存: {filename}"
        email_status = "未配置邮件"
        
        # 3. 发送邮件
        if settings['email_host']:
            success, msg = send_email_backup(backup_path, settings)
            email_status = "邮件已发送" if success else f"邮件失败: {msg}"
        
        # 4. 更新上次备份时间
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        conn.execute('UPDATE system_settings SET last_backup_at = ? WHERE id = 1', (now_str,))
        conn.commit()
        
        conn.close()
        return True, f"{log_msg} | {email_status}"
    
    except Exception as e:
        conn.close()
        return False, f"备份出错: {e}"

def auto_backup_check():
    """在 App 启动/运行时被动检查是否需要备份"""
    conn = get_db_connection()
    try:
        row = conn.execute('SELECT backup_frequency, last_backup_at FROM system_settings WHERE id = 1').fetchone()
        if not row: return

        freq = row['backup_frequency']
        last_at = row['last_backup_at']
        
        if freq == '关闭':
            return
            
        should_backup = False
        now = datetime.now()
        
        if not last_at:
            should_backup = True
        else:
            last_date = datetime.strptime(last_at, '%Y-%m-%d %H:%M:%S')
            delta = now - last_date
            
            if freq == '每天' and delta.days >= 1:
                should_backup = True
            elif freq == '每周' and delta.days >= 7:
                should_backup = True
            elif freq == '每月' and delta.days >= 30:
                should_backup = True
        
        if should_backup:
            # 执行备份 (不阻塞 UI 太久，使用 toast 提示)
            st.toast("正在后台执行自动备份...", icon="⏳")
            success, msg = perform_backup(manual=False)
            if success:
                st.toast(f"自动备份完成！\n{msg}", icon="✅")
            else:
                st.error(f"自动备份失败: {msg}")
                
    except Exception as e:
        print(f"Auto backup check failed: {e}")
    finally:
        conn.close()

def page_settings():
    import pandas as pd
    st.header("⚙️ 系统设置与管理")
    conn = get_db_connection()
    
    # 读取当前配置
    settings = conn.execute('SELECT * FROM system_settings WHERE id = 1').fetchone()
    
    tab1, tab2, tab3 = st.tabs(["🔄 备份策略与邮箱", "📂 本地备份管理", "👥 成员管理(危险)"])
    
    # === Tab 1: 策略配置 (保持不变) ===
    with tab1:
        st.subheader("1. 自动备份策略")
        with st.form("settings_form"):
            new_freq = st.radio("备份频率", ["关闭", "每天", "每周", "每月"], 
                              index=["关闭", "每天", "每周", "每月"].index(settings['backup_frequency']),
                              horizontal=True)
            st.divider()
            st.subheader("2. 邮箱推送设置")
            c1, c2 = st.columns(2)
            with c1:
                email_host = st.text_input("SMTP 服务器", value=settings['email_host'] or "")
                email_port = st.number_input("SMTP 端口", value=settings['email_port'] or 465)
            with c2:
                email_user = st.text_input("邮箱账号", value=settings['email_user'] or "")
                email_password = st.text_input("授权码/密码", value=settings['email_password'] or "", type="password")
            email_to = st.text_input("接收邮箱", value=settings['email_to'] or "")
            if st.form_submit_button("💾 保存配置"):
                conn.execute('''UPDATE system_settings SET backup_frequency=?, email_host=?, email_port=?, email_user=?, email_password=?, email_to=? WHERE id=1''', (new_freq, email_host, email_port, email_user, email_password, email_to))
                conn.commit()
                st.success("配置已保存！")
                st.rerun()

    # === Tab 2: 本地管理 (保持不变) ===
    with tab2:
        st.subheader("📂 本地备份文件管理")
        if st.button("🚀 立即手动备份"):
            success, msg = perform_backup(manual=True)
            if success: st.success(msg); st.rerun()
            else: st.error(msg)
        # ... (此处省略部分展示代码，假设你已经有了) ...

    # === Tab 3: 成员管理 (修复版) ===
    with tab3:
        st.subheader("💀 危险区域：删除成员")
        st.warning("注意：此操作不可逆！将删除该成员名下的所有资产、记录和笔记。")
        
        # 1. 获取所有用户
        all_users = conn.execute('SELECT user_id, username FROM users').fetchall()
        user_options = {u['username']: u['user_id'] for u in all_users}
        
        if not user_options:
            st.info("暂无用户。")
        else:
            # 2. 选择用户
            # 注意：加上 key，防止切换 tab 时状态丢失
            target_username = st.selectbox(
                "选择要移除的成员", 
                options=list(user_options.keys()),
                key="sel_user_to_del_fixed"
            )
            
            # --- 核心修复：使用 checkbox 而不是嵌套 button ---
            # Checkbox 有状态，勾选后一直保持 True，直到你取消勾选
            confirm_mode = st.checkbox(f"🔓 解锁删除按钮 (目标: {target_username})", key="del_unlock_checkbox")
            
            if confirm_mode:
                st.error(f"⚠️ 严重警告：你确定要彻底删除 【{target_username}】 吗？")
                st.write("该操作会连带删除：资产记录、定投计划、所有笔记。数据无法恢复！")
                
                # 真正的执行按钮
                if st.button("🧨 确认删除", type="primary", key="btn_real_delete"):
                    target_id = user_options[target_username]
                    
                    # 执行删除
                    success, msg = delete_user_fully(target_id)
                    
                    if success:
                        st.toast(f"成员 {target_username} 已被移除。", icon="✅")
                        
                        # 如果删的是当前登录的人，清空 session
                        if 'user' in st.session_state and st.session_state.user and st.session_state.user['username'] == target_username:
                            st.session_state.user = None
                        
                        # 稍微等一下让 toast 显示完，然后强制刷新页面
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error(msg)

    conn.close()

# ==============================================================================
# 🚀 主程序入口 (Main) - 动态读取用户版
# ==============================================================================
def main():
    # 1. 基础初始化
    init_db()
    auto_backup_check()

    # --- 改造核心：侧边栏用户切换器 ---
    with st.sidebar:
        st.header("个人资产管理系统")
        
        # 1. 动态获取数据库里的所有用户
        existing_users = get_all_usernames()
        
        # 2. 构造下拉菜单选项：现有用户 + 新增选项
        # 即使数据库是空的，至少会有一个“新增成员”的选项
        menu_options = existing_users + ["➕ 新增成员..."]
        
        # 3. 确定下拉框的默认选中项
        # 如果当前 session 里已经登录了用户，且该用户在列表里，就默认选中他
        # 否则默认选列表第一个
        default_index = 0
        if 'user' in st.session_state and st.session_state.user:
            current_name = st.session_state.user['username']
            if current_name in existing_users:
                default_index = existing_users.index(current_name)
        
        # 4. 显示下拉框
        selected_option = st.selectbox(
            "当前成员", 
            menu_options, 
            index=default_index,
            key="user_selector_dynamic"
        )

        # 5. 分支逻辑：是切换老用户，还是创建新用户？
        if selected_option == "➕ 新增成员...":
            st.info("👋 欢迎新成员加入！")
            new_username = st.text_input("请输入你的昵称/名字", placeholder="例如：奶奶")
            
            if st.button("确认创建并进入", type="primary"):
                if new_username.strip():
                    if new_username in existing_users:
                        st.error("这个名字已经存在啦，直接在下拉框选就行。")
                    else:
                        # 调用之前的 get_or_create 函数创建新用户
                        new_user = get_or_create_user_by_name(new_username)
                        st.session_state.user = new_user
                        st.success(f"欢迎 {new_username}！")
                        st.rerun() # 刷新页面，让新名字出现在下拉框里
                else:
                    st.warning("名字不能为空哦")
            
            # 如果正在创建新用户，就不要显示下面的导航栏了，强制暂停
            st.stop()
            
        else:
            # === 选中了现有用户 ===
            # 检查是否需要切换 session
            # 如果当前没登录，或者登录的人跟选的人不一样，就切换
            if 'user' not in st.session_state or st.session_state.user is None or st.session_state.user['username'] != selected_option:
                user_obj = get_or_create_user_by_name(selected_option) # 这里其实只起到 get 的作用
                st.session_state.user = user_obj
                st.toast(f"已切换到账户: {selected_option}", icon="👋")
                st.rerun()

        st.divider()

        # === 以下是原本的导航逻辑 (保持不变) ===
        # 只有在选中了有效用户后，才会执行到这里
        
        # A. 用户信息区
        st.caption(f"正在管理 {st.session_state.user['username']} 的资产")
        
        # B. 导航菜单
        nav_map = {
            "📊 资产看板": "nav_dashboard",
            "👍 AI 投顾": "nav_ai_advisor",  # 🔥 新增这一行
            "💰 现金流与本金": "nav_cashflow",
            "📒 投资笔记": "nav_notes",
            "🏦 资产管理": "nav_assets",
            "📝 数据录入": "nav_entry",
            "📅 定投计划": "nav_plans",
            "⚖️ 投资再平衡": "nav_rebalance",
            "🔥 FIRE推演": "nav_fire",
            "⚙️ 系统设置": "nav_settings"
        }
        
        selected_label = st.radio("功能菜单", list(nav_map.keys()))
        selected_key = nav_map[selected_label]
        
        # --- 在 main() 函数内部，侧边栏逻辑之后 ---

        # 如果是 demo 账号，显示全局警告
        if 'user' in st.session_state and st.session_state.user and st.session_state.user['username'] == 'demo':
            st.warning("⚠️ **演示模式 (Demo Mode)**：当前展示数据均为 AI 随机生成的虚拟样本，仅供功能演示，非真实资产。", icon="🤖")
            # 甚至可以搞个侧边栏的气泡
            st.sidebar.info("当前处于 Demo 演示模式")

        if IS_RASPBERRY_PI:
            st.divider()
            if st.button("🔄 强制刷新数据"):
                st.cache_data.clear()
                st.toast("缓存已清除，正在重新加载...", icon="🚀")
                st.rerun()

    # === 页面路由分发 (保持不变) ===
    if selected_key == "nav_dashboard":
        page_dashboard()
    elif selected_key == "nav_cashflow":
        page_cashflow()
    elif selected_key == "nav_ai_advisor": # 🔥 新增分支
        page_ai_advisor()
    elif selected_key == "nav_notes":
        page_investment_notes()
    elif selected_key == "nav_assets":
        page_assets_tags()
    elif selected_key == "nav_entry":
        page_data_entry()
    elif selected_key == "nav_plans":
        page_investment_plans()
    elif selected_key == "nav_fire":
        page_fire_projection()
    elif selected_key == "nav_settings":
        page_settings()
    elif selected_key == "nav_rebalance":
        page_rebalance()

if __name__ == '__main__':
    main()