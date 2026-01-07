import streamlit as st
import sqlite3
from datetime import datetime
import hashlib
import os
import shutil
from pathlib import Path
import re
import calendar # 用于处理月份天数
# 在 app.py 头部引入
from streamlit import cache_data  # 如果之前没引
# ❌ 删除或注释掉这些行：
#import pandas as pd
#import plotly.express as px
#import numpy as np
#import plotly.graph_objects as go
from datetime import timedelta
import uuid
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
        # --- 修改点: SQL 查询增加 currency ---
        assets_df = pd.read_sql(
            'SELECT asset_id, name, code, type, currency, remarks FROM assets WHERE user_id = ?', 
            conn, params=(user_id,)
        )
        
        # 应用筛选 (保持不变)
        assets_df = apply_advanced_filters(assets_df, "tab1")
        
        st.caption(f"共显示 {len(assets_df)} 条资产")
        
        # --- 修改点: 配置 currency 列 ---
        edited_assets = st.data_editor(
            assets_df,
            num_rows="dynamic",
            column_config={
                "asset_id": st.column_config.NumberColumn("ID", disabled=True),
                "name": st.column_config.TextColumn("资产名称", required=True),
                "code": "代码",
                "type": st.column_config.SelectboxColumn("大类", options=["基金", "股票", "债券", "现金", "其他"]),
                # 新增币种选择
                "currency": st.column_config.SelectboxColumn(
                    "币种", 
                    options=["CNY", "USD", "HKD", "JPY", "EUR", "GBP", "BTC"],
                    required=True,
                    default="CNY",
                    width="small"
                ),
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

def page_data_entry():
    import pandas as pd  # 👈 加上这句
    st.header("📝 每日资产快照录入")
    user_id = st.session_state.user['user_id']
    conn = get_db_connection()
    
    # 日期选择
    col_date, _ = st.columns([1, 2])
    with col_date:
        date = st.date_input("选择快照日期", datetime.now())
        str_date = date.strftime('%Y-%m-%d')

    # 1. 准备基础数据 (包含币种)
    assets = pd.read_sql('SELECT asset_id, name, code, currency FROM assets WHERE user_id = ?', conn, params=(user_id,))
    
    if assets.empty:
        st.warning("暂无资产，请先去【资产与标签管理】添加资产。")
        conn.close()
        return

    # --- 2. 汇率录入区 (保持不变) ---
    if 'currency' in assets.columns:
        unique_currencies = assets['currency'].unique().tolist()
        foreign_currencies = [c for c in unique_currencies if c and c != 'CNY']
    else:
        foreign_currencies = []
    
    if foreign_currencies:
        with st.expander(f"💱 设置当日汇率 ({str_date})", expanded=True):
            st.caption("检测到您持有外币资产，请确认当日汇率（对人民币）：")
            saved_rates = pd.read_sql("SELECT currency, rate FROM exchange_rates WHERE date = ?", conn, params=(str_date,))
            saved_rate_map = dict(zip(saved_rates['currency'], saved_rates['rate']))
            cols = st.columns(len(foreign_currencies) + 1)
            rates_to_save = {}
            for i, curr in enumerate(foreign_currencies):
                default_val = saved_rate_map.get(curr, 1.0)
                with cols[i]:
                    r = st.number_input(f"{curr} ➡️ CNY", value=float(default_val), format="%.4f", key=f"rate_{curr}_{str_date}")
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

    # --- 3. 筛选与排序工具 (升级版) ---
    with st.expander("🔍 筛选与排序工具", expanded=True):
        # 第一行：核心筛选
        c1, c2, c3 = st.columns([2, 1, 2])
        with c1:
            kw = st.text_input("关键字搜索", placeholder="名称/代码")
        with c2:
            # 🔥 新增：隐藏已清仓开关 (默认开启)
            hide_cleared = st.checkbox("🙈 隐藏已清仓资产", value=True, help="勾选后，上次记录为【已清仓】的资产将不会显示在下方")
        with c3:
            all_groups = pd.read_sql("SELECT DISTINCT tag_group FROM tags WHERE user_id = ?", conn, params=(user_id,))
            grp_list = ["(不筛选)"] + all_groups['tag_group'].tolist()
            sel_group = st.selectbox("标签组", grp_list)
            
        # 第二行：标签细分与排序
        s1, s2 = st.columns([2, 2])
        with s1:
            sel_tags = []
            if sel_group != "(不筛选)":
                t_df = pd.read_sql("SELECT tag_name FROM tags WHERE user_id=? AND tag_group=?", conn, params=(user_id, sel_group))
                opts = ["【无此标签】"] + t_df['tag_name'].tolist()
                sel_tags = st.multiselect("标签名", opts)
        with s2:
            sort_option = st.radio("排序依据", ["默认 (ID)", "💰 总金额 (高→低)", "💰 总金额 (低→高)", "📈 持有收益 (高→低)"], horizontal=True)

    # --- 4. 数据预处理：获取“清仓状态” ---
    # 我们需要知道每个资产“最近一次”的状态是什么
    # 使用 SQL 窗口函数或分组取最大日期来获取每个资产最新的 is_cleared 状态
    # 这里的逻辑是：不管你选哪天录入，我们都参考该资产“也就是数据库里最新的一条记录”的状态
    
    # 先把资产ID列表拿出来
    all_asset_ids = tuple(assets['asset_id'].tolist())
    if len(all_asset_ids) == 1: str_ids = f"({all_asset_ids[0]})"
    else: str_ids = str(all_asset_ids)
    
    # 查出每个资产最近一次快照的 is_cleared 状态
    # 注意：我们要查的是“历史记录”，所以不限制日期，直接找最新的
    last_status_df = pd.read_sql(f'''
        SELECT asset_id, is_cleared 
        FROM snapshots 
        WHERE asset_id IN {str_ids}
        ORDER BY date DESC
    ''', conn)
    # 去重保留每个 asset_id 的第一条（也就是最新的）
    last_status_df = last_status_df.drop_duplicates(subset=['asset_id'])
    
    # 将最新状态合并回 assets 表
    assets = pd.merge(assets, last_status_df, on='asset_id', how='left')
    # 如果以前没记录，默认为 0 (未清仓)
    assets['is_cleared'] = assets['is_cleared'].fillna(0).astype(bool)

    # --- 5. 执行筛选 ---
    filtered_df = assets.copy()
    
    # A. 隐藏已清仓逻辑 (核心功能)
    if hide_cleared:
        # 只保留 is_cleared == False 的 (即未清仓的)
        filtered_df = filtered_df[filtered_df['is_cleared'] == False]
    
    # B. 关键字
    if kw:
        filtered_df = filtered_df[filtered_df['name'].str.contains(kw, case=False) | filtered_df['code'].str.contains(kw, case=False, na=False)]
    
    # C. 标签 (逻辑不变)
    if sel_group != "(不筛选)" and sel_tags:
        sql_labeled = '''SELECT atm.asset_id, t.tag_name FROM asset_tag_map atm JOIN tags t ON atm.tag_id = t.tag_id WHERE t.user_id = ? AND t.tag_group = ?'''
        df_labeled = pd.read_sql(sql_labeled, conn, params=(user_id, sel_group))
        target_ids = set()
        current_ids = set(filtered_df['asset_id'])
        if "【无此标签】" in sel_tags: target_ids.update(current_ids - set(df_labeled['asset_id']))
        real_tags = [t for t in sel_tags if t != "【无此标签】"]
        if real_tags: target_ids.update(set(df_labeled[df_labeled['tag_name'].isin(real_tags)]['asset_id']))
        filtered_df = filtered_df[filtered_df['asset_id'].isin(target_ids)]

    # --- 6. 准备编辑表格 ---
    if filtered_df.empty:
        st.info("没有符合条件的资产 (可能都被隐藏了，尝试取消勾选'隐藏已清仓')。")
    else:
        final_ids = tuple(filtered_df['asset_id'].tolist())
        if len(final_ids) == 1: q_ids = f"({final_ids[0]})"
        else: q_ids = str(final_ids)
        
        # 获取【选中日期】的快照数据
        # 注意：这里我们还要取 is_cleared，以便回显当天的数据
        snap_query = f'''SELECT asset_id, amount, profit, cost, yield_rate, is_cleared 
                         FROM snapshots WHERE date = ? AND asset_id IN {q_ids}'''
        
        current_snapshots = pd.read_sql(snap_query, conn, params=(str_date,))
        
        # 合并：资产基础信息 + 当日快照信息
        # 注意：这里有两个 is_cleared。
        # assets 表里的 is_cleared 是“历史最新状态”(用于筛选)，
        # current_snapshots 表里的 is_cleared 是“当天已保存的状态”(用于编辑)。
        # 我们优先使用“当天已保存的状态”，如果当天还没存，默认使用“历史最新状态”来填充（这就是所谓的继承！）
        
        merged = pd.merge(filtered_df, current_snapshots, on='asset_id', how='left', suffixes=('_last', '_today'))
        
        # 填充数值
        merged['amount'] = merged['amount'].fillna(0.0)
        merged['profit'] = merged['profit'].fillna(0.0)
        merged['yield_rate'] = merged['yield_rate'].fillna(0.0)
        
        # 核心继承逻辑：
        # 如果 _today 是 NaN (说明今天还没填)，就用 _last (上次的状态)
        # 如果 _today 有值，就用 _today
        merged['is_cleared'] = merged['is_cleared_today'].combine_first(merged['is_cleared_last'])
        # 确保是布尔值
        merged['is_cleared'] = merged['is_cleared'].astype(bool)

        # 排序
        if "总金额 (高→低)" in sort_option: merged = merged.sort_values(by='amount', ascending=False)
        elif "总金额 (低→高)" in sort_option: merged = merged.sort_values(by='amount', ascending=True)
        elif "持有收益 (高→低)" in sort_option: merged = merged.sort_values(by='profit', ascending=False)
        
        # --- 7. 显示表格 ---
        st.caption(f"当前显示: {len(merged)} 条 | 💡 勾选【🏁】列表示已清仓，下次录入时会自动隐藏")

        col_cfg = {
            "asset_id": st.column_config.NumberColumn("ID", disabled=True, width="small"),
            "name": st.column_config.TextColumn("资产名称", disabled=True),
            "code": st.column_config.TextColumn("代码", disabled=True),
            "amount": st.column_config.NumberColumn("💰 市值 (原币)", format="%.2f", required=True),
            "profit": st.column_config.NumberColumn("📈 收益 (原币)", format="%.2f", required=True),
            "cost": st.column_config.NumberColumn("本金", disabled=True, format="%.2f"),
            "yield_rate": st.column_config.NumberColumn("收益率", disabled=True, format="%.2f%%"),
            # 🔥 新增列配置
            "is_cleared": st.column_config.CheckboxColumn("🏁 清仓?", help="勾选后表示该资产已清仓"),
        }
        if 'currency' in merged.columns:
            col_cfg["currency"] = st.column_config.TextColumn("币", disabled=True, width="small")

        edited_snapshot = st.data_editor(
            merged,
            column_config=col_cfg,
            hide_index=True,
            use_container_width=True,
            # 这里的 key 很重要，加上 hide_cleared 状态，确保切换筛选时表格重绘
            key=f"entry_{str_date}_{kw}_{hide_cleared}_{sort_option}"
        )

        # --- 8. 保存逻辑 ---
        if st.button("💾 保存当前数据", type="primary"):
            try:
                c = 0
                for _, row in edited_snapshot.iterrows():
                    amt = float(row['amount'])
                    prof = float(row['profit'])
                    # 如果用户勾选了清仓，通常金额应该是0，但我们不强制改写，保留用户输入
                    is_clr = 1 if row['is_cleared'] else 0
                    
                    cost = amt - prof
                    y_rate = (prof / cost * 100) if cost != 0 else 0.0
                    
                    # 插入或更新，包含 is_cleared
                    conn.execute('''
                        INSERT INTO snapshots (asset_id, date, amount, profit, cost, yield_rate, is_cleared) 
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(asset_id, date) DO UPDATE SET 
                        amount=excluded.amount, profit=excluded.profit, 
                        cost=excluded.cost, yield_rate=excluded.yield_rate,
                        is_cleared=excluded.is_cleared
                    ''', (row['asset_id'], str_date, amt, prof, cost, y_rate, is_clr))
                    c += 1
                conn.commit()
                st.cache_data.clear()
                st.success(f"已保存 {c} 条记录！")
                # 稍微延迟一下自动刷新，让用户看到成功提示
                import time
                time.sleep(0.5)
                st.rerun()
            except Exception as e:
                st.error(f"保存失败: {e}")

        # --- [插入位置开始] ---
        st.write("")
        st.write("")
        st.divider()
        
        # 9. 删除/重置当日数据 (新增功能)
        # 先检查一下当天有没有数据，有数据才显示删除按钮
        # 这里的逻辑是：查询 user_id 下，日期为 str_date 的所有快照数量
        exist_count = conn.execute('''
            SELECT COUNT(*) FROM snapshots s
            JOIN assets a ON s.asset_id = a.asset_id
            WHERE s.date = ? AND a.user_id = ?
        ''', (str_date, user_id)).fetchone()[0]

        if exist_count > 0:
            with st.expander(f"🗑️ 删除/重置 【{str_date}】 的数据", expanded=False):
                st.warning(f"警告：检测到 {str_date} 已有 {exist_count} 条资产记录。")
                st.info("如果你是不小心录错日期（例如把昨天的录成了今天），点击下方按钮可以彻底清除今日记录。清除后，看板将不会把今天算作 0，而是直接跳过今天。")
                
                # 双重确认按钮（防止误触）
                col_del_1, col_del_2 = st.columns([1, 4])
                with col_del_1:
                    if st.button("🧨 确认彻底删除", type="primary", key="btn_delete_daily"):
                        try:
                            # 执行删除操作
                            # 逻辑：删除 snapshots 表中，属于该用户且日期为选定日期的所有记录
                            conn.execute('''
                                DELETE FROM snapshots 
                                WHERE date = ? 
                                AND asset_id IN (SELECT asset_id FROM assets WHERE user_id = ?)
                            ''', (str_date, user_id))
                            
                            conn.commit()
                            st.success(f"已成功删除 {str_date} 的所有记录！")
                            
                            # 稍微停顿一下让用户看到提示，然后刷新页面
                            import time
                            time.sleep(1)
                            st.rerun()
                            
                        except Exception as e:
                            st.error(f"删除失败: {e}")
        else:
            # 如果当天没数据，显示一个灰色的提示
            st.caption(f"📅 当前日期 {str_date} 暂无录入数据，无需删除。")
        # --- [插入位置结束] ---
    conn.close()


def page_cashflow():
    import pandas as pd
    import plotly.express as px
    
    st.header("💰 现金流与本金归集")
    st.caption("“模糊记账法”核心：只记大额进出，倒推本金投入。")
    
    user_id = st.session_state.user['user_id']
    conn = get_db_connection()

    # --- 1. 顶部：极简录入区 ---
    with st.container(border=True):
        st.subheader("➕ 新增记录")
        c1, c2, c3, c4, c5 = st.columns([2, 2, 2, 3, 1])
        
        with c1:
            record_date = st.date_input("日期", datetime.now(), key="cf_date")
        
        with c2:
            flow_type = st.selectbox("类型", ["📥 收入 (投入本金)", "📤 支出 (消耗本金)"], key="cf_type")
            
        with c3:
            amount = st.number_input("金额", min_value=0.0, step=1000.0, format="%.2f", key="cf_amt")
            
        with c4:
            # 根据类型动态改变建议选项
            if "收入" in flow_type:
                options = ["工资/奖金", "理财赎回", "其他收入"]
            else:
                options = ["信用卡/花呗账单", "房贷/房租", "大额转账", "其他大额支出"]
            category = st.selectbox("类别 (可编辑)", options, key="cf_cat") # 也可以用 text_input + suggestions
            
        with c5:
            st.write("")
            st.write("")
            if st.button("💾 记一笔", type="primary", use_container_width=True):
                if amount > 0:
                    real_type = "收入" if "收入" in flow_type else "支出"
                    conn.execute('''
                        INSERT INTO cashflows (user_id, date, type, amount, category, created_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (user_id, record_date.strftime('%Y-%m-%d'), real_type, amount, category, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
                    conn.commit()
                    st.success("已记录")
                    import time
                    time.sleep(0.5)
                    st.rerun()
                else:
                    st.warning("金额需大于0")

    # --- 2. 中部：年度统计卡片 ---
    current_year = datetime.now().year
    df_cf = pd.read_sql('''
        SELECT id, date, type, amount, category, note 
        FROM cashflows 
        WHERE user_id = ? 
        ORDER BY date DESC
    ''', conn, params=(user_id,))
    
    if not df_cf.empty:
        df_cf['date'] = pd.to_datetime(df_cf['date'])
        df_cf['year'] = df_cf['date'].dt.year
        
        # 本年度统计
        df_this_year = df_cf[df_cf['year'] == current_year]
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

    # --- 3. 底部：数据管理 (DataEditor) ---
    st.divider()
    st.subheader("📋 历史明细管理")
    
    if not df_cf.empty:
        # 为了 DataEditor 显示友好，做一点处理
        df_display = df_cf[['id', 'date', 'type', 'amount', 'category', 'note']].copy()
        df_display['date'] = df_display['date'].dt.date
        
        edited_df = st.data_editor(
            df_display,
            column_config={
                "id": st.column_config.NumberColumn("ID", disabled=True, width="small"),
                "date": st.column_config.DateColumn("日期", format="YYYY-MM-DD"),
                "type": st.column_config.SelectboxColumn("类型", options=["收入", "支出"], required=True),
                "amount": st.column_config.NumberColumn("金额", format="%.2f", min_value=0),
                "category": st.column_config.TextColumn("类别"),
                "note": st.column_config.TextColumn("备注"),
            },
            use_container_width=True,
            num_rows="dynamic",
            key="cf_editor"
        )
        
        if st.button("💾 保存修改 (支持删除)", type="secondary"):
            # 复用你的 save_changes_to_db 逻辑，或者简单写个处理
            # 这里简单写个处理 ID 的逻辑
            try:
                # 1. 找出被删除的
                orig_ids = set(df_cf['id'].tolist())
                new_ids = set(edited_df['id'].dropna().tolist())
                del_ids = orig_ids - new_ids
                
                for did in del_ids:
                    conn.execute("DELETE FROM cashflows WHERE id = ?", (did,))
                
                # 2. 更新/新增
                for index, row in edited_df.iterrows():
                    if pd.isna(row['id']): # 新增
                         conn.execute("INSERT INTO cashflows (user_id, date, type, amount, category, note) VALUES (?,?,?,?,?,?)",
                                      (user_id, row['date'], row['type'], row['amount'], row['category'], row['note']))
                    elif row['id'] in new_ids: # 修改
                         conn.execute("UPDATE cashflows SET date=?, type=?, amount=?, category=?, note=? WHERE id=?",
                                      (row['date'], row['type'], row['amount'], row['category'], row['note'], row['id']))
                
                conn.commit()
                st.success("更新成功")
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

# --- 新版看板页面 ---
def page_dashboard():
    # 👇 这里要加一大堆
    import pandas as pd
    import plotly.express as px
    import plotly.graph_objects as go
    import numpy as np
    
    # 补丁挪到这里
    if not hasattr(np, 'bool8'):
        np.bool8 = np.bool_
    st.header("📊 深度资产透视")
    user_id = st.session_state.user['user_id']
    
    #conn = get_db_connection()
    ## 处理数据
    #df_assets, df_tags = process_analytics_data(conn, user_id)
    #conn.close()

    df_assets, df_tags = get_cached_analytics_data(user_id)

    if df_assets is None or df_assets.empty:
        st.info("👋 暂无数据，请先前往【数据录入】页面添加资产快照。")
        return

# === 🔥 新增：AI 投顾入口 ===
    with st.expander("🤖 AI 智能投顾 (离线版)", expanded=False):
        c_ai_1, c_ai_2 = st.columns([3, 1])
        with c_ai_1:
            st.markdown("""
            **功能说明**：选择一个 **复盘周期**，系统将计算该期间的资产变动、最大回撤和期末持仓结构，生成专业的提示词发送给您。
            """)
            
            ac1, ac2 = st.columns(2)
            
            # 获取数据中的最早和最晚日期
            min_db_date = df_assets['date'].min().date()
            max_db_date = df_assets['date'].max().date()
            
            with ac1:
                # 🔥 改为日期范围选择器
                ai_date_range = st.date_input(
                    "📅 选择复盘周期 (开始 - 结束)",
                    value=(min_db_date, max_db_date),
                    min_value=min_db_date,
                    max_value=max_db_date,
                    help="请选择开始日期和结束日期"
                )
            
            with ac2:
                ai_tag_groups = []
                if df_tags is not None and not df_tags.empty:
                    ai_tag_groups = df_tags['tag_group'].unique().tolist()
                
                selected_ai_group = st.selectbox("📊 分析维度", options=ai_tag_groups, index=0) if ai_tag_groups else "默认"

        with c_ai_2:
            st.write(""); st.write("") 
            # 检查是否选了两个日期
            is_range_valid = isinstance(ai_date_range, tuple) and len(ai_date_range) == 2
            
            if st.button("📧 发送 Prompt", type="primary", use_container_width=True, disabled=(not ai_tag_groups or not is_range_valid)):
                if is_range_valid:
                    start_d, end_d = ai_date_range
                    with st.spinner("正在生成分析..."):
                        success, msg = generate_and_send_ai_prompt(
                            user_id, 
                            selected_ai_group, 
                            start_d.strftime('%Y-%m-%d'), 
                            end_d.strftime('%Y-%m-%d')
                        )
                        if success: st.success(msg)
                        else: st.error(msg)
                else:
                    st.warning("请在日历中选择完整的【开始】和【结束】两个日期。")

    st.divider()
    # 全局日期范围
    min_date = df_assets['date'].min().date()
    max_date = df_assets['date'].max().date()
    
    st.caption(f"数据统计范围：{min_date} ~ {max_date}")
    
    tab1, tab2, tab3, tab4 = st.tabs(["📈 趋势分析", "🍰 每日透视", "⚠️ 数据校验", "🏆 年度财富复盘"])
    
    # === TAB 1: 趋势分析 (优化版：置顶水位监控) ===
    with tab1:
        st.subheader("💰 资产净值走势")

       # =========================================================
        # 🌊 0. 全局风险与水位监控 (置顶)
        # 逻辑说明：
        # 1. 资产、回撤 -> 取决于【总资产】(Snapshot Amount)
        # 2. 累计收益   -> 取决于【真实收益】(Snapshot Amount - Cashflow Principal)
        # =========================================================
        
        # 1. 准备基础资产数据 (按日期聚合)
        daily_monitor = df_assets.groupby('date')[['amount']].sum().reset_index().sort_values('date')
        
        if not daily_monitor.empty:
            # --- A. 准备真实本金 (从 Cashflows 计算) ---
            conn_temp = get_db_connection()
            df_cf = pd.read_sql("SELECT date, type, amount FROM cashflows WHERE user_id = ?", conn_temp, params=(user_id,))
            conn_temp.close()
            
            # 默认为 0 (如果没有现金流记录)
            daily_monitor['final_principal'] = 0.0
            
            if not df_cf.empty:
                df_cf['date'] = pd.to_datetime(df_cf['date'])
                # 收入=+，支出=-
                df_cf['net_flow'] = df_cf.apply(lambda x: x['amount'] if x['type'] == '收入' else -x['amount'], axis=1)
                
                # 计算累计净投入
                df_principal = df_cf.groupby('date')['net_flow'].sum().sort_index().cumsum().reset_index()
                df_principal.rename(columns={'net_flow': 'cumulative_principal'}, inplace=True)
                
                # 合并：找到每一天资产对应的最新本金
                daily_monitor = pd.merge_asof(daily_monitor, df_principal, on='date', direction='backward')
                daily_monitor['final_principal'] = daily_monitor['cumulative_principal'].fillna(0)
            
            # --- B. 计算核心序列 ---
            # 序列1: 总资产 (用于计算水位、回撤)
            asset_series = daily_monitor.set_index('date')['amount']
            
            # 序列2: 真实累计收益 (用于计算收益创新高) = 总资产 - 现金流本金
            # 注意：这里不再使用快照里的 profit，而是实时重算
            monitor_profit_series = asset_series - daily_monitor.set_index('date')['final_principal']

            # --- C. 计算六大指标 ---
            
            # 1. 资产指标
            curr_asset = asset_series.iloc[-1]
            ath_asset = asset_series.max()
            
            # 2. 回撤指标 (基于总资产)
            rolling_max = asset_series.cummax()
            drawdown_amt = rolling_max - asset_series
            drawdown_pct = (drawdown_amt / rolling_max * 100).fillna(0.0)
            
            curr_dd_pct = drawdown_pct.iloc[-1]
            curr_dd_amt = drawdown_amt.iloc[-1]
            max_dd_pct = drawdown_pct.max()
            max_dd_amt = drawdown_amt.max()
            
            # 3. 收益指标 (基于真实收益)
            curr_profit = monitor_profit_series.iloc[-1]
            ath_profit = monitor_profit_series.max() # 历史最高累计收益

            # --- D. 界面展示 ---
            with st.container():
                m1, m2, m3, m4, m5, m6 = st.columns(6)
                
                # 1. 当前总资产
                m1.metric("当前总资产", f"¥{curr_asset/10000:,.2f}万")
                
                # 2. 历史最高 (ATH)
                m2.metric("历史最高 (ATH)", f"¥{ath_asset/10000:,.2f}万", 
                          delta=f"距高点 -{(ath_asset-curr_asset)/10000:.2f}万" if curr_asset < ath_asset else "创新高!",
                          delta_color="inverse")
                
                # 3. 当前回撤
                m3.metric("当前总资产回撤", f"{curr_dd_pct:.2f}%", 
                          delta=f"-¥{curr_dd_amt:,.0f}", 
                          delta_color="inverse")
                
                # 4. 历史最大回撤
                m4.metric("历史最大回撤", f"{max_dd_pct:.2f}%", 
                          delta=f"-¥{max_dd_amt:,.0f}",
                          delta_color="inverse")

                # 5. 当前累计收益 (修正版)
                m5.metric("当前累计收益", f"¥{curr_profit/10000:,.2f}万",
                          delta_color="normal" if curr_profit > 0 else "inverse")
                
                # 6. 历史最高收益 (修正版)
                m6.metric("历史最高收益", f"¥{ath_profit/10000:,.2f}万",
                          help="历史上【总资产 - 本金】差值的最大值")
                
            st.divider()

        # =========================================================
        # 📉 1. 视图模式选择 & 图表绘制
        # =========================================================
        
        chart_mode = st.radio(
            "📉 统计口径", 
            [
                "1. 总资产模式", 
                "2. 剔除现金 (仅看投资仓位)",
                "3. 投入本金/收益模式"
            ], 
            horizontal=True,
            help="①总资产模式: 全口径统计\n②剔除现金: 只看波动资产\n③收益模式: 重点监控【累计收益】的创新高与回撤情况"
        )
        
        # 准备画布
        fig_total = go.Figure()
        
        # ... (以下绘图逻辑保持不变，为了节省篇幅，直接复用之前的逻辑) ...
        # =========================================================
        # 模式 3：账户全貌 (基于 Cashflow 算本金)
        # =========================================================
        if "3." in chart_mode:
            # A. 准备资产总额
            daily_assets = df_assets.groupby('date')[['amount']].sum().reset_index().sort_values('date')
            
            # B. 准备本金 (Cashflows)
            conn_temp = get_db_connection()
            df_cf = pd.read_sql("SELECT date, type, amount FROM cashflows WHERE user_id = ?", conn_temp, params=(user_id,))
            conn_temp.close()
            
            use_cf_data = False
            if not df_cf.empty:
                df_cf['date'] = pd.to_datetime(df_cf['date'])
                df_cf['net_flow'] = df_cf.apply(lambda x: x['amount'] if x['type'] == '收入' else -x['amount'], axis=1)
                df_principal = df_cf.groupby('date')['net_flow'].sum().sort_index().cumsum().reset_index()
                df_principal.rename(columns={'net_flow': 'cumulative_principal'}, inplace=True)
                daily_assets = pd.merge_asof(daily_assets, df_principal, on='date', direction='backward')
                daily_assets['final_principal'] = daily_assets['cumulative_principal'].fillna(0)
                use_cf_data = True
            else:
                st.warning("⚠️ 暂无现金流，降级使用 Cost 字段。")
                temp_group = df_assets.groupby('date')['cost'].sum().reset_index()
                daily_assets = pd.merge(daily_assets, temp_group, on='date', how='left')
                daily_assets['final_principal'] = daily_assets['cost']

            # C. 计算关键指标
            daily_assets['profit'] = daily_assets['amount'] - daily_assets['final_principal']
            
            # D. 绘图
            daily_assets['p_w'] = daily_assets['final_principal'] / 10000
            daily_assets['a_w'] = daily_assets['amount'] / 10000
            daily_assets['prof_w'] = daily_assets['profit'] / 10000
            
            fig_total.add_trace(go.Scatter(x=daily_assets['date'], y=daily_assets['a_w'], name='总资产', mode='lines',fill='tozeroy', line=dict(color='#2E86C1', width=3), hovertemplate='总资产: %{y:.2f}万<extra></extra>'))
            fig_total.add_trace(go.Scatter(x=daily_assets['date'], y=daily_assets['p_w'], name='投入本金', mode='lines', line=dict(color='#95A5A6', width=2), hovertemplate='本金: %{y:.2f}万<extra></extra>'))
            fig_total.add_trace(go.Scatter(x=daily_assets['date'], y=daily_assets['prof_w'], name='累计收益', mode='lines', line=dict(color='#27AE60', width=2, dash='dot'), hovertemplate='收益: %{y:.2f}万<extra></extra>'))

        # =========================================================
        # 模式 1 & 2：经典视图 (补充了收益金额曲线)
        # =========================================================
        else:
            plot_df = df_assets.copy()
            
            # 特殊处理：剔除现金
            if "2." in chart_mode:
                if 'type' in plot_df.columns:
                    plot_df = plot_df[plot_df['type'] != '现金']
                else:
                    st.error("数据库缺少 type 字段。")

            # 聚合
            daily_simple = plot_df.groupby('date')[['amount', 'profit', 'cost']].sum().reset_index().sort_values('date')
            
            # 计算绘图数据
            daily_simple['yield_rate'] = daily_simple.apply(lambda row: (row['profit'] / row['cost'] * 100) if row['cost'] != 0 else 0.0, axis=1)
            daily_simple['amt_w'] = daily_simple['amount'] / 10000
            daily_simple['prof_w'] = daily_simple['profit'] / 10000  # 🔥 新增：收益金额(万)
            
            # 绘图
            line_color = '#2E86C1'
            
            # 1. 资产市值 (面积图)
            fig_total.add_trace(go.Scatter(
                x=daily_simple['date'], y=daily_simple['amt_w'], 
                name="资产市值", mode='lines', fill='tozeroy', 
                line=dict(color=line_color, width=2), 
                hovertemplate='市值: %{y:.2f}万<extra></extra>'
            ))
            
            # 2. 持有收益 (绿色虚线) -> 🔥 这就是你想要补充的
            fig_total.add_trace(go.Scatter(
                x=daily_simple['date'], y=daily_simple['prof_w'], 
                name='持有收益', mode='lines', 
                line=dict(color='#27AE60', width=2, dash='dot'), 
                hovertemplate='收益: %{y:.2f}万<extra></extra>'
            ))
            
            # 3. 收益率 (右轴，红色虚线)
            fig_total.add_trace(go.Scatter(
                x=daily_simple['date'], y=daily_simple['yield_rate'], 
                name='收益率', mode='lines', 
                line=dict(color='#E74C3C', width=1, dash='dot'), #稍微调细一点区分
                yaxis='y2', 
                hovertemplate='收益率: %{y:.2f}%<extra></extra>'
            ))
            
            # 配置双轴
            fig_total.update_layout(
                yaxis2=dict(
                    title=dict(text="收益率 (%)", font=dict(color="#E74C3C")), 
                    tickfont=dict(color="#E74C3C"), 
                    overlaying='y', 
                    side='right'
                )
            )
        # --- 图表布局与导出 ---
        fig_total.update_layout(
            hovermode="x unified",
            yaxis=dict(title="金额 (万元)"),
            # x=0, xanchor="left" 表示左对齐；y=1.02 表示在图表上方一点点
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
            margin=dict(l=0, r=0, t=30, b=0) # 稍微增加顶部 t 的留白，防止顶到头
        )
        st.plotly_chart(fig_total, use_container_width=True)

        st.download_button(
            label=f"📥 导出数据", 
            data=pd.DataFrame().to_csv().encode('utf-8-sig'), 
            file_name=f'trend_export.csv', 
            mime='text/csv'
        )

        st.divider()

        # --- 3. 结构化趋势详细对比 ---
        st.subheader("📊 结构化趋势详细对比")
        
        c1, c2, c3 = st.columns([1, 1, 2])
        with c1:
            view_mode = st.radio("分析维度", ["按具体资产", "按标签组"], horizontal=True, key="trend_view")
        with c2:
            metric_type = st.selectbox("画图指标 (Y轴)", ["总金额 (Amount)", "持有收益 (Profit)", "收益率 (Yield %)", "占比 (Share %)"], key="trend_metric")
        with c3:
            tooltip_extras = st.multiselect("🖱️ 悬停显示额外指标", ["总金额", "持有收益", "本金", "收益率", "占比"], default=["占比", "持有收益", "收益率"], key="trend_tooltip")

        plot_df = None
        color_col = ""
        
        
        if view_mode == "按具体资产":
            plot_df = df_assets.copy()
            color_col = "name"
            
            # --- 🔥 升级版筛选器 (关键字 + 标签组联动) ---
            st.markdown("##### 🔍 资产精准筛选")
            
            # 1. 布局：三列筛选 (关键字 | 标签组 | 标签名)
            f_col1, f_col2, f_col3 = st.columns([2, 2, 2])
            
            with f_col1:
                # 1. 关键字输入
                filter_kw = st.text_input("1. 关键字 (名称/代码)", placeholder="搜股票、基金...", key="trend_kw")
            
            # 准备标签数据 (需要临时连接查一下最新的标签关系)
            conn_temp = get_db_connection()
            try:
                # 查出所有标签及其关联的资产ID
                df_tag_map = pd.read_sql('''
                    SELECT t.tag_group, t.tag_name, atm.asset_id 
                    FROM tags t
                    JOIN asset_tag_map atm ON t.tag_id = atm.tag_id
                    WHERE t.user_id = ?
                ''', conn_temp, params=(user_id,))
            finally:
                conn_temp.close()

            with f_col2:
                # 2. 标签组选择
                if not df_tag_map.empty:
                    all_groups = sorted(df_tag_map['tag_group'].unique().tolist())
                    sel_filter_group = st.selectbox("2. 筛选标签组", ["(全部)"] + all_groups, key="trend_f_group")
                else:
                    sel_filter_group = "(全部)"
                    st.selectbox("2. 筛选标签组", ["(无标签数据)"], disabled=True, key="trend_f_group_empty")
                    
            with f_col3:
                # 3. 标签名选择 (根据选中的组动态变化)
                if sel_filter_group != "(全部)" and not df_tag_map.empty:
                    available_tags = sorted(df_tag_map[df_tag_map['tag_group'] == sel_filter_group]['tag_name'].unique().tolist())
                    sel_filter_tag = st.selectbox("3. 筛选标签名", ["(全部)"] + available_tags, key="trend_f_tag")
                else:
                    sel_filter_tag = "(全部)"
                    st.selectbox("3. 筛选标签名", ["(先选标签组)"], disabled=True, key="trend_f_tag_disabled")

            # --- 2. 执行筛选逻辑 (求交集：AND 关系) ---
            # 初始候选池：所有历史出现过的资产ID
            valid_asset_ids = set(plot_df['asset_id'].unique())

            # A. 标签筛选
            if sel_filter_group != "(全部)" and not df_tag_map.empty:
                # 找出符合组的资产ID
                target_map = df_tag_map[df_tag_map['tag_group'] == sel_filter_group]
                if sel_filter_tag != "(全部)":
                    target_map = target_map[target_map['tag_name'] == sel_filter_tag]
                
                tag_matched_ids = set(target_map['asset_id'])
                # 求交集：既要在历史数据里，又得符合标签
                valid_asset_ids = valid_asset_ids.intersection(tag_matched_ids)
            
            # B. 关键字筛选
            if filter_kw:
                # 从 plot_df 中找匹配 Name 或 Code 的
                kw_matched = plot_df[
                    plot_df['name'].str.contains(filter_kw, case=False) | 
                    plot_df['code'].str.contains(filter_kw, case=False, na=False)
                ]
                kw_matched_ids = set(kw_matched['asset_id'])
                # 求交集：必须同时也满足关键字
                valid_asset_ids = valid_asset_ids.intersection(kw_matched_ids)
                
            # --- 3. 生成最终候选项 ---
            # 仅提取符合条件的资产名称供选择
            asset_meta = plot_df[['asset_id', 'name']].drop_duplicates()
            asset_meta = asset_meta[asset_meta['asset_id'].isin(valid_asset_ids)]
            available_names = sorted(asset_meta['name'].unique().tolist())
            
            if not available_names:
                st.warning("⚠️ 没有找到符合上述条件的资产，请调整筛选。")
                plot_df = pd.DataFrame() # 空表防报错
            else:
                # 4. 最终选择框 (Options 是经过层层筛选后的结果)
                selected_assets = st.multiselect(
                    f"4. 勾选要对比的资产 (筛选后可选 {len(available_names)} 个)",
                    options=available_names,
                    placeholder="留空则显示筛选出的【所有】资产...",
                    key="trend_final_select"
                )
                
                # 逻辑：
                # 如果勾选了特定资产 -> 只看勾选的
                # 如果留空 -> 显示符合前面筛选条件的所有资产 (比如看了所有“美股”)
                if selected_assets:
                    plot_df = plot_df[plot_df['name'].isin(selected_assets)]
                else:
                    plot_df = plot_df[plot_df['asset_id'].isin(valid_asset_ids)]
                
        else: 
            if df_tags is None or df_tags.empty:
                st.warning("暂无标签数据。")
            else:
                groups = df_tags['tag_group'].unique()
                selected_group = st.selectbox("选择标签分组", groups, key="trend_group")
                plot_df = df_tags[df_tags['tag_group'] == selected_group].copy()
                color_col = "tag_name"

        if plot_df is not None:
            # 预计算绘图字段
            plot_df['amt_w'] = plot_df['amount'] / 10000
            plot_df['prof_w'] = plot_df['profit'] / 10000
            plot_df['cost_w'] = plot_df['cost'] / 10000
            daily_sums = plot_df.groupby('date')['amount'].transform('sum')
            plot_df['share'] = (plot_df['amount'] / daily_sums * 100).fillna(0)

            # 决定 Y 轴
            y_col, y_unit, y_title = "amt_w", "w", "金额 (万)"
            if metric_type.startswith("持有收益"): y_col, y_unit, y_title = "prof_w", "w", "收益 (万)"
            elif metric_type.startswith("收益率"): y_col, y_unit, y_title = "yield_rate", "%", "收益率 (%)"
            elif metric_type.startswith("占比"): y_col, y_unit, y_title = "share", "%", "占比 (%)"

            # 绘图
            custom_data_cols = ['amt_w', 'prof_w', 'cost_w', 'yield_rate', 'share']
            fig = px.line(plot_df, x='date', y=y_col, color=color_col, markers=True, custom_data=custom_data_cols)
            
            # 定制 tooltip
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

            csv_struct = plot_df.to_csv(index=False).encode('utf-8-sig')
            st.download_button(label=f"📥 导出当前筛选数据 ({view_mode})", data=csv_struct, file_name=f'trend_structure.csv', mime='text/csv')

            # =========================================================
            # 🔥 核心修改：分组柱状图对比 (美化 Tooltip 版)
            # =========================================================
            st.divider()
            st.subheader("两期数据横向比对")
            st.caption(f"对比维度：**{view_mode}** | 直观展示两个时间点的数值变化")
            # 获取有效日期范围供组件限制
            valid_min = plot_df['date'].min().date()
            valid_max = plot_df['date'].max().date()
            
            with st.container():
                dc1, dc2, dc3 = st.columns([2, 2, 3])
                with dc1:
                    # 🔥 改为 date_input
                    d1_input = st.date_input("📅 日期 A (旧)", value=valid_min, min_value=valid_min, max_value=valid_max, key="diff_d1")
                with dc2:
                    # 🔥 改为 date_input
                    d2_input = st.date_input("📅 日期 B (新)", value=valid_max, min_value=valid_min, max_value=valid_max, key="diff_d2")
                with dc3:
                    diff_metric = st.radio("对比指标", ["总金额 (Amount)", "持有收益 (Profit)", "收益率 (Yield %)", "占比 (Share %)"], horizontal=True)

            # 转换 input 为 datetime 以便和 dataframe 比较
            d1_ts = pd.Timestamp(d1_input)
            d2_ts = pd.Timestamp(d2_input)

            # 检查所选日期是否有数据
            has_d1 = not plot_df[plot_df['date'] == d1_ts].empty
            has_d2 = not plot_df[plot_df['date'] == d2_ts].empty

            if d1_ts == d2_ts:
                st.info("请选择两个不同的日期。")
            elif not has_d1 or not has_d2:
                st.warning(f"所选日期无数据。请确保选中的日期 ({d1_input} 或 {d2_input}) 有资产快照记录。")
            else:
                # ... (原来的绘图逻辑完全不用动，只需要把原来的 d1, d2 变量替换成 d1_ts, d2_ts) ...
                if "总金额" in diff_metric: val_col = "amount"; unit_suffix = "元"
                elif "持有收益" in diff_metric: val_col = "profit"; unit_suffix = "元"
                elif "收益率" in diff_metric: val_col = "yield_rate"; unit_suffix = "%"
                elif "占比" in diff_metric: val_col = "share"; unit_suffix = "%"

                df_d1 = plot_df[plot_df['date'] == d1_ts].copy() # 使用 ts
                df_d1['Period'] = d1_ts.strftime('%Y-%m-%d')
                
                df_d2 = plot_df[plot_df['date'] == d2_ts].copy() # 使用 ts
                df_d2['Period'] = d2_ts.strftime('%Y-%m-%d')
                
                df_viz = pd.concat([df_d1, df_d2], ignore_index=True)
                
                # ... (后续绘图代码保持不变，直到 Tab 2) ...
                rank_order = df_d2.sort_values(val_col, ascending=False)[color_col].tolist()
                fig_compare = px.bar(
                    df_viz, x=color_col, y=val_col, color='Period', barmode='group', 
                    title=f"{diff_metric} 对比: {d1_ts.strftime('%m-%d')} vs {d2_ts.strftime('%m-%d')}",
                    category_orders={color_col: rank_order}, text_auto='.2s' if unit_suffix == "元" else '.2f'
                )
                # ... (Tooltip 代码不变) ...
                metric_label = diff_metric.split(' ')[0]
                if unit_suffix == "元":
                    hover_template = f"<b>%{{x}}</b><br>📅 %{{fullData.name}}<br>{metric_label}: <b>¥%{{y:,.2f}}</b><extra></extra>"
                else:
                    hover_template = f"<b>%{{x}}</b><br>📅 %{{fullData.name}}<br>{metric_label}: <b>%{{y:.2f}}%</b><extra></extra>"
                fig_compare.update_traces(hovertemplate=hover_template)
                fig_compare.update_layout(yaxis_title=diff_metric, xaxis_title="", legend_title_text="", hovermode="x unified")
                st.plotly_chart(fig_compare, use_container_width=True)

                with st.expander(f"查看 {diff_metric} 具体变动数值"):
                    df_pivot = df_viz.pivot(index=color_col, columns='Period', values=val_col).reset_index()
                    d1_str = d1_ts.strftime('%Y-%m-%d')
                    d2_str = d2_ts.strftime('%Y-%m-%d')
                    df_pivot = df_pivot.fillna(0)
                    df_pivot['变动量'] = df_pivot[d2_str] - df_pivot[d1_str]
                    df_pivot = df_pivot.sort_values(d2_str, ascending=False)
                    st.dataframe(df_pivot, hide_index=True, use_container_width=True)

    # === TAB 2: 每日透视 (已升级为日历组件) ===
    with tab2:
        st.subheader("🍰 每日资产快照分析")
        
        # 1. 顶部控制栏
        control_c1, control_c2 = st.columns(2)
        with control_c1:
            # 获取数据中的日期范围，限制日历选择器的上下限
            default_date = df_assets['date'].max().date()
            min_date = df_assets['date'].min().date()
            
            # 🔥 修改点：使用 date_input 日历组件
            selected_date_input = st.date_input(
                "📅 选择要查看的日期", 
                value=default_date,
                min_value=min_date,
                max_value=default_date,
                help="点击右侧日历图标选择日期"
            )
            # 关键：将 date 类型转为 pandas 的 Timestamp，否则跟数据库的时间格式对不上
            selected_date = pd.Timestamp(selected_date_input)
        
        with control_c2:
            # 维度选择器
            tag_groups = list(df_tags['tag_group'].unique()) if (df_tags is not None and not df_tags.empty) else []
            dim_options = ["按具体资产"] + tag_groups
            selected_dim = st.selectbox("🔍 分析维度 (筛选标签组)", dim_options)

        st.divider()

        # 2. 数据准备与校验
        # 检查选中的这一天到底有没有数据
        if selected_dim == "按具体资产":
            # 筛选 assets 表
            day_data = df_assets[df_assets['date'] == selected_date].copy()
            name_col = 'name'
        else:
            # 筛选 tags 表
            if df_tags is None:
                day_data = pd.DataFrame()
            else:
                day_data = df_tags[
                    (df_tags['date'] == selected_date) & 
                    (df_tags['tag_group'] == selected_dim)
                ].copy()
                name_col = 'tag_name'

        # 3. 如果当天无数据，显示提示；有数据则显示图表
        if day_data.empty:
            st.warning(f"📅 {selected_date_input} 当天没有录入数据。请尝试选择其他日期。")
        else:
            # --- 预计算辅助列 (用于 Tooltip 显示 '万') ---
            day_data['amount_w'] = day_data['amount'] / 10000
            day_data['profit_w'] = day_data['profit'] / 10000

            # --- A. 核心指标卡片 ---
            day_total_amt = day_data['amount'].sum()
            day_total_profit = day_data['profit'].sum()
            
            m1, m2, m3 = st.columns(3)
            with m1:
                st.metric("当日总资产", f"¥{day_total_amt/10000:,.2f}万")
            with m2:
                st.metric("当日持有收益", f"¥{day_total_profit/10000:,.2f}万", 
                          delta_color="normal" if day_total_profit >= 0 else "inverse")
            with m3:
                # 计算当天的综合收益率
                # 逻辑：收益 / (总资产 - 收益) = 收益 / 本金
                total_cost = day_total_amt - day_total_profit
                if total_cost != 0:
                     total_yield = (day_total_profit / total_cost) * 100
                     m3.metric("当日综合收益率", f"{total_yield:.2f}%")
                else:
                     m3.metric("当日综合收益率", "0.00%")

            # --- B. 饼图区域 ---
            chart_c1, chart_c2 = st.columns(2)
            
            # 饼图 1: 总金额占比
            with chart_c1:
                fig_pie_amt = px.pie(
                    day_data, 
                    values='amount', 
                    names=name_col, 
                    title=f"【总金额】占比 ({selected_dim})", 
                    hole=0.4,
                    custom_data=['amount_w'] # 传入万单位数据
                )
                fig_pie_amt.update_traces(
                    textposition='inside', 
                    textinfo='percent+label',
                    hovertemplate='<b>%{label}</b>: 💰%{customdata[0]:.2f}万 (🍰%{percent})<extra></extra>'
                )
                st.plotly_chart(fig_pie_amt, use_container_width=True)
            
            # 饼图 2: 收益贡献占比
            with chart_c2:
                # 只有当存在正收益时才画这个图，否则全是负的画饼图很怪
                if (day_data['profit'] > 0).any():
                    # 只展示赚钱的部分，或者全部展示（看个人喜好，这里逻辑是全部）
                    # 为了饼图好看，通常只画正值。如果想看亏损，建议看下面的表格。
                    pos_profit_data = day_data[day_data['profit'] > 0]
                    if not pos_profit_data.empty:
                        fig_pie_prof = px.pie(
                            pos_profit_data, 
                            values='profit', 
                            names=name_col, 
                            title=f"【正收益】贡献占比 ({selected_dim})", 
                            hole=0.4,
                            custom_data=['profit_w']
                        )
                        fig_pie_prof.update_traces(
                            textposition='inside', 
                            textinfo='percent+label',
                            hovertemplate='<b>%{label}</b>: 📈%{customdata[0]:.2f}万 (🍰%{percent})<extra></extra>'
                        )
                        st.plotly_chart(fig_pie_prof, use_container_width=True)
                    else:
                        st.info("当日无正收益资产。")
                else:
                    st.info("当日所有资产均为负收益或零收益，暂不展示贡献图。")

            # --- C. 详细数据表格 ---
            st.subheader(f"📋 详细数据清单")
            
            # 整理显示列
            display_cols = [name_col, 'amount', 'profit', 'yield_rate']
            if 'cost' in day_data.columns: 
                display_cols.insert(2, 'cost')
            
            show_df = day_data[display_cols].copy()
            show_df = show_df.sort_values('amount', ascending=False)
            
            st.dataframe(
                show_df,
                column_config={
                    name_col: "名称/标签",
                    "amount": st.column_config.NumberColumn("总金额 (¥)", format="%.2f"),
                    "cost": st.column_config.NumberColumn("本金 (¥)", format="%.2f"),
                    "profit": st.column_config.NumberColumn("持有收益 (¥)", format="%.2f"),
                    "yield_rate": st.column_config.NumberColumn("收益率", format="%.2f%%"),
                },
                use_container_width=True,
                hide_index=True
            )

    # === TAB 3 (保持不变) ===
    with tab3:
        st.subheader("⚠️ 数据完整性检查")
        if df_tags is not None and not df_tags.empty:
            incomplete_df = df_tags[df_tags['is_complete'] == False].copy()
            if not incomplete_df.empty:
                st.error(f"发现 {len(incomplete_df)} 条聚合记录数据缺失！")
                incomplete_df['date'] = incomplete_df['date'].dt.date
                st.dataframe(incomplete_df[['date', 'tag_group', 'tag_name', 'missing_count']])
            else:
                st.success("🎉 数据完整。")
        else:
            st.write("暂无标签数据。")

    # === TAB 4: 年度财富复盘 (核心联动功能) ===
    with tab4:
        st.subheader("🏆 年度财富归因分析")
        st.caption("上帝视角：你的钱到底是【赚】来的，还是【存】来的？")
        
        # --- 1. 数据准备 ---
        # A. 获取每年的资产增量 (Asset Delta)
        # 逻辑：取每年最后一天的总资产 - 上一年最后一天的总资产
        
        # 为了准确，我们按年分组，取 max(date)
        df_assets['year'] = df_assets['date'].dt.year
        
       # --- ✅ 优化后的代码 (确保先算出当天的总钱数) ---
        # 1. 先算出每天的总资产
        daily_sum = df_assets.groupby('date')['amount'].sum().reset_index()
        daily_sum['year'] = daily_sum['date'].dt.year
        
        # 2. 再取每年的最后一天
        yearly_end = daily_sum.sort_values('date').groupby('year').last().reset_index()
        yearly_end.rename(columns={'amount': 'end_amount'}, inplace=True)
        
        # 计算每年的增量
        # 先获取整个数据最早日期之前的初始状态（假设为0，或者用户录入的第一笔就是初始）
        # 这里用 shift 简单计算：今年的增量 = 今年底 - 去年底
        yearly_end['prev_amount'] = yearly_end['end_amount'].shift(1).fillna(0) # 第一年默认增量就是年底余额（假设从0开始），这可能不准，但对于趋势分析可以接受
        yearly_end['asset_delta'] = yearly_end['end_amount'] - yearly_end['prev_amount']
        conn = get_db_connection()  # <--- 加上这一行
        # B. 获取每年的净投入 (Net Input)
        df_cf = pd.read_sql("SELECT date, type, amount FROM cashflows WHERE user_id = ?", conn, params=(user_id,))
        if df_cf.empty:
            st.warning("⚠️ 暂无现金流记录，无法计算本金投入。请先去【现金流与本金归集】页面录入工资和账单。")
            yearly_cf = pd.DataFrame(columns=['year', 'net_input'])
        else:
            df_cf['date'] = pd.to_datetime(df_cf['date'])
            df_cf['year'] = df_cf['date'].dt.year
            # 收入记正，支出记负
            df_cf['signed_amount'] = df_cf.apply(lambda x: x['amount'] if x['type'] == '收入' else -x['amount'], axis=1)
            yearly_cf = df_cf.groupby('year')['signed_amount'].sum().reset_index()
            yearly_cf.rename(columns={'signed_amount': 'net_input'}, inplace=True)
            
        # C. 合并数据
        df_attribution = pd.merge(yearly_end, yearly_cf, on='year', how='left')
        df_attribution['net_input'] = df_attribution['net_input'].fillna(0)
        
        # D. 计算市场收益 (Market Alpha)
        # 公式：市场收益 = 资产增量 - 净投入
        df_attribution['market_alpha'] = df_attribution['asset_delta'] - df_attribution['net_input']
        
        # 单位换算 (万)
        for c in ['end_amount', 'asset_delta', 'net_input', 'market_alpha']:
            df_attribution[f'{c}_w'] = df_attribution[c] / 10000

        # --- 2. 绘图 (堆叠柱状图) ---
        if not df_attribution.empty:
            # 转换格式适配 Plotly
            # 我们需要把 data 变长：Year, Type, Value
            viz_data = []
            for _, row in df_attribution.iterrows():
                # 1. 净投入柱子
                viz_data.append({
                    'Year': str(int(row['year'])),
                    'Type': '🌱 净投入本金 (工资结余)',
                    'Value': row['net_input_w'],
                    'RawValue': row['net_input'],
                    'Color': '#3498DB' # 蓝色
                })
                # 2. 市场收益柱子
                viz_data.append({
                    'Year': str(int(row['year'])),
                    'Type': '🚀 市场投资收益 (Alpha)',
                    'Value': row['market_alpha_w'],
                    'RawValue': row['market_alpha'],
                    'Color': '#E74C3C' if row['market_alpha'] < 0 else '#2ECC71' # 绿赚红亏
                })
                
            df_viz = pd.DataFrame(viz_data)
            
            # 使用 Graph Objects 画图以获得最大自由度 (相对模式)
            fig = go.Figure()
            
            # 分组处理不同 Type
            for t in df_viz['Type'].unique():
                subset = df_viz[df_viz['Type'] == t]
                fig.add_trace(go.Bar(
                    x=subset['Year'],
                    y=subset['Value'],
                    name=t,
                    marker_color=subset['Color'],
                    text=subset['Value'].apply(lambda x: f"{x:+.1f}w"),
                    textposition='auto',
                    hovertemplate='<b>%{x}年 - %{data.name}</b><br>金额: %{y:.2f}万<extra></extra>'
                ))
            
            # 叠加一条“总资产增量”的折线，方便对比
            fig.add_trace(go.Scatter(
                x=df_attribution['year'].astype(str),
                y=df_attribution['asset_delta_w'],
                name='💰 当年总资产增量',
                mode='lines+markers',
                line=dict(color='#F1C40F', width=3, dash='dot'),
                hovertemplate='当年总增量: %{y:.2f}万<extra></extra>'
            ))

            fig.update_layout(
                barmode='relative', # 关键！允许正负值堆叠
                yaxis_title="金额 (万元)",
                hovermode="x unified",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
            )
            
            st.plotly_chart(fig, use_container_width=True)

            # --- 3. 详细数据表 ---
            st.divider()
            with st.expander("查看详细归因数据表"):
                st.dataframe(
                    df_attribution[['year', 'asset_delta', 'net_input', 'market_alpha', 'end_amount']],
                    column_config={
                        "year": st.column_config.NumberColumn("年份", format="%d"),
                        "asset_delta": st.column_config.NumberColumn("总资产增量", format="¥%.2f"),
                        "net_input": st.column_config.NumberColumn("净投入本金", format="¥%.2f"),
                        "market_alpha": st.column_config.NumberColumn("市场收益", format="¥%.2f"),
                        "end_amount": st.column_config.NumberColumn("年末总资产", format="¥%.2f"),
                    },
                    hide_index=True,
                    use_container_width=True
                )
            
            # --- 4. 智能点评 ---
            last_year = df_attribution.iloc[-1]
            if last_year['market_alpha'] > last_year['net_input'] and last_year['market_alpha'] > 0:
                st.success(f"🎉 **双轮驱动 ({(int(last_year['year']))})**：恭喜！今年你的【睡后收入】(¥{last_year['market_alpha_w']:.1f}万) 超过了你的【工资结余】(¥{last_year['net_input_w']:.1f}万)。这是 FIRE 路上重要的里程碑！")
            elif last_year['market_alpha'] < 0:
                st.info(f"🛡️ **积谷防饥 ({(int(last_year['year']))})**：今年市场环境艰难 (亏损 ¥{abs(last_year['market_alpha_w']):.1f}万)，但好在你通过努力工作存下了 ¥{last_year['net_input_w']:.1f}万，守住了财富底线。")
            else:
                st.info(f"🧱 **通过积累成长 ({(int(last_year['year']))})**：今年财富增长主要来自于本金投入。继续保持储蓄率，等待市场风起！")

        else:
            st.info("数据不足，无法生成年度复盘。需要至少一年的跨度数据。")

# --- 新增页面：定投计划与看板 ---
def page_investment_plans():
    import pandas as pd          # 👈 加上这句
    import plotly.express as px  # 👈 加上这句
    st.header("📅 定投计划与未来现金流")
    user_id = st.session_state.user['user_id']
    conn = get_db_connection()

    tab1, tab2 = st.tabs(["⚙️ 计划管理", "🔮 未来现金流看板"])

    # === TAB 1: 计划管理 (CRUD) ===
    with tab1:
        st.caption("在这里管理你的自动定投计划。注意：这里的金额是指【原币种】金额。")
        
        # 1. 新增计划表单 (带高级筛选)
        with st.expander("➕ 新增定投计划", expanded=True):
            
            # --- A. 准备基础数据 ---
            # 修改：同时读取 currency
            all_assets = pd.read_sql('SELECT asset_id, name, code, currency FROM assets WHERE user_id = ?', conn, params=(user_id,))
            
            if all_assets.empty:
                st.warning("⚠️ 请先去【资产与标签管理】页面添加至少一个资产。")
            else:
                # --- B. 筛选工具栏 (逻辑保持不变，略微省略以节省篇幅，直接使用) ---
                st.markdown("##### 🔍 第一步：筛选资产")
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

                # 筛选逻辑...
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
                        # 格式化显示：加入币种信息
                        sel_asset = st.selectbox(
                            f"选择资产 (当前筛选出 {len(final_assets)} 个)", 
                            options=final_assets['asset_id'], 
                            format_func=lambda x: f"{final_assets[final_assets['asset_id']==x]['name'].values[0]} ({final_assets[final_assets['asset_id']==x]['currency'].values[0]})",
                            key="plan_new_asset"
                        )
                        # 获取选中资产的币种，提示用户
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

                    st.write("") 
                    
                    if st.button("💾 保存定投计划", type="primary", key="btn_save_plan"):
                        if amount <= 0:
                            st.error("定投金额必须大于 0")
                        else:
                            try:
                                conn.execute('''
                                    INSERT INTO investment_plans (user_id, asset_id, amount, frequency, execution_day)
                                    VALUES (?, ?, ?, ?, ?)
                                ''', (user_id, sel_asset, amount, freq, exec_day))
                                conn.commit()
                                st.success(f"✅ 已添加定投计划！")
                                st.rerun()
                            except Exception as e:
                                st.error(f"保存失败: {e}")

        # 2. 现有计划列表
        st.subheader("📋 正在运行的计划")
        
        # 修改：同时查出 assets 表的 currency
        plans_df = pd.read_sql('''
            SELECT p.plan_id, a.name, a.currency, p.amount, p.frequency, p.execution_day, p.is_active
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
            
            edited_plans = st.data_editor(
                plans_df,
                column_config={
                    "plan_id": st.column_config.NumberColumn("ID", disabled=True, width="small"),
                    "name": st.column_config.TextColumn("资产名称", disabled=True),
                    "currency": st.column_config.TextColumn("币种", disabled=True, width="small"),
                    "amount": st.column_config.NumberColumn("金额 (原币)", format="%.2f"),
                    "frequency": st.column_config.TextColumn("频率", disabled=True),
                    "execution_day": None, 
                    "描述": st.column_config.TextColumn("定投时间", disabled=True),
                    "is_active": st.column_config.CheckboxColumn("启用中"),
                },
                hide_index=True,
                use_container_width=True,
                num_rows="dynamic",
                key="plans_editor"
            )
            
            if st.button("💾 保存计划变更"):
                # --- 🔥 修复：剔除纯展示用的列，防止写入数据库报错 ---
                # 'name', 'currency' 是从 assets 表联查出来的，'描述' 是前端计算的
                # 数据库表 investment_plans 里没有这些字段
                cols_to_drop = ['name', 'currency', '描述']
                
                # 过滤掉这些列，只保留数据库表里有的字段 (如 amount, frequency, execution_day, is_active)
                df_to_save = edited_plans.drop(columns=[c for c in cols_to_drop if c in edited_plans.columns])
                
                if save_changes_to_db(df_to_save, plans_df, 'investment_plans', 'plan_id', user_id, fixed_cols={'user_id':user_id}):
                    st.rerun()
        else:
            st.info("暂无定投计划。")

    # === TAB 2: 现金流看板 (核心修改) ===
    with tab2:
        # 1. 计算未来现金流逻辑
        st.subheader("🗓️ 未来 30 天资金需求推演 (折合人民币)")
        
        # 获取最新汇率表
        rates_map = get_latest_rates(conn)
        
        # 获取所有启用的计划 (包含币种)
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
                        # 🔥 核心修正：金额折算
                        raw_amt = plan['amount']
                        curr = plan['currency']
                        rate = 1.0 if curr == 'CNY' else rates_map.get(curr, 1.0)
                        cny_amt = raw_amt * rate
                        
                        projection_data.append({
                            "date": current_date,
                            "asset_id": plan['asset_id'],
                            "asset_name": plan['name'],
                            "amount_cny": cny_amt, # 使用折算后的金额
                            "raw_info": f"{raw_amt} {curr}" # 备注原币金额
                        })

            if not projection_data:
                st.warning("未来30天内没有匹配的定投日。")
            else:
                df_proj = pd.DataFrame(projection_data)
                
                # --- 可视化 A: 总览 (CNY) ---
                total_needed = df_proj['amount_cny'].sum()
                col1, col2 = st.columns(2)
                col1.metric("未来 30 天总定投 (CNY)", f"¥{total_needed:,.2f}")
                col2.metric("平均每日流出 (CNY)", f"¥{total_needed/30:,.2f}")

                st.divider()

                # --- 可视化 B: 堆叠柱状图 ---
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

                # 按 amount_cny 聚合
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
                
                fig.update_traces(
                    hovertemplate='<b>%{fullData.name}</b>: ¥%{y:,.0f} (%{customdata[0]:.1f}%)<extra></extra>'
                )
                
                fig.update_layout(
                    hovermode="x unified",
                    legend_title_text="" 
                )
                
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

def page_performance():
    import pandas as pd
    import plotly.express as px
    import calendar
    from datetime import datetime, timedelta

    st.header("🏆 投资战绩与月度复盘")
    st.caption("手动记录每月的最终战果，不纠结过程，只看结果。")

    user_id = st.session_state.user['user_id']
    conn = get_db_connection()

    # --- 1. 核心维度选择 (数据隔离墙) ---
    all_groups_df = pd.read_sql("SELECT DISTINCT tag_group FROM tags WHERE user_id = ?", conn, params=(user_id,))
    
    if all_groups_df.empty:
        st.warning("⚠️ 请先去【资产与标签管理】定义标签组（例如新建一个组叫“资金渠道”）。")
        conn.close()
        return

    # 智能定位默认组
    default_idx = 0
    g_list = all_groups_df['tag_group'].tolist()
    for kw in ["渠道", "账户", "来源"]:
        matches = [i for i, x in enumerate(g_list) if kw in x]
        if matches:
            default_idx = matches[0]
            break
            
    selected_group = st.selectbox("📂 记账维度", g_list, index=default_idx)
    
    # 获取该组下的标签
    tags_in_group = pd.read_sql("SELECT tag_name FROM tags WHERE user_id = ? AND tag_group = ?", 
                              conn, params=(user_id, selected_group))
    
    if tags_in_group.empty:
        st.info(f"标签组【{selected_group}】下没有标签，请先去添加。")
        conn.close()
        return
        
    tag_names = tags_in_group['tag_name'].tolist()

    st.divider()

    # --- 2. 数据录入区 (双下拉框 + 自动覆盖) ---
    with st.expander("📝 录入/修改 月度数据", expanded=False):
        # 优雅的年月选择
        today = datetime.now()
        last_month_date = today.replace(day=1) - timedelta(days=1)
        default_year = last_month_date.year
        default_month = last_month_date.month

        c_y, c_m, _ = st.columns([1, 1, 3])
        with c_y:
            sel_year = st.selectbox("年份", list(range(default_year - 5, default_year + 3)), index=5, key="perf_sel_year")
        with c_m:
            sel_month = st.selectbox("月份", range(1, 13), index=default_month - 1, key="perf_sel_month")

        month_str = f"{sel_year}-{sel_month:02d}"
        
        # 预读取数据
        existing_data = pd.read_sql('''
            SELECT tag_name, amount FROM monthly_profits 
            WHERE user_id = ? AND month = ? AND tag_group = ?
        ''', conn, params=(user_id, month_str, selected_group))
        data_map = dict(zip(existing_data['tag_name'], existing_data['amount']))
        
        existing_note = conn.execute('''
            SELECT content FROM monthly_reviews 
            WHERE user_id = ? AND month = ? AND tag_group = ?
        ''', (user_id, month_str, selected_group)).fetchone()
        note_val = existing_note['content'] if existing_note else ""

        with st.form("perf_entry_form"):
            st.caption(f"当前录入：{selected_group} - {month_str}")
            cols = st.columns(3)
            input_values = {}
            
            for i, tag in enumerate(tag_names):
                col = cols[i % 3]
                with col:
                    input_values[tag] = st.number_input(
                        tag, 
                        value=float(data_map.get(tag, 0.0)), 
                        step=100.0,
                        format="%.2f",
                        key=f"perf_{month_str}_{tag}"
                    )
            
            st.write("")
            new_note = st.text_area("📝 月度复盘 / 备注", value=note_val, height=80, placeholder="本月总结...")
            
            if st.form_submit_button("💾 保存 / 更新", type="primary", use_container_width=True):
                try:
                    for tag, amt in input_values.items():
                        conn.execute('''
                            INSERT INTO monthly_profits (user_id, month, tag_group, tag_name, amount, updated_at)
                            VALUES (?, ?, ?, ?, ?, ?)
                            ON CONFLICT(user_id, month, tag_group, tag_name) 
                            DO UPDATE SET amount=excluded.amount, updated_at=excluded.updated_at
                        ''', (user_id, month_str, selected_group, tag, amt, datetime.now()))
                    
                    if new_note.strip():
                        conn.execute('''
                            INSERT INTO monthly_reviews (user_id, month, tag_group, content, updated_at)
                            VALUES (?, ?, ?, ?, ?)
                            ON CONFLICT(user_id, month, tag_group)
                            DO UPDATE SET content=excluded.content, updated_at=excluded.updated_at
                        ''', (user_id, month_str, selected_group, new_note, datetime.now()))
                    else:
                        conn.execute('DELETE FROM monthly_reviews WHERE user_id=? AND month=? AND tag_group=?', 
                                   (user_id, month_str, selected_group))

                    conn.commit()
                    st.toast(f"✅ {month_str} 数据已保存！", icon="💾")
                    import time
                    time.sleep(0.5)
                    st.rerun()
                except Exception as e:
                    st.error(f"保存失败: {e}")

    # --- 3. 战绩墙 (纯视觉卡片) ---
    df_all = pd.read_sql('''
        SELECT month, amount
        FROM monthly_profits 
        WHERE user_id = ? AND tag_group = ?
        ORDER BY month DESC
    ''', conn, params=(user_id, selected_group))
    
    # 因为 monthly_profits 是细分到 tag 的，我们需要先按 month 聚合总金额
    if df_all.empty:
        st.info(f"🏷️ 标签组【{selected_group}】暂无收益记录。")
    else:
        # 按月聚合
        df_agg = df_all.groupby('month')['amount'].sum().reset_index().sort_values('month', ascending=False)
        df_agg['year'] = df_agg['month'].str.slice(0, 4)
        
        unique_years = sorted(df_agg['year'].unique().tolist(), reverse=True)
        
        tabs = st.tabs([f"{y} 年度" for y in unique_years])
        
        for i, year in enumerate(unique_years):
            with tabs[i]:
                df_year = df_agg[df_agg['year'] == year]
                
                # A. 顶部统计
                total_profit = df_year['amount'].sum()
                
                k1, k2, k3 = st.columns(3)
                k1.metric("年度累计收益", f"¥{total_profit:,.2f}", delta_color="normal" if total_profit >= 0 else "inverse")
                k2.metric("盈利月份", f"{len(df_year[df_year['amount']>0])} 个")
                k3.metric("亏损月份", f"{len(df_year[df_year['amount']<0])} 个")
                
                st.divider()

                # B. 月份色块矩阵
                # 改为 6 列，让卡片看起来更窄
                grid_cols = st.columns(6)
                
                for idx, row in enumerate(df_year.to_dict('records')):
                    m_str = row['month']
                    m_total = row['amount']
                    
                    # 颜色定义 (A股配色：红涨绿跌)
                    # 使用柔和一点的色值，防止刺眼
                    # 红: #e74c3c (Alizarin), 绿: #2ecc71 (Emerald), 灰: #95a5a6
                    if m_total > 0:
                        bg_color = "#e74c3c" 
                        sign = "+"
                    elif m_total < 0:
                        bg_color = "#2ecc71" # 如果你习惯美股配色(绿涨红跌)，这里互换颜色即可
                        sign = ""
                    else:
                        bg_color = "#95a5a6"
                        sign = ""

                    col_idx = idx % 6
                    
                    with grid_cols[col_idx]:
                        # 使用 HTML/CSS 绘制卡片
                        # height: 80px 加上 narrow column 实现了"高窄"视觉
                        card_html = f"""
                        <div style="
                            background-color: {bg_color};
                            color: white;
                            padding: 10px 2px;
                            border-radius: 6px;
                            text-align: center;
                            margin-bottom: 10px;
                            height: 90px;
                            display: flex;
                            flex-direction: column;
                            justify-content: center;
                            align-items: center;
                            box-shadow: 2px 2px 5px rgba(0,0,0,0.1);
                        ">
                            <div style="font-size: 0.85em; opacity: 0.9; margin-bottom: 4px;">{m_str}</div>
                            <div style="font-size: 1.1em; font-weight: bold;">{sign}{m_total:,.0f}</div>
                        </div>
                        """
                        st.markdown(card_html, unsafe_allow_html=True)

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

def generate_and_send_ai_prompt(user_id, target_group, start_date_str, end_date_str):
    """
    生成 AI 顾问提示词 (CIO 宏观视角版 - 包含精准水位与本金分析)
    """
    import pandas as pd
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    
    conn = get_db_connection()
    
    # --- 1. 获取系统设置 ---
    settings = conn.execute('SELECT * FROM system_settings WHERE id = 1').fetchone()
    if not settings['email_host']:
        conn.close()
        return False, "未配置邮箱 SMTP，无法发送。"

    # --- 2. 搜集与计算核心数据 (对齐看板逻辑) ---
    # A. 获取资产快照
    df_assets, df_tags = get_cached_analytics_data(user_id)
    
    if df_assets is None or df_assets.empty:
        conn.close()
        return False, "暂无资产数据，无法生成分析。"

    # 转换日期格式
    start_date = pd.to_datetime(start_date_str)
    end_date = pd.to_datetime(end_date_str)

    # B. 准备每日总资产 (Amount)
    daily_monitor = df_assets.groupby('date')[['amount']].sum().reset_index().sort_values('date')
    
    # C. 准备真实本金 (Cashflows) - 核心逻辑复用
    df_cf = pd.read_sql("SELECT date, type, amount FROM cashflows WHERE user_id = ?", conn, params=(user_id,))
    
    # 初始化本金列
    daily_monitor['final_principal'] = 0.0
    
    if not df_cf.empty:
        df_cf['date'] = pd.to_datetime(df_cf['date'])
        # 收入=+，支出=-
        df_cf['net_flow'] = df_cf.apply(lambda x: x['amount'] if x['type'] == '收入' else -x['amount'], axis=1)
        # 计算累计净投入
        df_principal = df_cf.groupby('date')['net_flow'].sum().sort_index().cumsum().reset_index()
        df_principal.rename(columns={'net_flow': 'cumulative_principal'}, inplace=True)
        # 合并
        daily_monitor = pd.merge_asof(daily_monitor, df_principal, on='date', direction='backward')
        daily_monitor['final_principal'] = daily_monitor['cumulative_principal'].fillna(0)
    else:
        # 如果没现金流记录，降级使用 Cost (虽不准但比报错好)
        daily_cost = df_assets.groupby('date')[['cost']].sum().reset_index()
        daily_monitor = pd.merge(daily_monitor, daily_cost, on='date', how='left')
        daily_monitor['final_principal'] = daily_monitor['cost']

    # D. 计算每日收益 (Profit)
    daily_monitor['profit'] = daily_monitor['amount'] - daily_monitor['final_principal']

    # --- 3. 提取关键节点数据 ---
    
    # 获取 起点(Start) 和 终点(End) 的行数据
    # 使用 asof 或直接查找 (这里假设 start_date 可能不是交易日，用 asof 找最近的前一天比较稳妥，或者精确匹配)
    # 为了简化，这里先尝试精确匹配，匹配不到找最近的
    
    def get_closest_row(target_date):
        # 找小于等于 target_date 的最后一条
        mask = daily_monitor['date'] <= target_date
        if not mask.any(): return None
        return daily_monitor[mask].iloc[-1]

    row_start = get_closest_row(start_date)
    row_end = get_closest_row(end_date)

    if row_end is None:
        conn.close()
        return False, f"找不到 {end_date_str} 之前的任何数据。"

    # 提取端点值
    # 期初
    s_amt = row_start['amount'] if row_start is not None else 0.0
    s_prin = row_start['final_principal'] if row_start is not None else 0.0
    s_prof = row_start['profit'] if row_start is not None else 0.0
    
    # 期末
    e_amt = row_end['amount']
    e_prin = row_end['final_principal']
    e_prof = row_end['profit']
    
    # 计算期间变动
    period_yield_val = e_prof - s_prof # 期间产生的利润
    # 期间收益率 (分母用 期初本金 或 期初资产，这里用期初资产作为参考)
    period_yield_pct = (period_yield_val / s_amt * 100) if s_amt > 0 else 0.0

    # --- 4. 计算六大水位指标 (基于截至 End Date 的历史数据) ---
    # 截取历史切片 (直到复盘结束那一天)
    history_slice = daily_monitor[daily_monitor['date'] <= end_date].copy()
    
    # 1. 当前资产 (End Date)
    curr_asset = e_amt
    
    # 2. 历史最高 (ATH)
    ath_asset = history_slice['amount'].max()
    
    # 3. 回撤计算
    history_slice['rolling_max'] = history_slice['amount'].cummax()
    history_slice['dd_amt'] = history_slice['rolling_max'] - history_slice['amount']
    # 处理分母0
    history_slice['dd_pct'] = 0.0
    mask = history_slice['rolling_max'] > 0
    history_slice.loc[mask, 'dd_pct'] = (history_slice.loc[mask, 'dd_amt'] / history_slice.loc[mask, 'rolling_max']) * 100
    
    # 当前回撤 (End Date)
    curr_dd_pct = history_slice.iloc[-1]['dd_pct']
    curr_dd_amt = history_slice.iloc[-1]['dd_amt']
    
    # 历史最大回撤 (在 End Date 之前发生过的最惨回撤)
    max_dd_pct = history_slice['dd_pct'].max()
    max_dd_amt = history_slice['dd_amt'].max()
    
    # 4. 收益指标
    curr_profit = e_prof
    max_profit = history_slice['profit'].max() # 历史最高累计收益

    # --- 5. 核心持仓结构 (占比 > 0.5%) ---
    target_assets = df_assets[df_assets['date'] == end_date].copy()
    target_assets = target_assets.sort_values('amount', ascending=False)
    target_assets['ratio'] = target_assets['amount'] / e_amt if e_amt > 0 else 0
    
    significant_assets = target_assets[target_assets['ratio'] > 0.005]
    
    holdings_str = ""
    if significant_assets.empty:
        holdings_str = "无单一资产占比超过 0.5%。"
    else:
        for i, row in significant_assets.iterrows():
            currency_info = f" ({row['currency']})" if 'currency' in row and row['currency'] != 'CNY' else ""
            holdings_str += f"- {row['name']}{currency_info}: ¥{row['amount']:,.0f} (占比 {row['ratio']*100:.2f}%)\n"

    # --- 6. 维度配置变化复盘 (Start vs End) ---
    analysis_str = ""
    if df_tags is not None and not df_tags.empty:
        # 注意：这里需要重新按照 start_date 和 end_date 筛选 tags 数据
        # 因为 df_tags 是预计算好的，可以直接过滤
        tags_start = df_tags[(df_tags['date'] == start_date) & (df_tags['tag_group'] == target_group)].copy()
        tags_end = df_tags[(df_tags['date'] == end_date) & (df_tags['tag_group'] == target_group)].copy()
        
        # 如果 precise match 失败，尝试找最近的 (简单处理：如果为空就不展示了，或者你可以加类似 get_closest 的逻辑)
        # 这里保持原逻辑，假设 tags 数据是连续的
        
        tags_start = tags_start[['tag_name', 'amount']].rename(columns={'amount': 's_amt'})
        tags_end = tags_end[['tag_name', 'amount']].rename(columns={'amount': 'e_amt'})
        
        df_compare = pd.merge(tags_end, tags_start, on='tag_name', how='outer').fillna(0)
        
        df_compare['s_ratio'] = (df_compare['s_amt'] / s_amt * 100) if s_amt > 0 else 0.0
        df_compare['e_ratio'] = (df_compare['e_amt'] / e_amt * 100) if e_amt > 0 else 0.0
        
        df_compare = df_compare.sort_values('e_amt', ascending=False)
        
        analysis_str += f"基于【{target_group}】维度的变化对比：\n"
        for _, row in df_compare.iterrows():
            if row['s_amt'] < 100 and row['e_amt'] < 100: continue
            analysis_str += (
                f"- **{row['tag_name']}**:\n"
                f"  - 资金: ¥{row['s_amt']:,.0f} ➡️ ¥{row['e_amt']:,.0f}\n"
                f"  - 占比: {row['s_ratio']:.1f}% ➡️ {row['e_ratio']:.1f}%\n"
            )
    else:
        analysis_str = "(暂无标签数据)"

    conn.close()

    # --- 7. 组装 Prompt 模板 (更新版) ---
    prompt_content = f"""
===== 请将以下内容完整发送给 AI (如 ChatGPT/Claude) =====

# Role / 角色设定
**你是一位拥有华尔街顶级投行背景的首席投资官 (CIO)。**
你精通全球宏观经济分析、大类资产配置策略（如耶鲁模式、全天候策略）以及行为金融学。你不仅关注账户的绝对数字，更擅长将个人投资组合的表现置于宏观市场背景下进行“归因分析”。你的分析风格是：客观、犀利、数据驱动，并能给出可落地的战术建议。

# Context / 复盘背景
- **复盘周期**：{start_date_str} 至 {end_date_str}
- **用户画像**：中国个人投资者，以人民币计价。

# Internal Data / 内部投资组合数据

## 1. 资金面概况 (Financial Overview)

### A. 周期端点快照 (Snapshot)
- **期初 ({start_date_str})**:
  - 投入本金: ¥{s_prin:,.0f}
  - 累计收益: ¥{s_prof:,.0f}
  - 资产总值: ¥{s_amt:,.0f}
- **期末 ({end_date_str})**:
  - 投入本金: ¥{e_prin:,.0f}
  - 累计收益: ¥{e_prof:,.0f}
  - 资产总值: ¥{e_amt:,.0f}

**👉 期间变化**: 本金投入变动 ¥{e_prin - s_prin:+,.0f}，期间创造利润 ¥{period_yield_val:+,.0f}。

### B. 风险水位监控 (截至期末 {end_date_str})
> 以下指标基于全历史数据统计：
- **当前总资产**: ¥{curr_asset:,.0f} (历史最高 ATH: ¥{ath_asset:,.0f})
- **当前回撤**: {curr_dd_pct:.2f}% (浮亏金额: -¥{curr_dd_amt:,.0f})
- **历史最大回撤**: {max_dd_pct:.2f}% (最大亏损额: -¥{max_dd_amt:,.0f})
- **当前累计收益**: ¥{curr_profit:,.0f} (历史最高收益: ¥{max_profit:,.0f})

## 2. 核心持仓 (Top Holdings > 0.5%)
{holdings_str}

## 3. 结构演变 (维度：{target_group})
{analysis_str}

---

# Action Required / 你的任务
请务必执行以下步骤进行分析：

## 第一步：外部市场环境扫描 (必须联网搜索)
请利用你的联网能力，**查询 {start_date_str} 至 {end_date_str} 期间的以下市场数据**，作为分析的基准锚点：
1.  **关键指数表现**：纳斯达克100 (NDX)、标普500 (SPX)、黄金 (Gold)。
2.  **核心宏观事件**：期间是否有美联储议息、重大地缘政治事件、或科技巨头(如 NVDA/AAPL)的财报发布？

## 第二步：深度归因分析
基于查询到的外部数据和上述内部数据，回答以下两个问题：

### 1. 风险与收益评估 (Risk & Return)
- **水位分析**：用户当前的累计收益 ({curr_profit:,.0f}) 距离历史最高收益 ({max_profit:,.0f}) 还有多远？结合当前的回撤水平 ({curr_dd_pct:.2f}%)，评价当前账户的“安全垫”厚度。
- **阿尔法验证**：用户的期间利润 ({period_yield_val:+,.0f}) 是来自市场的 Beta 普涨，还是用户的 Alpha 选择？(对比同期的指数表现)

### 2. 战术建议 (Tactical Advice)
- **再平衡指引**：基于期末的持仓结构和当前宏观环境，给出具体的调仓建议。

================================
    """

    # --- 8. 发送邮件 ---
    try:
        msg = MIMEMultipart()
        msg['Subject'] = f'🤖 AI 宏观对冲复盘 ({start_date_str} ~ {end_date_str})'
        msg['From'] = settings['email_user']
        msg['To'] = settings['email_to'] if settings['email_to'] else settings['email_user']
        
        body = "这是为您自动生成的 CIO 级深度复盘提示词。\n\n" + prompt_content
        msg.attach(MIMEText(body, 'plain'))

        server = smtplib.SMTP_SSL(settings['email_host'], settings['email_port'])
        server.login(settings['email_user'], settings['email_password'])
        server.send_message(msg)
        server.quit()
        
        return True, f"已发送 {start_date_str} 至 {end_date_str} 的深度分析提示词！"
    except Exception as e:
        return False, f"邮件发送失败: {str(e)}"
    
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
                        import time
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
            "💰 现金流与本金": "nav_cashflow",
            "🏆 累计收益": "nav_performance",
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
    elif selected_key == "nav_performance":
        page_performance()
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