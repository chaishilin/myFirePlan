import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime
import hashlib
import os
import shutil
from pathlib import Path
import plotly.express as px
import numpy as np
import re
from datetime import timedelta
import plotly.graph_objects as go
import uuid
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from languages import TRANSLATIONS

# --- 兼容性修复 ---
# 某些旧版库可能还在找 np.bool8，这里做一个简单的映射防止报错
if not hasattr(np, 'bool8'):
    np.bool8 = np.bool_

# --- 配置 ---
st.set_page_config(
    page_title="个人资产管理系统",
    page_icon="💼",
    layout="wide"
)

DB_FILE = 'asset_tracker.db'

# --- 数据库工具函数 ---
def get_db_connection():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

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

# --- 用户认证 ---
def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

def verify_user(username, password):
    conn = get_db_connection()
    user = conn.execute('SELECT * FROM users WHERE username = ?', (username,)).fetchone()
    conn.close()
    if user and user['password_hash'] == hash_password(password):
        return user
    return None

def create_user(username, password):
    conn = get_db_connection()
    try:
        conn.execute('INSERT INTO users (username, password_hash) VALUES (?, ?)', 
                    (username, hash_password(password)))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()

# --- 会话管理 (保持登录状态) ---
def create_session(user_id):
    """生成一个有效期为 1 小时的会话 Token"""
    conn = get_db_connection()
    token = str(uuid.uuid4())
    # 设置过期时间：当前时间 + 1小时
    expires_at = datetime.now() + timedelta(hours=1)
    
    # 为了保持单点登录，可以先清理该用户旧的会话（可选）
    conn.execute('DELETE FROM user_sessions WHERE user_id = ?', (user_id,))
    
    conn.execute('INSERT INTO user_sessions (token, user_id, expires_at) VALUES (?, ?, ?)',
                (token, user_id, expires_at))
    conn.commit()
    conn.close()
    return token

def get_user_from_token(token):
    """根据 Token 自动登录"""
    conn = get_db_connection()
    try:
        # 联表查询：验证 Token 是否存在且未过期，并获取用户信息
        row = conn.execute('''
            SELECT u.* FROM users u
            JOIN user_sessions s ON u.user_id = s.user_id
            WHERE s.token = ? AND s.expires_at > ?
        ''', (token, datetime.now())).fetchone()
        
        if row:
            return dict(row)
        return None
    finally:
        conn.close()
# --- 页面模块 ---
def page_login():
    st.title("💼 个人资产管理")
    tab1, tab2 = st.tabs(["登录", "注册"])
    
    with tab1:
        u = st.text_input("用户名", key="l_u")
        p = st.text_input("密码", type="password", key="l_p")
        if st.button("登录", type="primary"):
            user = verify_user(u, p)
            if user:
                # 1. 设置内存状态
                st.session_state.user = dict(user)
                
                # 2. 生成 Token 并写入数据库
                token = create_session(user['user_id'])
                
                # 3. 将 Token 放入 URL 参数中 (Streamlit 1.30+ 新写法)
                st.query_params["token"] = token
                
                st.success("登录成功！")
                st.rerun()
            else:
                st.error("账号或密码错误")
                
    with tab2:
        nu = st.text_input("新用户名", key="r_u")
        np_val = st.text_input("新密码", type="password", key="r_p")
        if st.button("注册"):
            if create_user(nu, np_val):
                st.success("注册成功，请登录")
            else:
                st.error("用户名已存在")

def page_assets_tags():
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
        # --- 修改点 A: SQL 查询中增加 remarks ---
        assets_df = pd.read_sql(
            'SELECT asset_id, name, code, type, remarks FROM assets WHERE user_id = ?', 
            conn, params=(user_id,)
        )
        
        # 应用筛选
        assets_df = apply_advanced_filters(assets_df, "tab1")
        
        st.caption(f"共显示 {len(assets_df)} 条资产")
        
        # --- 修改点 B: 在编辑器中配置 remarks 列 ---
        edited_assets = st.data_editor(
            assets_df,
            num_rows="dynamic",
            column_config={
                "asset_id": st.column_config.NumberColumn("ID", disabled=True),
                "name": st.column_config.TextColumn("资产名称", required=True),
                "code": "代码",
                "type": st.column_config.SelectboxColumn("大类", options=["基金", "股票", "债券", "现金", "其他"]),
                "remarks": st.column_config.TextColumn("备注", width="medium", help="例如：养老金、准备卖出...") # 新增这一行
            },
            key="editor_assets",
            use_container_width=True
        )
        
        if st.button("💾 保存资产变动", type="primary"):
            # save_changes_to_db 函数够聪明，它会自动检测到 DataFrame 里多了 remarks 列，
            # 并自动生成对应的 SQL 更新语句，所以这里不需要改代码。
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
    st.header("📝 每日资产快照录入")
    user_id = st.session_state.user['user_id']
    conn = get_db_connection()
    
    # 日期选择
    col_date, _ = st.columns([1, 2])
    with col_date:
        date = st.date_input("选择快照日期", datetime.now())
        str_date = date.strftime('%Y-%m-%d')

    # 1. 准备基础数据
    assets = pd.read_sql('SELECT asset_id, name, code FROM assets WHERE user_id = ?', conn, params=(user_id,))
    
    if assets.empty:
        st.warning("暂无资产。")
        conn.close()
        return

    # 2. 筛选与排序区域
    with st.expander("🔍 筛选与排序工具", expanded=True):
        # 第一行：筛选条件
        c1, c2, c3 = st.columns([2, 1, 2])
        with c1:
            kw = st.text_input("关键字搜索", placeholder="名称/代码")
        with c2:
            all_groups = pd.read_sql("SELECT DISTINCT tag_group FROM tags WHERE user_id = ?", conn, params=(user_id,))
            grp_list = ["(不筛选)"] + all_groups['tag_group'].tolist()
            sel_group = st.selectbox("标签组", grp_list)
        with c3:
            sel_tags = []
            if sel_group != "(不筛选)":
                t_df = pd.read_sql("SELECT tag_name FROM tags WHERE user_id=? AND tag_group=?", conn, params=(user_id, sel_group))
                opts = ["【无此标签】"] + t_df['tag_name'].tolist()
                sel_tags = st.multiselect("标签名", opts)

        # 第二行：排序条件 (新增功能)
        st.divider()
        s1, s2 = st.columns([1, 3])
        with s1:
            st.caption("设置列表排序方式：")
        with s2:
            sort_option = st.radio(
                "排序依据", 
                ["默认 (ID)", "💰 总金额 (高→低)", "💰 总金额 (低→高)", 
                 "📈 持有收益 (高→低)", "📉 持有收益 (低→高)", 
                 "🚀 收益率 (高→低)", "🥀 收益率 (低→高)"],
                horizontal=True
            )

    # 3. 执行筛选
    filtered_ids = set(assets['asset_id'].tolist())
    
    # A. 关键字
    if kw:
        matched = assets[assets['name'].str.contains(kw, case=False) | assets['code'].str.contains(kw, case=False, na=False)]
        filtered_ids = filtered_ids.intersection(set(matched['asset_id']))
    
    # B. 标签
    if sel_group != "(不筛选)" and sel_tags:
        sql_labeled = '''
            SELECT atm.asset_id, t.tag_name 
            FROM asset_tag_map atm JOIN tags t ON atm.tag_id = t.tag_id 
            WHERE t.user_id = ? AND t.tag_group = ?
        '''
        df_labeled = pd.read_sql(sql_labeled, conn, params=(user_id, sel_group))
        
        target_group_ids = set()
        if "【无此标签】" in sel_tags:
            target_group_ids.update(filtered_ids - set(df_labeled['asset_id']))
        
        real_tags = [t for t in sel_tags if t != "【无此标签】"]
        if real_tags:
            target_group_ids.update(set(df_labeled[df_labeled['tag_name'].isin(real_tags)]['asset_id']))
            
        filtered_ids = filtered_ids.intersection(target_group_ids)

    # 4. 获取数据并合并
    final_df = assets[assets['asset_id'].isin(filtered_ids)].copy()
    
    if final_df.empty:
        st.info("没有符合条件的资产。")
    else:
        # 获取快照
        ids_tuple = tuple(final_df['asset_id'].tolist())
        if len(ids_tuple) == 1:
            query_str = f"({ids_tuple[0]})"
        else:
            query_str = str(ids_tuple)
            
        snap_query = f'''SELECT asset_id, amount, profit, cost, yield_rate 
                         FROM snapshots WHERE date = ? AND asset_id IN {query_str}'''
        
        snapshots = pd.read_sql(snap_query, conn, params=(str_date,))
        merged = pd.merge(final_df, snapshots, on='asset_id', how='left')
        
        # 填充空值 (保证排序时不报错)
        merged['amount'] = merged['amount'].fillna(0.0)
        merged['profit'] = merged['profit'].fillna(0.0)
        merged['yield_rate'] = merged['yield_rate'].fillna(0.0)

        # --- 5. 执行排序 (新增核心逻辑) ---
        if "总金额 (高→低)" in sort_option:
            merged = merged.sort_values(by='amount', ascending=False)
        elif "总金额 (低→高)" in sort_option:
            merged = merged.sort_values(by='amount', ascending=True)
        elif "持有收益 (高→低)" in sort_option:
            merged = merged.sort_values(by='profit', ascending=False)
        elif "持有收益 (低→高)" in sort_option: # 亏损最多的排前面
            merged = merged.sort_values(by='profit', ascending=True)
        elif "收益率 (高→低)" in sort_option:
            merged = merged.sort_values(by='yield_rate', ascending=False)
        elif "收益率 (低→高)" in sort_option:
            merged = merged.sort_values(by='yield_rate', ascending=True)
        # 默认按ID排序 (pandas 默认索引顺序)

        # --- 6. 显示表格 ---
        st.caption(f"当前显示: {len(merged)} 条")

        edited_snapshot = st.data_editor(
            merged,
            column_config={
                "asset_id": st.column_config.NumberColumn("ID", disabled=True, width="small"),
                "name": st.column_config.TextColumn("资产名称", disabled=True),
                "code": st.column_config.TextColumn("代码", disabled=True),
                "amount": st.column_config.NumberColumn("💰 总市值", format="¥%.2f", required=True),
                "profit": st.column_config.NumberColumn("📈 持有收益", format="¥%.2f", required=True),
                "cost": st.column_config.NumberColumn("本金", disabled=True, format="¥%.2f"),
                "yield_rate": st.column_config.NumberColumn("收益率", disabled=True, format="%.2f%%"),
            },
            hide_index=True,
            use_container_width=True,
            # Key 加入 sort_option，保证切换排序时表格刷新
            key=f"entry_{len(merged)}_{kw}_{sel_group}_{sort_option}"
        )

        # --- 7. 保存逻辑 ---
        if st.button("💾 保存当前数据", type="primary"):
            try:
                c = 0
                for _, row in edited_snapshot.iterrows():
                    amt = float(row['amount'])
                    prof = float(row['profit'])
                    cost = amt - prof
                    y_rate = (prof / cost * 100) if cost != 0 else 0.0
                    
                    conn.execute('''
                        INSERT INTO snapshots (asset_id, date, amount, profit, cost, yield_rate) 
                        VALUES (?, ?, ?, ?, ?, ?)
                        ON CONFLICT(asset_id, date) DO UPDATE SET 
                        amount=excluded.amount, profit=excluded.profit, 
                        cost=excluded.cost, yield_rate=excluded.yield_rate
                    ''', (row['asset_id'], str_date, amt, prof, cost, y_rate))
                    c += 1
                conn.commit()
                st.success(f"已保存 {c} 条记录！")
            except Exception as e:
                st.error(f"保存失败: {e}")

    conn.close()

# --- 辅助函数：核心数据处理逻辑 ---
def process_analytics_data(conn, user_id):
    """
    提取所有快照数据并进行标签聚合。
    因为数据库现在直接存储了 cost 和 profit，无需再倒推计算。
    """
    # 1. 获取基础数据 (直接读库，无需计算)
    df_raw = pd.read_sql('''
        SELECT s.date, s.asset_id, s.amount, s.profit, s.cost, s.yield_rate, a.name
        FROM snapshots s
        JOIN assets a ON s.asset_id = a.asset_id
        WHERE a.user_id = ?
    ''', conn, params=(user_id,))

    if df_raw.empty:
        return None, None

    df_raw['date'] = pd.to_datetime(df_raw['date'])
    
    # 2. 获取标签关联关系
    df_tags = pd.read_sql('''
        SELECT t.tag_group, t.tag_name, atm.asset_id
        FROM tags t
        JOIN asset_tag_map atm ON t.tag_id = atm.tag_id
        WHERE t.user_id = ?
    ''', conn, params=(user_id,))

    # 3. 标签维度聚合逻辑
    tag_analytics = []
    
    if not df_tags.empty:
        # 将快照与标签关联
        merged = pd.merge(df_raw, df_tags, on='asset_id', how='inner')
        
        # 统计每个标签下理论上的资产数量 (用于完整性检查)
        tag_asset_counts = df_tags.groupby(['tag_group', 'tag_name'])['asset_id'].nunique().to_dict()
        
        # 按 [日期, 标签组, 标签名] 聚合
        grouped = merged.groupby(['date', 'tag_group', 'tag_name'])
        
        for name, group in grouped:
            date, tag_group, tag_name = name
            
            # 直接累加
            total_amount = group['amount'].sum()
            total_profit = group['profit'].sum()
            total_cost = group['cost'].sum()
            
            # 重新计算聚合后的收益率: 总收益 / 总本金
            # (不能简单平均，必须加权，这里用 总收益/总本金 最准)
            weighted_yield = (total_profit / total_cost * 100) if total_cost != 0 else 0.0
            
            # 数据完整性检查
            current_count = group['asset_id'].nunique()
            expected_count = tag_asset_counts.get((tag_group, tag_name), 0)
            is_complete = current_count == expected_count
            
            tag_analytics.append({
                'date': date,
                'tag_group': tag_group,
                'tag_name': tag_name,
                'amount': total_amount,
                'profit': total_profit,
                'cost': total_cost, # 也可以展示本金趋势
                'yield_rate': weighted_yield,
                'is_complete': is_complete,
                'missing_count': expected_count - current_count
            })
            
    df_tags_agg = pd.DataFrame(tag_analytics)
    return df_raw, df_tags_agg

# --- 新版看板页面 ---
def page_dashboard():
    st.header("📊 深度资产透视")
    user_id = st.session_state.user['user_id']
    conn = get_db_connection()

    # 处理数据
    df_assets, df_tags = process_analytics_data(conn, user_id)
    conn.close()

    if df_assets is None or df_assets.empty:
        st.info("👋 暂无数据，请先前往【数据录入】页面添加资产快照。")
        return

    # 全局日期范围
    min_date = df_assets['date'].min().date()
    max_date = df_assets['date'].max().date()
    
    st.caption(f"数据统计范围：{min_date} ~ {max_date}")
    
    tab1, tab2, tab3 = st.tabs(["📈 趋势分析", "🍰 每日透视", "⚠️ 数据校验"])

    # === TAB 1: 趋势分析 (保持不变) ===
    with tab1:
        # --- 1. 总资产净值走势 ---
        st.subheader("💰 总资产净值走势")
        
        daily_total = df_assets.groupby('date')[['amount', 'profit', 'cost']].sum().reset_index()
        
        daily_total['yield_rate'] = daily_total.apply(
            lambda row: (row['profit'] / row['cost'] * 100) if row['cost'] != 0 else 0.0, 
            axis=1
        )
        
        daily_total['amount_w'] = daily_total['amount'] / 10000
        daily_total['profit_w'] = daily_total['profit'] / 10000
        
        fig_total = go.Figure()
        fig_total.add_trace(go.Scatter(
            x=daily_total['date'], y=daily_total['amount_w'],
            name='总资产', mode='lines', fill='tozeroy', 
            line=dict(color='#2E86C1', width=2),
            hovertemplate='总资产: %{y:.2f}万<extra></extra>'
        ))
        fig_total.add_trace(go.Scatter(
            x=daily_total['date'], y=daily_total['profit_w'],
            name='持有收益', mode='lines',
            line=dict(color='#27AE60', width=2), 
            hovertemplate='持有收益: %{y:.2f}万<extra></extra>'
        ))
        fig_total.add_trace(go.Scatter(
            x=daily_total['date'], y=daily_total['yield_rate'],
            name='收益率', mode='lines',
            line=dict(color='#E74C3C', width=2, dash='dot'), 
            yaxis='y2',
            hovertemplate='收益率: %{y:.2f}%<extra></extra>'
        ))
        fig_total.update_layout(
            hovermode="x unified",
            yaxis=dict(title=dict(text="金额 (万)", font=dict(color="#2E86C1")), tickfont=dict(color="#2E86C1")),
            yaxis2=dict(title=dict(text="收益率 (%)", font=dict(color="#E74C3C")), tickfont=dict(color="#E74C3C"), overlaying='y', side='right'),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        st.plotly_chart(fig_total, use_container_width=True)

        csv_total = daily_total.to_csv(index=False).encode('utf-8-sig')
        st.download_button(
            label="📥 导出总资产趋势数据 (CSV)",
            data=csv_total,
            file_name=f'total_assets_trend_{datetime.now().strftime("%Y%m%d")}.csv',
            mime='text/csv',
            help="包含每日的总资产、总收益、总本金和综合收益率"
        )

        st.divider()

        # --- 2. 结构化趋势详细对比 ---
        st.subheader("📊 结构化趋势详细对比")
        
        c1, c2, c3 = st.columns([1, 1, 2])
        with c1:
            view_mode = st.radio("分析维度", ["按具体资产", "按标签组"], horizontal=True, key="trend_view")
        with c2:
            metric_type = st.selectbox("画图指标 (Y轴)", 
                                     ["总金额 (Amount)", "持有收益 (Profit)", "收益率 (Yield %)", "占比 (Share %)"], 
                                     key="trend_metric")
        with c3:
            tooltip_extras = st.multiselect(
                "🖱️ 悬停显示额外指标", 
                ["总金额", "持有收益", "本金", "收益率", "占比"],
                default=["总金额", "持有收益", "收益率"], 
                key="trend_tooltip"
            )

        plot_df = None
        color_col = ""
        y_col = ""
        y_unit = ""
        y_title = ""

        if view_mode == "按具体资产":
            plot_df = df_assets.copy()
            color_col = "name"
        else: 
            if df_tags is None or df_tags.empty:
                st.warning("暂无标签数据。")
            else:
                groups = df_tags['tag_group'].unique()
                selected_group = st.selectbox("选择标签分组", groups, key="trend_group")
                plot_df = df_tags[df_tags['tag_group'] == selected_group].copy()
                color_col = "tag_name"
        
        if plot_df is not None:
            plot_df['amt_w'] = plot_df['amount'] / 10000
            plot_df['prof_w'] = plot_df['profit'] / 10000
            plot_df['cost_w'] = plot_df['cost'] / 10000
            daily_sums = plot_df.groupby('date')['amount'].transform('sum')
            plot_df['share'] = (plot_df['amount'] / daily_sums * 100).fillna(0)

            if metric_type == "总金额 (Amount)":
                y_col = "amt_w"; y_unit = "w"; y_title = "金额 (万)"
            elif metric_type == "持有收益 (Profit)":
                y_col = "prof_w"; y_unit = "w"; y_title = "收益 (万)"
            elif metric_type == "收益率 (Yield %)":
                y_col = "yield_rate"; y_unit = "%"; y_title = "收益率 (%)"
            elif metric_type == "占比 (Share %)":
                y_col = "share"; y_unit = "%"; y_title = "占比 (%)"

            custom_data_cols = ['amt_w', 'prof_w', 'cost_w', 'yield_rate', 'share']
            if metric_type == "占比 (Share %)":
                fig = px.area(plot_df, x='date', y=y_col, color=color_col, groupnorm='percent', custom_data=custom_data_cols)
            else:
                fig = px.line(plot_df, x='date', y=y_col, color=color_col, markers=True, custom_data=custom_data_cols)
            
            hover_html = f"<b>%{{fullData.name}}</b>: "
            info_parts = []
            info_parts.append(f"<b>{metric_type.split(' ')[0]}:%{{y:.2f}}{y_unit}</b>")
            
            if tooltip_extras:
                if "总金额" in tooltip_extras: info_parts.append("💰%{customdata[0]:.2f}w")
                if "持有收益" in tooltip_extras: info_parts.append("📈%{customdata[1]:.2f}w")
                if "本金" in tooltip_extras: info_parts.append("🌱%{customdata[2]:.2f}w")
                if "收益率" in tooltip_extras: info_parts.append("🚀%{customdata[3]:.1f}%")
                if "占比" in tooltip_extras: info_parts.append("🍰%{customdata[4]:.1f}%")
            
            hover_html += "   ".join(info_parts)
            hover_html += "<extra></extra>"
            fig.update_traces(hovertemplate=hover_html)
            fig.update_layout(hovermode="x unified", yaxis_title=y_title, legend_title_text="")
            st.plotly_chart(fig, use_container_width=True)

            export_cols = ['date', color_col, 'amount', 'profit', 'cost', 'yield_rate', 'share']
            csv_struct = plot_df[export_cols].to_csv(index=False).encode('utf-8-sig')
            st.download_button(
                label=f"📥 导出当前筛选数据 ({view_mode})",
                data=csv_struct,
                file_name=f'trend_structure_{view_mode}_{datetime.now().strftime("%Y%m%d")}.csv',
                mime='text/csv'
            )

    # === TAB 2: 每日透视 (核心修改区域) ===
    with tab2:
        st.subheader("🍰 每日资产快照分析")
        
        control_c1, control_c2 = st.columns(2)
        with control_c1:
            available_dates = sorted(df_assets['date'].unique(), reverse=True)
            selected_date = st.selectbox("📅 选择要查看的日期", available_dates, format_func=lambda x: x.strftime('%Y-%m-%d'))
        
        with control_c2:
            tag_groups = list(df_tags['tag_group'].unique()) if (df_tags is not None and not df_tags.empty) else []
            dim_options = ["按具体资产"] + tag_groups
            selected_dim = st.selectbox("🔍 分析维度 (筛选标签组)", dim_options)

        st.divider()

        if selected_dim == "按具体资产":
            day_data = df_assets[df_assets['date'] == selected_date].copy()
            name_col = 'name'
        else:
            if df_tags is None:
                st.warning("无标签数据")
                day_data = pd.DataFrame()
            else:
                day_data = df_tags[
                    (df_tags['date'] == selected_date) & 
                    (df_tags['tag_group'] == selected_dim)
                ].copy()
                name_col = 'tag_name'

        if not day_data.empty:
            # 预计算 '万' 单位数据，用于饼图悬停
            day_data['amount_w'] = day_data['amount'] / 10000
            day_data['profit_w'] = day_data['profit'] / 10000

            day_total_amt = day_data['amount'].sum()
            day_total_profit = day_data['profit'].sum()
            
            m1, m2, m3 = st.columns(3)
            m1.metric("当日总资产", f"{day_total_amt/10000:,.2f}w")
            m2.metric("当日持有收益", f"{day_total_profit/10000:,.2f}w", 
                      delta_color="normal" if day_total_profit >= 0 else "inverse")
            if day_total_amt - day_total_profit != 0:
                 total_yield = day_total_profit / (day_total_amt - day_total_profit) * 100
                 m3.metric("当日综合收益率", f"{total_yield:.2f}%")

            chart_c1, chart_c2 = st.columns(2)
            
            # --- 饼图 A: 总金额 ---
            with chart_c1:
                fig_pie_amt = px.pie(
                    day_data, 
                    values='amount', 
                    names=name_col, 
                    title=f"【总金额】占比 ({selected_dim})", 
                    hole=0.4,
                    # 将计算好的 '万' 数据传进去
                    custom_data=['amount_w']
                )
                fig_pie_amt.update_traces(
                    textposition='inside', 
                    textinfo='percent+label',
                    # 格式：名称: 💰金额w (🍰百分比)
                    hovertemplate='<b>%{label}</b>: 💰%{customdata[0]:.2f}w (🍰%{percent})<extra></extra>'
                )
                st.plotly_chart(fig_pie_amt, use_container_width=True)
            
            # --- 饼图 B: 收益/贡献 ---
            with chart_c2:
                if (day_data['profit'] < 0).any():
                    st.caption("⚠️ 注意：饼图仅展示盈利部分。")
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
                            hovertemplate='<b>%{label}</b>: 📈%{customdata[0]:.2f}w (🍰%{percent})<extra></extra>'
                        )
                        st.plotly_chart(fig_pie_prof, use_container_width=True)
                else:
                    fig_pie_prof = px.pie(
                        day_data, 
                        values='profit', 
                        names=name_col, 
                        title=f"【持有收益】占比 ({selected_dim})", 
                        hole=0.4,
                        custom_data=['profit_w']
                    )
                    fig_pie_prof.update_traces(
                        textposition='inside', 
                        textinfo='percent+label',
                        hovertemplate='<b>%{label}</b>: 📈%{customdata[0]:.2f}w (🍰%{percent})<extra></extra>'
                    )
                    st.plotly_chart(fig_pie_prof, use_container_width=True)

            # 4. 详细数据表
            st.subheader(f"📋 详细数据表")
            display_cols = [name_col, 'amount', 'profit', 'yield_rate']
            if 'cost' in day_data.columns: display_cols.insert(2, 'cost')
            
            show_df = day_data[display_cols].copy()
            show_df = show_df.sort_values('amount', ascending=False)
            
            st.dataframe(
                show_df,
                column_config={
                    name_col: "名称/标签",
                    "amount": st.column_config.NumberColumn("总金额", format="¥%.2f"),
                    "cost": st.column_config.NumberColumn("本金", format="¥%.2f"),
                    "profit": st.column_config.NumberColumn("持有收益", format="¥%.2f"),
                    "yield_rate": st.column_config.NumberColumn("收益率", format="%.2f%%"),
                },
                use_container_width=True,
                hide_index=True
            )

            csv_day = show_df.to_csv(index=False).encode('utf-8-sig')
            st.download_button(
                label=f"📥 导出当日数据表 ({selected_date.strftime('%Y-%m-%d')})",
                data=csv_day,
                file_name=f'daily_snapshot_{selected_date.strftime("%Y%m%d")}.csv',
                mime='text/csv'
            )
        else:
            st.info("该日期/维度下无数据。")

    # === TAB 3 ===
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

# --- 新增页面：定投计划与看板 ---
def page_investment_plans():
    st.header("📅 定投计划与未来现金流")
    user_id = st.session_state.user['user_id']
    conn = get_db_connection()

    tab1, tab2 = st.tabs(["⚙️ 计划管理", "🔮 未来现金流看板"])

    # === TAB 1: 计划管理 (CRUD) ===
    with tab1:
        st.caption("在这里管理你的自动定投计划（仅用于统计未来资金需求，不会自动记账）。")
        
        # 1. 新增计划表单 (带高级筛选)
        with st.expander("➕ 新增定投计划", expanded=True):
            
            # --- A. 准备基础数据 ---
            all_assets = pd.read_sql('SELECT asset_id, name, code FROM assets WHERE user_id = ?', conn, params=(user_id,))
            
            if all_assets.empty:
                st.warning("⚠️ 请先去【资产与标签管理】页面添加至少一个资产。")
            else:
                # --- B. 筛选工具栏 (新增) ---
                st.markdown("##### 🔍 第一步：筛选资产")
                f_col1, f_col2, f_col3 = st.columns([2, 1, 2])
                
                with f_col1:
                    filter_kw = st.text_input("关键字搜索", placeholder="名称/代码...", key="plan_filter_kw")
                
                with f_col2:
                    # 获取标签组
                    all_groups = pd.read_sql("SELECT DISTINCT tag_group FROM tags WHERE user_id = ?", conn, params=(user_id,))
                    grp_list = ["(不筛选)"] + all_groups['tag_group'].tolist()
                    sel_group = st.selectbox("标签组", grp_list, key="plan_filter_group")
                
                with f_col3:
                    sel_tags = []
                    if sel_group != "(不筛选)":
                        t_df = pd.read_sql("SELECT tag_name FROM tags WHERE user_id=? AND tag_group=?", conn, params=(user_id, sel_group))
                        opts = ["【无此标签】"] + t_df['tag_name'].tolist()
                        sel_tags = st.multiselect("标签状态", opts, key="plan_filter_tags")

                # --- C. 执行筛选逻辑 ---
                filtered_ids = set(all_assets['asset_id'].tolist())
                
                # 1. 关键字
                if filter_kw:
                    matched = all_assets[all_assets['name'].str.contains(filter_kw, case=False) | all_assets['code'].str.contains(filter_kw, case=False, na=False)]
                    filtered_ids = filtered_ids.intersection(set(matched['asset_id']))
                
                # 2. 标签
                if sel_group != "(不筛选)" and sel_tags:
                    sql_labeled = '''
                        SELECT atm.asset_id, t.tag_name 
                        FROM asset_tag_map atm JOIN tags t ON atm.tag_id = t.tag_id 
                        WHERE t.user_id = ? AND t.tag_group = ?
                    '''
                    df_labeled = pd.read_sql(sql_labeled, conn, params=(user_id, sel_group))
                    
                    target_group_ids = set()
                    if "【无此标签】" in sel_tags:
                        target_group_ids.update(filtered_ids - set(df_labeled['asset_id']))
                    
                    real_tags = [t for t in sel_tags if t != "【无此标签】"]
                    if real_tags:
                        target_group_ids.update(set(df_labeled[df_labeled['tag_name'].isin(real_tags)]['asset_id']))
                        
                    filtered_ids = filtered_ids.intersection(target_group_ids)
                
                # --- D. 渲染结果与表单 ---
                final_assets = all_assets[all_assets['asset_id'].isin(filtered_ids)].copy()
                
                st.divider()
                st.markdown("##### 📝 第二步：设置定投参数")
                
                if final_assets.empty:
                    st.info("没有符合筛选条件的资产。")
                else:
                    c1, c2 = st.columns(2)
                    with c1:
                        # 下拉框只显示筛选后的资产
                        sel_asset = st.selectbox(
                            f"选择资产 (当前筛选出 {len(final_assets)} 个)", 
                            options=final_assets['asset_id'], 
                            format_func=lambda x: final_assets[final_assets['asset_id']==x]['name'].values[0],
                            key="plan_new_asset"
                        )
                        amount = st.number_input("每次定投金额", min_value=0.0, step=100.0, key="plan_new_amount")
                    
                    with c2:
                        freq = st.selectbox("频率", ["每周", "每月", "每天"], key="plan_new_freq")
                        
                        exec_day = 0
                        if freq == "每周":
                            weekdays = {0:"周一", 1:"周二", 2:"周三", 3:"周四", 4:"周五", 5:"周六", 6:"周日"}
                            exec_day = st.selectbox("选择周几", options=list(weekdays.keys()), 
                                                  format_func=lambda x: weekdays[x],
                                                  key="plan_new_day_week")
                        elif freq == "每月":
                            exec_day = st.number_input("选择每月几号 (建议1-28日)", min_value=1, max_value=28, value=1,
                                                     key="plan_new_day_month")

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
                                st.success(f"✅ 已添加对【{final_assets[final_assets['asset_id']==sel_asset]['name'].values[0]}】的定投计划！")
                                st.rerun()
                            except Exception as e:
                                st.error(f"保存失败: {e}")

        # 2. 现有计划列表 (保持不变)
        st.subheader("📋 正在运行的计划")
        
        plans_df = pd.read_sql('''
            SELECT p.plan_id, a.name, p.amount, p.frequency, p.execution_day, p.is_active
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
                    "amount": st.column_config.NumberColumn("金额", format="¥%.2f"),
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
                if save_changes_to_db(edited_plans, plans_df, 'investment_plans', 'plan_id', user_id, fixed_cols={'user_id':user_id}):
                    st.rerun()
        else:
            st.info("暂无定投计划。")

    # === TAB 2: 现金流看板 ===
    with tab2:
        # 1. 计算未来现金流逻辑
        st.subheader("🗓️ 未来 30 天资金需求推演")
        
        # 获取所有启用的计划
        active_plans = pd.read_sql('''
            SELECT p.asset_id, a.name, p.amount, p.frequency, p.execution_day
            FROM investment_plans p
            JOIN assets a ON p.asset_id = a.asset_id
            WHERE p.user_id = ? AND p.is_active = 1
        ''', conn, params=(user_id,))
        
        # 获取资产的标签信息
        asset_tags = pd.read_sql('''
            SELECT atm.asset_id, t.tag_group, t.tag_name
            FROM asset_tag_map atm
            JOIN tags t ON atm.tag_id = t.tag_id
            WHERE t.user_id = ?
        ''', conn, params=(user_id,))

        if active_plans.empty:
            st.info("请先启用至少一个定投计划。")
        else:
            # 推演未来30天
            today = datetime.now().date()
            future_days = 30
            projection_data = []

            for i in range(future_days):
                current_date = today + timedelta(days=i)
                current_weekday = current_date.weekday() # 0=周一
                current_day = current_date.day
                
                for _, plan in active_plans.iterrows():
                    hit = False
                    if plan['frequency'] == '每天':
                        hit = True
                    elif plan['frequency'] == '每周' and int(plan['execution_day']) == current_weekday:
                        hit = True
                    elif plan['frequency'] == '每月' and int(plan['execution_day']) == current_day:
                        hit = True
                    
                    if hit:
                        projection_data.append({
                            "date": current_date,
                            "asset_id": plan['asset_id'],
                            "asset_name": plan['name'],
                            "amount": plan['amount']
                        })

            if not projection_data:
                st.warning("未来30天内没有匹配的定投日。")
            else:
                df_proj = pd.DataFrame(projection_data)
                
                # --- 可视化 A: 总览 ---
                total_needed = df_proj['amount'].sum()
                col1, col2 = st.columns(2)
                col1.metric("未来 30 天总定投金额", f"¥{total_needed:,.2f}")
                col2.metric("平均每日资金流出", f"¥{total_needed/30:,.2f}")

                st.divider()

                # --- 可视化 B: 堆叠柱状图 (升级版) ---
                
                # 1. 维度选择
                all_groups = asset_tags['tag_group'].unique().tolist() if not asset_tags.empty else []
                # 新增 "按具体资产" 选项
                dim_options = ["按具体资产"] + all_groups
                selected_dim = st.selectbox("选择分析维度 (堆叠方式)", dim_options)
                
                # 2. 数据处理与聚合
                df_viz = df_proj.copy()
                
                if selected_dim == "按具体资产":
                    # 直接使用资产名作为分类
                    df_viz['category'] = df_viz['asset_name']
                else:
                    # 按标签组关联
                    tags_in_group = asset_tags[asset_tags['tag_group'] == selected_dim]
                    df_viz = pd.merge(df_viz, tags_in_group, on='asset_id', how='left')
                    df_viz['tag_name'] = df_viz['tag_name'].fillna('未分类')
                    df_viz['category'] = df_viz['tag_name']

                # 核心聚合：按 [日期, 分类] 汇总金额
                # 这样如果同一天有两个 "高风险" 资产，它们会合并成一个 "高风险" 的柱子片段
                df_agg = df_viz.groupby(['date', 'category'])['amount'].sum().reset_index()
                
                # 计算每日占比 (用于悬停显示)
                # 算出每天的总金额
                daily_totals = df_agg.groupby('date')['amount'].transform('sum')
                # 计算当前分类占当天的比例
                df_agg['share'] = (df_agg['amount'] / daily_totals) * 100

                # 3. 绘图
                fig = px.bar(
                    df_agg, 
                    x='date', 
                    y='amount', 
                    color='category',
                    title=f"未来 30 天每日定投分布 ({selected_dim})",
                    labels={'amount': '金额', 'date': '日期', 'category': '类别'},
                    # 把占比数据传进去，方便 tooltip 调用
                    custom_data=['share'] 
                )
                
                # 4. 样式优化 (核心修改)
                # hovertemplate: 定制每一行的显示格式
                # %{customdata[0]:.1f}% 读取上面传入的 share 数据
                fig.update_traces(
                    hovertemplate='<b>%{fullData.name}</b>: ¥%{y:,.0f} (%{customdata[0]:.1f}%)<extra></extra>'
                )
                
                # hovermode="x unified": 开启统一悬停框，鼠标指到哪一天，显示那一天所有类别的数据
                fig.update_layout(
                    hovermode="x unified",
                    legend_title_text="" # 隐藏图例标题更清爽
                )
                
                st.plotly_chart(fig, use_container_width=True)

                # --- 可视化 C: 日历清单 ---
                with st.expander("查看详细扣款日历"):
                    st.dataframe(
                        df_proj.sort_values('date'),
                        column_config={
                            "date": "日期",
                            "asset_name": "扣款资产",
                            "amount": st.column_config.NumberColumn("扣款金额", format="¥%.2f")
                        },
                        hide_index=True,
                        use_container_width=True
                    )

    conn.close()

def page_investment_notes():
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
    st.header("🔥 FIRE 财富自由展望")
    st.caption("推演未来 50 年的资产复利增长，看看你在多少岁能实现财务自由。")
    
    user_id = st.session_state.user['user_id']
    conn = get_db_connection()
    
    # --- 1. 获取当前总资产 (起点) ---
    latest_date_row = conn.execute('SELECT MAX(date) as d FROM snapshots JOIN assets ON snapshots.asset_id = assets.asset_id WHERE assets.user_id = ?', (user_id,)).fetchone()
    
    current_total_assets = 0.0
    start_year = datetime.now().year
    
    if latest_date_row and latest_date_row['d']:
        latest_date = latest_date_row['d']
        res = conn.execute('''
            SELECT SUM(amount) as total 
            FROM snapshots s
            JOIN assets a ON s.asset_id = a.asset_id
            WHERE a.user_id = ? AND s.date = ?
        ''', (user_id, latest_date)).fetchone()
        if res and res['total']:
            current_total_assets = res['total']
            
    conn.close()

    # --- 2. 参数设置区域 ---
    with st.container():
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            # 修改点：单位改为万元，默认值除以10000
            base_amount_wan = st.number_input(
                "当前总资产 (万元)", 
                value=float(current_total_assets) / 10000.0, 
                step=1.0, 
                format="%.2f"
            )
            base_amount = base_amount_wan * 10000 # 换算回元参与核心计算
            
        with c2:
            current_age = st.number_input("当前年龄 (岁)", value=28, step=1, format="%d")
        with c3:
            annual_rate = st.number_input("预期年化收益率 (%)", value=8.0, step=0.5)
        with c4:
            # 单位：万元
            annual_addition_wan = st.number_input("每年追加本金 (万元)", value=12.0, step=1.0, help="假设每年工资结余用于投资的金额")
            annual_addition = annual_addition_wan * 10000 # 换算回元参与核心计算

    st.divider()

    # --- 3. 复利推演计算 ---
    years_to_project = 50
    projection_data = []
    
    current_balance = base_amount
    cumulative_principal = base_amount 
    cumulative_profit = 0.0
    
    # 关键里程碑 (万)
    milestones = [100, 300, 500, 1000, 2000, 3000, 5000, 10000] 
    achieved_milestones = set()
    milestone_events = [] 

    # 第0年（现在）
    projection_data.append({
        "year": start_year,
        "age": current_age,
        "balance": current_balance,
        "principal": cumulative_principal,
        "profit": 0.0
    })
    
    # 检查起步是否已经达成某些成就
    for m in milestones:
        if current_balance >= m * 10000:
            achieved_milestones.add(m)

    for i in range(1, years_to_project + 1):
        # 核心复利公式
        current_balance = current_balance * (1 + annual_rate / 100.0) + annual_addition
        cumulative_principal += annual_addition
        cumulative_profit = current_balance - cumulative_principal # 计算累计收益
        
        this_year = start_year + i
        this_age = current_age + i
        
        projection_data.append({
            "year": this_year,
            "age": this_age,
            "balance": current_balance,
            "principal": cumulative_principal,
            "profit": cumulative_profit
        })
        
        # 检查里程碑
        for m in milestones:
            if m not in achieved_milestones and current_balance >= m * 10000:
                achieved_milestones.add(m)
                milestone_events.append({
                    "year": this_year,
                    "age": this_age, # 记录达成年龄
                    "amount": current_balance,
                    "milestone": m,
                    "text": f"🚩 {this_age}岁: 破 {m} 万" 
                })

    df_proj = pd.DataFrame(projection_data)
    # 单位换算为万 (用于绘图)
    df_proj['balance_w'] = df_proj['balance'] / 10000
    df_proj['principal_w'] = df_proj['principal'] / 10000
    df_proj['profit_w'] = df_proj['profit'] / 10000

    # --- 4. 绘图 (Plotly Graph Objects) ---
    fig = go.Figure()

    # A. 总资产曲线 (红色实线，最粗)
    fig.add_trace(go.Scatter(
        x=df_proj['year'], 
        y=df_proj['balance_w'],
        mode='lines',
        name='总资产 (复利)',
        line=dict(color='#E74C3C', width=4),
        hovertemplate='<b>总资产</b>: %{y:.0f}万<extra></extra>'
    ))
    
    # B. 累计收益曲线 (绿色实线)
    fig.add_trace(go.Scatter(
        x=df_proj['year'], 
        y=df_proj['profit_w'],
        mode='lines',
        name='累计复利收益',
        line=dict(color='#2ECC71', width=2),
        hovertemplate='<b>累计收益</b>: %{y:.0f}万<extra></extra>'
    ))

    # C. 投入本金曲线 (灰色虚线)
    fig.add_trace(go.Scatter(
        x=df_proj['year'], 
        y=df_proj['principal_w'],
        mode='lines',
        name='投入本金',
        line=dict(color='#95A5A6', width=2, dash='dot'),
        hovertemplate='<b>投入本金</b>: %{y:.0f}万<extra></extra>'
    ))

    # D. 添加里程碑标记
    for event in milestone_events:
        fig.add_annotation(
            x=event['year'],
            y=event['amount'] / 10000,
            text=event['text'],
            showarrow=True,
            arrowhead=2,
            arrowsize=1,
            arrowwidth=2,
            arrowcolor="#F39C12", 
            ax=0,
            ay=-40,
            font=dict(size=11, color="#D35400", family="Arial Black"),
            bgcolor="rgba(255, 255, 255, 0.7)",
            bordercolor="#F39C12",
            borderwidth=1
        )

    fig.update_layout(
        title="未来 50 年资产增长趋势 (单位: 万)",
        xaxis_title="年份",
        yaxis_title="金额 (万)",
        hovermode="x unified",
        legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01),
        height=600 
    )

    st.plotly_chart(fig, use_container_width=True)

    # --- 5. 总结文字 ---
    if not df_proj.empty:
        # 找几个关键节点
        p10 = df_proj.iloc[10]
        p20 = df_proj.iloc[20]
        p30 = df_proj.iloc[30]
        
        # 计算“收益超过本金”的年份
        cross_point = df_proj[df_proj['profit'] > df_proj['principal']].head(1)
        cross_text = ""
        if not cross_point.empty:
            cp = cross_point.iloc[0]
            cross_text = f"🚀 **神奇时刻**：在 **{cp['age']}岁 ({cp['year']}年)**，你的累计复利收益（{cp['profit_w']:.0f}万）将首次超过你的累计投入本金！从这一天起，钱为你打工的效率超过了你为钱打工。"

        st.markdown(f"""
        ### 💡 人生财富剧本
        假设你从 **{current_age}岁** 开始，现有 **{base_amount_wan:.2f}万**，保持 **{annual_rate}%** 的年化收益，每年坚持定投 **{annual_addition_wan:.1f}万**：
        
        * **{p10['age']}岁 ({p10['year']}年)**：资产达到 **{p10['balance_w']:.0f}万**（本金 {p10['principal_w']:.0f}万 + 收益 {p10['profit_w']:.0f}万）。
        * **{p20['age']}岁 ({p20['year']}年)**：资产达到 **{p20['balance_w']:.0f}万**。
        * **{p30['age']}岁 ({p30['year']}年)**：资产达到 **{p30['balance_w']:.0f}万**。
        
        {cross_text}
        """)

    # --- 6. 详细数据表 (含年龄列) ---
    with st.expander("查看详细年份数据"):
        st.dataframe(
            df_proj[['year', 'age', 'balance', 'principal', 'profit']],
            column_config={
                "year": st.column_config.NumberColumn("年份", format="%d"),
                "age": st.column_config.NumberColumn("年龄", format="%d岁"),
                "balance": st.column_config.NumberColumn("预估总资产", format="¥%.2f"),
                "principal": st.column_config.NumberColumn("累计本金", format="¥%.2f"),
                "profit": st.column_config.NumberColumn("累计收益", format="¥%.2f"),
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
    st.header("⚙️ 系统设置与备份")
    conn = get_db_connection()
    
    # 读取当前配置
    settings = conn.execute('SELECT * FROM system_settings WHERE id = 1').fetchone()
    
    tab1, tab2 = st.tabs(["🔄 备份策略与邮箱", "📂 本地备份管理 & 恢复"])
    
    # === Tab 1: 策略配置 ===
    with tab1:
        st.subheader("1. 自动备份策略")
        st.caption("系统将在你打开应用时，根据上次备份时间自动判断是否需要执行备份。")
        
        with st.form("settings_form"):
            new_freq = st.radio("备份频率", ["关闭", "每天", "每周", "每月"], 
                              index=["关闭", "每天", "每周", "每月"].index(settings['backup_frequency']),
                              horizontal=True)
            
            st.divider()
            st.subheader("2. 邮箱推送设置 (推荐)")
            st.caption("配置 SMTP 后，每次备份都会将数据库文件发送到你的邮箱。这是防止 SD 卡损坏的最佳保障。")
            
            c1, c2 = st.columns(2)
            with c1:
                email_host = st.text_input("SMTP 服务器", value=settings['email_host'] or "", placeholder="例如 smtp.qq.com")
                email_port = st.number_input("SMTP 端口", value=settings['email_port'] or 465)
            with c2:
                email_user = st.text_input("邮箱账号", value=settings['email_user'] or "", placeholder="你的邮箱@qq.com")
                email_password = st.text_input("授权码/密码", value=settings['email_password'] or "", type="password", help="注意：QQ邮箱请使用授权码")
            
            email_to = st.text_input("接收邮箱 (留空则发给自己)", value=settings['email_to'] or "")

            if st.form_submit_button("💾 保存配置", type="primary"):
                conn.execute('''
                    UPDATE system_settings 
                    SET backup_frequency=?, email_host=?, email_port=?, email_user=?, email_password=?, email_to=?
                    WHERE id=1
                ''', (new_freq, email_host, email_port, email_user, email_password, email_to))
                conn.commit()
                st.success("配置已保存！")
                st.rerun()

        # 测试邮件按钮
        if settings['email_host']:
            st.write("")
            if st.button("📧 发送测试邮件"):
                with st.spinner("正在发送..."):
                    # 创建一个空的测试文件
                    test_file = "test_email.txt"
                    with open(test_file, "w") as f: f.write("This is a test.")
                    
                    success, msg = send_email_backup(test_file, settings)
                    os.remove(test_file)
                    
                    if success:
                        st.success(f"测试成功！{msg}")
                    else:
                        st.error(msg)

    # === Tab 2: 本地管理 ===
    with tab2:
        st.subheader("📂 本地备份文件管理")
        
        c1, c2 = st.columns([1, 3])
        with c1:
            if st.button("🚀 立即手动备份", type="primary"):
                with st.spinner("正在备份中..."):
                    success, msg = perform_backup(manual=True)
                    if success:
                        st.success(msg)
                        st.rerun()
                    else:
                        st.error(msg)
        
        # 列出文件
        backup_dir = "backups"
        if not os.path.exists(backup_dir):
            os.makedirs(backup_dir)
            
        files = sorted(Path(backup_dir).glob("*.db"), key=os.path.getmtime, reverse=True)
        
        if not files:
            st.info("暂无本地备份文件。")
        else:
            # 转换为 DataFrame 展示
            data = []
            for f in files:
                stat = f.stat()
                data.append({
                    "文件名": f.name,
                    "备份时间": datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d %H:%M:%S'),
                    "大小 (KB)": round(stat.st_size / 1024, 2),
                    "path": str(f)
                })
            
            df_files = pd.DataFrame(data)
            
            # 展示表格
            st.dataframe(df_files[["文件名", "备份时间", "大小 (KB)"]], use_container_width=True, hide_index=True)
            
            # 下载与恢复区
            st.divider()
            st.write("🛠️ 操作特定备份")
            
            sel_file = st.selectbox("选择备份文件", options=df_files['path'], format_func=lambda x: os.path.basename(x))
            
            ac1, ac2, ac3 = st.columns(3)
            with ac1:
                # 下载按钮
                if sel_file:
                    with open(sel_file, "rb") as f:
                        st.download_button("📥 下载此备份", f, file_name=os.path.basename(sel_file))
            
            with ac2:
                # 恢复按钮
                if st.button("⏪ 从此备份恢复数据"):
                    # 再次确认 (Streamlit原生没弹窗，用 session state 模拟或者简单警告)
                    try:
                        # 1. 先把当前的数据库重命名备份一下，防止误操作
                        shutil.copy2(DB_FILE, f"{DB_FILE}.before_restore")
                        # 2. 覆盖
                        shutil.copy2(sel_file, DB_FILE)
                        st.success("恢复成功！请刷新页面。")
                        st.cache_data.clear() # 清除缓存
                        st.rerun()
                    except Exception as e:
                        st.error(f"恢复失败: {e}")
            
            with ac3:
                if st.button("🗑️ 删除此备份"):
                    os.remove(sel_file)
                    st.success("已删除")
                    st.rerun()

        st.divider()
        st.subheader("📥 外部数据导入 (迁移)")
        uploaded_file = st.file_uploader("上传 .db 数据库文件 (将覆盖当前所有数据)", type="db")
        if uploaded_file:
            if st.button("⚠️ 确认覆盖并导入", type="primary"):
                with open(DB_FILE, "wb") as f:
                    f.write(uploaded_file.getbuffer())
                st.success("导入成功！系统已重置为上传的数据。")
                st.rerun()

    conn.close()

# --- 翻译助手函数 ---
def t(key):
    """根据当前语言返回翻译，找不到则返回 key"""
    lang = st.session_state.get('language', 'zh') # 默认为中文
    return TRANSLATIONS.get(lang, TRANSLATIONS['zh']).get(key, key)

# ==============================================================================
# 🚀 主程序入口 (Main)
# ==============================================================================
def main():
    # 1. 基础初始化
    init_db()
    
    # 2. 初始化语言状态
    if 'language' not in st.session_state:
        st.session_state.language = 'zh'

    # 3. 自动备份检查
    auto_backup_check()

    # 4. Token 自动登录
    if 'user' not in st.session_state or st.session_state.user is None:
        token = st.query_params.get("token")
        if token:
            user = get_user_from_token(token)
            if user:
                st.session_state.user = user

    # 5. 登录拦截逻辑
    if 'user' not in st.session_state or st.session_state.user is None:
        # 登录页侧边栏：语言选择
        with st.sidebar:
            st.markdown("### 🌐 Language")
            lang_code = st.selectbox(
                "选择语言 / Language",
                options=["zh", "en", "ja"],
                format_func=lambda x: {"zh": "🇨🇳 中文", "en": "🇺🇸 English", "ja": "🇯🇵 日本語"}[x],
                index=["zh", "en", "ja"].index(st.session_state.language),
                key="lang_select_login"
            )
            if lang_code != st.session_state.language:
                st.session_state.language = lang_code
                st.rerun()
                
        page_login() 
    else:
        # === 已登录状态：侧边栏导航 ===
        with st.sidebar:
            # A. 用户信息区 (独占一行，大标题)
            # 使用 subheader 让名字显眼，但不像 title 那么占地
            st.subheader(t('sidebar_welcome').format(st.session_state.user['username']))
            
            # B. 语言切换区 (独占一行，标准宽度)
            # 这里的 label 可以留空，因为图标已经很直观了，或者写个通用的 "🌐 Language"
            lang_code = st.selectbox(
                "🌐 Language / 言語",
                options=["zh", "en", "ja"],
                format_func=lambda x: {"zh": "🇨🇳 中文", "en": "🇺🇸 English", "ja": "🇯🇵 日本語"}[x],
                index=["zh", "en", "ja"].index(st.session_state.language),
                key="lang_select_sidebar"
            )
            if lang_code != st.session_state.language:
                st.session_state.language = lang_code
                st.rerun()

            st.divider()

            # C. 动态导航菜单
            nav_keys = [
                "nav_dashboard", 
                "nav_notes", 
                "nav_assets", 
                "nav_entry", 
                "nav_plans", 
                "nav_fire", 
                "nav_settings"
            ]
            nav_labels = [t(k) for k in nav_keys]
            
            selected_label = st.radio(t("sidebar_nav"), nav_labels)
            
            selected_index = nav_labels.index(selected_label)
            selected_key = nav_keys[selected_index]
            
            # D. 退出按钮
            st.divider()
            if st.button(t("btn_logout"), use_container_width=True):
                st.session_state.user = None
                st.query_params.clear()
                st.rerun()
        
        # === 页面路由分发 ===
        if selected_key == "nav_dashboard":
            page_dashboard()
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

if __name__ == '__main__':
    main()