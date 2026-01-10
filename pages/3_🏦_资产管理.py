import streamlit as st
import pandas as pd
import time

# 🔥 引入公共工具函数
from utils import (
    get_db_connection, 
    save_changes_to_db, 
    show_sidebar_user_picker
)

# ==========================================
# 0. 全局配置与用户校验
# ==========================================
st.set_page_config(page_title="资产管理", page_icon="🏦", layout="wide")

# 必须先检查用户登录状态
if "user" not in st.session_state or not st.session_state.user:
    st.warning("请先在侧边栏选择用户或登录")
    show_sidebar_user_picker()
    st.stop()

# 渲染侧边栏
show_sidebar_user_picker()

# ==========================================
# 1. 内部辅助函数 (筛选逻辑)
# ==========================================
def apply_advanced_filters(df, context_key, user_id, conn):
    """
    公共筛选逻辑: 关键字 + 标签组 + 标签名
    df: 必须包含 asset_id, name, code 列
    """
    with st.expander("🔍 高级筛选 (支持查找未分类资产)", expanded=False):
        c1, c2, c3 = st.columns([2, 1, 2])
        
        # 1. 关键字搜索
        with c1:
            kw = st.text_input("1. 关键字搜索", placeholder="资产名或代码...", key=f"kw_{context_key}")
        
        # 2. 标签组选择
        all_groups = pd.read_sql("SELECT DISTINCT tag_group FROM tags WHERE user_id = ?", conn, params=(user_id,))
        groups_list = ["(不筛选)"] + all_groups['tag_group'].tolist()
        
        with c2:
            sel_group = st.selectbox("2. 选择标签组", groups_list, key=f"grp_{context_key}")
        
        # 3. 标签名选择 (根据组动态变化)
        selected_tag_names = []
        if sel_group != "(不筛选)":
            tags_in_group = pd.read_sql("SELECT tag_name FROM tags WHERE user_id = ? AND tag_group = ?", 
                                      conn, params=(user_id, sel_group))
            options = ["【无此标签】"] + tags_in_group['tag_name'].tolist()
            
            with c3:
                selected_tag_names = st.multiselect(
                    f"3. 筛选 '{sel_group}' 下的状态", 
                    options=options,
                    key=f"tag_{context_key}",
                    placeholder="留空则显示全部"
                )
    
    # --- 开始执行筛选 ---
    # A. 关键字过滤
    if kw:
        df = df[df['name'].str.contains(kw, case=False) | df['code'].str.contains(kw, case=False, na=False)]
        
    # B. 标签过滤
    if sel_group != "(不筛选)" and selected_tag_names:
        # 找出在该组下，拥有特定标签的资产ID
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
            ids_with_tags = set(df_labeled['asset_id'].unique())
            all_current_ids = set(df['asset_id'].unique())
            ids_without_tags = all_current_ids - ids_with_tags
            target_ids.update(ids_without_tags)
        
        # 情况2: 用户选了具体的标签
        real_tags = [t for t in selected_tag_names if t != "【无此标签】"]
        if real_tags:
            ids_with_specific_tags = set(df_labeled[df_labeled['tag_name'].isin(real_tags)]['asset_id'])
            target_ids.update(ids_with_specific_tags)
        
        # 取交集
        df = df[df['asset_id'].isin(target_ids)]
        
    return df

# ==========================================
# 2. 页面主逻辑
# ==========================================

st.header("资产与标签管理")
user_id = st.session_state.user['user_id']
conn = get_db_connection()

try:
    tab1, tab2, tab3 = st.tabs(["1. 资产列表", "2. 标签定义", "3. 关联打标"])
    
    # --- Tab 1: 资产管理 ---
    with tab1:
        # 读取数据 (包含 auto_update)
        assets_df = pd.read_sql(
            'SELECT asset_id, name, code, type, currency, remarks, auto_update FROM assets WHERE user_id = ?', 
            conn, params=(user_id,)
        )
        
        # 应用筛选
        assets_df = apply_advanced_filters(assets_df, "tab1", user_id, conn)
        
        st.caption(f"共显示 {len(assets_df)} 条资产")
        
        edited_assets = st.data_editor(
            assets_df,
            num_rows="dynamic",
            column_config={
                "asset_id": st.column_config.NumberColumn("ID", disabled=True),
                "name": st.column_config.TextColumn("资产名称", required=True),
                "code": "代码",
                "type": st.column_config.SelectboxColumn("大类", options=["基金", "股票", "债券", "现金", "其他"]),
                "currency": st.column_config.SelectboxColumn("币种", options=["CNY", "USD", "HKD", "JPY", "EUR", "GBP", "BTC"], required=True, default="CNY", width="small"),
                "auto_update": st.column_config.CheckboxColumn("自动更新?", help="勾选后，'一键更新'功能会自动拉取该资产净值", default=False),
                "remarks": st.column_config.TextColumn("备注", width="medium")
            },
            key="editor_assets",
            use_container_width=True
        )
        
        if st.button("💾 保存资产变动", type="primary"):
            if save_changes_to_db(edited_assets, assets_df, 'assets', 'asset_id', user_id, fixed_cols={'user_id': user_id}):
                st.rerun()

    # --- Tab 2: 标签管理 ---
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

    # --- Tab 3: 关联打标 (批量操作) ---
    with tab3:
        st.write("### 🏷️ 批量资产打标")
        
        # A. 准备资产列表数据
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
        
        # 初始化选择列
        df_assets_tags.insert(0, "选择", False)

        # B. 应用高级筛选
        df_filtered = apply_advanced_filters(df_assets_tags, "tab3", user_id, conn)
        
        # C. 全选/反选 控制区
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
                st.session_state.tag_batch_version += 1
                st.rerun()
        
        with c_btn2:
            if st.button("⬜ 取消全选", key="btn_sel_none", help="取消所有勾选", use_container_width=True):
                st.session_state.tag_batch_default_val = False
                st.session_state.tag_batch_version += 1
                st.rerun()

        df_filtered["选择"] = st.session_state.tag_batch_default_val

        # D. 表格显示
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
        
        # E. 操作区域 (级联标签选择)
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
                # 统计选中行
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
                        
                        # 重置全选状态
                        st.session_state.tag_batch_default_val = False
                        st.session_state.tag_batch_version += 1
                        
                        time.sleep(0.5)
                        st.rerun()
                    except Exception as e:
                        conn.rollback()
                        st.error(str(e))

finally:
    conn.close()