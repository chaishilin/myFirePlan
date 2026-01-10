import streamlit as st
import pandas as pd
import time
from datetime import datetime

# 🔥 引入核心依赖
from utils import (
    get_db_connection, 
    recalculate_daily_nav,  # 引用 utils 里的净值刷新逻辑
    show_sidebar_user_picker
)
from data_provider import DataProvider  # 用于拉取汇率
import recalc_fund_history  # 用于保存后重算个人基金净值

# ==========================================
# 0. 全局配置与用户校验
# ==========================================
st.set_page_config(page_title="数据录入", page_icon="📝", layout="wide")

# 必须先检查用户登录状态
if "user" not in st.session_state or not st.session_state.user:
    st.warning("请先在侧边栏选择用户或登录")
    show_sidebar_user_picker()
    st.stop()

# 渲染侧边栏
show_sidebar_user_picker()

# ==========================================
# 1. 页面主逻辑
# ==========================================

st.header("📝 每日资产快照录入 (余额法)")
user_id = st.session_state.user['user_id']
conn = get_db_connection()

try:
    # --- 1. 日期选择 ---
    col_date, _ = st.columns([1, 2])
    with col_date:
        date_input = st.date_input("选择快照日期", datetime.now())
        str_date = date_input.strftime('%Y-%m-%d')

    # 准备基础资产数据
    assets = pd.read_sql('SELECT asset_id, name, code, type, currency, last_shares, auto_update FROM assets WHERE user_id = ?', conn, params=(user_id,))
    
    if assets.empty:
        st.warning("暂无资产，请先去【资产管理】添加资产。")
        st.stop()

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
                if st.button("🔄 自动拉取汇率", help="调用接口获取中国银行当日中间价", key="btn_auto_rate"):
                    with st.spinner("正在连接接口..."):
                        fetched_count = 0
                        for curr in foreign_currencies:
                            # 调用 DataProvider 获取汇率
                            r = DataProvider.get_exchange_rate(curr, str_date)
                            if r:
                                # 更新 session_state 以刷新 number_input 的值
                                k = f"rate_{curr}_{str_date}"
                                st.session_state[k] = r
                                fetched_count += 1
                        
                        if fetched_count > 0:
                            st.toast(f"成功拉取 {fetched_count} 个币种汇率", icon="✅")
                            time.sleep(0.5)
                            st.rerun() # 刷新界面显示数值
                        else:
                            st.error("未能获取汇率，请检查日期是否为交易日，或手动输入。")

            # 读取数据库已存的，或者 Session State 里的
            saved_rates = pd.read_sql("SELECT currency, rate FROM exchange_rates WHERE date = ?", conn, params=(str_date,))
            saved_rate_map = dict(zip(saved_rates['currency'], saved_rates['rate']))
            
            cols = st.columns(len(foreign_currencies) + 1)
            rates_to_save = {}
            
            for i, curr in enumerate(foreign_currencies):
                input_key = f"rate_{curr}_{str_date}"
                
                # 如果 session_state 里没有，才去数据库取默认值
                if input_key not in st.session_state:
                    default_val = saved_rate_map.get(curr, 1.0)
                else:
                    default_val = st.session_state[input_key] 
                
                with cols[i]:
                    r = st.number_input(
                        f"{curr} ➡️ CNY", 
                        value=float(default_val) if input_key not in st.session_state else None,
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


    # --- 3. 筛选与排序工具 ---
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
            
            sort_option = st.radio("排序", ["默认", "金额(高→低)", "收益(高→低)"], horizontal=True, label_visibility="collapsed")
        else:
            sort_option = "默认"

    # --- 4. 数据准备与合并 ---
    # 获取清仓状态
    all_asset_ids = tuple(assets['asset_id'].tolist())
    if not all_asset_ids: 
        st.info("请先添加资产。")
        st.stop()

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
        
        # --- 填充逻辑 ---
        # 优先用今天的；如果没有，用上次的；还没有，用0
        merged['amount'] = merged['amount'].fillna(merged['amount_last']).fillna(0.0)
        merged['profit'] = merged['profit'].fillna(merged['profit_last']).fillna(0.0)
        
        # 净值优先用今天的；如果没有，用上次的；再没有，用1.0
        merged['unit_nav'] = merged['unit_nav'].fillna(merged['unit_nav_last']).fillna(1.0)
        
        # 现金类特殊处理：净值默认为1
        if 'type' in merged.columns:
            merged.loc[merged['type'] == '现金', 'unit_nav'] = 1.0

        # 反推逻辑演示 (用于 Display)
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
            visible_ids = merged['asset_id'].tolist()
            # 调用 utils 里的 recalculate_daily_nav
            if st.button("🔄 刷新当前列表净值", help="从网络拉取最新净值，填入表格（不改变市值，只影响反推的份额）"):
                progress_bar = st.progress(0.0)
                status_text = st.empty()
                success, res = recalculate_daily_nav(user_id, str_date, progress_bar, status_text, limit_asset_ids=visible_ids)
                status_text.empty(); progress_bar.empty()
                if success:
                    st.toast("净值已更新，请检查数据", icon="✅")
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error(f"更新失败: {res}")

        # --- 7. DataEditor (余额法核心) ---
        st.caption("💡 **余额法操作指南**：直接对照理财APP，修改【市值】和【持有收益】即可。系统会自动反推份额和成本。")
        
        col_cfg = {
            "asset_id": st.column_config.NumberColumn("ID", disabled=True, width="small"),
            "name": st.column_config.TextColumn("名称", disabled=True),
            "code": st.column_config.TextColumn("代码", disabled=True),
            
            # 🔥 核心输入列
            "amount": st.column_config.NumberColumn("💰 总市值 (查APP)", required=True, format="%.2f", help="对照APP填写当前总金额"),
            "profit": st.column_config.NumberColumn("🎉 持有收益 (查APP)", required=True, format="%.2f", help="对照APP填写显示的持有收益"),
            "unit_nav": st.column_config.NumberColumn("📈 当日净值", required=True, format="%.4f", help="可手动修改，或点刷新按钮自动拉取"),
            
            # 🔥 衍生列 (只读)
            "shares_est": st.column_config.NumberColumn("份额 (反推)", disabled=True, format="%.2f"),
            "unit_cost_est": st.column_config.NumberColumn("成本价 (反推)", disabled=True, format="%.4f"),
            "yield_est": st.column_config.NumberColumn("收益率", disabled=True, format="%.2f%%"),
            
            "is_cleared": st.column_config.CheckboxColumn("🏁 清仓?", help="勾选后表示该资产已清仓"),
        }
        if 'currency' in merged.columns:
            col_cfg["currency"] = st.column_config.TextColumn("币", disabled=True, width="small")

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
                    
                    # 1. 获取核心数据
                    amount = float(row['amount'])
                    profit = float(row['profit'])
                    nav = float(row['unit_nav'])
                    is_clr = 1 if row['is_cleared'] else 0
                    
                    # 2. 执行反推
                    shares = amount / nav if nav > 0 else 0.0
                    cost = amount - profit
                    unit_cost = cost / shares if shares > 0 else 0.0
                    y_rate = (profit / cost * 100) if cost != 0 else 0.0
                    
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
                    
                    # 4. 同步更新 assets 表 (为了下次继承)
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
                time.sleep(1)
                st.rerun()
                
            except Exception as e:
                st.error(f"保存失败: {e}")

        # --- 9. 删除/重置 ---
        st.write(""); st.write(""); st.divider()
        exist_count = conn.execute('SELECT COUNT(*) FROM snapshots s JOIN assets a ON s.asset_id = a.asset_id WHERE s.date = ? AND a.user_id = ?', (str_date, user_id)).fetchone()[0]

        if exist_count > 0:
            with st.expander(f"🗑️ 删除/重置 【{str_date}】 的数据", expanded=False):
                if st.button("🧨 确认彻底删除", type="primary", key="btn_del_daily"):
                    conn.execute('DELETE FROM snapshots WHERE date = ? AND asset_id IN (SELECT asset_id FROM assets WHERE user_id = ?)', (str_date, user_id))
                    conn.commit()
                    st.success(f"已删除 {str_date} 记录！")
                    time.sleep(1)
                    st.rerun()
    
finally:
    conn.close()