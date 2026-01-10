import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta

# 🔥 引入公共工具函数
from utils import (
    get_db_connection, 
    save_changes_to_db, 
    show_sidebar_user_picker,
    get_latest_rates  # 用于未来现金流计算时的汇率折算
)

# ==========================================
# 0. 全局配置与用户校验
# ==========================================
st.set_page_config(page_title="定投计划", page_icon="📅", layout="wide")

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

st.header("📅 定投计划与未来现金流")
user_id = st.session_state.user['user_id']
conn = get_db_connection()

try:
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
                st.warning("⚠️ 请先去【资产管理】页面添加至少一个资产。")
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

                # 筛选逻辑
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

                    # 选择扣款来源
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
            
            # 核心转换：把 source_asset_id (数字) 转成 source_name (文本) 方便编辑
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
                def map_back_id(row):
                    val = row['source_name']
                    if val == "(不自动扣款)": return None
                    return cash_map_name_to_id.get(val, None)

                edited_plans['source_asset_id'] = edited_plans.apply(map_back_id, axis=1)
                
                # 2. 剔除纯展示用的列
                cols_to_drop = ['name', 'currency', '描述', 'source_name']
                df_to_save = edited_plans.drop(columns=[c for c in cols_to_drop if c in edited_plans.columns])
                
                # 3. 提交保存 (利用 utils 的通用保存函数)
                if save_changes_to_db(df_to_save, plans_df, 'investment_plans', 'plan_id', user_id, fixed_cols={'user_id':user_id}):
                    st.rerun()
        else:
            st.info("暂无定投计划。")

    # === TAB 2: 现金流看板 ===
    with tab2:
        # 1. 计算未来现金流逻辑
        st.subheader("🗓️ 未来 30 天资金需求推演 (折合人民币)")
        
        # 获取最新汇率表 (调用 utils 函数)
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

finally:
    conn.close()