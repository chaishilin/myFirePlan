import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime

# 🔥 引入公共工具函数
from utils import (
    get_db_connection, 
    show_sidebar_user_picker,
    get_latest_rates
)

# ==========================================
# 0. 全局配置与用户校验
# ==========================================
st.set_page_config(page_title="FIRE 推演", page_icon="🔥", layout="wide")

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

st.header("🔥 FIRE 财富自由展望 2.0")
st.caption("引入通胀调节与风险区间，还原最真实的财富自由之路。")

user_id = st.session_state.user['user_id']
conn = get_db_connection()

try:
    # --- 1. 获取当前总资产 (起点) ---
    # 需要将外币资产折算为人民币
    rates_map = get_latest_rates(conn)
    
    # 获取最近一次有记录的日期
    latest_date_row = conn.execute(
        'SELECT MAX(date) as d FROM snapshots JOIN assets ON snapshots.asset_id = assets.asset_id WHERE assets.user_id = ?', 
        (user_id,)
    ).fetchone()
    
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
    if len(df_proj) > 20:
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

finally:
    conn.close()