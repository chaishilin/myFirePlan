import streamlit as st
import pandas as pd
import plotly.express as px
import time
from datetime import datetime

# 🔥 引入公共工具函数和重算模块
from utils import (
    get_db_connection, 
    show_sidebar_user_picker
)
import recalc_fund_history  # 用于在修改流水后重算净值

# ==========================================
# 0. 全局配置与用户校验
# ==========================================
st.set_page_config(page_title="现金流管理", page_icon="💰", layout="wide")

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

st.header("💰 现金流与本金归集")
st.caption("“模糊记账法”核心：只记大额进出 (外部收支)，倒推本金投入。")

user_id = st.session_state.user['user_id']
username = st.session_state.user['username'] # 获取当前用户名作为默认操作人
conn = get_db_connection()

try:
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
            # 默认填自己，可改
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
                    
                    conn.execute('''
                        INSERT INTO cashflows (user_id, date, type, amount, category, operator, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
                    ''', (user_id, record_date.strftime('%Y-%m-%d'), real_type, amount, category, operator))
                    
                    conn.commit()
                    st.success("已记录")
                    time.sleep(0.5)
                    st.rerun()
                else:
                    st.warning("金额需大于0")

    # --- 2. 中部：年度统计卡片 ---
    current_year = datetime.now().year
    
    # 查询年度统计 (排除内部交易)
    df_stat = pd.read_sql('''
        SELECT type, amount, date 
        FROM cashflows 
        WHERE user_id = ? 
        AND category NOT IN ('定投扣款', '内部调仓') 
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
    
    # 核心查询：关联 my_fund_history 获取当时的净值
    # 左连接 (Left Join)，因为可能有些日子还没生成净值
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
        
        # 填充缺失净值并计算份额
        df_display['nav_at_date'] = df_display['nav_at_date'].fillna(1.0)
        df_display['shares_calc'] = df_display['amount'] / df_display['nav_at_date']
        
        edited_df = st.data_editor(
            df_display,
            column_config={
                "id": st.column_config.NumberColumn("ID", disabled=True, width="small"),
                "date": st.column_config.DateColumn("日期", format="YYYY-MM-DD"),
                "type": st.column_config.SelectboxColumn("类型", options=["收入", "支出"], required=True, width="small"),
                "amount": st.column_config.NumberColumn("金额", format="%.2f", min_value=0),
                "operator": st.column_config.TextColumn("操作人", width="small"),
                
                # 展示列 (只读)
                "nav_at_date": st.column_config.NumberColumn("当日净值", format="%.4f", disabled=True, help="该日期对应的个人基金净值"),
                "shares_calc": st.column_config.NumberColumn("对应份额", format="%.2f", disabled=True, help="金额 ÷ 净值"),
                
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
                
                # 🔥 关键联动：修改现金流后，历史净值肯定变了，自动触发重算
                with st.spinner("正在因流水变动重算历史净值..."):
                    recalc_fund_history.recalculate_user_history(user_id)
                
                st.success("更新成功！历史净值已同步修正。")
                time.sleep(1)
                st.rerun()
                
            except Exception as e:
                st.error(f"保存失败: {e}")
    else:
        st.info("暂无记录，请在上方添加。")

finally:
    conn.close()