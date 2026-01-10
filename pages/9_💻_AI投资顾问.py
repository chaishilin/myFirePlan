import streamlit as st
import pandas as pd
from datetime import datetime, timedelta

# 🔥 引入公共工具函数
from utils import (
    get_db_connection, 
    show_sidebar_user_picker,
    generate_and_send_ai_prompt  # 核心生成逻辑已封装在 utils 中
)

# ==========================================
# 0. 全局配置与用户校验
# ==========================================
st.set_page_config(page_title="AI 投顾", page_icon="🤖", layout="wide")

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

st.header("🤖 AI 智能投顾助理")
st.caption("生成包含每日净值、结构变化、核心持仓的深度 Prompt，发送给 ChatGPT/Claude 进行专业诊断。")

user_id = st.session_state.user['user_id']
conn = get_db_connection()

try:
    # --- 1. 获取所有有数据的日期 (用于智能推断默认时间) ---
    df_dates = pd.read_sql('SELECT DISTINCT date FROM my_fund_history WHERE user_id = ? ORDER BY date', conn, params=(user_id,))
    
    if df_dates.empty:
        st.warning("⚠️ 暂无基金净值数据。请先去【数据录入】页保存至少两天的快照，并等待净值计算完成。")
        st.stop()

    # 转换为 date 对象列表
    valid_dates = pd.to_datetime(df_dates['date']).dt.date.tolist()
    latest_date = valid_dates[-1] # 列表中最后一个就是最近的日期
    
    # === 智能计算默认开始日期 ===
    # 目标：找 7 天前的那个日期
    target_date = latest_date - timedelta(days=7)
    default_start = target_date 
    
    # 逻辑：
    # 1. 尝试找 <= target_date 的日期中，离 target_date 最近的一个 (往前找)
    candidates_past = [d for d in valid_dates if d <= target_date]
    if candidates_past:
        default_start = candidates_past[-1]
    else:
        # 2. 如果往前找不到 (说明用户可能才用了不到7天)，那就往后找
        candidates_future = [d for d in valid_dates if d > target_date and d < latest_date]
        if candidates_future:
            default_start = candidates_future[0]
        else:
            # 3. 实在不行就取最早的一天
            default_start = valid_dates[0] if len(valid_dates) > 1 else latest_date

    # ==========================================
    # 2. 设置区域
    # ==========================================
    with st.container(border=True):
        st.subheader("🛠️ 生成配置")
        c1, c2 = st.columns(2)
        with c1:
            date_range = st.date_input("1. 选择复盘时间段", value=(default_start, latest_date), max_value=latest_date)
        with c2:
            all_groups = pd.read_sql("SELECT DISTINCT tag_group FROM tags WHERE user_id = ?", conn, params=(user_id,))
            group_opts = ["按具体资产"] + all_groups['tag_group'].tolist()
            selected_dim = st.selectbox("2. 选择结构分析维度", group_opts, index=0)

        st.info("💡 **提示**：系统将提取每日净值走势、结构对比及核心持仓，组合成 Prompt 发送至邮箱。")
        
        if st.button("🚀 生成并发送 AI Prompt 到邮箱", type="primary"):
            if isinstance(date_range, tuple) and len(date_range) == 2:
                start_d, end_d = date_range
                
                if start_d >= end_d:
                    st.error("开始日期必须早于结束日期。")
                else:
                    with st.spinner("正在提取每日数据、计算结构变化、组装 Prompt..."):
                        s_str = start_d.strftime('%Y-%m-%d')
                        e_str = end_d.strftime('%Y-%m-%d')
                        
                        # 调用 utils
                        success, msg, content = generate_and_send_ai_prompt(user_id, s_str, e_str, selected_dim)
                        
                        if success:
                            st.balloons()
                            st.success(f"✅ {msg}")
                            
                            st.divider()
                            
                            # 🔥 修改点 1：直接渲染 Markdown (所见即所得，表格会很好看)
                            st.subheader("📖 提示词内容预览")
                            with st.container(border=True):
                                st.markdown(content)
                            
                            # 🔥 修改点 2：提供折叠的一键复制区
                            st.caption("👇 需要复制？请展开下方区域")
                            with st.expander("📋 点击复制完整 Prompt (Raw Markdown)"):
                                st.code(content, language="markdown")
                                
                        else:
                            st.error(f"❌ {msg}")
            else:
                st.error("请选择完整的开始和结束日期。")

finally:
    conn.close()