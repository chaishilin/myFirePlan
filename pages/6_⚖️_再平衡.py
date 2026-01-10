import streamlit as st
import pandas as pd
import plotly.graph_objects as go

# 🔥 引入公共工具函数
from utils import (
    get_db_connection, 
    get_cached_analytics_data, 
    show_sidebar_user_picker
)

# ==========================================
# 0. 全局配置与用户校验
# ==========================================
st.set_page_config(page_title="投资再平衡", page_icon="⚖️", layout="wide")

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

st.header("⚖️ 投资组合再平衡助手")
st.caption("设定你的理想资产配比，系统将计算如何调整仓位以维持风险平衡。")

user_id = st.session_state.user['user_id']
conn = get_db_connection()

try:
    # --- 1. 选择要进行再平衡的维度 ---
    # 通常我们只对大的维度做再平衡，比如 "资产大类" (股/债/金) 或 "风险等级"
    all_groups = pd.read_sql("SELECT DISTINCT tag_group FROM tags WHERE user_id = ?", conn, params=(user_id,))
    
    if all_groups.empty:
        st.warning("请先去【标签定义】页面设置标签。")
        st.stop()

    col1, col2 = st.columns([1, 2])
    with col1:
        # 默认尝试选中 "资产大类" 或 "风险等级"，如果没有就选第一个
        default_idx = 0
        groups_list = all_groups['tag_group'].tolist()
        if "资产大类" in groups_list: default_idx = groups_list.index("资产大类")
        elif "风险等级" in groups_list: default_idx = groups_list.index("风险等级")
        
        selected_group = st.selectbox("选择配置维度", groups_list, index=default_idx)

    # --- 2. 获取当前持仓数据 (Real) ---
    # 利用缓存数据加速计算
    _, df_tags = get_cached_analytics_data(user_id)
    
    if df_tags is None or df_tags.empty:
        st.info("暂无资产数据，无法计算持仓占比。")
        st.stop()

    # 过滤出当前维度的最新数据
    latest_date = df_tags['date'].max()
    current_portfolio = df_tags[
        (df_tags['date'] == latest_date) & 
        (df_tags['tag_group'] == selected_group)
    ].copy()
    
    if current_portfolio.empty:
        st.warning(f"在 {selected_group} 维度下暂无持仓数据。")
        st.stop()

    total_asset_val = current_portfolio['amount'].sum() # 总资产 (折合人民币)

    # --- 3. 获取/设置目标配置 (Target) ---
    # 读取已保存的目标
    saved_targets = pd.read_sql(
        "SELECT tag_name, target_percentage FROM rebalance_targets WHERE user_id = ? AND tag_group = ?",
        conn, params=(user_id, selected_group)
    )
    
    # 构造编辑表格数据
    # 拿到该组下所有的标签名 (包括目前还没持有的，方便用户设定目标去买入)
    all_tags_in_group = pd.read_sql(
        "SELECT tag_name FROM tags WHERE user_id = ? AND tag_group = ?", 
        conn, params=(user_id, selected_group)
    )
    
    # 合并：标签名 + 现有目标 + 当前持仓
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
        st.caption(f"基于当前总资产折合人民币：¥{total_asset_val:,.2f} 进行测算")

        # 计算具体买卖金额
        # 逻辑：理想金额 = 总资产 * 目标% - 实际持有的金额
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
        # 忽略 100 元以内的小额偏差
        to_buy = df_calc[df_calc['diff_amount'] > 100].sort_values('diff_amount', ascending=False)
        to_sell = df_calc[df_calc['diff_amount'] < -100].sort_values('diff_amount', ascending=True)
        
        col_buy, col_sell = st.columns(2)
        
        with col_buy:
            if not to_buy.empty:
                st.success("🔵 建议买入 / 加仓")
                for _, row in to_buy.iterrows():
                    st.markdown(f"**{row['tag_name']}**: 需买入 **¥{row['diff_amount']:,.0f}**")
                    # 进度条展示缺口比例
                    curr_ratio = row['amount'] / row['target_amount'] if row['target_amount']>0 else 0
                    st.progress(min(1.0, curr_ratio))
            else:
                st.write("✅ 无需买入")

        with col_sell:
            if not to_sell.empty:
                st.error("🔴 建议卖出 / 减仓")
                for _, row in to_sell.iterrows():
                    sell_val = abs(row['diff_amount'])
                    st.markdown(f"**{row['tag_name']}**: 需卖出 **¥{sell_val:,.0f}**")
                    # 进度条展示超配程度 (超过的部分)
                    over_ratio = (row['amount'] - row['target_amount']) / row['target_amount'] if row['target_amount']>0 else 1
                    # 这里的逻辑是：如果超配了 20%，进度条显示大概的样子
                    st.progress(min(1.0, over_ratio))
            else:
                st.write("✅ 无需卖出")

finally:
    conn.close()