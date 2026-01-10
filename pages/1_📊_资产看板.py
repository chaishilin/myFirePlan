import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

# 🔥 引入公共工具函数
# 注意：utils.py 必须在项目根目录
from utils import (
    get_db_connection, 
    get_cached_analytics_data, 
    get_market_index_data_cached, 
    show_sidebar_user_picker,
    get_user_notice,    # 新增
    update_user_notice  # 新增
)

# ==========================================
# 0. 全局配置与用户校验
# ==========================================
st.set_page_config(page_title="资产看板", page_icon="📊", layout="wide")

# 必须先检查用户登录状态
if "user" not in st.session_state or not st.session_state.user:
    st.warning("请先在侧边栏选择用户或登录")
    show_sidebar_user_picker()
    st.stop()

# 渲染侧边栏 (用户切换、Demo提示等)
show_sidebar_user_picker()
        
# ==========================================
# 1. 页面主逻辑
# ==========================================

st.header("📊 资产看板")
user_id = st.session_state.user['user_id']
conn = get_db_connection()

try:
    # ==========================================
    # 🔥 个人基金公告栏 (原地编辑模式)
    # ==========================================
    
    # 1. 获取当前公告
    current_notice = get_user_notice(user_id)
    
    # 2. 初始化编辑状态
    if 'dash_notice_editing' not in st.session_state:
        st.session_state.dash_notice_editing = False

    # 3. 根据状态渲染不同 UI
    if st.session_state.dash_notice_editing:
        # === ✏️ 编辑模式 (原地变成输入框) ===
        with st.container(border=True):
            st.caption("编辑你的投资宣言 (支持 Markdown)")
            # 这是一个 Form，防止每输入一个字就刷新
            with st.form("notice_edit_form"):
                new_text = st.text_area(
                    "Content", 
                    value=current_notice, 
                    height=200, 
                    label_visibility="collapsed",
                    placeholder="写点什么..."
                )
                
                b1, b2 = st.columns([1, 6])
                with b1:
                    submitted = st.form_submit_button("💾 保存", type="primary", use_container_width=True)
                with b2:
                    canceled = st.form_submit_button("❌ 取消")
                
                if submitted:
                    update_user_notice(user_id, new_text)
                    st.session_state.dash_notice_editing = False
                    st.rerun()
                
                if canceled:
                    st.session_state.dash_notice_editing = False
                    st.rerun()

    else:
        # === 👁️ 展示模式 (显示 info 框) ===
        display_text = current_notice if current_notice else "暂无公告，点击右侧铅笔图标编辑..."
        
        # 布局：左边是大框，右边是小按钮
        col_text, col_btn = st.columns([0.94, 0.06])
        
        with col_text:
            st.info(f"{display_text}")
        
        with col_btn:
            # 这里的 vertical_alignment 是为了让按钮不跑偏 (Streamlit 1.37+)
            # 如果你的版本较低报错，可以去掉 vertical_alignment 参数
            if st.button("✏️", help="编辑公告", key="btn_edit_mode"):
                st.session_state.dash_notice_editing = True
                st.rerun()

    st.divider()
    
    # ==========================================
    # 2. 顶部核心指标 (KPIs)
    # ==========================================
    df_fund = pd.read_sql('SELECT * FROM my_fund_history WHERE user_id = ? ORDER BY date ASC', conn, params=(user_id,))
    
    if not df_fund.empty:
        df_fund['date'] = pd.to_datetime(df_fund['date'])
        
        # 🔥 新增逻辑：计算单位持仓成本 (Unit Cost)
        # 单位成本 = 总本金 / 总份额
        # 做了除零保护，如果份额为0，成本视为1.0（初始状态）
        df_fund['unit_cost'] = df_fund.apply(
            lambda x: (x['principal'] / x['total_shares']) if x['total_shares'] > 0.001 else 1.0, 
            axis=1
        )

        latest = df_fund.iloc[-1]
        
        # 计算累计收益率 (百分比)
        total_ret_pct = (latest['unit_nav'] - 1.0) * 100
        
        # 计算当前单位成本
        current_unit_cost = latest['unit_cost']

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
            st.metric("单位持仓成本", f"{current_unit_cost:.4f}", 
                      delta=f"{(latest['unit_nav'] - current_unit_cost)/current_unit_cost*100:.1f}% (安全垫)",
                      help="总本金 / 总份额。如果净值高于此线，说明盈利。")
        
        with k5:
            dd_val = latest['drawdown'] if 'drawdown' in latest else 0.0
            min_dd = df_fund['drawdown'].min() if 'drawdown' in df_fund.columns else 0.0
            st.metric("历史最大回撤", f"{min_dd*100:.2f}%", delta_color="inverse")
        
        st.divider()
    else:
        st.info("⏳ 暂无基金净值数据，请先去【数据录入】保存一次快照，并等待后台计算。")
        st.stop()
    # ==========================================
    # 3. 准备详细资产数据 (缓存加速)
    # ==========================================
    df_assets, df_tags = get_cached_analytics_data(user_id)
    
    # ==========================================
    # 4. 功能标签页
    # ==========================================
    tab1, tab2, tab3 = st.tabs(["🚀 净值与回撤", "📈 结构对比", "🍰 每日透视"])
    
    # --- Tab 1: 基金净值与回撤 ---
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
                common_xaxis_config = dict(
                    title="日期",
                    tickformat="%Y-%m-%d", 
                    tickmode='auto',
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
                        hovertemplate='日期: %{x|%Y-%m-%d}<br>总资产: %{y:.2f} 万元<extra></extra>'
                    ))
                    fig_asset.update_layout(
                        hovermode="x unified", height=350, margin=dict(t=10),
                        yaxis=dict(title="金额 (万元)", tickformat=",.2f"),
                        xaxis=common_xaxis_config
                    )
                    st.plotly_chart(fig_asset, use_container_width=True)

                with c_top2:
                    st.subheader("持有收益变化")
                    if 'accumulated_profit' not in df_slice.columns:
                         df_slice['accumulated_profit'] = df_slice['total_assets'] - df_slice.get('principal', 0)

                    fig_profit = go.Figure()
                    fig_profit.add_trace(go.Scatter(
                        x=df_slice['date'], y=df_slice['accumulated_profit'],
                        mode='lines', name='持有收益',
                        line=dict(width=2, color='#E74C3C'), 
                        fill='tozeroy', 
                        fillcolor='rgba(231, 76, 60, 0.2)', 
                        hovertemplate='日期: %{x|%Y-%m-%d}<br>持有收益: %{y:,.2f} 元<extra></extra>'
                    ))
                    fig_profit.update_layout(
                        hovermode="x unified", height=350, margin=dict(t=10),
                        yaxis=dict(title="金额 (元)", tickformat=",.2f"),
                        xaxis=common_xaxis_config
                    )
                    st.plotly_chart(fig_profit, use_container_width=True)

                st.divider()

                # === B. 第二排：业绩走势 (含持仓成本) & 回撤修复 ===
                
                nav_start = df_slice.iloc[0]['unit_nav']
                nav_end = df_slice.iloc[-1]['unit_nav']
                period_return = (nav_end - nav_start) / nav_start if nav_start != 0 else 0
                return_color = "red" if period_return >= 0 else "green"
                return_sign = "+" if period_return >= 0 else ""

                c_chart1, c_chart2 = st.columns(2)
                
                with c_chart1:
                    # 标题栏
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

                    st.markdown(f"区间涨跌: <span style='color:{return_color}; font-weight:bold; font-size:1.1em'>{return_sign}{period_return*100:.2f}%</span>", unsafe_allow_html=True)
                    
                    fig_nav = go.Figure()

                    # 1. 个人基金净值曲线
                    fig_nav.add_trace(go.Scatter(
                        x=df_slice['date'], y=df_slice['unit_nav'],
                        mode='lines', name='我的净值',
                        line=dict(color='#0E44E5', width=2.5),
                        hovertemplate='净值: %{y:.4f}<extra></extra>'
                    ))

                    # 2. 🔥 新增：持仓成本曲线 (Cost Line)
                    fig_nav.add_trace(go.Scatter(
                        x=df_slice['date'], y=df_slice['unit_cost'],
                        mode='lines', name='持仓成本',
                        line=dict(color='#95A5A6', width=1.5, dash='dash'), # 灰色虚线
                        hovertemplate='成本: %{y:.4f}<extra></extra>'
                    ))
                    
                    # 3. 对比指数曲线
                    if benchmark_name != "(无)":
                        s_str = df_slice['date'].min().strftime('%Y-%m-%d')
                        e_str = df_slice['date'].max().strftime('%Y-%m-%d')
                        df_bench = get_market_index_data_cached(benchmark_name, s_str, e_str)
                        
                        if not df_bench.empty and len(df_bench) > 1:
                            my_start_nav = df_slice.iloc[0]['unit_nav']
                            bench_start_val = df_bench.iloc[0]['close']
                            
                            if bench_start_val > 0:
                                df_bench['rebased_nav'] = (df_bench['close'] / bench_start_val) * my_start_nav
                                bench_ret = (df_bench.iloc[-1]['close'] - bench_start_val) / bench_start_val
                                b_sign = "+" if bench_ret >= 0 else ""
                                
                                fig_nav.add_trace(go.Scatter(
                                    x=df_bench['date'], 
                                    y=df_bench['rebased_nav'],
                                    mode='lines',
                                    name=f'{benchmark_name} ({b_sign}{bench_ret*100:.1f}%)',
                                    line=dict(color="#0E44E5", width=2.5), 
                                    opacity=0.3,
                                    hovertemplate=f'{benchmark_name}: %{{y:.4f}}<extra></extra>'
                                ))
                                        
                    fig_nav.update_layout(
                        hovermode="x unified", 
                        yaxis_title="单位净值", 
                        height=380, 
                        margin=dict(t=10),
                        # 🔥 修改：图例移动到左上角 (Horizontal, Top-Left)
                        legend=dict(
                            orientation="h", 
                            yanchor="top", y=0.99, 
                            xanchor="left", x=0.01, 
                            bgcolor="rgba(0,0,0,0)"
                        ),
                        xaxis=common_xaxis_config
                    )
                    st.plotly_chart(fig_nav, use_container_width=True)

                with c_chart2:
                    st.subheader("回撤修复")
                    
                    # 现场计算回撤逻辑
                    if 'period_dd' not in df_slice.columns:
                        df_slice['rolling_max'] = df_slice['unit_nav'].cummax()
                        df_slice['period_dd'] = (df_slice['unit_nav'] - df_slice['rolling_max']) / df_slice['rolling_max']
                    
                    min_dd_val = df_slice['period_dd'].min()
                    
                    # 状态计算
                    repair_status = "修复中..."
                    if not df_slice.empty:
                         # 如果当前净值 >= 历史最大净值 (允许极小误差)，则说明已新高
                        curr_nav = df_slice.iloc[-1]['unit_nav']
                        hist_max = df_slice['unit_nav'].max()
                        if curr_nav >= hist_max * 0.9999:
                            repair_status = "已创新高 🎉"

                    st.markdown(f"区间最大回撤: **{min_dd_val*100:.2f}%** | 状态: **{repair_status}**")

                    # 🔥 修改：计算回撤区间 (用于画半透明背景)
                    trough_idx = df_slice['period_dd'].idxmin()
                    trough_date = df_slice.loc[trough_idx]['date']
                    trough_nav = df_slice.loc[trough_idx]['unit_nav']
                    peak_val_at_trough = df_slice.loc[trough_idx]['rolling_max']

                    # 1. 找起点：跌破前高点的那一天
                    pre_data = df_slice[df_slice['date'] <= trough_date]
                    # 往前找最后一个 nav >= peak_val 的点
                    peak_point = pre_data[pre_data['unit_nav'] >= peak_val_at_trough * 0.9999].iloc[-1]
                    peak_date = peak_point['date']

                    # 2. 找终点：涨回前高点的那一天 (如果还没涨回去，就选最后一天)
                    post_data = df_slice[df_slice['date'] > trough_date]
                    recover_points = post_data[post_data['unit_nav'] >= peak_val_at_trough * 0.9999]
                    
                    if not recover_points.empty:
                        recover_date = recover_points.iloc[0]['date']
                    else:
                        recover_date = df_slice.iloc[-1]['date']

                    fig_repair = go.Figure()
                    
                    # 主曲线：普通的折线 (去掉 fill='tozeroy')
                    fig_repair.add_trace(go.Scatter(
                        x=df_slice['date'], y=df_slice['unit_nav'], 
                        mode='lines', name='净值', 
                        line=dict(color='#2980B9', width=2),
                        hovertemplate='日期: %{x|%Y-%m-%d}<br>单位净值: %{y:.4f}<extra></extra>'
                    ))
                    
                    if abs(min_dd_val) > 0.001:
                        # 🔥 核心修改：只在受损区间添加半透明背景
                        fig_repair.add_vrect(
                            x0=peak_date, x1=recover_date,
                            fillcolor="rgba(231, 76, 60, 0.2)", layer="below", line_width=0,
                        )
                        
                        # 标记回撤底点
                        fig_repair.add_trace(go.Scatter(
                            x=[trough_date], y=[trough_nav],
                            mode='markers+text',
                            text=[f"回撤底\n{min_dd_val*100:.1f}%"],
                            textposition="bottom center",
                            marker=dict(color='red', size=8), showlegend=False,
                            hovertemplate='最大回撤点: %{y:.4f}<extra></extra>'
                        ))

                    fig_repair.update_layout(
                        showlegend=False,
                        hovermode="x unified", yaxis_title="单位净值", height=380, margin=dict(t=10),
                        xaxis=common_xaxis_config
                    )
                    st.plotly_chart(fig_repair, use_container_width=True)
    # --- Tab 2: 结构对比 ---
    with tab2:
        st.subheader("📊 结构化趋势分析")
        
        c1, c2, c3 = st.columns([1, 1, 2])
        with c1:
            view_mode = st.radio("分析维度", ["按具体资产", "按标签组"], horizontal=True, key="trend_view")
        with c2:
            metric_type = st.selectbox("画图指标 (Y轴)", ["总金额 (Amount)", "持有收益 (Profit)", "收益率 (Yield %)", "占比 (Share %)"], key="trend_metric")
        with c3:
            tooltip_extras = st.multiselect("🖱️ 悬停显示额外指标", ["总金额", "持有收益", "本金", "收益率", "占比"], default=["占比", "持有收益", "收益率"], key="trend_tooltip")

        plot_df = None
        color_col = ""
        
        # 1. 数据筛选逻辑
        if view_mode == "按具体资产":
            plot_df = df_assets.copy()
            color_col = "name"
            
            with st.expander("🔍 资产精准筛选", expanded=False):
                f_col1, f_col2, f_col3 = st.columns([2, 2, 2])
                with f_col1:
                    filter_kw = st.text_input("1. 关键字 (名称/代码)", placeholder="搜股票、基金...", key="trend_kw")
                
                # 临时查标签映射
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
                
                if filter_kw and 'name' in plot_df.columns:
                    kw_matched = plot_df[plot_df['name'].str.contains(filter_kw, case=False, na=False)]
                    valid_asset_ids = valid_asset_ids.intersection(set(kw_matched['asset_id']))
                
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

        # 2. 绘制折线图
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

            # 3. 两期对比
            st.divider()
            st.subheader("两期数据横向比对")
            
            available_dates = sorted(plot_df['date'].dt.date.unique())
            if not available_dates:
                st.warning("当前无数据")
            else:
                default_d2 = available_dates[-1]
                default_d1 = available_dates[-2] if len(available_dates) > 1 else available_dates[0]

                dc1, dc2, dc3 = st.columns([2, 2, 3])
                with dc1:
                    d1_input = st.date_input("📅 日期 A (旧)", value=default_d1, min_value=available_dates[0], max_value=available_dates[-1], key="diff_d1")
                with dc2:
                    d2_input = st.date_input("📅 日期 B (新)", value=default_d2, min_value=available_dates[0], max_value=available_dates[-1], key="diff_d2")
                with dc3:
                    diff_metric = st.radio("对比指标", ["总金额 (Amount)", "持有收益 (Profit)", "收益率 (Yield %)", "占比 (Share %)"], horizontal=True, key="diff_m")

                d1_ts = pd.Timestamp(d1_input)
                d2_ts = pd.Timestamp(d2_input)
                
                if d1_ts == d2_ts:
                    st.info("请选择两个不同的日期。")
                else:
                    if "总金额" in diff_metric: val_col = "amount"; unit_suffix = "元"
                    elif "持有收益" in diff_metric: val_col = "profit"; unit_suffix = "元"
                    elif "收益率" in diff_metric: val_col = "yield_rate"; unit_suffix = "%"
                    elif "占比" in diff_metric: val_col = "share"; unit_suffix = "%"

                    df_d1 = plot_df[plot_df['date'] == d1_ts].copy(); df_d1['Period'] = d1_ts.strftime('%Y-%m-%d')
                    df_d2 = plot_df[plot_df['date'] == d2_ts].copy(); df_d2['Period'] = d2_ts.strftime('%Y-%m-%d')
                    df_viz = pd.concat([df_d1, df_d2], ignore_index=True)
                    
                    if not df_viz.empty:
                        rank_order = df_d2.sort_values(val_col, ascending=False)[color_col].tolist()
                        
                        fig_compare = px.bar(
                            df_viz, x=color_col, y=val_col, color='Period', barmode='group', 
                            category_orders={color_col: rank_order}, text_auto='.2s' if unit_suffix == "元" else '.2f'
                        )
                        fig_compare.update_layout(yaxis_title=diff_metric, xaxis_title="", legend_title_text="", hovermode="x unified")
                        st.plotly_chart(fig_compare, use_container_width=True)

                        with st.expander(f"📋 查看 {diff_metric.split(' ')[0]} 明细", expanded=True):
                            df_pivot = df_viz.pivot(index=color_col, columns='Period', values=val_col).reset_index().fillna(0)
                            d1_str, d2_str = d1_ts.strftime('%Y-%m-%d'), d2_ts.strftime('%Y-%m-%d')
                            if d1_str in df_pivot.columns and d2_str in df_pivot.columns:
                                df_pivot['变动量'] = df_pivot[d2_str] - df_pivot[d1_str]
                                df_pivot = df_pivot.sort_values(d2_str, ascending=False)
                                st.dataframe(df_pivot, hide_index=True, use_container_width=True)

    # --- Tab 3: 每日透视 ---
    with tab3:
        st.subheader("🍰 每日资产快照分析")
        
        control_c1, control_c2 = st.columns(2)
        with control_c1:
            default_date = df_assets['date'].max().date() if not df_assets.empty else datetime.now().date()
            min_date = df_assets['date'].min().date() if not df_assets.empty else default_date
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

finally:
    conn.close()