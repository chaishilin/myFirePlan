import streamlit as st
import pandas as pd
import time

# 🔥 引入公共工具函数
from utils import (
    get_db_connection, 
    show_sidebar_user_picker,
    perform_backup,    # 用于执行手动备份
    delete_user_fully, # 用于删除成员
    get_all_usernames  # 用于列出成员
)

# ==========================================
# 0. 全局配置与用户校验
# ==========================================
st.set_page_config(page_title="系统设置", page_icon="⚙️", layout="wide")

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

st.header("⚙️ 系统设置与管理")
conn = get_db_connection()

try:
    # 读取当前系统配置
    settings = conn.execute('SELECT * FROM system_settings WHERE id = 1').fetchone()
    
    # 如果还没有初始化配置，先给个默认字典防止报错
    if not settings:
        settings = {
            'backup_frequency': '关闭',
            'email_host': '', 'email_port': 465, 
            'email_user': '', 'email_password': '', 'email_to': ''
        }
    
    tab1, tab2, tab3 = st.tabs(["🔄 备份策略与邮箱", "📂 本地备份管理", "👥 成员管理(危险)"])
    
    # === Tab 1: 策略配置 ===
    with tab1:
        st.subheader("1. 自动备份策略")
        with st.form("settings_form"):
            # 备份频率
            current_freq = settings['backup_frequency']
            freq_options = ["关闭", "每天", "每周", "每月"]
            # 防止数据库里的值不在选项里
            default_idx = freq_options.index(current_freq) if current_freq in freq_options else 0
            
            new_freq = st.radio("备份频率", freq_options, index=default_idx, horizontal=True)
            
            st.divider()
            
            st.subheader("2. 邮箱推送设置")
            st.caption("配置 SMTP 服务后，系统可发送数据库备份文件和 AI 投顾提示词。")
            
            c1, c2 = st.columns(2)
            with c1:
                email_host = st.text_input("SMTP 服务器 (如 smtp.qq.com)", value=settings['email_host'] or "")
                email_port = st.number_input("SMTP 端口 (SSL通常为465)", value=settings['email_port'] or 465)
            with c2:
                email_user = st.text_input("邮箱账号", value=settings['email_user'] or "")
                email_password = st.text_input("授权码/密码", value=settings['email_password'] or "", type="password", help="注意：通常是邮箱开启POP3/SMTP服务时生成的授权码，而非登录密码")
            
            email_to = st.text_input("接收邮箱 (留空则默认发给自己)", value=settings['email_to'] or "")
            
            if st.form_submit_button("💾 保存配置"):
                conn.execute('''
                    UPDATE system_settings 
                    SET backup_frequency=?, email_host=?, email_port=?, email_user=?, email_password=?, email_to=? 
                    WHERE id=1
                ''', (new_freq, email_host, email_port, email_user, email_password, email_to))
                conn.commit()
                st.success("配置已保存！")
                time.sleep(0.5)
                st.rerun()

    # === Tab 2: 本地管理 ===
    with tab2:
        st.subheader("📂 本地备份操作")
        st.caption("点击下方按钮可立即生成一份数据库快照，保存在 `backups/` 目录下。如果配置了邮箱，也会同时发送。")
        
        if st.button("🚀 立即执行手动备份"):
            with st.spinner("正在打包备份..."):
                # 调用 utils 里的 perform_backup
                success, msg = perform_backup(manual=True)
                if success: 
                    st.success(msg)
                    time.sleep(1)
                else: 
                    st.error(msg)

    # === Tab 3: 成员管理 ===
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
            target_username = st.selectbox(
                "选择要移除的成员", 
                options=list(user_options.keys()),
                key="sel_user_to_del_fixed"
            )
            
            # 3. 解锁确认
            confirm_mode = st.checkbox(f"🔓 解锁删除按钮 (目标: {target_username})", key="del_unlock_checkbox")
            
            if confirm_mode:
                st.error(f"⚠️ 严重警告：你确定要彻底删除 【{target_username}】 吗？")
                st.markdown("""
                该操作会连带删除：
                - 🏦 所有资产记录
                - 📅 所有定投计划
                - 📒 所有投资笔记
                - 💰 所有资金流水
                """)
                
                # 4. 执行删除
                if st.button("🧨 确认删除", type="primary", key="btn_real_delete"):
                    target_id = user_options[target_username]
                    
                    # 调用 utils 里的删除函数
                    success, msg = delete_user_fully(target_id)
                    
                    if success:
                        st.toast(f"成员 {target_username} 已被移除。", icon="✅")
                        
                        # 如果删的是当前登录的人，清空 session 并刷新
                        if st.session_state.user['username'] == target_username:
                            st.session_state.user = None
                        
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error(msg)

finally:
    conn.close()