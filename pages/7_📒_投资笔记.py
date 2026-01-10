import streamlit as st
import pandas as pd
from datetime import datetime

# 🔥 引入公共工具函数
from utils import (
    get_db_connection, 
    show_sidebar_user_picker
)

# ==========================================
# 0. 全局配置与用户校验
# ==========================================
st.set_page_config(page_title="投资笔记", page_icon="📒", layout="wide")

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

st.header("📒 投资笔记与复盘")
st.caption("记录每一次决策的思考，构建自己的投资体系。")

user_id = st.session_state.user['user_id']
conn = get_db_connection()

try:
    # --- 状态管理 ---
    # 记录当前正在编辑哪一个 note_id
    if 'editing_note_id' not in st.session_state:
        st.session_state.editing_note_id = None

    # --- A. 顶部：新建笔记 ---
    # 使用 expander 收纳，显得页面更干净
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

                    # 2. 正文 (修复换行显示问题)
                    # 如果 content 为 None, 使用空字符串
                    content_display = note['content'] if note['content'] else ""
                    st.markdown(content_display.replace('\n', '  \n'))
                    
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
    
finally:
    conn.close()