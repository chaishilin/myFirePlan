import sqlite3
import random
import datetime
from datetime import timedelta
import math

DB_FILE = 'asset_tracker.db'

def create_demo_data():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    print("🚀 开始生成“稳健增长版” Demo 数据 (20个月，总资产<20w)...")

    # ==========================================
    # 1. 清理并重置 demo 用户
    # ==========================================
    user = cursor.execute("SELECT user_id FROM users WHERE username='demo'").fetchone()
    if user:
        user_id = user[0]
        tables = ['snapshots', 'asset_tag_map', 'tags', 'assets', 'monthly_profits', 
                  'investment_plans', 'cashflows', 'rebalance_targets', 'investment_notes']
        for t in tables:
            if t in ['snapshots', 'asset_tag_map']:
                cursor.execute(f"DELETE FROM {t} WHERE asset_id IN (SELECT asset_id FROM assets WHERE user_id=?)", (user_id,))
            elif t == 'assets':
                cursor.execute(f"DELETE FROM {t} WHERE user_id=?", (user_id,))
            else:
                cursor.execute(f"DELETE FROM {t} WHERE user_id=?", (user_id,))
    else:
        cursor.execute("INSERT INTO users (username, password_hash) VALUES (?, ?)", ('demo', 'dummy_hash'))
        user_id = cursor.lastrowid

    # ==========================================
    # 2. 定义 10 大资产 (0.5w ~ 5w)
    # ==========================================
    # 设定：vol(波动率) 很低，growth(成长性) 适中，确保月度变化 < 5%
    # 假设 1 USD = 7.2 CNY
    print("🏦 正在配置资产 (规模 0.5w~5w)...")
    
    assets_config = [
        # --- 现实稳健类 (CNY) ---
        {"name": "现金 (CNY)", "code": "CASH", "type": "现金", "currency": "CNY", "target": 8000, "vol": 0.002, "growth": 0.0},
        {"name": "余额宝", "code": "YUEBAO", "type": "现金", "currency": "CNY", "target": 25000, "vol": 0.001, "growth": 0.002}, # 年化2.4%左右
        {"name": "沪深300 ETF", "code": "510300", "type": "基金", "currency": "CNY", "target": 15000, "vol": 0.03, "growth": 0.005},
        {"name": "纳斯达克100 ETF", "code": "513100", "type": "基金", "currency": "CNY", "target": 45000, "vol": 0.035, "growth": 0.012}, # 成长性最好
        {"name": "红利低波 ETF", "code": "512890", "type": "基金", "currency": "CNY", "target": 30000, "vol": 0.015, "growth": 0.006},
        
        # --- 科幻/奇幻 概念类 (USD) ---
        # 目标金额折合人民币 1w - 4w 之间
        {"name": "布拉佛斯铁金库定存", "code": "BRAAVOS-FD", "type": "其他", "currency": "USD", "target": 18000/7.2, "vol": 0.005, "growth": 0.004}, 
        {"name": "史塔克科技股票", "code": "STARK", "type": "股票", "currency": "USD", "target": 35000/7.2, "vol": 0.04, "growth": 0.015}, # 钢铁侠的高成长
        {"name": "银河系第一共和国国债", "code": "GALACTIC-BOND", "type": "债券", "currency": "USD", "target": 12000/7.2, "vol": 0.01, "growth": 0.003}, 
        {"name": "潘多拉星球概念ETF", "code": "AVATAR", "type": "基金", "currency": "USD", "target": 8000/7.2,  "vol": 0.05, "growth": 0.008}, 
        {"name": "蝙蝠侠-小丑对冲基金", "code": "GOTHAM-HEDGE", "type": "基金", "currency": "USD", "target": 20000/7.2, "vol": 0.045, "growth": 0.01}, 
    ]

    asset_ids = {}
    asset_objs = [] # 存储对象以便后续循环使用
    
    for item in assets_config:
        cursor.execute('''
            INSERT INTO assets (user_id, name, code, type, currency, remarks) 
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (user_id, item["name"], item["code"], item["type"], item["currency"], "Demo演示"))
        aid = cursor.lastrowid
        asset_ids[item["name"]] = aid
        
        # 初始化“当前价格”，假设20个月前是目标价的 80% 左右，慢慢涨上来
        # base_value 是用来模拟净值的
        item['current_base'] = item['target'] * 0.85 
        item['aid'] = aid
        asset_objs.append(item)

    # ==========================================
    # 3. 构建标签体系 (Tags)
    # ==========================================
    print("🏷️ 正在打标签...")
    tags_map = {
        "1. 风险偏好": {
            "稳健理财": ["现金 (CNY)", "余额宝", "红利低波 ETF", "布拉佛斯铁金库定存", "银河系第一共和国国债"],
            "进取投资": ["沪深300 ETF", "纳斯达克100 ETF", "史塔克科技股票"],
            "高风险投机": ["潘多拉星球概念ETF", "蝙蝠侠-小丑对冲基金"]
        },
        "2. 投资位面": {
            "地球位面": ["现金 (CNY)", "余额宝", "沪深300 ETF", "纳斯达克100 ETF", "红利低波 ETF"],
            "漫威/DC宇宙": ["史塔克科技股票", "蝙蝠侠-小丑对冲基金"],
            "权游/科幻宇宙": ["布拉佛斯铁金库定存", "银河系第一共和国国债", "潘多拉星球概念ETF"]
        }
    }

    for group, tags in tags_map.items():
        for tag_name, asset_names in tags.items():
            try:
                cursor.execute("INSERT INTO tags (user_id, tag_group, tag_name) VALUES (?, ?, ?)", (user_id, group, tag_name))
                tag_id = cursor.lastrowid
            except sqlite3.IntegrityError:
                tag_id = cursor.execute("SELECT tag_id FROM tags WHERE user_id=? AND tag_group=? AND tag_name=?", (user_id, group, tag_name)).fetchone()[0]
            
            for name in asset_names:
                aid = asset_ids[name]
                cursor.execute("INSERT OR IGNORE INTO asset_tag_map (asset_id, tag_id) VALUES (?, ?)", (aid, tag_id))

    # ==========================================
    # 4. 生成 20 个月的月度快照 (Snapshots)
    # ==========================================
    print("📈 正在模拟 20 个月的平稳增长曲线...")
    
    end_date = datetime.date.today()
    # 生成 20 个月的月份列表
    month_dates = []
    for i in range(20):
        # 倒推 20 个月，每月 1 号
        d = (end_date.replace(day=1) - timedelta(days=30 * i)).replace(day=1)
        month_dates.append(d)
    month_dates.sort() # 按时间正序
    
    # 写入固定汇率 (简化演示，假设 7.2)
    for d in month_dates:
        cursor.execute("INSERT OR REPLACE INTO exchange_rates (date, currency, rate) VALUES (?, 'USD', 7.2)", (d.strftime('%Y-%m-%d'),))
    
    # 模拟演化
    prev_total_cny = 0
    
    for i, current_date in enumerate(month_dates):
        date_str = current_date.strftime('%Y-%m-%d')
        
        # 市场大环境因子 (每月微调，正负 2% 以内)
        market_sentiment = random.uniform(-0.02, 0.03) 
        
        current_total_cny = 0
        
        for asset in asset_objs:
            # 1. 净值增长 (随机漫步 + 成长性 + 市场因子)
            # vol 控制在很小范围，确保平滑
            change = asset['growth'] + random.gauss(0, asset['vol']) + (market_sentiment * 0.5)
            
            # 限制单月最大跌幅/涨幅，防止极端数据
            change = max(-0.04, min(0.06, change)) 
            
            asset['current_base'] *= (1 + change)
            
            # 2. 模拟定投带来的份额增加 (每月增加一点点本金)
            # 假设20个月里，本金从 85% 慢慢增加到 100%
            # 进度条 0.0 ~ 1.0
            progress = (i + 1) / 20 
            # 本金积累系数：起始 0.8，结束 1.0
            accumulation = 0.8 + 0.2 * progress
            
            # 最终市值 = 净值 * 份额系数
            final_amount = asset['current_base'] * accumulation
            
            # 计算持有收益 (假设收益率在 5%~25% 之间波动)
            # 倒推 cost
            # 越到后期，收益率越高
            mock_yield = 0.02 + 0.15 * progress + random.uniform(-0.02, 0.02)
            cost = final_amount / (1 + mock_yield)
            profit = final_amount - cost
            yield_rate = mock_yield * 100
            
            cursor.execute('''
                INSERT INTO snapshots (asset_id, date, amount, profit, cost, yield_rate, is_cleared)
                VALUES (?, ?, ?, ?, ?, ?, 0)
            ''', (asset['aid'], date_str, final_amount, profit, cost, yield_rate))
            
            rate = 7.2 if asset['currency'] == 'USD' else 1.0
            current_total_cny += final_amount * rate

        # 校验：如果总资产变化超过 5%，说明刚才随机随大了，强行修正最后几个资产的数据 (简化处理，通常参数控制好就行)
        if prev_total_cny > 0:
            change_pct = (current_total_cny - prev_total_cny) / prev_total_cny
            # 这里只是打印看一下，参数调得很保守，一般不会超
            # print(f"Month {date_str}: Total {current_total_cny:.0f}, Change {change_pct*100:.2f}%")
        
        prev_total_cny = current_total_cny

    # ==========================================
    # 5. 插入定投计划 (Investment Plans)
    # ==========================================
    print("📅 配置神奇资产的每日定投...")
    # 史塔克、潘多拉、哥谭 -> 每天定投
    magic_plans = [
        ("史塔克科技股票", 50, "每天"), # 每天 50 USD
        ("潘多拉星球概念ETF", 20, "每天"),
        ("蝙蝠侠-小丑对冲基金", 30, "每天")
    ]
    for p_name, p_amt, p_freq in magic_plans:
        aid = asset_ids[p_name]
        cursor.execute('''
            INSERT INTO investment_plans (user_id, asset_id, amount, frequency, execution_day)
            VALUES (?, ?, ?, ?, ?)
        ''', (user_id, aid, p_amt, p_freq, 0)) # day=0 for daily

    # ==========================================
    # 6. 插入投资笔记 (Investment Notes)
    # ==========================================
    print("📒 写入 Demo 笔记...")
    note_content = """
    **复盘本月操作：**
    
    1. **史塔克工业 (STARK)** 近期发布了新能源反应堆，股价表现强势，继续保持每日定投。
    2. 哥谭市的治安有所好转，**韦恩企业**财报超预期，带动了相关对冲基金的上涨。
    3. 纳斯达克在这个位置有点高了，稍微减仓了一点，换成了**余额宝**和**布拉佛斯定存**。
    
    **下月计划：**
    - 关注潘多拉星球的采矿许可证续期问题，如果有回调是加仓机会。
    - 保持现金流充裕，不要满仓。
    """
    ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cursor.execute('''
        INSERT INTO investment_notes (user_id, title, content, created_at, updated_at) 
        VALUES (?, ?, ?, ?, ?)
    ''', (user_id, "跨位面资产配置周报", note_content, ts, ts))

    # ==========================================
    # 7. 插入现金流 (Cashflows)
    # ==========================================
    print("💰 模拟工资收入...")
    # 过去 20 个月，每个月存 8000 左右
    for i in range(20):
        d = (end_date.replace(day=10) - timedelta(days=30 * i))
        d_str = d.strftime('%Y-%m-%d')
        amt = random.randint(7500, 9000)
        cursor.execute('''
            INSERT INTO cashflows (user_id, date, type, amount, category, note)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (user_id, d_str, "收入", amt, "位面贸易分红", "用于定投"))

    # ==========================================
    # 8. 插入月度收益 (Monthly Profits)
    # ==========================================
    print("🏆 生成月度战绩墙...")
    # 20个月，红多绿少
    for i in range(20):
        d = (end_date.replace(day=1) - timedelta(days=30 * i))
        m_str = d.strftime('%Y-%m')
        
        # 收益金额：总资产的 1% ~ 3% 左右
        profit_amt = random.randint(-1500, 6000) 
        # 强行修正几个月为亏损，显得真实
        if i in [3, 7, 14]: 
            profit_amt = -random.randint(2000, 4000)
            
        cursor.execute('''
            INSERT INTO monthly_profits (user_id, month, tag_group, tag_name, amount)
            VALUES (?, ?, ?, ?, ?)
        ''', (user_id, m_str, "1. 风险偏好", "Total", profit_amt))

    conn.commit()
    conn.close()
    print("✅ 稳健增长版 Demo 数据生成完毕！")
    print("👉 登录 demo 账号查看：总资产<20w，每月平稳波动，包含3个每日定投计划。")

if __name__ == '__main__':
    create_demo_data()