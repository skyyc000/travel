# -*- coding: utf-8 -*-
# 文件名：travel.py (最终整合版)
import streamlit as st
import pandas as pd
import datetime
import time
from copy import deepcopy
import json # 用于处理 CSV 中复杂类型的转换
import os   # 用于检查文件是否存在
import traceback # 用于打印详细错误

# --- 全局定义 ---
PAYMENT_METHODS_OPTIONS = ["支付宝", "微信", "对公转账", "现金", "其他"]
CSV_FILE_PATH = "travel_orders.csv" # 定义 CSV 文件名

# --- 初始化数据存储 (Session State) ---
def init_session_state():
    # 核心数据
    if 'orders' not in st.session_state: st.session_state.orders = []
    if 'order_id_counter' not in st.session_state: st.session_state.order_id_counter = 1

    # 新建订单相关状态
    if 'new_partners' not in st.session_state or not st.session_state.new_partners or not isinstance(st.session_state.new_partners, list):
        st.session_state.new_partners = [{'id': 0, 'name': '', 'settlement': 0.0, 'collection': 0.0, 'notes': ''}]
    if 'new_partner_next_id' not in st.session_state:
        max_id = -1
        if st.session_state.get('new_partners'):
             try:
                  valid_ids = [p.get('id', -1) for p in st.session_state.new_partners if isinstance(p, dict) and isinstance(p.get('id'), int)]
                  if valid_ids: max_id = max(valid_ids)
             except ValueError: pass
        st.session_state.new_partner_next_id = max_id + 1

    # 新建订单表单字段默认值 (用于无 form 时的状态管理和清理)
    new_form_keys = [
        'new_customer_name', 'new_customer_phone', 'new_departure_date', 'new_customer_notes',
        'new_payment_methods', 'new_deposit', 'new_final_payment', 'new_total_payment',
        'new_lines', 'new_adult_count', 'new_adult_price', 'new_child_count', 'new_child_price'
    ]
    default_values = {
        'new_customer_name': '', 'new_customer_phone': '', 'new_departure_date': datetime.date.today(),
        'new_customer_notes': '', 'new_payment_methods': [], 'new_deposit': 0.0, 'new_final_payment': 0.0,
        'new_total_payment': 0.0, 'new_lines': '', 'new_adult_count': 0, 'new_adult_price': 0.0,
        'new_child_count': 0, 'new_child_price': 0.0
    }
    for key in new_form_keys:
        if key not in st.session_state:
            st.session_state[key] = default_values.get(key)

    # 控制状态
    if 'submit_lock' not in st.session_state: st.session_state.submit_lock = False
    if 'csv_data_loaded' not in st.session_state: st.session_state.csv_data_loaded = False

    # 清理标记
    if 'clear_new_order_form_flag' not in st.session_state:
        st.session_state.clear_new_order_form_flag = False
    if 'clear_edit_selection_flag' not in st.session_state:
        st.session_state.clear_edit_selection_flag = False

# --- Helper Functions (计算逻辑 - 不变) ---
def calculate_pax_price(adult_count=0, adult_price=0.0, child_count=0, child_price=0.0):
    adult_count = int(adult_count) if pd.notna(adult_count) else 0
    adult_price = float(adult_price) if pd.notna(adult_price) else 0.0
    child_count = int(child_count) if pd.notna(child_count) else 0
    child_price = float(child_price) if pd.notna(child_price) else 0.0
    return (adult_count * adult_price) + (child_count * child_price)

def calculate_received_payment(deposit=0.0, final_payment=0.0):
    deposit = float(deposit) if pd.notna(deposit) else 0.0
    final_payment = float(final_payment) if pd.notna(final_payment) else 0.0
    return deposit + final_payment

def calculate_partner_totals(partners_list):
    total_cost = 0.0; total_collection = 0.0
    if isinstance(partners_list, list):
        for p in partners_list:
            # 确保 p 是字典并且 'name' 存在且非空
            if isinstance(p, dict) and p.get('name','').strip():
                settlement = p.get('settlement', 0.0)
                collection = p.get('collection', 0.0)
                total_cost += float(settlement) if pd.notna(settlement) else 0.0
                total_collection += float(collection) if pd.notna(collection) else 0.0
    return total_cost, total_collection

# --- CSV 数据处理函数 ---
def safe_json_loads(s):
    """安全地将字符串解析为 Python 对象 (列表或字典)，处理各种错误情况"""
    if not isinstance(s, str) or not s.strip(): return []
    try:
        # 处理 pandas 可能写入的 'nan' 字符串
        if s.lower() == 'nan': return []
        # 替换单引号为双引号以便解析标准 JSON
        result = json.loads(s.replace("'", '"'))
        # 确保结果是列表 (适用于 partners, lines, payment_methods)
        return result if isinstance(result, list) else []
    except json.JSONDecodeError:
        # print(f"警告: JSON 解析失败，字符串: {s}")
        return []
    except Exception as e:
        # print(f"警告: 解析时发生未知错误: {e}, 字符串: {s}")
        return []

def load_data_from_csv():
    """从 CSV 文件加载订单数据，并处理数据类型"""
    orders_list = []
    if os.path.exists(CSV_FILE_PATH):
        try:
            df = pd.read_csv(CSV_FILE_PATH)
            # --- 处理数据类型转换 ---
            numeric_cols = ['deposit_amount', 'final_payment_amount', 'total_payment_amount', 'adult_count', 'child_count', 'adult_price', 'child_price', 'total_pax_price', 'total_revenue', 'total_cost', 'profit', 'total_collection']
            for col in numeric_cols:
                 if col in df.columns:
                      # 先转 numeric (coerce 会把无法转换的变成 NaN), 再填 NaN 为 0
                      df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                 else:
                      df[col] = 0 # 如果列不存在，则添加并填充0
            # 处理列表/字典列 (从字符串转回对象)
            list_cols = ['payment_methods', 'lines', 'partners']
            for col in list_cols:
                 if col in df.columns:
                      # 填充 NaN 或空字符串为空列表字符串 '[]'，以便 safe_json_loads 能处理
                      df[col] = df[col].fillna('[]').astype(str)
                      df[col] = df[col].apply(safe_json_loads)
                 else:
                     # 如果列不存在，添加并为每一行填充一个空列表
                     df[col] = [[] for _ in range(len(df))]

            # 确保 ID 是整数
            if 'id' in df.columns:
                 df['id'] = pd.to_numeric(df['id'], errors='coerce').fillna(0).astype(int)
            else:
                 df['id'] = 0 # 如果 ID 列不存在
            # 处理日期列，保持为字符串格式
            if 'departure_date' in df.columns:
                df['departure_date'] = df['departure_date'].astype(str) # 确保是字符串，避免 NaT 问题

            orders_list = df.to_dict('records')
            print(f"成功从 {CSV_FILE_PATH} 加载 {len(orders_list)} 条订单。")
        except pd.errors.EmptyDataError: print(f"{CSV_FILE_PATH} 文件为空，将返回空列表。")
        except Exception as e: st.error(f"从 CSV 文件加载数据时出错: {e}"); print(f"错误详情: {traceback.format_exc()}"); print(f"请检查 {CSV_FILE_PATH} 文件格式或内容。")
    else: print(f"未找到 {CSV_FILE_PATH} 文件，将返回空列表。")
    return orders_list

def save_data_to_csv(orders_list):
    """将订单数据列表保存到 CSV 文件"""
    expected_cols = ["id", "customer_name", "customer_phone", "departure_date", "customer_notes", "payment_methods", "deposit_amount", "final_payment_amount", "total_payment_amount", "lines", "adult_count", "child_count", "adult_price", "child_price", "total_pax_price", "partners", "total_revenue", "total_cost", "profit", "total_collection", "created_at", "updated_at"]
    try:
        if not orders_list: df = pd.DataFrame(columns=expected_cols)
        else:
             df = pd.DataFrame(orders_list)
             # 确保列表/字典列在保存前是 JSON 字符串格式
             list_cols = ['payment_methods', 'lines', 'partners']
             for col in list_cols:
                  if col in df.columns:
                       def safe_dump(x):
                           if isinstance(x, (list, dict)): return json.dumps(x, ensure_ascii=False) # ensure_ascii=False 保留中文
                           elif isinstance(x, str):
                               # 尝试判断是否已经是 JSON 字符串，避免重复加引号
                               try:
                                   if x.strip().startswith(('[', '{')) and x.strip().endswith((']', '}')):
                                        json.loads(x.replace("'", '"')) # 尝试解析
                                        return x # 如果是有效的，直接返回
                                   else: return json.dumps(x, ensure_ascii=False) # 否则当作普通字符串处理
                               except: return json.dumps(x, ensure_ascii=False) # 解析失败也当作普通字符串
                           else: return json.dumps(x, ensure_ascii=False) # 其他类型直接 dump
                       df[col] = df[col].apply(safe_dump)
        # 确保所有期望的列都存在，不存在的列填充默认值
        for col in expected_cols:
            if col not in df.columns:
                if col in ['deposit_amount', 'final_payment_amount', 'total_payment_amount', 'adult_count', 'child_count', 'adult_price', 'child_price', 'total_pax_price', 'total_revenue', 'total_cost', 'profit', 'total_collection']: df[col] = 0.0
                elif col in ['payment_methods', 'lines', 'partners']: df[col] = '[]' # 保存为 JSON 空列表字符串
                else: df[col] = '' # 其他文本或日期等
        # 按照期望的顺序排列列
        df = df[expected_cols]
        df.to_csv(CSV_FILE_PATH, index=False, encoding='utf-8-sig') # utf-8-sig 避免 Excel 中文乱码
        print(f"成功将 {len(orders_list)} 条订单保存到 {CSV_FILE_PATH}。"); return True
    except Exception as e: st.error(f"保存数据到 CSV 文件时出错: {e}"); print(f"错误详情: {traceback.format_exc()}"); return False

# --- 应用数据加载 ---
def init_app_data():
    """初始化应用数据，从 CSV 加载"""
    if not st.session_state.get('csv_data_loaded', False):
        st.session_state.orders = load_data_from_csv()
        if st.session_state.orders:
            # 尝试获取最大ID，处理可能的非整数ID或空列表
            valid_ids = [int(order.get('id', 0)) for order in st.session_state.orders if isinstance(order.get('id'), (int, float, str)) and str(order.get('id')).isdigit()]
            max_id = max(valid_ids) if valid_ids else 0
            st.session_state.order_id_counter = max_id + 1
        else:
            st.session_state.order_id_counter = 1 # 如果CSV为空或加载失败，从1开始
        st.session_state.csv_data_loaded = True
    # 确保 orders 总是列表
    if 'orders' not in st.session_state or not isinstance(st.session_state.orders, list):
        st.session_state.orders = []

# --- 回调函数 (用于添加/删除合作伙伴) ---
def add_partner_callback(state_key):
    """回调：向指定的 session_state 列表添加一个空伙伴字典"""
    if state_key not in st.session_state: st.session_state[state_key] = []
    partner_list = st.session_state[state_key]
    id_counter_key = state_key + '_next_id'
    # 查找当前最大 ID 或初始化计数器
    if id_counter_key not in st.session_state:
        max_id = -1
        if partner_list:
             try:
                 valid_partner_ids = [p.get('id', -1) for p in partner_list if isinstance(p, dict) and isinstance(p.get('id'), int)]
                 if valid_partner_ids: max_id = max(valid_partner_ids)
             except ValueError: pass # 处理空列表或无效 ID 的情况
        st.session_state[id_counter_key] = max_id + 1
    else:
         # 确保计数器总是领先于现有最大ID
         current_max_id = -1
         if partner_list:
             try:
                  valid_ids = [p.get('id', -1) for p in partner_list if isinstance(p, dict) and isinstance(p.get('id'), int)]
                  if valid_ids: current_max_id = max(valid_ids)
             except ValueError: pass
         st.session_state[id_counter_key] = max(st.session_state.get(id_counter_key, 0), current_max_id + 1)

    new_partner_id = st.session_state[id_counter_key]
    partner_list.append({'id': new_partner_id, 'name': '', 'settlement': 0.0, 'collection': 0.0, 'notes': ''})
    # 增加计数器以备下次使用
    st.session_state[id_counter_key] += 1

def remove_partner_callback(state_key, partner_id_to_remove):
    """回调：从指定的 session_state 列表移除指定ID的伙伴"""
    if state_key in st.session_state:
        current_list = st.session_state[state_key]
        # 过滤掉要删除的伙伴，同时确保列表中的项是字典且有ID
        st.session_state[state_key] = [p for p in current_list if isinstance(p, dict) and p.get('id') != partner_id_to_remove]

# --- Streamlit 页面逻辑 ---
st.set_page_config(layout="wide")

# --- 初始化和加载数据 ---
init_session_state()
init_app_data() # 加载数据

# --- 页面选择 ---
page = st.sidebar.radio("选择页面", ["新建订单", "数据统计与管理"])

# --- 侧边栏刷新按钮 ---
if st.sidebar.button("🔄 刷新数据 (从CSV)"):
    st.session_state.csv_data_loaded = False
    # 清理所有可能残留的状态
    keys_to_clear = [k for k in st.session_state.keys() if k.startswith('new_') or k.startswith('edit_') or k.startswith('partner_') or k == 'select_order_to_edit' or k.endswith('_flag')]
    for key in keys_to_clear:
        try: del st.session_state[key]
        except KeyError: pass
    init_session_state() # 重新初始化状态
    st.rerun()

# =========================================
# ============ 新建订单页面 ============
# =========================================
if page == "新建订单":
    st.header("📝 新建旅游订单")

    # --- 页面顶部：检查并执行新建表单清理 ---
    if st.session_state.get('clear_new_order_form_flag', False):
        print("检测到清理标记，正在清理新建订单表单状态...") # 调试信息
        keys_to_reset = [
            'new_customer_name', 'new_customer_phone', 'new_customer_notes',
            'new_payment_methods', 'new_deposit', 'new_final_payment', 'new_total_payment',
            'new_lines', 'new_adult_count', 'new_adult_price', 'new_child_count', 'new_child_price'
        ]
        default_values_for_reset = {
            'new_customer_name': '', 'new_customer_phone': '', 'new_departure_date': datetime.date.today(),
            'new_customer_notes': '', 'new_payment_methods': [], 'new_deposit': 0.0, 'new_final_payment': 0.0,
            'new_total_payment': 0.0, 'new_lines': '', 'new_adult_count': 0, 'new_adult_price': 0.0,
            'new_child_count': 0, 'new_child_price': 0.0
        }
        # 重置表单字段状态
        for key in keys_to_reset:
            if key in st.session_state:
                 if key == 'new_departure_date':
                     st.session_state[key] = datetime.date.today()
                 else:
                     st.session_state[key] = default_values_for_reset.get(key)
        # 重置合作伙伴列表和计数器
        st.session_state.new_partners = [{'id': 0, 'name': '', 'settlement': 0.0, 'collection': 0.0, 'notes': ''}]
        st.session_state.new_partner_next_id = 1
        # 清除标记！
        st.session_state.clear_new_order_form_flag = False
        print("清理完成。") # 调试信息

    # --- 布局和预览区 ---
    main_col, preview_col = st.columns([3, 1])

    with preview_col:
        st.subheader("📊 数据预览 (实时)")
        # 直接从 session_state 读取当前值用于预览
        preview_deposit = st.session_state.get('new_deposit', 0.0)
        preview_final_payment = st.session_state.get('new_final_payment', 0.0)
        preview_total_payment = st.session_state.get('new_total_payment', 0.0)
        preview_adult_count = st.session_state.get('new_adult_count', 0)
        preview_adult_price = st.session_state.get('new_adult_price', 0.0)
        preview_child_count = st.session_state.get('new_child_count', 0)
        preview_child_price = st.session_state.get('new_child_price', 0.0)
        current_new_partners = st.session_state.get('new_partners', [])
        preview_partners_valid = [p for p in current_new_partners if isinstance(p, dict)] # 确保是字典列表
        # 计算预览值
        preview_pax_price = calculate_pax_price(preview_adult_count, preview_adult_price, preview_child_count, preview_child_price)
        preview_received_payment = calculate_received_payment(preview_deposit, preview_final_payment)
        preview_total_cost, preview_total_collection = calculate_partner_totals(preview_partners_valid)
        preview_profit = preview_total_payment - preview_total_cost # 利润基于应收总额
        # 显示预览指标
        st.metric("人数总价", f"¥ {preview_pax_price:,.2f}")
        st.metric("应收总额", f"¥ {preview_total_payment:,.2f}")
        st.metric("已收金额 (定金+尾款)", f"¥ {preview_received_payment:,.2f}")
        st.metric("总成本(结算)", f"¥ {preview_total_cost:,.2f}")
        st.metric("利润", f"¥ {preview_profit:,.2f}")
        st.metric("总代收", f"¥ {preview_total_collection:,.2f}")
        st.caption("此预览根据当前输入实时更新")

    with main_col:
        # --- 主要信息输入 (无 st.form) ---
        # 每个输入控件的值会自动更新到 st.session_state[key]
        st.subheader("👤 客户信息")
        st.text_input("客户姓名 *", value=st.session_state.get('new_customer_name',''), key="new_customer_name")
        st.text_input("联系电话 *", value=st.session_state.get('new_customer_phone',''), key="new_customer_phone")
        st.date_input("出发日期", value=st.session_state.get('new_departure_date', datetime.date.today()), key="new_departure_date")
        st.text_area("客户资料备注", value=st.session_state.get('new_customer_notes',''), key="new_customer_notes")
        st.divider()

        st.subheader("💰 支付信息")
        st.multiselect("客人支付方式", PAYMENT_METHODS_OPTIONS, default=st.session_state.get('new_payment_methods',[]), key="new_payment_methods")
        col_pay1, col_pay2, col_pay3 = st.columns(3)
        with col_pay1: st.number_input("定金金额", min_value=0.0, step=100.0, format="%.2f", value=st.session_state.get('new_deposit',0.0), key="new_deposit")
        with col_pay2: st.number_input("尾款金额", min_value=0.0, step=100.0, format="%.2f", value=st.session_state.get('new_final_payment',0.0), key="new_final_payment")
        with col_pay3: st.number_input("总款金额 *", min_value=0.0, step=100.0, format="%.2f", value=st.session_state.get('new_total_payment',0.0), key="new_total_payment", help="订单的合同总金额")
        # 参考计算值直接读取 session_state
        ref_deposit = st.session_state.get('new_deposit', 0.0)
        ref_final = st.session_state.get('new_final_payment', 0.0)
        st.caption(f"计算参考 (定金+尾款): ¥ {calculate_received_payment(ref_deposit, ref_final):,.2f}")
        st.divider()

        st.subheader("🗺️ 线路信息")
        st.text_area("旅游线路名称 (每行一条)", value=st.session_state.get('new_lines',''), key="new_lines")
        st.divider()

        st.subheader("🧑‍🤝‍🧑 人数信息")
        col_adult, col_child = st.columns(2)
        with col_adult:
            st.number_input("成人人数", min_value=0, step=1, value=st.session_state.get('new_adult_count',0), key="new_adult_count")
            st.number_input("成人单价", min_value=0.0, step=100.0, format="%.2f", value=st.session_state.get('new_adult_price',0.0), key="new_adult_price")
        with col_child:
            st.number_input("儿童人数", min_value=0, step=1, value=st.session_state.get('new_child_count',0), key="new_child_count")
            st.number_input("儿童单价", min_value=0.0, step=50.0, format="%.2f", value=st.session_state.get('new_child_price',0.0), key="new_child_price")
        st.divider()

        # --- 合作伙伴管理 ---
        st.subheader("🤝 成本核算 (合作伙伴)")
        st.caption("在此处添加或删除合作伙伴信息。")
        # 确保 new_partners 状态存在且是列表
        if 'new_partners' not in st.session_state or not isinstance(st.session_state.new_partners, list):
             st.session_state.new_partners = [{'id': 0, 'name': '', 'settlement': 0.0, 'collection': 0.0, 'notes': ''}]
             st.session_state.new_partner_next_id = 1
        partners_to_render = st.session_state.new_partners
        if not partners_to_render: pass # 如果列表为空则不渲染
        else:
            # 迭代渲染合作伙伴输入行
            for i, partner_state in enumerate(partners_to_render):
                # 安全性检查：确保 partner_state 是字典且有 id
                if isinstance(partner_state, dict) and 'id' in partner_state:
                    partner_id = partner_state.get('id')
                    # 为每个伙伴实例生成唯一的 key 前缀，包含索引以防 ID 重复（理论上不应发生）
                    base_key = f"partner_{partner_id}_new_{i}"
                    cols = st.columns([4, 2, 2, 3, 1]) # 名称, 结算, 代收, 备注, 删除按钮
                    try:
                        # 渲染输入框，其值会直接更新到 session_state.new_partners[i] 中对应的字段
                        st.session_state.new_partners[i]['name'] = cols[0].text_input(
                            f"名称 #{partner_id}", value=partner_state.get('name', ''), key=f"{base_key}_name"
                        )
                        st.session_state.new_partners[i]['settlement'] = cols[1].number_input(
                            f"结算 #{partner_id}", value=float(partner_state.get('settlement', 0.0)), min_value=0.0, format="%.2f", step=100.0, key=f"{base_key}_settlement"
                        )
                        st.session_state.new_partners[i]['collection'] = cols[2].number_input(
                            f"代收 #{partner_id}", value=float(partner_state.get('collection', 0.0)), min_value=0.0, format="%.2f", step=100.0, key=f"{base_key}_collection"
                        )
                        st.session_state.new_partners[i]['notes'] = cols[3].text_area(
                            f"备注 #{partner_id}", value=partner_state.get('notes', ''), key=f"{base_key}_notes" # 无需设置 height
                        )
                        # 删除按钮
                        cols[4].button("❌", key=f"{base_key}_remove", on_click=remove_partner_callback, args=('new_partners', partner_id), help="删除此合作伙伴")
                    except Exception as render_e:
                        st.warning(f"渲染合作伙伴 (ID: {partner_id}, Index: {i}) 时出错: {render_e}")
                else:
                     # 如果列表中的项格式不正确，发出警告
                     st.warning(f"检测到无效或格式错误的合作伙伴条目，索引: {i}，内容: {partner_state}")

        st.button("➕ 添加合作伙伴", on_click=add_partner_callback, args=('new_partners',), key="add_partner_new")
        st.divider()

        # --- 保存按钮 (普通按钮，非 Form Submit) ---
        save_button_clicked = st.button("💾 保存订单", key="save_new_order")

        # --- 保存逻辑 (仅在按钮被点击时执行) ---
        if save_button_clicked and not st.session_state.get('submit_lock', False):
            st.session_state.submit_lock = True # 加锁防止重复提交

            # --- 从 session_state 获取所有最新的输入值 ---
            customer_name = st.session_state.get('new_customer_name', '')
            customer_phone = st.session_state.get('new_customer_phone', '')
            departure_date = st.session_state.get('new_departure_date', None) # date_input 返回 date 对象
            customer_notes = st.session_state.get('new_customer_notes', '')
            payment_methods = st.session_state.get('new_payment_methods', [])
            deposit_amount = st.session_state.get('new_deposit', 0.0)
            final_payment_amount = st.session_state.get('new_final_payment', 0.0)
            total_payment_amount = st.session_state.get('new_total_payment', 0.0)
            lines_text = st.session_state.get('new_lines', '')
            adult_count = st.session_state.get('new_adult_count', 0)
            adult_price = st.session_state.get('new_adult_price', 0.0)
            child_count = st.session_state.get('new_child_count', 0)
            child_price = st.session_state.get('new_child_price', 0.0)
            partners_state_raw = st.session_state.get('new_partners', [])

            # --- 基本验证 ---
            errors = []
            if not customer_name: errors.append("客户姓名不能为空！")
            if not customer_phone: errors.append("联系电话不能为空！")
            if not departure_date: errors.append("出发日期不能为空！")
            if total_payment_amount < 0: errors.append("总款金额不能为负！")
            # 可以在这里添加更多验证，例如电话号码格式等

            if errors:
                # 如果有错误，显示错误信息并解锁
                for error in errors: st.error(error)
                st.session_state.submit_lock = False
            else:
                # --- 整合数据并准备保存 ---
                # 处理合作伙伴数据（过滤空名称，移除临时ID）
                final_partners_data = []
                for p in partners_state_raw:
                    if isinstance(p, dict) and p.get('name','').strip():
                        partner_data = p.copy()
                        partner_data.pop('id', None) # 移除 UI 用的临时 ID
                        # 确保数值类型正确
                        partner_data['settlement'] = float(partner_data.get('settlement', 0.0))
                        partner_data['collection'] = float(partner_data.get('collection', 0.0))
                        final_partners_data.append(partner_data)
                # 处理线路
                lines_list = [line.strip() for line in lines_text.split('\n') if line.strip()]
                # 计算最终值
                final_pax_price = calculate_pax_price(adult_count, adult_price, child_count, child_price)
                final_total_cost, final_total_collection = calculate_partner_totals(final_partners_data)
                final_profit = total_payment_amount - final_total_cost # 利润 = 应收总额 - 总成本
                final_total_revenue_field = total_payment_amount # total_revenue 列存储应收总额

                # 准备订单字典
                local_id = st.session_state.order_id_counter
                new_order_data = {
                    "id": local_id,
                    "customer_name": customer_name, "customer_phone": customer_phone,
                    "departure_date": departure_date.strftime("%Y-%m-%d") if departure_date else "", # 处理可能的 None
                    "customer_notes": customer_notes,
                    "payment_methods": payment_methods,
                    "deposit_amount": deposit_amount,
                    "final_payment_amount": final_payment_amount,
                    "total_payment_amount": total_payment_amount, # 保存总款金额
                    "lines": lines_list,
                    "adult_count": adult_count, "child_count": child_count,
                    "adult_price": adult_price, "child_price": child_price,
                    "total_pax_price": final_pax_price,
                    "partners": final_partners_data, # 保存处理后的伙伴列表
                    "total_revenue": final_total_revenue_field, # 保存应收总额
                    "total_cost": final_total_cost,
                    "profit": final_profit,
                    "total_collection": final_total_collection,
                    "created_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "updated_at": "" # 新建订单无更新时间
                }

                # --- 保存到 Session State 和 CSV ---
                st.session_state.orders.append(new_order_data)
                save_success = save_data_to_csv(st.session_state.orders)

                if save_success:
                    # 保存成功：增加订单ID计数器，设置清理标记，显示成功消息，解锁，重新运行
                    st.session_state.order_id_counter += 1
                    st.session_state.clear_new_order_form_flag = True # 设置清理标记！
                    print("保存成功，设置清理标记。") # 调试信息
                    st.success(f"🎉 订单 (ID: {local_id}) 已保存到本地 CSV 文件！")
                    st.balloons()
                    time.sleep(1) # 短暂显示成功信息
                    st.session_state.submit_lock = False # 解锁
                    st.rerun() # 重新运行以触发清理逻辑并刷新页面
                else:
                    # 保存失败：显示错误，从 session state 移除刚添加的数据，解锁
                    st.error("保存订单到 CSV 文件失败！请检查程序权限或文件是否被占用。")
                    st.session_state.orders.pop() # 移除添加失败的订单
                    st.session_state.submit_lock = False # 解锁

# =========================================
# ======== 数据统计与管理页面 ========
# =========================================
elif page == "数据统计与管理":
    st.header("📊 数据统计与管理")

    # --- 检查订单数据是否存在 ---
    if not st.session_state.get('orders', []):
        st.warning("没有加载到任何订单数据。请尝试刷新或检查 CSV 文件。")
        st.stop() # 如果没有订单数据，停止执行此页面后续代码

    valid_orders = [o for o in st.session_state.orders if isinstance(o, dict) and 'id' in o]
    if not valid_orders:
        st.warning("没有有效的订单数据可供显示。")
        st.stop()

    # --- 创建 DataFrame ---
    try:
        df_orders = pd.DataFrame(valid_orders)
        # 确保关键数字列存在且为数字类型，处理可能因手动修改CSV引入的非数字值
        essential_numeric = ['total_revenue', 'total_cost', 'profit', 'id', 'total_payment_amount', 'deposit_amount', 'final_payment_amount', 'adult_count', 'child_count', 'adult_price', 'child_price', 'total_pax_price', 'total_collection']
        for col in essential_numeric:
            if col not in df_orders.columns: df_orders[col] = 0 # 如果列不存在则添加并设为0
            df_orders[col] = pd.to_numeric(df_orders[col], errors='coerce').fillna(0)
    except Exception as df_e:
        st.error(f"创建 DataFrame 时出错: {df_e}")
        print(traceback.format_exc())
        st.stop()

    # --- 数据概览 ---
    st.subheader("📈 数据概览")
    try:
        total_revenue_all = df_orders['total_revenue'].sum() # 应收总额
        total_cost_all = df_orders['total_cost'].sum()
        total_profit_all = df_orders['profit'].sum()
        total_orders_count = len(df_orders)
        # 计算总已收款
        total_received_all = (df_orders['deposit_amount'] + df_orders['final_payment_amount']).sum()
        # 显示指标
        col1, col2, col3, col4, col5 = st.columns(5)
        col1.metric("总订单数", total_orders_count)
        col2.metric("总应收额", f"¥ {total_revenue_all:,.2f}")
        col3.metric("总已收款", f"¥ {total_received_all:,.2f}")
        col4.metric("总成本", f"¥ {total_cost_all:,.2f}")
        col5.metric("总利润", f"¥ {total_profit_all:,.2f}")
    except KeyError as e: st.error(f"计算数据概览时出错：缺少键 {e}。请检查 CSV 文件表头是否正确。")
    except Exception as e: st.error(f"计算数据概览时发生未知错误: {e}"); print(traceback.format_exc())
    st.divider()

    # --- 搜索与列表 ---
    st.subheader("🔍 搜索与列表")
    search_term = st.text_input("输入关键词搜索 (姓名、电话、线路、伙伴名等)", key="search_term_manage")
    df_display = df_orders.copy() # 使用原始数据进行过滤
    # 执行搜索过滤
    if search_term:
        search_term_lower = search_term.lower()
        mask = pd.Series([False] * len(df_display)) # 初始化全 False 的蒙版
        # 搜索文本列
        str_cols_to_search = ['customer_name', 'customer_phone', 'customer_notes']
        for col in str_cols_to_search:
            if col in df_display.columns:
                mask |= df_display[col].astype(str).str.lower().str.contains(search_term_lower, na=False)
        # 搜索列表列 (线路)
        if 'lines' in df_display.columns:
             # 确保 lines 列是列表，然后检查每个元素
             mask |= df_display['lines'].apply(lambda lines: isinstance(lines, list) and any(search_term_lower in str(line).lower() for line in lines))
        # 搜索列表列 (合作伙伴名称)
        if 'partners' in df_display.columns:
             # 确保 partners 是列表，p 是字典，再检查名称
             mask |= df_display['partners'].apply(lambda partners: isinstance(partners, list) and any(search_term_lower in str(p.get('name','')).lower() for p in partners if isinstance(p, dict)))
        df_display = df_display[mask] # 应用蒙版过滤
        st.write(f"找到 {len(df_display)} 条相关订单：")
    else:
        st.write("所有订单列表：")

    # --- 格式化显示 (包括中文列名) ---
    if not df_display.empty:
        df_display_formatted = df_display.copy() # 创建用于格式化的副本

        # 格式化货币列
        money_cols = ['deposit_amount', 'final_payment_amount', 'total_payment_amount', 'adult_price', 'child_price', 'total_pax_price', 'total_revenue', 'total_cost', 'profit', 'total_collection']
        for col in money_cols:
              if col in df_display_formatted.columns:
                  # 确保先转为数字再格式化
                  df_display_formatted[col] = pd.to_numeric(df_display_formatted[col], errors='coerce').fillna(0).apply(lambda x: f'¥ {x:,.2f}')

        # 格式化列表列为逗号分隔字符串
        list_cols_to_format = ['lines', 'partners', 'payment_methods']
        for col in list_cols_to_format:
             if col in df_display_formatted.columns:
                 if col == 'partners':
                      # 特殊处理：只显示伙伴名称
                      df_display_formatted[col] = df_display_formatted[col].apply(lambda x: ', '.join([str(p.get('name', '')) for p in x if isinstance(p, dict) and p.get('name')]) if isinstance(x, list) else '')
                 else:
                      # 其他列表列直接 join
                      df_display_formatted[col] = df_display_formatted[col].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else '')

        # 定义中英文列名对照表
        column_mapping = {
            "id": "订单ID", "customer_name": "客户姓名", "customer_phone": "联系电话",
            "departure_date": "出发日期", "lines": "旅游线路", "partners": "合作伙伴",
            "total_payment_amount": "总款金额", "total_cost": "总成本", "profit": "利润",
            "total_collection": "总代收", "created_at": "创建时间", "updated_at": "更新时间",
            "payment_methods": "支付方式", "deposit_amount": "定金", "final_payment_amount": "尾款",
            "adult_count": "成人", "child_count": "儿童", "adult_price": "成人价",
            "child_price": "儿童价", "total_pax_price": "人数总价", "customer_notes": "客户备注",
            "total_revenue": "总应收额" # 对应 total_payment_amount
        }

        # 定义理想显示的列顺序 (使用英文名)
        cols_to_show_ideal = ['id', 'customer_name', 'customer_phone', 'departure_date', 'lines', 'partners', 'total_payment_amount', 'total_cost', 'profit', 'total_collection', 'created_at', 'updated_at']
        # 找出实际存在于格式化后 DataFrame 中的列 (仍然是英文名)
        cols_to_show_actual_english = [c for c in cols_to_show_ideal if c in df_display_formatted.columns]

        # 1. 只选择要显示的列
        df_for_display = df_display_formatted[cols_to_show_actual_english]
        # 2. 重命名这些列为中文
        df_for_display = df_for_display.rename(columns=column_mapping)

        # 显示最终处理过的 DataFrame
        st.dataframe(df_for_display, use_container_width=True, hide_index=True)
    else:
        # 如果过滤后 df_display 为空
        if search_term: st.info("没有找到匹配搜索条件的订单。")
        # else: st.info("当前没有订单可显示。") # 如果是初始加载空列表，不显示此消息

    st.divider()

    # --- 修改和删除订单 ---
    st.subheader("✏️ 修改或删除订单")
    # 只有在有订单可操作时才显示选择框等
    if not df_orders.empty: # 改为检查原始 df_orders 是否为空
        # --- 页面顶部附近：检查并执行编辑选择清理 ---
        if st.session_state.get('clear_edit_selection_flag', False):
            print("检测到清理标记，正在重置编辑下拉框...") # 调试信息
            if 'select_order_to_edit' in st.session_state:
                st.session_state.select_order_to_edit = None # 设置为 None 以取消选择
            st.session_state.clear_edit_selection_flag = False # 清除标记
            print("下拉框状态已重置。") # 调试信息

        # --- 选择订单的下拉框 ---
        # 从过滤后的 df_display 获取可选 ID 列表
        order_ids_local = df_display['id'].unique().tolist() if not df_display.empty else []
        order_options = {}
        # 使用原始 df_orders 获取完整客户名以生成选项标签
        for order_id in order_ids_local:
             name_series = df_orders.loc[df_orders['id'] == order_id, 'customer_name']
             name = name_series.iloc[0] if not name_series.empty else '未知客户'
             order_options[order_id] = f"ID: {order_id} - {name}"

        options_list = [None] + sorted(order_ids_local) # 添加 None 选项并排序
        format_func = lambda x: order_options.get(x, "请选择...") if x is not None else "请选择一个订单..."

        # selectbox 会根据 st.session_state.select_order_to_edit 的值显示
        selected_local_id = st.selectbox(
            "选择要操作的订单 (按本地ID)", options=options_list, format_func=format_func,
            index=0, # 默认选中 "请选择..."
            key="select_order_to_edit" # state key 会被上面的清理逻辑重置
        )

        # --- 如果选中了一个订单 ---
        if selected_local_id is not None:
            # 在原始 session_state.orders 列表中查找选定订单的索引
            order_index = next((i for i, order in enumerate(st.session_state.orders) if isinstance(order, dict) and order.get('id') == selected_local_id), None)

            if order_index is not None:
                # 获取原始订单数据的深拷贝，用于填充编辑表单
                selected_order_original = deepcopy(st.session_state.orders[order_index])

                # --- 编辑区域 (使用 Expander) ---
                with st.expander(f"修改订单 (本地ID: {selected_local_id})", expanded=False):
                    # --- 编辑时的合作伙伴管理 ---
                    edit_partners_state_key = f'edit_partners_{selected_local_id}' # 特定于此订单的伙伴状态 key
                    st.subheader("🤝 成本核算 (合作伙伴 - 编辑)")
                    st.caption("在此处添加或删除合作伙伴信息。")
                    # 初始化编辑伙伴状态（如果尚未存在）
                    if edit_partners_state_key not in st.session_state:
                       initial_partners_raw = selected_order_original.get('partners', [])
                       initial_partners = deepcopy([p for p in initial_partners_raw if isinstance(p, dict)]) # 确保是字典列表
                       partner_id_counter = 0
                       for p in initial_partners: p['id'] = partner_id_counter; partner_id_counter += 1 # 添加临时 UI ID
                       st.session_state[edit_partners_state_key] = initial_partners
                       st.session_state[edit_partners_state_key + '_next_id'] = partner_id_counter

                    # 渲染编辑伙伴输入行
                    if isinstance(st.session_state.get(edit_partners_state_key), list):
                        partners_to_edit = st.session_state[edit_partners_state_key]
                        if not partners_to_edit: pass # 如果列表为空则不渲染
                        else:
                            for i, partner_state in enumerate(partners_to_edit): # 使用 enumerate 获取索引
                                if isinstance(partner_state, dict) and 'id' in partner_state:
                                    partner_id = partner_state.get('id')
                                    base_key_edit = f"partner_{partner_id}_edit_{selected_local_id}_{i}" # 包含索引的唯一 key
                                    cols_edit = st.columns([4, 2, 2, 3, 1])
                                    try:
                                        # 渲染并更新对应的 session state
                                        st.session_state[edit_partners_state_key][i]['name'] = cols_edit[0].text_input(f"伙伴名 #{partner_id}", value=partner_state.get('name', ''), key=f"{base_key_edit}_name")
                                        st.session_state[edit_partners_state_key][i]['settlement'] = cols_edit[1].number_input(f"结算 #{partner_id}", value=float(partner_state.get('settlement', 0.0)), min_value=0.0, format="%.2f", step=100.0, key=f"{base_key_edit}_settlement")
                                        st.session_state[edit_partners_state_key][i]['collection'] = cols_edit[2].number_input(f"代收 #{partner_id}", value=float(partner_state.get('collection', 0.0)), min_value=0.0, format="%.2f", step=100.0, key=f"{base_key_edit}_collection")
                                        st.session_state[edit_partners_state_key][i]['notes'] = cols_edit[3].text_area(f"备注 #{partner_id}", value=partner_state.get('notes', ''), key=f"{base_key_edit}_notes")
                                        cols_edit[4].button("❌", key=f"{base_key_edit}_remove", on_click=remove_partner_callback, args=(edit_partners_state_key, partner_id), help="删除此合作伙伴")
                                    except Exception as render_edit_e: st.warning(f"渲染编辑伙伴 (ID: {partner_id}, Index: {i}) 时出错: {render_edit_e}")
                                else: st.warning(f"检测到无效的编辑伙伴条目，索引: {i}，内容: {partner_state}")
                    st.button("➕ 添加合作伙伴", key=f"edit_add_partner_{selected_local_id}", on_click=add_partner_callback, args=(edit_partners_state_key,))
                    st.divider()

                    # --- 编辑表单主体 (使用 st.form 保持回车提交行为) ---
                    form_key = f"edit_order_main_form_{selected_local_id}"
                    with st.form(form_key, clear_on_submit=False): # 编辑表单通常不自动清空
                        # --- 各部分编辑输入框 (客户, 支付, 线路, 人数) ---
                        st.subheader("👤 客户信息 (编辑)")
                        edit_customer_name = st.text_input("客户姓名 *", value=selected_order_original.get('customer_name',''), key=f"edit_name_{selected_local_id}")
                        edit_customer_phone = st.text_input("联系电话 *", value=selected_order_original.get('customer_phone',''), key=f"edit_phone_{selected_local_id}")
                        default_edit_date = None; date_str = selected_order_original.get('departure_date')
                        if date_str:
                            try: default_edit_date = datetime.datetime.strptime(str(date_str), "%Y-%m-%d").date()
                            except ValueError: pass # 如果日期格式错误则忽略
                        edit_departure_date = st.date_input("出发日期 *", value=default_edit_date, key=f"edit_date_{selected_local_id}")
                        edit_customer_notes = st.text_area("客户资料备注", value=selected_order_original.get('customer_notes',''), key=f"edit_notes_{selected_local_id}")
                        st.divider()

                        st.subheader("💰 支付信息 (编辑)")
                        default_pay_methods = selected_order_original.get('payment_methods',[])
                        if not isinstance(default_pay_methods, list): default_pay_methods = [] # 确保是列表
                        edit_payment_methods = st.multiselect("客人支付方式", PAYMENT_METHODS_OPTIONS, default=default_pay_methods, key=f"edit_paymethods_{selected_local_id}")
                        col_pay_edit1, col_pay_edit2, col_pay_edit3 = st.columns(3)
                        edit_deposit_val = float(selected_order_original.get('deposit_amount',0.0))
                        edit_final_val = float(selected_order_original.get('final_payment_amount',0.0))
                        # 如果CSV中没有总额，尝试用定金+尾款计算，否则用CSV中的值
                        edit_total_val = float(selected_order_original.get('total_payment_amount', calculate_received_payment(edit_deposit_val, edit_final_val)))
                        with col_pay_edit1: edit_deposit = st.number_input("定金金额", value=edit_deposit_val, min_value=0.0, format="%.2f", step=100.0, key=f"edit_deposit_{selected_local_id}")
                        with col_pay_edit2: edit_final_payment = st.number_input("尾款金额", value=edit_final_val, min_value=0.0, format="%.2f", step=100.0, key=f"edit_final_{selected_local_id}")
                        with col_pay_edit3: edit_total_payment = st.number_input("总款金额 *", value=edit_total_val, min_value=0.0, format="%.2f", step=100.0, key=f"edit_total_payment_{selected_local_id}", help="订单的合同总金额")
                        st.caption(f"计算参考 (定金+尾款): ¥ {calculate_received_payment(edit_deposit, edit_final_payment):,.2f}")
                        st.divider()

                        st.subheader("🗺️ 线路信息 (编辑)")
                        lines_list_orig = selected_order_original.get('lines', [])
                        if not isinstance(lines_list_orig, list): lines_list_orig = [] # 确保是列表
                        edit_lines_text = st.text_area("旅游线路名称 (每行一条)", value="\n".join(map(str, lines_list_orig)), key=f"edit_lines_{selected_local_id}")
                        st.divider()

                        st.subheader("🧑‍🤝‍🧑 人数信息 (编辑)")
                        col_adult_edit, col_child_edit = st.columns(2)
                        with col_adult_edit:
                            edit_adult_count = st.number_input("成人人数", value=int(selected_order_original.get('adult_count',0)), min_value=0, step=1, key=f"edit_adult_c_{selected_local_id}")
                            edit_adult_price = st.number_input("成人单价", value=float(selected_order_original.get('adult_price',0.0)), min_value=0.0, format="%.2f", step=100.0, key=f"edit_adult_p_{selected_local_id}")
                        with col_child_edit:
                            edit_child_count = st.number_input("儿童人数", value=int(selected_order_original.get('child_count',0)), min_value=0, step=1, key=f"edit_child_c_{selected_local_id}")
                            edit_child_price = st.number_input("儿童单价", value=float(selected_order_original.get('child_price',0.0)), min_value=0.0, format="%.2f", step=50.0, key=f"edit_child_p_{selected_local_id}")
                        st.divider()

                        # --- 编辑时的预览 ---
                        st.subheader("📊 数据预览 (编辑时估算)")
                        edit_preview_partners_raw = st.session_state.get(edit_partners_state_key, [])
                        edit_preview_partners = [p for p in edit_preview_partners_raw if isinstance(p, dict)]
                        edit_preview_pax_price = calculate_pax_price(edit_adult_count, edit_adult_price, edit_child_count, edit_child_price)
                        edit_preview_received = calculate_received_payment(edit_deposit, edit_final_payment)
                        edit_preview_cost, edit_preview_collection = calculate_partner_totals(edit_preview_partners)
                        edit_preview_total_payment_form = edit_total_payment # 使用编辑框中的总额
                        edit_preview_profit = edit_preview_total_payment_form - edit_preview_cost
                        # 显示预览指标
                        col_pax_edit, col_rev_edit, col_rec_edit, col_cost_edit, col_prof_edit, col_coll_edit = st.columns(6)
                        col_pax_edit.metric("人数总价", f"¥ {edit_preview_pax_price:,.2f}")
                        col_rev_edit.metric("应收总额", f"¥ {edit_preview_total_payment_form:,.2f}")
                        col_rec_edit.metric("已收金额", f"¥ {edit_preview_received:,.2f}")
                        col_cost_edit.metric("总成本", f"¥ {edit_preview_cost:,.2f}")
                        col_prof_edit.metric("利润", f"¥ {edit_preview_profit:,.2f}")
                        col_coll_edit.metric("总代收", f"¥ {edit_preview_collection:,.2f}")

                        # --- 编辑表单的提交按钮 ---
                        edit_submitted = st.form_submit_button("💾 保存修改")

                        # --- 编辑保存逻辑 ---
                        if edit_submitted:
                            # 基本验证
                            edit_errors = []
                            if not edit_customer_name: edit_errors.append("客户姓名不能为空！")
                            if not edit_customer_phone: edit_errors.append("联系电话不能为空！")
                            if not edit_departure_date: edit_errors.append("出发日期不能为空！")
                            if edit_total_payment < 0: edit_errors.append("总款金额不能为负！")

                            if edit_errors:
                                for error in edit_errors: st.error(error)
                            else:
                                # 整合编辑后的数据
                                final_edit_partners_state_raw = st.session_state.get(edit_partners_state_key, [])
                                edit_final_partners_data = []
                                for p in final_edit_partners_state_raw:
                                     if isinstance(p, dict) and p.get('name','').strip():
                                         partner_data = p.copy(); partner_data.pop('id', None)
                                         partner_data['settlement'] = float(partner_data.get('settlement', 0.0))
                                         partner_data['collection'] = float(partner_data.get('collection', 0.0))
                                         edit_final_partners_data.append(partner_data)
                                edit_lines_list = [line.strip() for line in edit_lines_text.split('\n') if line.strip()]
                                edit_final_pax_price = calculate_pax_price(edit_adult_count, edit_adult_price, edit_child_count, edit_child_price)
                                edit_final_total_cost, edit_final_total_collection = calculate_partner_totals(edit_final_partners_data)
                                edit_final_profit = edit_total_payment - edit_final_total_cost
                                edit_final_total_revenue_field = edit_total_payment # 保存编辑后的总额

                                # 创建更新后的订单字典
                                updated_order_data = {
                                    "id": selected_local_id, # 保持原始 ID
                                    "customer_name": edit_customer_name, "customer_phone": edit_customer_phone,
                                    "departure_date": edit_departure_date.strftime("%Y-%m-%d") if edit_departure_date else "",
                                    "customer_notes": edit_customer_notes, "payment_methods": edit_payment_methods,
                                    "deposit_amount": edit_deposit, "final_payment_amount": edit_final_payment,
                                    "total_payment_amount": edit_total_payment, "lines": edit_lines_list,
                                    "adult_count": edit_adult_count, "child_count": edit_child_count,
                                    "adult_price": edit_adult_price, "child_price": edit_child_price,
                                    "total_pax_price": edit_final_pax_price, "partners": edit_final_partners_data,
                                    "total_revenue": edit_final_total_revenue_field, "total_cost": edit_final_total_cost,
                                    "profit": edit_final_profit, "total_collection": edit_final_total_collection,
                                    "created_at": selected_order_original.get('created_at',''), # 保留原始创建时间
                                    "updated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") # 更新时间戳
                                }
                                # 更新 session state 中的订单
                                st.session_state.orders[order_index] = updated_order_data
                                # 保存整个列表到 CSV
                                save_success = save_data_to_csv(st.session_state.orders)
                                if save_success:
                                    # 清理此订单的编辑伙伴状态
                                    if edit_partners_state_key in st.session_state: del st.session_state[edit_partners_state_key]
                                    if edit_partners_state_key + '_next_id' in st.session_state: del st.session_state[edit_partners_state_key + '_next_id']
                                    st.success(f"订单 ID: {selected_local_id} 已更新并保存到本地 CSV 文件！")
                                    time.sleep(0.5) # 短暂停顿
                                    st.rerun() # 重新运行以刷新列表和关闭 expander
                                else:
                                    st.error("更新订单到 CSV 文件失败！请检查文件是否被占用。")
                                    # 可选：如果保存失败，可以考虑回滚 session state 的修改
                                    # st.session_state.orders[order_index] = selected_order_original

                # --- 删除功能 ---
                st.divider()
                st.error("--- 删除操作 ---") # 醒目提示
                col_del_confirm, col_del_btn = st.columns([3,1])
                delete_confirmed = col_del_confirm.checkbox(f"确认删除订单 (本地ID: {selected_local_id})?", key=f"delete_confirm_{selected_local_id}")
                if delete_confirmed:
                    # 只有勾选了复选框，删除按钮才有效
                    if col_del_btn.button("🗑️ 执行删除", key=f"delete_exec_{selected_local_id}", type="primary"):
                        try:
                            # 从 session state 移除订单 (pop 返回被移除的元素，可用于撤销)
                            deleted_order_for_undo = st.session_state.orders.pop(order_index)
                        except IndexError:
                            st.error(f"删除失败：无法在列表中找到订单索引 {order_index} (ID: {selected_local_id})。请刷新页面重试。")
                            st.stop() # 停止执行后续代码

                        # 保存更新后的订单列表到 CSV
                        save_success = save_data_to_csv(st.session_state.orders)

                        if save_success:
                            # --- 设置清理标记，以便下次刷新时重置下拉框 ---
                            st.session_state.clear_edit_selection_flag = True
                            print(f"删除成功 (ID: {selected_local_id})，设置清理标记。") # 调试信息

                            # 清理可能存在的编辑伙伴状态
                            edit_partners_state_key = f'edit_partners_{selected_local_id}'
                            if edit_partners_state_key in st.session_state: del st.session_state[edit_partners_state_key]
                            if edit_partners_state_key + '_next_id' in st.session_state: del st.session_state[edit_partners_state_key + '_next_id']

                            st.success(f"订单 ID: {selected_local_id} 已从本地列表和 CSV 文件删除！")
                            time.sleep(0.5) # 短暂显示成功信息
                            st.rerun() # 重新运行，触发下拉框清理并刷新列表
                        else:
                            # 如果保存 CSV 失败，恢复 session state 并提示用户
                            st.error("从 CSV 文件删除订单失败！文件可能被占用。")
                            st.session_state.orders.insert(order_index, deleted_order_for_undo) # 恢复列表
                            st.warning("本地列表中的订单已恢复，但 CSV 文件可能未更新，请检查。")
            else:
                # 这个情况理论上不应该发生，因为 selected_local_id 来自列表
                st.error(f"内部错误：无法在会话状态中找到订单索引 本地ID: {selected_local_id}。请尝试刷新数据。")
    else:
        # 如果 df_orders 为空 (初始加载或过滤后无数据)
        if search_term: st.info("没有找到匹配搜索条件的订单。")
        else: st.info("当前没有订单数据可供操作。")


# --- 文件末尾 ---