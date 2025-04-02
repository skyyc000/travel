# -*- coding: utf-8 -*-
# 文件名：travel.py (飞书整合版)
import streamlit as st
import pandas as pd
import datetime
import time
from copy import deepcopy
import json # 用于处理复杂类型的转换 (JSON字符串 <-> Python对象)
import os   # 用于检查文件是否存在 (虽然主要用飞书，但可能本地调试时需要)
import traceback # 用于打印详细错误
import requests # 用于调用飞书 API

# --- 全局定义 ---
PAYMENT_METHODS_OPTIONS = ["支付宝", "微信", "对公转账", "现金", "其他"]
# CSV_FILE_PATH = "travel_orders.csv" # 不再使用 CSV

# --- 飞书配置 (从 st.secrets 获取) ---
# 你需要在 Streamlit Secrets 中配置这些值
# FEISHU_APP_ID = st.secrets.get("FEISHU_APP_ID") # 例如: "cli_xxxxxxxxxxxx"
# FEISHU_APP_SECRET = st.secrets.get("FEISHU_APP_SECRET") # 例如: "xxxxxxxxxxxxxxxxxxxxxxxxxx"
# FEISHU_SPREADSHEET_TOKEN = st.secrets.get("FEISHU_SPREADSHEET_TOKEN") # 例如: "shtcnxxxxxxxxxxxxxxx"
# FEISHU_SHEET_ID_OR_NAME = st.secrets.get("FEISHU_SHEET_ID_OR_NAME", "Sheet1") # 默认使用 "Sheet1"，如果知道确切ID可以配置

# --- 飞书 API 地址 ---
FEISHU_GET_TOKEN_URL = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal/"
FEISHU_API_BASE_URL = "https://open.feishu.cn/open-apis/sheets/v2/spreadsheets"

# --- 预期的表格列 (必须与飞书表格第一行完全一致) ---
EXPECTED_COLS = ["id", "customer_name", "customer_phone", "departure_date", "customer_notes", "payment_methods", "deposit_amount", "final_payment_amount", "total_payment_amount", "lines", "adult_count", "child_count", "adult_price", "child_price", "total_pax_price", "partners", "total_revenue", "total_cost", "profit", "total_collection", "created_at", "updated_at"]
# 根据 EXPECTED_COLS 计算飞书表格范围 (假设列数固定)
# 22 列对应到 V 列
FEISHU_TABLE_LAST_COL = "V" # 第 22 个字母

# --- 全局变量存储 Token (简单实现，未处理过期) ---
_tenant_access_token = None
_token_expires_at = 0 # Token 过期时间戳

# --- 飞书 API 相关函数 ---
def get_feishu_tenant_token(app_id, app_secret):
    """获取飞书 Tenant Access Token，带简单缓存和过期处理"""
    global _tenant_access_token, _token_expires_at
    current_time = time.time()

    # 如果有缓存且未过期 (留 5 分钟缓冲)
    if _tenant_access_token and current_time < (_token_expires_at - 300):
        # print("使用缓存的飞书 Token")
        return _tenant_access_token

    # 否则，重新获取
    print("正在获取新的飞书 Token...")
    try:
        payload = {"app_id": app_id, "app_secret": app_secret}
        response = requests.post(FEISHU_GET_TOKEN_URL, json=payload, timeout=10) # 增加超时
        response.raise_for_status()
        result = response.json()
        if result.get("code") == 0:
            _tenant_access_token = result.get("tenant_access_token")
            expire_in_seconds = result.get("expire", 7200) # 默认2小时
            _token_expires_at = current_time + expire_in_seconds
            print(f"成功获取飞书 Token，有效期至: {datetime.datetime.fromtimestamp(_token_expires_at)}")
            return _tenant_access_token
        else:
            print(f"获取飞书 Token 失败: {result.get('msg')}")
            st.error(f"飞书认证失败 (无法获取令牌): {result.get('msg')}")
            _tenant_access_token = None # 获取失败，清空缓存
            _token_expires_at = 0
            return None
    except requests.exceptions.Timeout:
        print("请求飞书 Token 超时")
        st.error("连接飞书服务器超时，请稍后重试。")
        return None
    except requests.exceptions.RequestException as e:
        print(f"请求飞书 Token 时网络错误: {e}")
        st.error(f"连接飞书服务器失败: {e}")
        return None
    except Exception as e:
        print(f"获取飞书 Token 时发生未知错误: {e}")
        st.error(f"飞书认证时发生内部错误: {e}")
        return None

def safe_json_loads(s):
    """安全地将字符串解析为 Python 对象 (列表或字典)，处理各种错误情况"""
    if not isinstance(s, str) or not s.strip(): return []
    try:
        if s.lower() == 'nan': return []
        # 先尝试直接解析标准 JSON
        result = json.loads(s)
        # return result if isinstance(result, (list, dict)) else [] # 允许返回字典或列表
        return result # 直接返回解析结果，调用处再判断类型
    except json.JSONDecodeError:
        try:
            # 尝试替换单引号后解析 (不推荐，最好保证存入时就是标准JSON)
            result = json.loads(s.replace("'", '"'))
            # return result if isinstance(result, (list, dict)) else []
            return result
        except json.JSONDecodeError:
            # print(f"警告: JSON 解析失败，字符串: {s}")
            return [] # 或返回原始字符串 s ? 取决于后续处理逻辑
    except Exception as e:
        # print(f"警告: 解析时发生未知错误: {e}, 字符串: {s}")
        return []

def load_data_from_feishu(spreadsheet_token, sheet_id_or_name, app_id, app_secret):
    """从飞书表格加载数据"""
    token = get_feishu_tenant_token(app_id, app_secret)
    if not token:
        st.warning("无法获取飞书 Token，暂时无法加载数据。请检查配置或网络。")
        return [] # 返回空列表，允许应用继续运行但无数据

    headers = {"Authorization": f"Bearer {token}"}
    # 定义读取范围，从 A1 到最后一列的末尾 (假设数据不会超过10000行，或者使用 API 获取实际行数 - 较复杂)
    # 更安全的做法是先获取表格元数据得到实际行数，或读取一个较大的固定范围
    sheet_range = f"{sheet_id_or_name}!A1:{FEISHU_TABLE_LAST_COL}10000" # 读取足够大的范围
    read_url = f"{FEISHU_API_BASE_URL}/{spreadsheet_token}/values/{sheet_range}?valueRenderOption=ToString&dateTimeRenderOption=FormattedString"

    orders_list = []
    try:
        print(f"尝试从飞书读取: {read_url}")
        response = requests.get(read_url, headers=headers, timeout=20) # 增加超时
        response.raise_for_status()
        result = response.json()

        if result.get("code") == 0:
            data = result.get("data", {}).get("valueRange", {}).get("values", [])
            if not data or len(data) < 1: # 至少需要表头行
                print("飞书表格为空或无法读取。")
                # 检查表是否存在或是否有权限
                return []

            header_row = data[0] # 第一行是表头
            print(f"从飞书读取到的表头 ({len(header_row)} 列): {header_row}")

            # --- 关键检查：表头是否匹配 ---
            if header_row != EXPECTED_COLS:
                 st.error(f"飞书表格的表头与程序期望的不匹配！请立即检查飞书表格 '{sheet_id_or_name}' 的第一行。")
                 print(f"错误: 表头不匹配！")
                 print(f"  期望 ({len(EXPECTED_COLS)}): {EXPECTED_COLS}")
                 print(f"  实际 ({len(header_row)}): {header_row}")
                 # 比较差异
                 missing_expected = [col for col in EXPECTED_COLS if col not in header_row]
                 extra_actual = [col for col in header_row if col not in EXPECTED_COLS]
                 if missing_expected: print(f"  飞书表头缺少列: {missing_expected}")
                 if extra_actual: print(f"  飞书表头多了列: {extra_actual}")
                 # 检查顺序和名称 (如果长度相同)
                 if len(EXPECTED_COLS) == len(header_row):
                     diff = [(i, EXPECTED_COLS[i], header_row[i]) for i in range(len(EXPECTED_COLS)) if EXPECTED_COLS[i] != header_row[i]]
                     if diff: print(f"  列名或顺序不匹配的地方 (索引, 期望, 实际): {diff}")
                 st.warning("由于表头不匹配，数据加载已中止。")
                 return [] # 表头不匹配，绝对不能加载数据

            data_rows = data[1:] # 从第二行开始是数据
            print(f"从飞书读取到 {len(data_rows)} 行原始数据。")

            for row_index, row in enumerate(data_rows):
                # 跳过完全空行 (所有单元格都是空字符串或 None)
                if not any(cell is not None and cell != '' for cell in row):
                    # print(f"跳过空行 {row_index + 2}")
                    continue

                order_dict = {}
                # 填充缺失的列为空值，以匹配表头长度
                padded_row = row + [''] * (len(header_row) - len(row))

                for i, col_name in enumerate(header_row):
                    value = padded_row[i]
                    # --- 数据类型转换 ---
                    try:
                        if col_name in ['id', 'adult_count', 'child_count']:
                            order_dict[col_name] = int(float(value)) if value not in [None, ''] else 0 # 先转 float 再转 int 处理可能的小数
                        elif col_name in ['deposit_amount', 'final_payment_amount', 'total_payment_amount', 'adult_price', 'child_price', 'total_pax_price', 'total_revenue', 'total_cost', 'profit', 'total_collection']:
                            order_dict[col_name] = float(value) if value not in [None, ''] else 0.0
                        elif col_name in ['payment_methods', 'lines', 'partners']:
                            # 假设这些列表/字典在表格中存储为 JSON 字符串
                            parsed_value = safe_json_loads(value)
                            # 确保结果是列表 (适用于这三个字段)
                            order_dict[col_name] = parsed_value if isinstance(parsed_value, list) else []
                        elif col_name == 'departure_date':
                            # 飞书返回的可能是格式化日期字符串，直接用
                            order_dict[col_name] = value if value else ''
                        else: # 其他如 customer_name, phone, notes, created_at, updated_at 视为字符串
                            order_dict[col_name] = str(value) if value is not None else ''
                    except (ValueError, TypeError, json.JSONDecodeError) as e:
                        print(f"警告: 处理单元格数据时出错 (行: {row_index+2}, 列: '{col_name}', 值: '{value}', 错误: {e})。将使用默认值。")
                        # 根据列类型设置默认值
                        if col_name in ['id', 'adult_count', 'child_count']: order_dict[col_name] = 0
                        elif col_name in ['deposit_amount', 'final_payment_amount', 'total_payment_amount', 'adult_price', 'child_price', 'total_pax_price', 'total_revenue', 'total_cost', 'profit', 'total_collection']: order_dict[col_name] = 0.0
                        elif col_name in ['payment_methods', 'lines', 'partners']: order_dict[col_name] = []
                        else: order_dict[col_name] = '' # 其他默认为空字符串

                # 只有当订单 ID 有效时才添加到列表 (避免完全空行被错误处理后加入)
                if order_dict.get('id') is not None: # 或者检查 name/phone 等必要字段
                    orders_list.append(order_dict)

            print(f"成功从飞书加载并处理 {len(orders_list)} 条有效订单。")
            return orders_list
        else:
            print(f"读取飞书表格 API 返回错误码 {result.get('code')}: {result.get('msg')}")
            st.error(f"读取飞书数据时出错: {result.get('msg')} (请检查 Spreadsheet Token 或 Sheet ID/名称是否正确，以及应用权限)")
            return []
    except requests.exceptions.Timeout:
        print("请求飞书表格数据超时")
        st.error("读取飞书数据超时，请稍后重试或检查网络。")
        return []
    except requests.exceptions.RequestException as e:
        print(f"请求飞书表格数据时网络错误: {e}")
        st.error(f"连接飞书服务器读取数据失败: {e}")
        return []
    except Exception as e:
        print(f"处理飞书表格数据时发生未知错误: {e}")
        st.error(f"处理飞书数据时发生内部错误: {e}")
        print(f"错误详情: {traceback.format_exc()}") # 打印详细错误信息
        return []

def save_data_to_feishu(orders_list, spreadsheet_token, sheet_id_or_name, app_id, app_secret):
    """将订单数据列表保存到飞书表格 (覆盖模式)"""
    token = get_feishu_tenant_token(app_id, app_secret)
    if not token:
        st.error("无法获取飞书 Token，无法保存数据。")
        return False

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json; charset=utf-8"
    }

    # 准备要写入的数据 (list of lists)
    values_to_write = [EXPECTED_COLS] # 第一行是表头

    # 按 ID 排序后再写入，保持表格顺序相对稳定
    sorted_orders = sorted(orders_list, key=lambda x: x.get('id', 0))

    for order in sorted_orders:
        row_data = []
        for col_name in EXPECTED_COLS:
            value = order.get(col_name)
            # --- 数据类型转换为适合表格存储的格式 ---
            if isinstance(value, (list, dict)):
                # 将列表/字典转换为 JSON 字符串存储
                try:
                    # ensure_ascii=False 保证中文正常显示
                    row_data.append(json.dumps(value, ensure_ascii=False, separators=(',', ':'))) # separators 减少空格
                except TypeError:
                    print(f"警告: 无法将列 '{col_name}' 的值 ({value}) 序列化为 JSON，将存为空字符串。订单ID: {order.get('id')}")
                    row_data.append('[]') # 序列化失败存空列表字符串
            elif isinstance(value, datetime.date):
                 row_data.append(value.strftime("%Y-%m-%d"))
            elif isinstance(value, datetime.datetime):
                 row_data.append(value.strftime("%Y-%m-%d %H:%M:%S"))
            elif value is None:
                 row_data.append('') # None 存为空字符串
            else:
                 # 其他都转成字符串
                 row_data.append(str(value))
        values_to_write.append(row_data)

    # 写入数据 API (使用 v2 /values 接口覆盖)
    write_url = f"{FEISHU_API_BASE_URL}/{spreadsheet_token}/values"
    # 定义写入范围，从 A1 开始覆盖所有数据
    num_rows = len(values_to_write)
    write_range = f"{sheet_id_or_name}!A1:{FEISHU_TABLE_LAST_COL}{num_rows}" # 动态计算范围

    payload = {
        "valueRange": {
            "range": write_range,
            "values": values_to_write
        }
    }

    # --- 先清空旧数据 (从第二行开始)，避免旧数据残留 ---
    # 注意：如果表格很大，清空+写入可能比直接覆盖写入更慢或更容易超时
    # 也可以选择不清空，直接用 PUT /values 覆盖写入，飞书会自动调整表格大小
    clear_start_row = 2 # 从第二行开始清
    clear_range = f"{sheet_id_or_name}!A{clear_start_row}:{FEISHU_TABLE_LAST_COL}" # 清空 A2 到最后一列的末尾
    clear_url = f"{FEISHU_API_BASE_URL}/{spreadsheet_token}/clear_range" # 使用 v3 API

    # v3 清除范围 API payload
    clear_payload_v3 = {"range": clear_range}
    # v2 清除范围 API（如果 v3 不可用或权限问题）
    # clear_url_v2 = f"{FEISHU_API_BASE_URL}/{spreadsheet_token}/values"
    # clear_payload_v2 = {"valueRange": {"range": clear_range, "values": [["" for _ in range(len(EXPECTED_COLS))]]}} # 写入空值

    try:
        print(f"尝试清空飞书范围: {clear_range}")
        # 使用 v3 API 清空
        clear_response = requests.post(clear_url, headers=headers, json=clear_payload_v3, timeout=15)
        clear_result = clear_response.json()
        if clear_response.status_code == 200 and clear_result.get("code") == 0:
            print(f"成功清空范围 {clear_result.get('data', {}).get('clearRange', '')}")
        else:
            # 如果 v3 失败，尝试打印错误，但继续尝试写入
             print(f"警告: 清空飞书范围失败 (状态码 {clear_response.status_code}): {clear_result.get('msg', '无详细信息')}")
             st.warning(f"未能清空旧数据，将尝试直接覆盖写入。")

        # --- 写入新数据 ---
        print(f"尝试写入飞书范围: {write_range}")
        response = requests.put(write_url, headers=headers, json=payload, timeout=30) # 写入超时设置长一点
        response.raise_for_status()
        result = response.json()

        if result.get("code") == 0:
            print(f"成功将 {len(orders_list)} 条订单保存到飞书表格。更新范围: {result.get('data', {}).get('updatedRange', '')}")
            return True
        else:
            print(f"写入飞书表格 API 返回错误码 {result.get('code')}: {result.get('msg')}")
            print(f"失败详情: {result}") # 打印完整错误信息
            st.error(f"保存到飞书失败: {result.get('msg')} (Code: {result.get('code')})")
            return False
    except requests.exceptions.Timeout:
        print("请求飞书表格写入或清空时超时")
        st.error("保存到飞书超时，请稍后重试。")
        return False
    except requests.exceptions.RequestException as e:
        print(f"请求飞书表格写入或清空时网络错误: {e}")
        st.error(f"连接飞书服务器保存数据失败: {e}")
        return False
    except Exception as e:
        print(f"写入飞书表格时发生未知错误: {e}")
        st.error(f"保存到飞书时发生内部错误: {e}")
        print(f"错误详情: {traceback.format_exc()}")
        return False


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

    # 新建订单表单字段默认值
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
    if 'feishu_data_loaded' not in st.session_state: st.session_state.feishu_data_loaded = False # 改名

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
            if isinstance(p, dict) and p.get('name','').strip():
                settlement = p.get('settlement', 0.0)
                collection = p.get('collection', 0.0)
                # 添加对 settlement 和 collection 的类型检查和转换
                try:
                    total_cost += float(settlement) if pd.notna(settlement) else 0.0
                except (ValueError, TypeError):
                    print(f"警告: 合作伙伴结算金额无效，将计为0。伙伴: {p.get('name')}, 金额: {settlement}")
                try:
                    total_collection += float(collection) if pd.notna(collection) else 0.0
                except (ValueError, TypeError):
                     print(f"警告: 合作伙伴代收金额无效，将计为0。伙伴: {p.get('name')}, 金额: {collection}")

    return total_cost, total_collection

# --- 应用数据加载 (从飞书) ---
def init_app_data():
    """初始化应用数据，从 Feishu 加载"""
    # --- 获取配置 ---
    app_id = st.secrets.get("FEISHU_APP_ID")
    app_secret = st.secrets.get("FEISHU_APP_SECRET")
    spreadsheet_token = st.secrets.get("FEISHU_SPREADSHEET_TOKEN")
    sheet_id_or_name = st.secrets.get("FEISHU_SHEET_ID_OR_NAME", "Sheet1") # 默认 Sheet1

    # --- 检查配置是否存在 ---
    missing_secrets = []
    if not app_id: missing_secrets.append("FEISHU_APP_ID")
    if not app_secret: missing_secrets.append("FEISHU_APP_SECRET")
    if not spreadsheet_token: missing_secrets.append("FEISHU_SPREADSHEET_TOKEN")

    if missing_secrets:
         st.error(f"Feishu 配置信息不完整，请在 Streamlit Secrets 中设置: {', '.join(missing_secrets)}")
         st.warning("无法连接到飞书，应用将无法加载或保存数据。")
         st.session_state.orders = []
         st.session_state.feishu_data_loaded = True # 标记为已尝试加载（虽然失败）
         return # 阻止后续加载

    # --- 加载数据 ---
    if not st.session_state.get('feishu_data_loaded', False):
        with st.spinner("正在从飞书加载订单数据..."):
            st.session_state.orders = load_data_from_feishu(spreadsheet_token, sheet_id_or_name, app_id, app_secret)

        if st.session_state.orders: # 检查加载是否成功（返回非空列表）
            # 尝试获取最大ID
            valid_ids = [int(order.get('id', 0)) for order in st.session_state.orders if isinstance(order.get('id'), (int, float, str)) and str(order.get('id')).isdigit()]
            max_id = max(valid_ids) if valid_ids else 0
            st.session_state.order_id_counter = max_id + 1
            print(f"数据加载完成，当前最大订单ID: {max_id}, 下一个ID将是: {st.session_state.order_id_counter}")
        else:
             # 如果加载返回空列表 (可能表为空，或加载失败)
             print("从飞书加载数据为空或失败，订单ID计数器将从1开始。")
             st.session_state.order_id_counter = 1
        st.session_state.feishu_data_loaded = True # 标记已加载（无论成功与否）
    else:
         # print("数据已加载，跳过 Feishu 读取。")
         pass

    # 确保 orders 总是列表
    if 'orders' not in st.session_state or not isinstance(st.session_state.orders, list):
        st.session_state.orders = []
        print("警告: session_state.orders 不是列表，已重置为空列表。")


# --- 回调函数 (用于添加/删除合作伙伴 - 不变) ---
def add_partner_callback(state_key):
    if state_key not in st.session_state: st.session_state[state_key] = []
    partner_list = st.session_state[state_key]
    id_counter_key = state_key + '_next_id'
    if id_counter_key not in st.session_state:
        max_id = -1
        if partner_list:
             try:
                 valid_partner_ids = [p.get('id', -1) for p in partner_list if isinstance(p, dict) and isinstance(p.get('id'), int)]
                 if valid_partner_ids: max_id = max(valid_partner_ids)
             except ValueError: pass
        st.session_state[id_counter_key] = max_id + 1
    else:
         current_max_id = -1
         if partner_list:
             try:
                  valid_ids = [p.get('id', -1) for p in partner_list if isinstance(p, dict) and isinstance(p.get('id'), int)]
                  if valid_ids: current_max_id = max(valid_ids)
             except ValueError: pass
         st.session_state[id_counter_key] = max(st.session_state.get(id_counter_key, 0), current_max_id + 1)

    new_partner_id = st.session_state[id_counter_key]
    partner_list.append({'id': new_partner_id, 'name': '', 'settlement': 0.0, 'collection': 0.0, 'notes': ''})
    st.session_state[id_counter_key] += 1

def remove_partner_callback(state_key, partner_id_to_remove):
    if state_key in st.session_state:
        current_list = st.session_state[state_key]
        st.session_state[state_key] = [p for p in current_list if isinstance(p, dict) and p.get('id') != partner_id_to_remove]

# --- Streamlit 页面逻辑 ---
st.set_page_config(layout="wide")
st.title("✈️ 旅游订单管理系统 (飞书版)")

# --- 初始化 Session State ---
init_session_state()

# --- 加载数据 (放在这里，确保 secrets 可用) ---
init_app_data() # 从飞书加载数据

# --- 页面选择 ---
page = st.sidebar.radio("选择页面", ["新建订单", "数据统计与管理"])

# --- 侧边栏刷新按钮 ---
if st.sidebar.button("🔄 从飞书重新加载"):
    st.session_state.feishu_data_loaded = False # 重置加载标志
    # 清理状态的代码保留
    keys_to_clear = [k for k in st.session_state.keys() if k.startswith('new_') or k.startswith('edit_') or k.startswith('partner_') or k == 'select_order_to_edit' or k.endswith('_flag')]
    for key in keys_to_clear:
        try: del st.session_state[key]
        except KeyError: pass
    # 清理 order 相关状态，但不完全重置 session state
    if 'orders' in st.session_state: del st.session_state['orders']
    if 'order_id_counter' in st.session_state: del st.session_state['order_id_counter']
    init_session_state() # 重新初始化部分状态
    st.rerun()

st.sidebar.markdown("---")
st.sidebar.caption(f"当前订单数: {len(st.session_state.get('orders', []))}")
# 显示 Token 获取状态 (调试用，可移除)
# token_status = "有效" if _tenant_access_token and time.time() < (_token_expires_at - 300) else "无效或未获取"
# st.sidebar.caption(f"飞书 Token 状态: {token_status}")

# =========================================
# ============ 新建订单页面 ============
# =========================================
if page == "新建订单":
    st.header("📝 新建旅游订单")

    # --- 清理表单 ---
    if st.session_state.get('clear_new_order_form_flag', False):
        print("正在清理新建订单表单状态...")
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
        for key in keys_to_reset:
            if key in st.session_state:
                 if key == 'new_departure_date': st.session_state[key] = datetime.date.today()
                 else: st.session_state[key] = default_values_for_reset.get(key)
        st.session_state.new_partners = [{'id': 0, 'name': '', 'settlement': 0.0, 'collection': 0.0, 'notes': ''}]
        st.session_state.new_partner_next_id = 1
        st.session_state.clear_new_order_form_flag = False
        print("新建表单清理完成。")

    # --- 布局和预览区 ---
    main_col, preview_col = st.columns([3, 1])
    with preview_col:
        # (预览区代码不变)
        st.subheader("📊 数据预览 (实时)")
        preview_deposit = st.session_state.get('new_deposit', 0.0)
        preview_final_payment = st.session_state.get('new_final_payment', 0.0)
        preview_total_payment = st.session_state.get('new_total_payment', 0.0)
        preview_adult_count = st.session_state.get('new_adult_count', 0)
        preview_adult_price = st.session_state.get('new_adult_price', 0.0)
        preview_child_count = st.session_state.get('new_child_count', 0)
        preview_child_price = st.session_state.get('new_child_price', 0.0)
        current_new_partners = st.session_state.get('new_partners', [])
        preview_partners_valid = [p for p in current_new_partners if isinstance(p, dict)] # 确保是字典列表
        preview_pax_price = calculate_pax_price(preview_adult_count, preview_adult_price, preview_child_count, preview_child_price)
        preview_received_payment = calculate_received_payment(preview_deposit, preview_final_payment)
        preview_total_cost, preview_total_collection = calculate_partner_totals(preview_partners_valid)
        preview_profit = preview_total_payment - preview_total_cost # 利润基于应收总额
        st.metric("人数总价", f"¥ {preview_pax_price:,.2f}")
        st.metric("应收总额", f"¥ {preview_total_payment:,.2f}")
        st.metric("已收金额 (定金+尾款)", f"¥ {preview_received_payment:,.2f}")
        st.metric("总成本(结算)", f"¥ {preview_total_cost:,.2f}")
        st.metric("利润", f"¥ {preview_profit:,.2f}")
        st.metric("总代收", f"¥ {preview_total_collection:,.2f}")
        st.caption("此预览根据当前输入实时更新")

    with main_col:
        # --- 主要信息输入 (无 st.form - 代码不变) ---
        st.subheader("👤 客户信息")
        st.text_input("客户姓名 *", value=st.session_state.get('new_customer_name',''), key="new_customer_name")
        st.text_input("联系电话 *", value=st.session_state.get('new_customer_phone',''), key="new_customer_phone")
        st.date_input("出发日期", value=st.session_state.get('new_departure_date', datetime.date.today()), key="new_departure_date", min_value=datetime.date(2020, 1, 1))
        st.text_area("客户资料备注", value=st.session_state.get('new_customer_notes',''), key="new_customer_notes")
        st.divider()
        st.subheader("💰 支付信息")
        st.multiselect("客人支付方式", PAYMENT_METHODS_OPTIONS, default=st.session_state.get('new_payment_methods',[]), key="new_payment_methods")
        col_pay1, col_pay2, col_pay3 = st.columns(3)
        with col_pay1: st.number_input("定金金额", min_value=0.0, step=100.0, format="%.2f", value=st.session_state.get('new_deposit',0.0), key="new_deposit")
        with col_pay2: st.number_input("尾款金额", min_value=0.0, step=100.0, format="%.2f", value=st.session_state.get('new_final_payment',0.0), key="new_final_payment")
        with col_pay3: st.number_input("总款金额 *", min_value=0.0, step=100.0, format="%.2f", value=st.session_state.get('new_total_payment',0.0), key="new_total_payment", help="订单的合同总金额")
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

        # --- 合作伙伴管理 (代码不变) ---
        st.subheader("🤝 成本核算 (合作伙伴)")
        st.caption("在此处添加或删除合作伙伴信息。结算=给伙伴的钱, 代收=伙伴代收游客的钱。")
        if 'new_partners' not in st.session_state or not isinstance(st.session_state.new_partners, list):
             st.session_state.new_partners = [{'id': 0, 'name': '', 'settlement': 0.0, 'collection': 0.0, 'notes': ''}]
             st.session_state.new_partner_next_id = 1
        partners_to_render = st.session_state.new_partners
        if not partners_to_render: pass
        else:
            for i, partner_state in enumerate(partners_to_render):
                if isinstance(partner_state, dict) and 'id' in partner_state:
                    partner_id = partner_state.get('id')
                    base_key = f"partner_{partner_id}_new_{i}"
                    cols = st.columns([4, 2, 2, 3, 1])
                    try:
                        # 使用 get 获取值，提供默认值，防止 Key Error
                        current_name = partner_state.get('name', '')
                        current_settlement = float(partner_state.get('settlement', 0.0))
                        current_collection = float(partner_state.get('collection', 0.0))
                        current_notes = partner_state.get('notes', '')

                        # 渲染并更新 session state
                        st.session_state.new_partners[i]['name'] = cols[0].text_input(f"名称 #{partner_id}", value=current_name, key=f"{base_key}_name")
                        st.session_state.new_partners[i]['settlement'] = cols[1].number_input(f"结算 #{partner_id}", value=current_settlement, min_value=0.0, format="%.2f", step=100.0, key=f"{base_key}_settlement")
                        st.session_state.new_partners[i]['collection'] = cols[2].number_input(f"代收 #{partner_id}", value=current_collection, min_value=0.0, format="%.2f", step=100.0, key=f"{base_key}_collection")
                        st.session_state.new_partners[i]['notes'] = cols[3].text_area(f"备注 #{partner_id}", value=current_notes, key=f"{base_key}_notes", height=50) # 调整高度
                        cols[4].button("❌", key=f"{base_key}_remove", on_click=remove_partner_callback, args=('new_partners', partner_id), help="删除此合作伙伴")
                    except Exception as render_e:
                        st.warning(f"渲染合作伙伴 (ID: {partner_id}, Index: {i}) 时出错: {render_e}")
                else:
                     st.warning(f"检测到无效或格式错误的合作伙伴条目，索引: {i}，内容: {partner_state}")

        st.button("➕ 添加合作伙伴", on_click=add_partner_callback, args=('new_partners',), key="add_partner_new")
        st.divider()

        # --- 保存按钮 ---
        save_button_clicked = st.button("💾 保存订单到飞书", key="save_new_order", type="primary")

        # --- 保存逻辑 ---
        if save_button_clicked and not st.session_state.get('submit_lock', False):
            st.session_state.submit_lock = True

            # --- 获取输入值 (代码不变) ---
            customer_name = st.session_state.get('new_customer_name', '')
            customer_phone = st.session_state.get('new_customer_phone', '')
            departure_date = st.session_state.get('new_departure_date', None) # date 对象
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

            # --- 基本验证 (代码不变) ---
            errors = []
            if not customer_name: errors.append("客户姓名不能为空！")
            if not customer_phone: errors.append("联系电话不能为空！")
            if not departure_date: errors.append("出发日期不能为空！")
            if total_payment_amount <= 0: errors.append("总款金额必须大于 0！") # 确保总款大于0

            if errors:
                for error in errors: st.error(error)
                st.session_state.submit_lock = False
            else:
                # --- 整合数据 (代码不变) ---
                final_partners_data = []
                for p in partners_state_raw:
                    if isinstance(p, dict) and p.get('name','').strip():
                        partner_data = p.copy(); partner_data.pop('id', None)
                        try: partner_data['settlement'] = float(partner_data.get('settlement', 0.0))
                        except (ValueError, TypeError): partner_data['settlement'] = 0.0
                        try: partner_data['collection'] = float(partner_data.get('collection', 0.0))
                        except (ValueError, TypeError): partner_data['collection'] = 0.0
                        final_partners_data.append(partner_data)
                lines_list = [line.strip() for line in lines_text.split('\n') if line.strip()]
                final_pax_price = calculate_pax_price(adult_count, adult_price, child_count, child_price)
                final_total_cost, final_total_collection = calculate_partner_totals(final_partners_data)
                final_profit = total_payment_amount - final_total_cost
                final_total_revenue_field = total_payment_amount

                # --- 生成新订单 ID ---
                # 确保 order_id_counter 是最新的
                current_ids = [int(o.get('id', 0)) for o in st.session_state.orders if isinstance(o.get('id'), (int, float, str)) and str(o.get('id')).isdigit()]
                max_current_id = max(current_ids) if current_ids else 0
                st.session_state.order_id_counter = max(st.session_state.order_id_counter, max_current_id + 1)
                local_id = st.session_state.order_id_counter

                print(f"准备创建新订单，使用的 ID: {local_id}")

                new_order_data = {
                    "id": local_id,
                    "customer_name": customer_name, "customer_phone": customer_phone,
                    "departure_date": departure_date.strftime("%Y-%m-%d") if departure_date else "",
                    "customer_notes": customer_notes, "payment_methods": payment_methods,
                    "deposit_amount": deposit_amount, "final_payment_amount": final_payment_amount,
                    "total_payment_amount": total_payment_amount, "lines": lines_list,
                    "adult_count": adult_count, "child_count": child_count,
                    "adult_price": adult_price, "child_price": child_price,
                    "total_pax_price": final_pax_price, "partners": final_partners_data,
                    "total_revenue": final_total_revenue_field, "total_cost": final_total_cost,
                    "profit": final_profit, "total_collection": final_total_collection,
                    "created_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "updated_at": ""
                }

                # --- 保存到 Session State 和 飞书 ---
                st.session_state.orders.append(new_order_data) # 先加到本地列表

                # --- 获取最新配置用于保存 ---
                app_id_save = st.secrets.get("FEISHU_APP_ID")
                app_secret_save = st.secrets.get("FEISHU_APP_SECRET")
                spreadsheet_token_save = st.secrets.get("FEISHU_SPREADSHEET_TOKEN")
                sheet_id_or_name_save = st.secrets.get("FEISHU_SHEET_ID_OR_NAME", "Sheet1")

                if not all([app_id_save, app_secret_save, spreadsheet_token_save]):
                     st.error("飞书配置不完整，无法保存订单！请检查 Secrets。")
                     st.session_state.orders.pop() # 从本地列表移除
                     st.session_state.submit_lock = False
                else:
                    with st.spinner("正在保存订单到飞书..."):
                        save_success = save_data_to_feishu(
                            st.session_state.orders,
                            spreadsheet_token_save,
                            sheet_id_or_name_save,
                            app_id_save,
                            app_secret_save
                        )

                    if save_success:
                        st.session_state.order_id_counter += 1 # ID 增加
                        st.session_state.clear_new_order_form_flag = True
                        print(f"订单 {local_id} 保存成功，下一个 ID: {st.session_state.order_id_counter}。设置清理标记。")
                        st.success(f"🎉 订单 (ID: {local_id}) 已保存到飞书表格！")
                        st.balloons()
                        time.sleep(1)
                        st.session_state.submit_lock = False
                        st.rerun()
                    else:
                        st.error("保存订单到飞书表格失败！请检查网络、权限或飞书表格状态。")
                        # 从 session state 移除刚添加的数据，避免本地与远程不一致
                        st.session_state.orders.pop()
                        print(f"订单 {local_id} 保存到飞书失败，已从本地列表移除。")
                        st.session_state.submit_lock = False # 解锁

# =========================================
# ======== 数据统计与管理页面 ========
# =========================================
elif page == "数据统计与管理":
    st.header("📊 数据统计与管理")

    # --- 检查订单数据是否存在 ---
    if not st.session_state.get('orders', []):
        st.warning("当前没有订单数据。请尝试从飞书重新加载或新建订单。")
        # 尝试再次加载数据，以防首次加载失败
        if not st.session_state.get('feishu_data_loaded', False):
             st.warning("将尝试重新从飞书加载数据...")
             init_app_data() # 再次尝试加载
             if not st.session_state.get('orders', []):
                  st.stop() # 如果再次加载还是没有，则停止
             else:
                  st.rerun() # 加载成功则刷新页面
        else:
             st.stop() # 如果已标记加载过但仍为空，则停止

    # --- 过滤有效订单并创建 DataFrame ---
    valid_orders = [o for o in st.session_state.orders if isinstance(o, dict) and 'id' in o]
    if not valid_orders:
        st.info("没有有效的订单数据可供显示。")
        st.stop()

    try:
        # 使用深拷贝创建 DataFrame，避免修改原始 session state
        df_orders = pd.DataFrame(deepcopy(valid_orders))
        # 处理数据类型 (确保关键列是数字)
        essential_numeric = ['id', 'deposit_amount', 'final_payment_amount', 'total_payment_amount', 'adult_count', 'child_count', 'adult_price', 'child_price', 'total_pax_price', 'total_revenue', 'total_cost', 'profit', 'total_collection']
        for col in essential_numeric:
            if col not in df_orders.columns: df_orders[col] = 0
            # 转换为数字，无法转换的填 0
            df_orders[col] = pd.to_numeric(df_orders[col], errors='coerce').fillna(0)
            # 特别处理 ID，确保是整数
            if col == 'id': df_orders[col] = df_orders[col].astype(int)

        # 处理日期列为字符串（如果需要显示）
        if 'departure_date' in df_orders.columns:
            df_orders['departure_date'] = df_orders['departure_date'].astype(str)
        if 'created_at' in df_orders.columns:
            df_orders['created_at'] = df_orders['created_at'].astype(str)
        if 'updated_at' in df_orders.columns:
            df_orders['updated_at'] = df_orders['updated_at'].astype(str)

    except Exception as df_e:
        st.error(f"创建数据表格时出错: {df_e}")
        print(traceback.format_exc())
        st.stop()

    # --- 数据概览 (代码不变) ---
    st.subheader("📈 数据概览")
    try:
        total_revenue_all = df_orders['total_revenue'].sum() # 应收总额
        total_cost_all = df_orders['total_cost'].sum()
        total_profit_all = df_orders['profit'].sum()
        total_orders_count = len(df_orders)
        total_received_all = (df_orders['deposit_amount'] + df_orders['final_payment_amount']).sum()
        col1, col2, col3, col4, col5 = st.columns(5)
        col1.metric("总订单数", total_orders_count)
        col2.metric("总应收额", f"¥ {total_revenue_all:,.2f}")
        col3.metric("总已收款", f"¥ {total_received_all:,.2f}")
        col4.metric("总成本", f"¥ {total_cost_all:,.2f}")
        col5.metric("总利润", f"¥ {total_profit_all:,.2f}")
    except KeyError as e: st.error(f"计算数据概览时出错：缺少列 {e}。请检查飞书表格表头是否正确。")
    except Exception as e: st.error(f"计算数据概览时发生未知错误: {e}"); print(traceback.format_exc())
    st.divider()

    # --- 搜索与列表 (代码不变) ---
    st.subheader("🔍 搜索与列表")
    search_term = st.text_input("输入关键词搜索 (姓名、电话、线路、伙伴名等)", key="search_term_manage")
    df_display = df_orders.copy()
    if search_term:
        search_term_lower = search_term.lower()
        mask = pd.Series([False] * len(df_display))
        str_cols_to_search = ['customer_name', 'customer_phone', 'customer_notes', 'id'] # 加入 ID 搜索
        for col in str_cols_to_search:
            if col in df_display.columns:
                mask |= df_display[col].astype(str).str.lower().str.contains(search_term_lower, na=False)
        if 'lines' in df_display.columns:
             mask |= df_display['lines'].apply(lambda lines: isinstance(lines, list) and any(search_term_lower in str(line).lower() for line in lines))
        if 'partners' in df_display.columns:
             mask |= df_display['partners'].apply(lambda partners: isinstance(partners, list) and any(search_term_lower in str(p.get('name','')).lower() for p in partners if isinstance(p, dict)))
        df_display = df_display[mask]
        st.write(f"找到 {len(df_display)} 条相关订单：")
    else:
        st.write("所有订单列表：")

    # --- 格式化显示 (代码不变) ---
    if not df_display.empty:
        df_display_formatted = df_display.copy()
        money_cols = ['deposit_amount', 'final_payment_amount', 'total_payment_amount', 'adult_price', 'child_price', 'total_pax_price', 'total_revenue', 'total_cost', 'profit', 'total_collection']
        for col in money_cols:
              if col in df_display_formatted.columns:
                  df_display_formatted[col] = pd.to_numeric(df_display_formatted[col], errors='coerce').fillna(0).apply(lambda x: f'¥ {x:,.2f}')
        list_cols_to_format = ['lines', 'partners', 'payment_methods']
        for col in list_cols_to_format:
             if col in df_display_formatted.columns:
                 if col == 'partners':
                      df_display_formatted[col] = df_display_formatted[col].apply(lambda x: ', '.join([str(p.get('name', '')) for p in x if isinstance(p, dict) and p.get('name')]) if isinstance(x, list) else '')
                 else:
                      df_display_formatted[col] = df_display_formatted[col].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else '')
        column_mapping = {
            "id": "订单ID", "customer_name": "客户姓名", "customer_phone": "联系电话",
            "departure_date": "出发日期", "lines": "旅游线路", "partners": "合作伙伴",
            "total_payment_amount": "总款金额", "total_cost": "总成本", "profit": "利润",
            "total_collection": "总代收", "created_at": "创建时间", "updated_at": "更新时间",
            "payment_methods": "支付方式", "deposit_amount": "定金", "final_payment_amount": "尾款",
            "adult_count": "成人", "child_count": "儿童", "adult_price": "成人价",
            "child_price": "儿童价", "total_pax_price": "人数总价", "customer_notes": "客户备注",
            "total_revenue": "总应收额"
        }
        cols_to_show_ideal = ['id', 'customer_name', 'customer_phone', 'departure_date', 'lines', 'partners', 'total_payment_amount', 'total_cost', 'profit', 'total_collection', 'created_at', 'updated_at']
        cols_to_show_actual_english = [c for c in cols_to_show_ideal if c in df_display_formatted.columns]
        df_for_display = df_display_formatted[cols_to_show_actual_english]
        df_for_display = df_for_display.rename(columns=column_mapping)
        # 按 ID 降序显示最新订单在前面
        st.dataframe(df_for_display.sort_values(by="订单ID", ascending=False), use_container_width=True, hide_index=True)
    else:
        if search_term: st.info("没有找到匹配搜索条件的订单。")

    st.divider()

    # --- 修改和删除订单 ---
    st.subheader("✏️ 修改或删除订单")
    if not df_orders.empty:
        if st.session_state.get('clear_edit_selection_flag', False):
            print("重置编辑下拉框...")
            if 'select_order_to_edit' in st.session_state:
                st.session_state.select_order_to_edit = None
            st.session_state.clear_edit_selection_flag = False
            print("下拉框状态已重置。")

        # --- 选择订单下拉框 (使用原始 df_orders 生成选项，因为 df_display 可能被过滤) ---
        all_order_ids = sorted(df_orders['id'].unique(), reverse=True) # 按 ID 降序排列
        order_options_dict = {order_id: f"ID: {order_id} - {df_orders.loc[df_orders['id'] == order_id, 'customer_name'].iloc[0]}" for order_id in all_order_ids}

        options_list_select = [None] + all_order_ids
        format_func_select = lambda x: order_options_dict.get(x, "请选择...") if x is not None else "选择一个订单进行操作..."

        selected_local_id = st.selectbox(
            "选择要操作的订单", options=options_list_select, format_func=format_func_select,
            index=0, key="select_order_to_edit"
        )

        if selected_local_id is not None:
            # 在 session_state.orders 中查找索引
            order_index = next((i for i, order in enumerate(st.session_state.orders) if isinstance(order, dict) and order.get('id') == selected_local_id), None)

            if order_index is not None:
                selected_order_original = deepcopy(st.session_state.orders[order_index])

                # --- 编辑区域 ---
                with st.expander(f"修改订单 (ID: {selected_local_id})", expanded=False):
                    edit_partners_state_key = f'edit_partners_{selected_local_id}'
                    st.subheader("🤝 成本核算 (合作伙伴 - 编辑)")
                    # (合作伙伴编辑逻辑不变)
                    if edit_partners_state_key not in st.session_state:
                       initial_partners_raw = selected_order_original.get('partners', [])
                       initial_partners = deepcopy([p for p in initial_partners_raw if isinstance(p, dict)])
                       partner_id_counter = 0
                       for p in initial_partners: p['id'] = partner_id_counter; partner_id_counter += 1
                       st.session_state[edit_partners_state_key] = initial_partners
                       st.session_state[edit_partners_state_key + '_next_id'] = partner_id_counter
                    if isinstance(st.session_state.get(edit_partners_state_key), list):
                        partners_to_edit = st.session_state[edit_partners_state_key]
                        if not partners_to_edit: pass
                        else:
                            for i, partner_state in enumerate(partners_to_edit):
                                if isinstance(partner_state, dict) and 'id' in partner_state:
                                    partner_id = partner_state.get('id')
                                    base_key_edit = f"partner_{partner_id}_edit_{selected_local_id}_{i}"
                                    cols_edit = st.columns([4, 2, 2, 3, 1])
                                    try:
                                        st.session_state[edit_partners_state_key][i]['name'] = cols_edit[0].text_input(f"伙伴名 #{partner_id}", value=partner_state.get('name', ''), key=f"{base_key_edit}_name")
                                        st.session_state[edit_partners_state_key][i]['settlement'] = cols_edit[1].number_input(f"结算 #{partner_id}", value=float(partner_state.get('settlement', 0.0)), min_value=0.0, format="%.2f", step=100.0, key=f"{base_key_edit}_settlement")
                                        st.session_state[edit_partners_state_key][i]['collection'] = cols_edit[2].number_input(f"代收 #{partner_id}", value=float(partner_state.get('collection', 0.0)), min_value=0.0, format="%.2f", step=100.0, key=f"{base_key_edit}_collection")
                                        st.session_state[edit_partners_state_key][i]['notes'] = cols_edit[3].text_area(f"备注 #{partner_id}", value=partner_state.get('notes', ''), key=f"{base_key_edit}_notes", height=50)
                                        cols_edit[4].button("❌", key=f"{base_key_edit}_remove", on_click=remove_partner_callback, args=(edit_partners_state_key, partner_id), help="删除此合作伙伴")
                                    except Exception as render_edit_e: st.warning(f"渲染编辑伙伴 (ID: {partner_id}, Index: {i}) 时出错: {render_edit_e}")
                                else: st.warning(f"检测到无效的编辑伙伴条目，索引: {i}，内容: {partner_state}")
                    st.button("➕ 添加合作伙伴", key=f"edit_add_partner_{selected_local_id}", on_click=add_partner_callback, args=(edit_partners_state_key,))
                    st.divider()

                    # --- 编辑表单主体 ---
                    form_key = f"edit_order_main_form_{selected_local_id}"
                    with st.form(form_key, clear_on_submit=False):
                        # --- 编辑输入框 (基本不变, 确保 key 唯一) ---
                        st.subheader("👤 客户信息 (编辑)")
                        edit_customer_name = st.text_input("客户姓名 *", value=selected_order_original.get('customer_name',''), key=f"edit_name_{selected_local_id}")
                        edit_customer_phone = st.text_input("联系电话 *", value=selected_order_original.get('customer_phone',''), key=f"edit_phone_{selected_local_id}")
                        default_edit_date = None; date_str = selected_order_original.get('departure_date')
                        if date_str:
                            try: default_edit_date = datetime.datetime.strptime(str(date_str), "%Y-%m-%d").date()
                            except ValueError: pass
                        edit_departure_date = st.date_input("出发日期 *", value=default_edit_date, key=f"edit_date_{selected_local_id}", min_value=datetime.date(2020, 1, 1))
                        edit_customer_notes = st.text_area("客户资料备注", value=selected_order_original.get('customer_notes',''), key=f"edit_notes_{selected_local_id}")
                        st.divider()
                        st.subheader("💰 支付信息 (编辑)")
                        default_pay_methods = selected_order_original.get('payment_methods',[])
                        if not isinstance(default_pay_methods, list): default_pay_methods = []
                        edit_payment_methods = st.multiselect("客人支付方式", PAYMENT_METHODS_OPTIONS, default=default_pay_methods, key=f"edit_paymethods_{selected_local_id}")
                        col_pay_edit1, col_pay_edit2, col_pay_edit3 = st.columns(3)
                        edit_deposit_val = float(selected_order_original.get('deposit_amount',0.0))
                        edit_final_val = float(selected_order_original.get('final_payment_amount',0.0))
                        edit_total_val = float(selected_order_original.get('total_payment_amount', 0.0)) # 直接用原始总额
                        with col_pay_edit1: edit_deposit = st.number_input("定金金额", value=edit_deposit_val, min_value=0.0, format="%.2f", step=100.0, key=f"edit_deposit_{selected_local_id}")
                        with col_pay_edit2: edit_final_payment = st.number_input("尾款金额", value=edit_final_val, min_value=0.0, format="%.2f", step=100.0, key=f"edit_final_{selected_local_id}")
                        with col_pay_edit3: edit_total_payment = st.number_input("总款金额 *", value=edit_total_val, min_value=0.0, format="%.2f", step=100.0, key=f"edit_total_payment_{selected_local_id}", help="订单的合同总金额")
                        st.caption(f"计算参考 (定金+尾款): ¥ {calculate_received_payment(edit_deposit, edit_final_payment):,.2f}")
                        st.divider()
                        st.subheader("🗺️ 线路信息 (编辑)")
                        lines_list_orig = selected_order_original.get('lines', [])
                        if not isinstance(lines_list_orig, list): lines_list_orig = []
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

                        # --- 编辑时的预览 (代码不变) ---
                        st.subheader("📊 数据预览 (编辑时估算)")
                        edit_preview_partners_raw = st.session_state.get(edit_partners_state_key, [])
                        edit_preview_partners = [p for p in edit_preview_partners_raw if isinstance(p, dict)]
                        edit_preview_pax_price = calculate_pax_price(edit_adult_count, edit_adult_price, edit_child_count, edit_child_price)
                        edit_preview_received = calculate_received_payment(edit_deposit, edit_final_payment)
                        edit_preview_cost, edit_preview_collection = calculate_partner_totals(edit_preview_partners)
                        edit_preview_total_payment_form = edit_total_payment # 使用编辑框中的总额
                        edit_preview_profit = edit_preview_total_payment_form - edit_preview_cost
                        col_pax_edit, col_rev_edit, col_rec_edit, col_cost_edit, col_prof_edit, col_coll_edit = st.columns(6)
                        col_pax_edit.metric("人数总价", f"¥ {edit_preview_pax_price:,.2f}")
                        col_rev_edit.metric("应收总额", f"¥ {edit_preview_total_payment_form:,.2f}")
                        col_rec_edit.metric("已收金额", f"¥ {edit_preview_received:,.2f}")
                        col_cost_edit.metric("总成本", f"¥ {edit_preview_cost:,.2f}")
                        col_prof_edit.metric("利润", f"¥ {edit_preview_profit:,.2f}")
                        col_coll_edit.metric("总代收", f"¥ {edit_preview_collection:,.2f}")

                        # --- 编辑表单的提交按钮 ---
                        edit_submitted = st.form_submit_button("💾 保存修改到飞书")

                        # --- 编辑保存逻辑 ---
                        if edit_submitted:
                            edit_errors = [] # (验证逻辑不变)
                            if not edit_customer_name: edit_errors.append("客户姓名不能为空！")
                            if not edit_customer_phone: edit_errors.append("联系电话不能为空！")
                            if not edit_departure_date: edit_errors.append("出发日期不能为空！")
                            if edit_total_payment <= 0: edit_errors.append("总款金额必须大于 0！")

                            if edit_errors:
                                for error in edit_errors: st.error(error)
                            else:
                                # --- 整合编辑后的数据 (代码不变) ---
                                final_edit_partners_state_raw = st.session_state.get(edit_partners_state_key, [])
                                edit_final_partners_data = []
                                for p in final_edit_partners_state_raw:
                                     if isinstance(p, dict) and p.get('name','').strip():
                                         partner_data = p.copy(); partner_data.pop('id', None)
                                         try: partner_data['settlement'] = float(partner_data.get('settlement', 0.0))
                                         except (ValueError, TypeError): partner_data['settlement'] = 0.0
                                         try: partner_data['collection'] = float(partner_data.get('collection', 0.0))
                                         except (ValueError, TypeError): partner_data['collection'] = 0.0
                                         edit_final_partners_data.append(partner_data)
                                edit_lines_list = [line.strip() for line in edit_lines_text.split('\n') if line.strip()]
                                edit_final_pax_price = calculate_pax_price(edit_adult_count, edit_adult_price, edit_child_count, edit_child_price)
                                edit_final_total_cost, edit_final_total_collection = calculate_partner_totals(edit_final_partners_data)
                                edit_final_profit = edit_total_payment - edit_final_total_cost
                                edit_final_total_revenue_field = edit_total_payment

                                updated_order_data = { # (创建更新字典不变)
                                    "id": selected_local_id,
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
                                    "updated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                }
                                # 更新 session state 中的订单
                                st.session_state.orders[order_index] = updated_order_data

                                # --- 保存整个列表到飞书 ---
                                app_id_save = st.secrets.get("FEISHU_APP_ID")
                                app_secret_save = st.secrets.get("FEISHU_APP_SECRET")
                                spreadsheet_token_save = st.secrets.get("FEISHU_SPREADSHEET_TOKEN")
                                sheet_id_or_name_save = st.secrets.get("FEISHU_SHEET_ID_OR_NAME", "Sheet1")

                                if not all([app_id_save, app_secret_save, spreadsheet_token_save]):
                                     st.error("飞书配置不完整，无法保存修改！请检查 Secrets。")
                                     # 可以考虑回滚修改
                                     st.session_state.orders[order_index] = selected_order_original
                                else:
                                     with st.spinner("正在更新订单到飞书..."):
                                         save_success = save_data_to_feishu(
                                             st.session_state.orders,
                                             spreadsheet_token_save,
                                             sheet_id_or_name_save,
                                             app_id_save,
                                             app_secret_save
                                         )

                                     if save_success:
                                         # 清理编辑伙伴状态
                                         if edit_partners_state_key in st.session_state: del st.session_state[edit_partners_state_key]
                                         if edit_partners_state_key + '_next_id' in st.session_state: del st.session_state[edit_partners_state_key + '_next_id']
                                         st.success(f"订单 ID: {selected_local_id} 已更新并保存到飞书表格！")
                                         # 设置清理标记，刷新后取消选择
                                         st.session_state.clear_edit_selection_flag = True
                                         time.sleep(0.5)
                                         st.rerun()
                                     else:
                                         st.error("更新订单到飞书表格失败！请检查网络或权限。")
                                         # 回滚 session state 的修改
                                         st.session_state.orders[order_index] = selected_order_original
                                         st.warning("本地修改已回滚，数据未保存到飞书。")

                # --- 删除功能 ---
                st.divider()
                st.error("--- 删除操作 ---")
                col_del_confirm, col_del_btn = st.columns([3,1])
                # 为 checkbox 和 button 创建唯一的 key
                delete_confirm_key = f"delete_confirm_{selected_local_id}_{order_index}" # 加入 index 保证唯一性
                delete_exec_key = f"delete_exec_{selected_local_id}_{order_index}"

                delete_confirmed = col_del_confirm.checkbox(f"确认删除订单 (ID: {selected_local_id})?", key=delete_confirm_key)

                if delete_confirmed:
                    if col_del_btn.button("🗑️ 执行删除", key=delete_exec_key, type="primary"):
                        try:
                            # 先从 session state 移除
                            deleted_order_for_undo = st.session_state.orders.pop(order_index)
                            print(f"订单 {selected_local_id} 已从本地列表移除，准备同步到飞书...")
                        except IndexError:
                            st.error(f"删除失败：无法在本地列表中找到订单索引 {order_index} (ID: {selected_local_id})。请刷新页面重试。")
                            st.stop()

                        # --- 保存更新后的列表到飞书 ---
                        app_id_save = st.secrets.get("FEISHU_APP_ID")
                        app_secret_save = st.secrets.get("FEISHU_APP_SECRET")
                        spreadsheet_token_save = st.secrets.get("FEISHU_SPREADSHEET_TOKEN")
                        sheet_id_or_name_save = st.secrets.get("FEISHU_SHEET_ID_OR_NAME", "Sheet1")

                        if not all([app_id_save, app_secret_save, spreadsheet_token_save]):
                             st.error("飞书配置不完整，无法同步删除操作！")
                             st.session_state.orders.insert(order_index, deleted_order_for_undo) # 恢复列表
                             st.warning("删除操作未同步到飞书，本地列表已恢复。")
                        else:
                             with st.spinner("正在从飞书删除订单..."):
                                 save_success = save_data_to_feishu(
                                     st.session_state.orders, # 传入已经移除了订单的列表
                                     spreadsheet_token_save,
                                     sheet_id_or_name_save,
                                     app_id_save,
                                     app_secret_save
                                 )

                             if save_success:
                                 st.session_state.clear_edit_selection_flag = True
                                 print(f"订单 {selected_local_id} 已成功从飞书删除。设置清理标记。")
                                 # 清理编辑伙伴状态
                                 edit_partners_state_key = f'edit_partners_{selected_local_id}'
                                 if edit_partners_state_key in st.session_state: del st.session_state[edit_partners_state_key]
                                 if edit_partners_state_key + '_next_id' in st.session_state: del st.session_state[edit_partners_state_key + '_next_id']
                                 st.success(f"订单 ID: {selected_local_id} 已从飞书表格删除！")
                                 time.sleep(0.5)
                                 st.rerun()
                             else:
                                 st.error("从飞书表格删除订单失败！")
                                 st.session_state.orders.insert(order_index, deleted_order_for_undo) # 恢复列表
                                 st.warning("本地列表中的订单已恢复，但飞书表格可能未更新，请检查。")
            else:
                # 这个情况理论上不应该发生
                st.error(f"内部错误：无法在会话状态中找到订单索引 ID: {selected_local_id}。请尝试刷新数据。")
                # 清空选择，避免后续错误
                st.session_state.select_order_to_edit = None
                st.rerun()
    else:
        # 如果 df_orders 为空
        if search_term: st.info("没有找到匹配搜索条件的订单。")
        # else: st.info("当前没有订单数据可供操作。") # 初始加载时可能显示，避免干扰

# --- 页脚或其他信息 ---
st.sidebar.markdown("---")
st.sidebar.info("使用 Streamlit 和 Feishu API 构建")

# --- 文件末尾 ---
