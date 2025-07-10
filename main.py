import os
import json
import pandas as pd
from flask import Flask, jsonify
from pytrends.request import TrendReq
import gspread
from gspread_dataframe import set_with_dataframe
import time
import random 

# Khởi tạo ứng dụng Flask
app = Flask(__name__)

# --- HÀM LOGIC CỐT LÕI ---
def fetch_and_write_trends_data():
    """
    Hàm này chứa toàn bộ logic xử lý
    """
    print("--- BẮT ĐẦU QUY TRÌNH LẤY DỮ LIỆU ---")

    # 1. Đọc cấu hình
    print("1. Đang đọc cấu hình...")
    gcp_credentials_str = os.environ.get('GCP_CREDENTIALS')
    spreadsheet_id = os.environ.get('SPREADSHEET_ID')
    google_nid_cookie = os.environ.get('GOOGLE_NID_COOKIE')

    if not all([gcp_credentials_str, spreadsheet_id, google_nid_cookie]):
        raise ValueError("Thiếu một trong các biến môi trường: GCP_CREDENTIALS, SPREADSHEET_ID, GOOGLE_NID_COOKIE")
    
    gcp_credentials_dict = json.loads(gcp_credentials_str)
    
    # 2. Xác thực Google Sheets
    print("2. Đang xác thực với Google Sheets...")
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    gc = gspread.service_account_from_dict(gcp_credentials_dict, scopes=scopes)
    spreadsheet = gc.open_by_key(spreadsheet_id)
    
    # 3. Lấy từ khóa (giới hạn 100)
    print("3. Đang lấy danh sách từ khóa...")
    keyword_sheet = spreadsheet.worksheet('KEY')
    keywords = [kw for kw in keyword_sheet.get_values('A1:A100') if kw[0]]
    if not keywords:
        raise ValueError("Không có từ khóa nào trong sheet 'KEY' (A1:A100).")
    keywords = [item[0] for item in keywords]
    print(f"   => Tìm thấy {len(keywords)} từ khóa.")

    # 4. Lấy dữ liệu Google Trends
    print("4. Đang lấy dữ liệu từ Google Trends...")
    requests_args = {'headers': {'Cookie': google_nid_cookie}}
    pytrends = TrendReq(hl='vi-VN', tz=420, requests_args=requests_args)
    
    all_trends_df = pd.DataFrame()
    for i, kw in enumerate(keywords):
        print(f"   - Đang xử lý: '{kw}' ({i+1}/{len(keywords)})")
        try:
            pytrends.build_payload([kw], cat=0, timeframe='today 3-m', geo='VN', gprop='youtube')
            interest_df = pytrends.interest_over_time()
            if not interest_df.empty and kw in interest_df.columns:
                all_trends_df[kw] = interest_df[kw]
        except Exception as e:
            print(f"     => Lỗi với từ khóa '{kw}': {e}")
            # Bỏ qua từ khóa này và tiếp tục
            continue
        
        sleep_time = random.uniform(3, 7)
        print(f"     => Tạm nghỉ {sleep_time:.1f} giây...")
        time.sleep(sleep_time)
            
    if all_trends_df.empty:
        raise ValueError("Không lấy được bất kỳ dữ liệu nào từ Google Trends.")
        
    # 5. Ghi dữ liệu vào Sheet
    print("5. Đang ghi dữ liệu vào 'Trends_Data'...")
    all_trends_df.reset_index(inplace=True)
    all_trends_df.rename(columns={'date': 'Ngày'}, inplace=True)
    all_trends_df['Ngày'] = all_trends_df['Ngày'].dt.strftime('%d/%m/%Y')
    
    data_sheet = spreadsheet.worksheet('Trends_Data')
    data_sheet.clear()
    set_with_dataframe(data_sheet, all_trends_df, include_index=False, resize=True)
    
    print("--- HOÀN TẤT GHI DỮ LIỆU ---")
    return f"Đã cập nhật thành công dữ liệu cho {len(all_trends_df.columns) - 1} từ khóa."


# --- ĐỊNH NGHĨA ENDPOINT ---
@app.route('/run-process', methods=['POST'])
def handle_run_process():
    """
    Hàm này được kích hoạt khi có request POST đến /run-process.
    """
    try:
        success_message = fetch_and_write_trends_data()
        return jsonify({'status': 'success', 'message': success_message}), 200
    except Exception as e:
        print(f"LỖI TOÀN QUY TRÌNH: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5001))
    app.run(debug=False, host='0.0.0.0', port=port)
