import os
import json
import pandas as pd
from flask import Flask, jsonify
from pytrends.request import TrendReq
import gspread
from gspread_dataframe import set_with_dataframe
import time
import random

app = Flask(__name__)

def fetch_and_write_trends_data():
    print("--- BẮT ĐẦU QUY TRÌNH ---")
    
    # 1. Đọc cấu hình từ Biến Môi trường
    gcp_credentials_str = os.environ.get('GCP_CREDENTIALS')
    spreadsheet_id = os.environ.get('SPREADSHEET_ID')
    nid_cookie_value = os.environ.get('NID_COOKIE') # Đọc giá trị cookie

    if not all([gcp_credentials_str, spreadsheet_id, nid_cookie_value]):
        raise ValueError("Thiếu một trong các biến môi trường: GCP_CREDENTIALS, SPREADSHEET_ID, NID_COOKIE")
    
    gcp_credentials_dict = json.loads(gcp_credentials_str)
    
    # 2. Xác thực Google Sheets
    print("2. Đang xác thực với Google Sheets...")
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    gc = gspread.service_account_from_dict(gcp_credentials_dict, scopes=scopes)
    spreadsheet = gc.open_by_key(spreadsheet_id)
    
    # 3. Lấy từ khóa
    print("3. Đang lấy danh sách từ khóa...")
    keyword_sheet = spreadsheet.worksheet('KEY')
    keywords = [kw[0] for kw in keyword_sheet.get_values('A1:A100') if kw and kw[0]]
    if not keywords:
        raise ValueError("Không có từ khóa nào trong sheet 'KEY' (A1:A100).")
    print(f"   => Tìm thấy {len(keywords)} từ khóa.")

    # 4. Cấu hình pytrends - Áp dụng cách của Replit
    print("4. Đang cấu hình pytrends...")
    
    # TỐI ƯU HÓA: Tạo chuỗi cookie một cách an toàn, giống như Replit
    # Điều này yêu cầu biến môi trường NID_COOKIE chỉ chứa GIÁ TRỊ, không chứa "NID="
    requests_args = {
        'headers': {'Cookie': f'NID={nid_cookie_value}'}
    }
    
    # Khởi tạo pytrends không có timeout, để thư viện tự quản lý
    pytrends = TrendReq(hl='vi-VN', tz=420, requests_args=requests_args)
    
    # 5. Lấy dữ liệu (giữ nguyên logic tạo bảng DỮ LIỆU ĐÚNG)
    print("5. Đang lấy dữ liệu từ Google Trends...")
    all_trends_df = pd.DataFrame()
    for i, kw in enumerate(keywords):
        print(f"   - Đang xử lý: '{kw}' ({i+1}/{len(keywords)})")
        try:
            pytrends.build_payload([kw], cat=0, timeframe='today 3-m', geo='VN', gprop='youtube')
            interest_df = pytrends.interest_over_time()
            if not interest_df.empty and kw in interest_df.columns:
                print(f"     => TÌM THẤY DỮ LIỆU.")
                # Giữ nguyên cách xử lý dữ liệu đúng để Apps Script có thể vẽ biểu đồ
                all_trends_df[kw] = interest_df[kw]
        except Exception as e:
            print(f"     => LỖI với từ khóa '{kw}': {e}")
            # Nếu gặp lỗi 429, có thể tăng thời gian chờ ở đây
            if "429" in str(e):
                print("     => Lỗi 429, tạm nghỉ 30 giây...")
                time.sleep(30)
            continue
        
        # Luôn tạm nghỉ giữa các request
        sleep_time = random.uniform(5, 10)
        print(f"     => Tạm nghỉ {sleep_time:.2f} giây...")
        time.sleep(sleep_time)
            
    if all_trends_df.empty:
        raise ValueError("Không lấy được bất kỳ dữ liệu nào từ Google Trends.")
        
    # 6. Ghi dữ liệu vào Sheet
    print("6. Đang ghi dữ liệu vào 'Trends_Data'...")
    all_trends_df.reset_index(inplace=True)
    all_trends_df.rename(columns={'date': 'Ngày'}, inplace=True)
    all_trends_df['Ngày'] = all_trends_df['Ngày'].dt.strftime('%d/%m/%Y')
    
    data_sheet = spreadsheet.worksheet('Trends_Data')
    data_sheet.clear()
    set_with_dataframe(data_sheet, all_trends_df, include_index=False, resize=True)
    
    print("--- HOÀN TẤT GHI DỮ LIỆU ---")
    return f"Đã cập nhật thành công dữ liệu cho {len(all_trends_df.columns) - 1} từ khóa."


@app.route('/run-process', methods=['POST'])
def handle_run_process():
    try:
        success_message = fetch_and_write_trends_data()
        return jsonify({'status': 'success', 'message': success_message}), 200
    except Exception as e:
        print(f"LỖI TOÀN QUY TRÌNH: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5001))
    app.run(debug=False, host='0.0.0.0', port=port)
