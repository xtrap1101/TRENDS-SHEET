from flask import Flask
import pandas as pd
from pytrends.request import TrendReq
from gspread_dataframe import set_with_dataframe
import time
import gspread
import google.auth
import os
import traceback
import random

# Khởi tạo ứng dụng web
app = Flask(__name__)

# --- CẤU HÌNH ---
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID')
GCP_CREDENTIALS_JSON = os.environ.get('GCP_CREDENTIALS')
NID_COOKIE = os.environ.get('NID_COOKIE')
INPUT_SHEET_NAME = 'KEY'
OUTPUT_SHEET_NAME = 'Trends_Data'

@app.route('/')
def health_check():
    """Cổng vào mặc định cho Render kiểm tra."""
    return "Service is healthy.", 200

@app.route('/run-process-now')
def main_handler():
    """Hàm này được kích hoạt khi có yêu cầu từ Google Sheet."""
    print("--- BẮT ĐẦU QUY TRÌNH THEO YÊU CẦU ---")

    if not SPREADSHEET_ID or not GCP_CREDENTIALS_JSON:
        return ("LỖI CẤU HÌNH: Thiếu SPREADSHEET_ID hoặc GCP_CREDENTIALS.", 500)
    try:
        with open('gcp_credentials.json', 'w') as f:
            f.write(GCP_CREDENTIALS_JSON)

        print("1. Đang xác thực với Google Sheets...")
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        gc = gspread.service_account(filename='gcp_credentials.json', scopes=scopes)
        spreadsheet = gc.open_by_key(SPREADSHEET_ID)
        print("   => Xác thực thành công!")
    except Exception as e:
        error_message = f"LỖI XÁC THỰC: {type(e).__name__} - {e}"
        print(error_message)
        traceback.print_exc()
        return error_message

    print(f"2. Đang đọc từ khóa từ sheet '{INPUT_SHEET_NAME}'...")
    input_worksheet = spreadsheet.worksheet(INPUT_SHEET_NAME)
    keywords = [kw for kw in input_worksheet.col_values(1) if kw]
    print(f"   => Tìm thấy {len(keywords)} từ khóa.")
    if not keywords: return "Không có từ khóa nào trong sheet 'KEY'"

    print("3. Đang cấu hình pytrends...")
    requests_args = {}
    if NID_COOKIE:
        print("   => Đã tìm thấy NID Cookie.")
        requests_args['headers'] = {'Cookie': f'NID={NID_COOKIE}'}
    else:
        print("   => CẢNH BÁO: Không tìm thấy NID_COOKIE.")
    pytrends = TrendReq(hl='vi-VN', tz=420, requests_args=requests_args)

    list_of_dataframes = []
    found_data_count = 0
    for i, kw in enumerate(keywords):
        print(f"   - Đang xử lý từ khóa {i+1}/{len(keywords)}: '{kw}'")
        try:
            pytrends.build_payload([kw], cat=0, timeframe='today 3-m', geo='VN', gprop='youtube')
            interest_df = pytrends.interest_over_time()
            if not interest_df.empty and kw in interest_df.columns:
                print(f"     => TÌM THẤY DỮ LIỆU.")
                found_data_count += 1
                interest_df.reset_index(inplace=True)
                if 'isPartial' in interest_df.columns: interest_df = interest_df.drop(columns=['isPartial'])
                interest_df['date'] = interest_df['date'].dt.strftime('%d/%m/%y')
                interest_df.rename(columns={'date': f'Ngày ({kw})', kw: kw}, inplace=True)
                list_of_dataframes.append(interest_df)
            else:
                print(f"     => KHÔNG tìm thấy dữ liệu.")

            random_delay = random.uniform(4, 8)
            print(f"     => Tạm dừng {random_delay:.1f} giây để tránh bị chặn...")
            time.sleep(random_delay)

        except Exception as e:
            print(f"     => LỖI CHI TIẾT với từ khóa '{kw}':")
            if hasattr(e, 'response'):
                print(f"        - Status Code: {e.response.status_code}")
                print(f"        - Reason: {e.response.reason}")
                print(f"        - Response Text: {e.response.text[:200]}")
            else:
                print(f"        - Lỗi không có phản hồi HTTP: {e}")

            if '429' in str(e):
                print("     => Bị chặn (429). Đang dừng 15 giây...")
                time.sleep(15)
            continue

    print("4. Đang chuẩn bị ghi dữ liệu...")
    if list_of_dataframes:
        final_df = pd.concat(list_of_dataframes, axis=1)
        try:
            output_worksheet = spreadsheet.worksheet(OUTPUT_SHEET_NAME)
            output_worksheet.clear()
        except gspread.exceptions.WorksheetNotFound:
            output_worksheet = spreadsheet.add_worksheet(title=OUTPUT_SHEET_NAME, rows="100", cols="20")
        set_with_dataframe(output_worksheet, final_df, include_index=False, resize=True)
        result_message = f"Hoàn tất! Đã xử lý {len(keywords)} từ khóa, tìm thấy dữ liệu cho {found_data_count} từ khóa."
    else:
        result_message = f"Hoàn tất! Đã xử lý {len(keywords)} từ khóa nhưng không tìm thấy dữ liệu cho bất kỳ từ khóa nào."

    print(f"--- KẾT THÚC QUY TRÌNH. KẾT QUẢ: {result_message} ---")
    return result_message

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)
