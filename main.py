from flask import Flask
import pandas as pd
from pytrends.request import TrendReq
from gspread_dataframe import set_with_dataframe
import time
import gspread
import google.auth
import os
import traceback

# Khởi tạo ứng dụng web
app = Flask(__name__)

# --- CẤU HÌNH ---
# Hãy chắc chắn rằng các biến môi trường này đã được thiết lập trong phần Settings của Render
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID')
GCP_CREDENTIALS_JSON = os.environ.get('GCP_CREDENTIALS')
NID_COOKIE = os.environ.get('NID_COOKIE')

INPUT_SHEET_NAME = 'KEY'
OUTPUT_SHEET_NAME = 'Trends_Data'

def run_process():
    """Hàm này chứa toàn bộ logic và chỉ chạy khi được gọi."""
    print("--- BẮT ĐẦU QUY TRÌNH THEO YÊU CẦU ---")
    
    if not SPREADSHEET_ID or not GCP_CREDENTIALS_JSON:
        return ("LỖI CẤU HÌNH: Thiếu SPREADSHEET_ID hoặc GCP_CREDENTIALS trong biến môi trường.", 500)

    try:
        # Ghi credentials vào file tạm thời để gspread đọc
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
    keywords = input_worksheet.col_values(1)
    keywords = [kw for kw in keywords if kw]
    print(f"   => Tìm thấy {len(keywords)} từ khóa.")

    if not keywords:
        return "Không có từ khóa nào trong sheet 'KEY'"

    # Cấu hình Pytrends với Cookie
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
                if 'isPartial' in interest_df.columns:
                    interest_df = interest_df.drop(columns=['isPartial'])
                interest_df['date'] = interest_df['date'].dt.strftime('%d/%m/%y')
                interest_df.rename(columns={'date': f'Ngày ({kw})', kw: kw}, inplace=True)
                list_of_dataframes.append(interest_df)
            else:
                print(f"     => KHÔNG tìm thấy dữ liệu.")
            time.sleep(1.5)
        except Exception as e:
            print(f"     => LỖI với từ khóa '{kw}': {e}")
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
        return f"Hoàn tất! Đã xử lý {len(keywords)} từ khóa, tìm thấy dữ liệu cho {found_data_count} từ khóa."
    else:
        return f"Hoàn tất! Đã xử lý {len(keywords)} từ khóa nhưng không tìm thấy dữ liệu cho bất kỳ từ khóa nào."

@app.route('/')
def main_handler():
    """Hàm này là cổng vào duy nhất, chỉ được kích hoạt khi có yêu cầu từ Google Sheet."""
    try:
        result = run_process()
        print(f"--- KẾT THÚC QUY TRÌNH. KẾT QUẢ: {result} ---")
        return result
    except Exception as e:
        fatal_error_message = f"LỖI NGHIÊM TRỌNG TRONG HÀM MAIN_HANDLER: {type(e).__name__} - {e}"
        print(fatal_error_message)
        traceback.print_exc()
        return (fatal_error_message, 500)

# Đoạn mã này sẽ chỉ chạy khi Render thực thi lệnh "python main.py"
if __name__ == "__main__":
    # Render cung cấp cổng (PORT) qua một biến môi trường
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)
# ĐẤNG TỐI CAO SOI SÁNG ĐOẠN CODE AI NÀY HUHU
