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
# Lấy thông tin từ biến môi trường của Render
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID')
GCP_CREDENTIALS_JSON = os.environ.get('GCP_CREDENTIALS')
NID_COOKIE = os.environ.get('NID_COOKIE')

INPUT_SHEET_NAME = 'KEY'
OUTPUT_SHEET_NAME = 'Trends_Data'

@app.route('/')
def main_handler():
    """
    Hàm này là cổng vào duy nhất. Nó sẽ được kích hoạt khi Google Sheet
    gửi yêu cầu đến URL của Render.
    """
    print("--- BẮT ĐẦU QUY TRÌNH THEO YÊU CẦU ---")
    
    # Kiểm tra cấu hình biến môi trường
    if not SPREADSHEET_ID or not GCP_CREDENTIALS_JSON:
        error_msg = "LỖI CẤU HÌNH: Thiếu SPREADSHEET_ID hoặc GCP_CREDENTIALS trong biến môi trường."
        print(error_msg)
        return (error_msg, 500)

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
        print("   => Đã tìm thấy NID Cookie. Đang sử dụng để xác thực.")
        requests_args['headers'] = {'Cookie': f'NID={NID_COOKIE}'}
    else:
        print("   => CẢNH BÁO: Không tìm thấy NID_COOKIE. Kết quả có thể không đầy đủ.")
    
    pytrends = TrendReq(hl='vi-VN', tz=420, requests_args=requests_args)

    # Lấy và xử lý dữ liệu
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

    # Ghi dữ liệu vào Sheet
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
# ĐẤNG TỐI CAO CHIẾU SÁNG ĐOẠN CODE AI NÀY. CỨUUUUUUU
    return result_message
