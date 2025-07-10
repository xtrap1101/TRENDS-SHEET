from flask import Flask
import pandas as pd
from pytrends.request import TrendReq
from gspread_dataframe import set_with_dataframe
import time
import gspread
import google.auth
import os

app = Flask(__name__)

# --- CẤU HÌNH ---
SPREADSHEET_ID = '1GX2iZeOgKd-_gcTfNHp3mDR5bteG1zIgRHOHzGq7Uks'
INPUT_SHEET_NAME = 'KEY'
OUTPUT_SHEET_NAME = 'Trends_Data'

# Lấy thông tin xác thực từ Secrets của Replit
try:
    gcp_service_account_credentials = os.environ['GCP_CREDENTIALS']
    with open('gcp_credentials.json', 'w') as f:
        f.write(gcp_service_account_credentials)
except KeyError:
    pass


def run_process():
    print("--- BẮT ĐẦU QUY TRÌNH ---")

    try:
        print("1. Đang xác thực với Google Sheets...")
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        gc = gspread.service_account(filename='gcp_credentials.json',
                                     scopes=scopes)
        spreadsheet = gc.open_by_key(SPREADSHEET_ID)
        print("   => Xác thực thành công!")
    except Exception as e:
        error_message = f"LỖI XÁC THỰC: {type(e).__name__} - {e}"
        print(error_message)
        return error_message

    print(f"2. Đang đọc từ khóa từ sheet '{INPUT_SHEET_NAME}'...")
    input_worksheet = spreadsheet.worksheet(INPUT_SHEET_NAME)
    keywords = input_worksheet.col_values(1)
    keywords = [kw for kw in keywords if kw]
    print(f"   => Tìm thấy {len(keywords)} từ khóa.")

    if not keywords:
        return "Không có từ khóa nào trong sheet 'KEY'"

    # Cải tiến logic xử lý cookie
    print("3. Đang cấu hình pytrends...")
    nid_cookie = os.environ.get('NID_COOKIE')

    # === THAY ĐỔI DUY NHẤT TẠI ĐÂY: XÓA 'timeout' ===
    # Bắt đầu với các tham số cơ bản, không có timeout
    requests_args = {}

    # Chỉ thêm header Cookie nếu NID_COOKIE tồn tại
    if nid_cookie:
        print("   => Đã tìm thấy NID Cookie. Đang sử dụng để xác thực.")
        requests_args['headers'] = {'Cookie': f'NID={nid_cookie}'}
    else:
        print(
            "   => CẢNH BÁO: Không tìm thấy NID_COOKIE. Kết quả có thể không đầy đủ."
        )

    # Khởi tạo pytrends, để thư viện tự quản lý timeout
    pytrends = TrendReq(hl='vi-VN', tz=420, requests_args=requests_args)
    # ===============================================

    list_of_dataframes = []
    found_data_count = 0

    for i, kw in enumerate(keywords):
        print(f"   - Đang xử lý từ khóa {i+1}/{len(keywords)}: '{kw}'")
        try:
            pytrends.build_payload([kw],
                                   cat=0,
                                   timeframe='today 3-m',
                                   geo='VN',
                                   gprop='youtube')
            interest_df = pytrends.interest_over_time()
            if not interest_df.empty and kw in interest_df.columns:
                print(f"     => TÌM THẤY DỮ LIỆU.")
                found_data_count += 1
                interest_df.reset_index(inplace=True)
                if 'isPartial' in interest_df.columns:
                    interest_df = interest_df.drop(columns=['isPartial'])
                interest_df['date'] = interest_df['date'].dt.strftime(
                    '%d/%m/%y')
                interest_df.rename(columns={
                    'date': f'Ngày ({kw})',
                    kw: kw
                },
                                   inplace=True)
                list_of_dataframes.append(interest_df)
            else:
                print(f"     => KHÔNG tìm thấy dữ liệu.")
            time.sleep(1.5)
        except Exception as e:
            print(f"     => LỖI với từ khóa '{kw}': {e}")
            continue

    # Ghi dữ liệu... (phần còn lại giữ nguyên)
    print("4. Đang chuẩn bị ghi dữ liệu...")
    if list_of_dataframes:
        final_df = pd.concat(list_of_dataframes, axis=1)
        print(
            f"   => Bảng dữ liệu cuối cùng có {len(final_df)} hàng và {len(final_df.columns)} cột."
        )

        try:
            print(f"5. Đang ghi vào sheet '{OUTPUT_SHEET_NAME}'...")
            output_worksheet = spreadsheet.worksheet(OUTPUT_SHEET_NAME)
            output_worksheet.clear()
        except gspread.exceptions.WorksheetNotFound:
            print(
                f"   - Sheet '{OUTPUT_SHEET_NAME}' không tồn tại, đang tạo mới..."
            )
            output_worksheet = spreadsheet.add_worksheet(
                title=OUTPUT_SHEET_NAME, rows="100", cols="20")

        set_with_dataframe(output_worksheet,
                           final_df,
                           include_index=False,
                           resize=True)
        print("   => Ghi dữ liệu thành công!")
        return f"Hoàn tất! Đã xử lý {len(keywords)} từ khóa, tìm thấy dữ liệu cho {found_data_count} từ khóa."
    else:
        print("   => Không có dữ liệu nào để ghi.")
        return f"Hoàn tất! Đã xử lý {len(keywords)} từ khóa nhưng không tìm thấy dữ liệu cho bất kỳ từ khóa nào."


@app.route('/')
def main_handler():
    try:
        result = run_process()
        print(f"--- KẾT THÚC QUY TRÌNH. KẾT QUẢ: {result} ---")
        return result
    except Exception as e:
        fatal_error_message = f"LỖI NGHIÊM TRỌNG TRONG HÀM MAIN_HANDLER: {type(e).__name__} - {e}"
        print(fatal_error_message)
        return (fatal_error_message, 500)


def run():
    app.run(host='0.0.0.0', port=8080)


run()
