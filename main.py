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
import requests
import io

app = Flask(__name__)

# --- CẤU HÌNH ---
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID')
GCP_CREDENTIALS_JSON = os.environ.get('GCP_CREDENTIALS')
NID_COOKIE = os.environ.get('NID_COOKIE')
INPUT_SHEET_NAME = 'KEY'
OUTPUT_SHEET_NAME = 'Trends_Data'
TIMEFRAME = 'today 3-m'
GEO = 'VN'
GPROP = 'youtube'

@app.route('/')
def health_check():
    return "Service is healthy and ready.", 200

@app.route('/run-process-now')
def main_handler():
    print("--- BẮT ĐẦU QUY TRÌNH THEO YÊU CẦU ---")

    # --- Phần xác thực và đọc từ khóa giữ nguyên ---
    if not SPREADSHEET_ID or not GCP_CREDENTIALS_JSON:
        return ("LỖI CẤU HÌNH: Thiếu SPREADSHEET_ID hoặc GCP_CREDENTIALS.", 500)
    try:
        with open('gcp_credentials.json', 'w') as f: f.write(GCP_CREDENTIALS_JSON)
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        gc = gspread.service_account(filename='gcp_credentials.json', scopes=scopes)
        spreadsheet = gc.open_by_key(SPREADSHEET_ID)
    except Exception as e:
        return f"LỖI XÁC THỰC: {type(e).__name__} - {e}"

    input_worksheet = spreadsheet.worksheet(INPUT_SHEET_NAME)
    keywords = [kw for kw in input_worksheet.col_values(1) if kw]
    if not keywords: return "Không có từ khóa nào trong sheet 'KEY'"

    # --- Logic lấy dữ liệu ---
    session = requests.Session()
    if NID_COOKIE:
        session.headers.update({'Cookie': f'NID={NID_COOKIE}'})

    pytrends = TrendReq(hl='vi-VN', tz=420, requests_session=session)

    list_of_dataframes = []
    found_data_count = 0
    for i, kw in enumerate(keywords):
        print(f"   - Đang xử lý từ khóa {i+1}/{len(keywords)}: '{kw}'")
        try:
            pytrends.build_payload([kw], cat=0, timeframe=TIMEFRAME, geo=GEO, gprop=GPROP)
            token = pytrends.interest_over_time_widget['token']
            csv_url = f"https://trends.google.com/trends/api/widgetdata/multirange/csv?req={token}&token={token}&tz=420&hl=vi-VN"
            response = session.get(csv_url, timeout=30)
            response.raise_for_status()

            csv_content = response.text.splitlines()[2:]
            csv_string = "\n".join(csv_content)

            if not csv_string:
                print(f"     => KHÔNG tìm thấy dữ liệu (CSV rỗng).")
                continue

            interest_df = pd.read_csv(io.StringIO(csv_string))

            if not interest_df.empty:
                print(f"     => TÌM THẤY DỮ LIỆU.")
                found_data_count += 1
                interest_df.rename(columns={interest_df.columns[0]: 'date', interest_df.columns[1]: kw}, inplace=True)
                interest_df['date'] = pd.to_datetime(interest_df['date']).dt.strftime('%d/%m/%y')
                interest_df.rename(columns={'date': f'Ngày ({kw})'}, inplace=True)
                list_of_dataframes.append(interest_df)
            else:
                print(f"     => KHÔNG tìm thấy dữ liệu.")

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                print("     => Bị chặn (429). Đang dừng 15 giây...")
                time.sleep(15)
            else:
                print(f"     => LỖI HTTP với từ khóa '{kw}': {e}")
        except Exception as e:
            print(f"     => LỖI khác với từ khóa '{kw}': {e}")

        # === THAY ĐỔI DUY NHẤT TẠI ĐÂY ===
        random_delay = random.uniform(2, 6) # Dừng ngẫu nhiên từ 2 đến 6 giây
        # ==================================
        print(f"     => Tạm dừng {random_delay:.1f} giây...")
        time.sleep(random_delay)

    # --- Phần ghi dữ liệu giữ nguyên ---
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
