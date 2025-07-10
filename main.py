import pandas as pd
from pytrends.request import TrendReq
from gspread_dataframe import set_with_dataframe
import time
import gspread
import os
import json

def run_process():
    """
    Hàm chính để thực hiện toàn bộ quá trình:
    1. Lấy thông tin cấu hình từ Biến Môi trường.
    2. Xác thực với Google Sheets.
    3. Đọc danh sách từ khóa.
    4. Dùng pytrends để lấy dữ liệu với cookie.
    5. Ghi dữ liệu vào sheet.
    """
    print("--- BẮT ĐẦU QUY TRÌNH TỰ ĐỘNG ---")

    # 1. Lấy thông tin cấu hình từ Biến Môi trường
    print("1. Đang đọc cấu hình từ Biến Môi trường...")
    try:
        # Lấy nội dung file JSON từ biến môi trường
        gcp_credentials_str = os.environ.get('GCP_CREDENTIALS')
        if not gcp_credentials_str:
            raise ValueError("Biến môi trường GCP_CREDENTIALS không được thiết lập.")
        gcp_credentials_dict = json.loads(gcp_credentials_str)

        spreadsheet_id = os.environ.get('SPREADSHEET_ID')
        if not spreadsheet_id:
            raise ValueError("Biến môi trường SPREADSHEET_ID không được thiết lập.")

        google_nid_cookie = os.environ.get('GOOGLE_NID_COOKIE')
        if not google_nid_cookie:
            raise ValueError("Biến môi trường GOOGLE_NID_COOKIE không được thiết lập.")

        print("   => Đọc cấu hình thành công!")
    except Exception as e:
        print(f"LỖI CẤU HÌNH: {e}")
        return

    # 2. Xác thực với Google Sheets
    try:
        print("2. Đang xác thực với Google Sheets...")
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        # Xác thực bằng dictionary thay vì file
        gc = gspread.service_account_from_dict(gcp_credentials_dict, scopes=scopes)
        spreadsheet = gc.open_by_key(spreadsheet_id)
        print("   => Xác thực thành công!")
    except Exception as e:
        print(f"LỖI XÁC THỰC GOOGLE SHEETS: {e}")
        return

    # 3. Đọc từ khóa
    print("3. Đang đọc từ khóa...")
    input_worksheet = spreadsheet.worksheet('KEY')
    keywords = [kw for kw in input_worksheet.col_values(1) if kw]
    print(f"   => Tìm thấy {len(keywords)} từ khóa.")
    if not keywords:
        print("Không có từ khóa nào. Kết thúc.")
        return

    # 4. Lấy dữ liệu từ Google Trends (sử dụng Cookie)
    print("4. Đang cấu hình pytrends với cookie...")
    # Thêm cookie vào header của request để tránh bị chặn
    requests_args = {
        'headers': {
            'Cookie': google_nid_cookie
        }
    }
    pytrends = TrendReq(hl='vi-VN', tz=420, requests_args=requests_args)

    all_trends_df = pd.DataFrame()
    for i, kw in enumerate(keywords):
        print(f"   - Đang xử lý từ khóa {i+1}/{len(keywords)}: '{kw}'")
        try:
            pytrends.build_payload(
                [kw], cat=0, timeframe='today 3-m', geo='VN', gprop='youtube'
            )
            interest_df = pytrends.interest_over_time()
            if not interest_df.empty:
                # Chỉ lấy cột dữ liệu của từ khóa và loại bỏ cột 'isPartial'
                all_trends_df[kw] = interest_df[kw]
            else:
                print(f"     => KHÔNG tìm thấy dữ liệu.")
            time.sleep(2)  # Vẫn nên có một khoảng nghỉ nhỏ
        except Exception as e:
            print(f"     => LỖI với từ khóa '{kw}': {e}")
            # Nếu có lỗi, tạo một cột rỗng để không làm hỏng cấu trúc
            all_trends_df[kw] = None
            continue

    # 5. Ghi kết quả vào Google Sheet
    if not all_trends_df.empty:
        print("5. Đang chuẩn bị và ghi dữ liệu...")
        # Đặt lại index để cột 'date' trở thành cột 'Ngày'
        all_trends_df.reset_index(inplace=True)
        all_trends_df.rename(columns={'date': 'Ngày'}, inplace=True)
        
        try:
            output_worksheet = spreadsheet.worksheet('Trends_Data')
            output_worksheet.clear()
            set_with_dataframe(output_worksheet, all_trends_df, include_index=False, resize=True)
            print("   => Ghi dữ liệu thành công!")
        except gspread.exceptions.WorksheetNotFound:
            print("Sheet 'Trends_Data' không tồn tại. Vui lòng tạo sheet này trước khi chạy.")
    else:
        print("Không có dữ liệu nào để ghi.")

    print("--- HOÀN TẤT ---")

if __name__ == '__main__':
    run_process()
