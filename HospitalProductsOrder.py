import time
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
import warnings

warnings.filterwarnings('ignore')


class HospitalProductsOrder:
    def __init__(self):
        pass

    def start(self, excel_file):
        df = pd.read_excel(excel_file)
        input_date = df['Unnamed: 5'][4]
        meal_date = df['Unnamed: 5'][5]
        print(f"입고일: {input_date}")
        print(f"급식일: {meal_date}")

        df = df.dropna(subset=["Unnamed: 3"])
        df = df.replace(np.nan, '', regex=True)
        df = df.reset_index(drop=True)
        df.index += 1
        df = df.loc[:, :'Unnamed: 23']  # 오른쪽 컬럼 삭제
        df = df[3:]  # 위에 행 날리기

        # 행 삭제
        df = df[~df['Unnamed: 5'].str.contains("고춧가루")]
        df = df[~df['Unnamed: 5'].str.contains("고추가루")]
        df = df[~df['Unnamed: 5'].str.contains("짜장생면")]
        df = df[~df['Unnamed: 23'].str.contains("직송")]
        df = df[~df['Unnamed: 23'].str.contains("병원")]

        df['Unnamed: 12'] = df['Unnamed: 12'].astype('object')
        df['Unnamed: 14'] = df['Unnamed: 14'].astype('object')
        df['Unnamed: 17'] = df['Unnamed: 17'].astype('object')

        # 수산물과 육류 제외한 행에서 T열 또는 V열로 합계 복사
        for index, row in df.iterrows():
            df.at[index, 'Unnamed: 12'] = df.at[index, 'Unnamed: 14'] = df.at[index, 'Unnamed: 17'] = ''
            if "일요일" in input_date:
                df.at[index, 'Unnamed: 17'] = df.at[index, 'Unnamed: 23']
            else:
                df.at[index, 'Unnamed: 14'] = df.at[index, 'Unnamed: 23']

        df = df.sort_values(by=['Unnamed: 1'])

        if "월요일" in input_date or "일요일" in input_date:
            before_input_date = datetime.strptime(input_date[:12], "%Y년 %m월%d일") - timedelta(days=2)
        else:
            before_input_date = datetime.strptime(input_date[:12], "%Y년 %m월%d일") - timedelta(days=1)
        before_input_date = before_input_date.strftime('%m월%d일')

        earlier_date = meal_date[6:12]
        earlier_df = df[df['Unnamed: 23'].str.contains("선입고")]
        earlier_df['Unnamed: 0'] = f"선, {earlier_date} 사용일"
        df = df[~df['Unnamed: 23'].str.contains("선입고")]
        if len(earlier_df) != 0:
            earlier_df.to_excel(f"공산 선입고 {before_input_date} 업로드_{datetime.today().strftime('%H%M')}.xls", startrow=1, header=None, index=False)
        else:
            print("공산 선입고 파일 없음")
            
        today_df = df[df['Unnamed: 23'].str.contains("당일")]
        today_df['Unnamed: 0'] = f"당, {earlier_date} 사용일"
        df = df[~df['Unnamed: 23'].str.contains("당일")]
        if len(today_df) != 0:
            today_df.to_excel(f"공산 당일입고 {earlier_date} 업로드_{datetime.today().strftime('%H%M')}.xls", startrow=1, header=None, index=False)
        else:
            print("공산 당일입고 파일 없음")

        if "토요일" in input_date or "일요일" in input_date:
            df['Unnamed: 0'] = f"{earlier_date} 사용일"

        if "일요일" in input_date:
            file_input_date = datetime.strptime(input_date[:12], "%Y년 %m월%d일") - timedelta(days=1)
            file_input_date = file_input_date.strftime("%m월%d일")
        else:
            file_input_date = input_date[6:12]

        df.to_excel(f"공산 본발주서 {file_input_date} 업로드_{datetime.today().strftime('%H%M')}.xls", startrow=1, header=None, index=False)


if __name__ == "__main__":
    excel_file = input("엑셀 파일명: ")
    start_time = time.time()
    hospital_products_order = HospitalProductsOrder()
    hospital_products_order.start(excel_file)
    elapsed_time = time.time() - start_time
    input(f"소요 시간: {elapsed_time} ms")

