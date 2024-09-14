import copy
import time
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
import warnings

warnings.filterwarnings('ignore')


class HospitalOrder:
    def __init__(self):
        pass

    def start(self, excel_file, fish_start_index, fish_end_index, meat_start_index, meat_end_index, earlier_index_list):
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
        df = df[~df['Unnamed: 5'].str.contains("조기")]
        df = df[~df['Unnamed: 23'].str.contains("태백골")]
        df = df[~df['Unnamed: 23'].str.contains("직송")]
        df = df[~df['Unnamed: 23'].str.contains("인터넷")]
        df = df[~df['Unnamed: 23'].str.contains("병원")]

        garlic_list = df['Unnamed: 5'].str.contains("깐마늘")
        dried_fish_list = df['Unnamed: 5'].str.contains("황태채")

        df['Unnamed: 12'] = df['Unnamed: 12'].astype('object')
        df['Unnamed: 14'] = df['Unnamed: 14'].astype('object')
        df['Unnamed: 17'] = df['Unnamed: 17'].astype('object')

        # 수산물과 육류 제외한 행에서 T열 또는 V열로 합계 복사
        for index, row in df.iterrows():
            column_index = row["Unnamed: 1"]
            if column_index >= fish_start_index and column_index <= fish_end_index and not dried_fish_list[index]:
                continue
            if column_index >= meat_start_index and column_index <= meat_end_index:
                continue
            if garlic_list[index]:
                continue

            df.at[index, 'Unnamed: 12'] = df.at[index, 'Unnamed: 14'] = df.at[index, 'Unnamed: 17'] = ''
            if "일요일" in input_date:
                df.at[index, 'Unnamed: 17'] = df.at[index, 'Unnamed: 20']
            else:
                df.at[index, 'Unnamed: 14'] = df.at[index, 'Unnamed: 20']

        df = self.replicate_row(df, df['Unnamed: 5'].str.contains("대파") & df['Unnamed: 11'].str.contains("KG"), 5, input_date)
        df = self.replicate_row(df, df['Unnamed: 5'].str.contains("오이") & df['Unnamed: 11'].str.contains("KG"), 10, input_date)
        df = self.replicate_row(df, df['Unnamed: 5'].str.contains("토속"), 10, input_date)
        df = self.replicate_row(df, df['Unnamed: 5'].str.contains("선농"), 10, input_date)

        df = self.process_fish_rows(df, fish_end_index, fish_start_index)
        df = df.sort_values(by=['Unnamed: 1'])

        if "월요일" in input_date or "일요일" in input_date:
            before_input_date = datetime.strptime(input_date[:12], "%Y년 %m월%d일") - timedelta(days=2)
        else:
            before_input_date = datetime.strptime(input_date[:12], "%Y년 %m월%d일") - timedelta(days=1)
        before_input_date = before_input_date.strftime('%m월%d일')

        earlier_date = meal_date[6:12]
        earlier_df = df[df['Unnamed: 23'].str.contains("선입고")]
        for index in earlier_index_list:
            if index == 0:
                break
            index = df.index[[df['Unnamed: 1'] == index]].tolist()[0]
            if index not in earlier_df.index:
                earlier_df = earlier_df.append(df.loc[index])
                df = df.drop(index=index)
        earlier_df['Unnamed: 0'] = f"선, {earlier_date} 사용일"
        df = df[~df['Unnamed: 23'].str.contains("선입고")]
        if len(earlier_df) != 0:
            earlier_df.to_excel(f"선입고 {before_input_date} 업로드_{datetime.today().strftime('%H%M')}.xls", startrow=1, header=None, index=False)
        else:
            print("선입고 파일 없음")

        today_df = df[df['Unnamed: 23'].str.contains("당일")]
        today_df['Unnamed: 0'] = f"당, {earlier_date} 사용일"
        df = df[~df['Unnamed: 23'].str.contains("당일")]
        if len(today_df) != 0:
            today_df.to_excel(f"당일입고 {earlier_date} 업로드_{datetime.today().strftime('%H%M')}.xls", startrow=1, header=None, index=False)
        else:
            print("당일입고 파일 없음")

        if "토요일" in input_date or "일요일" in input_date:
            df['Unnamed: 0'] = f"{earlier_date} 사용일"

        rice_df = df[df['Unnamed: 5'] == "쌀"]
        if len(rice_df) != 0:
            for index in rice_df.index:
                df = df.drop(index=index)
            rice_df.to_excel(f"쌀발주서_{datetime.today().strftime('%H%M')}.xls", startrow=1, header=None, index=False)
        else:
            print("쌀발주서 파일 없음")

        if "일요일" in input_date:
            file_input_date = datetime.strptime(input_date[:12], "%Y년 %m월%d일") - timedelta(days=1)
            file_input_date = file_input_date.strftime("%m월%d일")
        else:
            file_input_date = input_date[6:12]

        df.to_excel(f"본발주서 {file_input_date} 업로드_{datetime.today().strftime('%H%M')}.xls", startrow=1, header=None, index=False)

    def process_fish_rows(self, df, fish_end_index, fish_start_index):
        if fish_start_index == 0:
            return df
        for index in range(fish_start_index, fish_end_index + 1):
            index = df.index[[df['Unnamed: 1'] == index]].tolist()[0]
            series = df.loc[index]
            if "KG" not in series['Unnamed: 11']:
                continue

            r = series['Unnamed: 12']
            t = series['Unnamed: 14']
            v = series['Unnamed: 17']
            if r == '':
                r_count = r_remained = 0
            else:
                r_remained = r % 10
                r_count = int((r - r_remained) / 10)
            if t == '':
                t_count = t_remained = 0
            else:
                t_remained = t % 10
                t_count = int((t - t_remained) / 10)
            if v == '':
                v_count = v_remained = 0
            else:
                v_remained = v % 10
                v_count = int((v - v_remained) / 10)

            df = df.drop(index=index)
            while True:
                new_item = copy.deepcopy(series)
                if r_count != 0:
                    new_item['Unnamed: 12'] = 10
                    r_count -= 1
                elif r_remained != 0:
                    new_item['Unnamed: 12'] = r_remained
                    r_remained = 0
                else:
                    new_item['Unnamed: 12'] = ''
                if t_count != 0:
                    new_item['Unnamed: 14'] = 10
                    t_count -= 1
                elif t_remained != 0:
                    new_item['Unnamed: 14'] = t_remained
                    t_remained = 0
                else:
                    new_item['Unnamed: 14'] = ''
                if v_count != 0:
                    new_item['Unnamed: 17'] = 10
                    v_count -= 1
                elif v_remained != 0:
                    new_item['Unnamed: 17'] = v_remained
                    v_remained = 0
                else:
                    new_item['Unnamed: 17'] = ''
                df = df.append(new_item)
                if r_count == 0 and r_remained == 0 and t_count == 0 and t_remained == 0 and v_count == 0 and v_remained == 0:
                    break
        return df

    def replicate_row(self, df, replicated_list, division, input_date):
        for index, _ in replicated_list[replicated_list == True].items():
            series = df.loc[index]
            sum = series['Unnamed: 20']
            remained = sum % division
            count = int((sum - remained) / division)
            if count == 0:
                continue

            df = df.drop(index=index)
            for i in range(count):
                new_item = copy.deepcopy(series)
                if "일요일" in input_date:
                    new_item['Unnamed: 17'] = division
                else:
                    new_item['Unnamed: 14'] = division
                df = df.append(new_item)
            if remained != 0:
                new_item = copy.deepcopy(series)
                if "일요일" in input_date:
                    new_item['Unnamed: 17'] = remained
                else:
                    new_item['Unnamed: 14'] = remained
                df = df.append(new_item)
        return df


if __name__ == "__main__":
    excel_file = input("엑셀 파일 명: ")
    fish_start_index = int(input("수산 시작 번호 (없을 시 0): "))
    fish_end_index = int(input("수산 끝 번호 (없을 시 0): "))
    meat_start_index = int(input("육류 시작 번호 (없을 시 0): "))
    meat_end_index = int(input("육류 끝 번호 (없을 시 0): "))
    earlier_index_input = input("선입고 번호들 (스페이스바로 구분): ")
    if len(earlier_index_input) == 0:
        earlier_index_list = []
    else:
        earlier_index_list = earlier_index_input.split(" ")
        earlier_index_list = [int(item) for item in earlier_index_list]
    hospital_order = HospitalOrder()
    start_time = time.time()
    hospital_order.start(excel_file, fish_start_index, fish_end_index, meat_start_index, meat_end_index,
                         earlier_index_list)
    elapsed_time = time.time() - start_time
    input(f"소요 시간: {elapsed_time} ms")
