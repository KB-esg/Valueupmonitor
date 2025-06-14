import asyncio
from playwright.async_api import async_playwright
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import json
import os
import requests
import time
import re
from PIL import Image
import pytesseract
import numpy as np

class ESGFundScraper:
    def __init__(self):
        self.base_url = "https://www.fundguide.net/hkcenter/esg"
        self.telegram_bot_token = os.environ.get('TELCO_NEWS_TOKEN')
        self.telegram_chat_id = os.environ.get('TELCO_NEWS_TESTER')
        
    async def fetch_tab_data(self, page, tab_value, tab_name):
        """특정 탭의 데이터 가져오기"""
        print(f"Fetching data for {tab_name}...")
        
        # 탭 클릭
        await page.click(f'button[value="{tab_value}"]')
        await page.wait_for_timeout(3000)  # 데이터 로딩 대기
        
        # 데이터 추출
        data = {
            'tab_name': tab_name,
            'top_funds': await self.parse_top_funds(page),
            'new_funds': await self.parse_new_funds(page),
            'chart_data': await self.parse_chart_data_with_hover_ocr(page, tab_name)
        }
        
        return data
    
    async def parse_chart_data_with_hover_ocr(self, page, tab_name):
        """마우스 호버 + 스크린샷 + OCR을 통한 차트 데이터 추출"""
        chart_data = {
            'dates': [],
            'setup_amounts': [],
            'returns': []
        }
        
        try:
            # 차트 영역 찾기
            chart_element = await page.query_selector('#lineAreaZone')
            if not chart_element:
                print("Chart element not found")
                return chart_data
            
            # 차트 영역의 크기와 위치 가져오기
            box = await chart_element.bounding_box()
            if not box:
                print("Could not get chart bounding box")
                return chart_data
            
            print(f"Chart area: x={box['x']}, y={box['y']}, width={box['width']}, height={box['height']}")
            
            # 스크린샷 저장 디렉토리
            screenshot_dir = 'chart_screenshots'
            if not os.path.exists(screenshot_dir):
                os.makedirs(screenshot_dir)
            
            # 차트의 중간 높이 (Y축)
            hover_y = box['y'] + box['height'] / 2
            
            # X축을 따라 이동하며 데이터 수집
            num_points = 15  # 차트를 15개 구간으로 나누어 호버
            step = box['width'] / num_points
            
            collected_data = []
            
            for i in range(num_points):
                hover_x = box['x'] + (i * step) + 20  # 왼쪽 여백 고려
                
                # 마우스를 해당 위치로 이동
                await page.mouse.move(hover_x, hover_y)
                await page.wait_for_timeout(500)  # 툴팁이 나타날 시간 대기
                
                # 현재 화면 스크린샷
                screenshot_path = f'{screenshot_dir}/{tab_name}_hover_{i}.png'
                await page.screenshot(path=screenshot_path)
                
                # PIL로 이미지 열기
                img = Image.open(screenshot_path)
                
                # 툴팁이 나타날 가능성이 있는 영역 확대
                # 마우스 위치 주변 영역을 크롭
                tooltip_area = img.crop((
                    max(0, int(hover_x - 150)),
                    max(0, int(hover_y - 100)),
                    min(img.width, int(hover_x + 150)),
                    min(img.height, int(hover_y + 100))
                ))
                
                # 툴팁 영역 저장 (디버깅용)
                tooltip_path = f'{screenshot_dir}/{tab_name}_tooltip_{i}.png'
                tooltip_area.save(tooltip_path)
                
                # OCR 수행
                custom_config = r'--oem 3 --psm 6'
                tooltip_text = pytesseract.image_to_string(tooltip_area, lang='kor+eng', config=custom_config)
                
                if tooltip_text.strip():
                    print(f"Point {i} OCR result: {tooltip_text.strip()}")
                    
                    # 데이터 파싱
                    data_point = {}
                    lines = tooltip_text.strip().split('\n')
                    
                    for line in lines:
                        # 날짜 패턴 (YYYY.MM.DD)
                        date_match = re.search(r'(\d{4}[.\s]+\d{1,2}[.\s]+\d{1,2})', line)
                        if date_match:
                            # 공백 제거하고 점으로 통일
                            date_str = re.sub(r'\s+', '', date_match.group(1))
                            date_str = re.sub(r'\.+', '.', date_str)
                            if len(date_str.split('.')) == 3:
                                data_point['date'] = date_str
                        
                        # 설정액 패턴 (숫자,숫자 억원)
                        amount_match = re.search(r'([\d,]+\.?\d*)\s*억원', line)
                        if amount_match:
                            value = amount_match.group(1).replace(',', '').replace(' ', '')
                            try:
                                data_point['setup_amount'] = float(value)
                            except:
                                pass
                        
                        # 수익률 패턴 (숫자%)
                        if '수익률' in line or '%' in line:
                            rate_match = re.search(r'([-+]?\d+\.?\d*)\s*%', line)
                            if rate_match:
                                try:
                                    data_point['return_rate'] = float(rate_match.group(1))
                                except:
                                    pass
                    
                    if data_point and 'date' in data_point:
                        # 중복 제거
                        if data_point['date'] not in [d.get('date') for d in collected_data]:
                            collected_data.append(data_point)
                            print(f"Collected data point: {data_point}")
                
                # 스크린샷 파일 삭제 (공간 절약)
                try:
                    os.remove(screenshot_path)
                except:
                    pass
            
            # 수집된 데이터 정리
            if collected_data:
                # 날짜순 정렬
                collected_data.sort(key=lambda x: x.get('date', ''))
                
                for data in collected_data:
                    if 'date' in data:
                        chart_data['dates'].append(data['date'])
                    if 'setup_amount' in data:
                        chart_data['setup_amounts'].append(data['setup_amount'])
                    else:
                        chart_data['setup_amounts'].append(None)
                    if 'return_rate' in data:
                        chart_data['returns'].append(data['return_rate'])
                    else:
                        chart_data['returns'].append(None)
                
                print(f"Total collected {len(collected_data)} data points through hover + OCR")
            
            # 데이터가 없는 경우 X축에서 날짜만이라도 추출
            if not chart_data['dates']:
                print("Hover + OCR failed, extracting dates from X-axis")
                
                # 전체 차트 영역 OCR
                full_chart_img = img.crop((
                    int(box['x']),
                    int(box['y']),
                    int(box['x'] + box['width']),
                    int(box['y'] + box['height'] + 50)
                ))
                
                chart_text = pytesseract.image_to_string(full_chart_img, lang='kor+eng', config=custom_config)
                
                # X축 날짜 추출
                date_pattern = r'(\d{4}[.\s]+\d{1,2}[.\s]+\d{1,2})'
                dates = re.findall(date_pattern, chart_text)
                
                for date_str in dates:
                    # 공백 제거하고 점으로 통일
                    clean_date = re.sub(r'\s+', '', date_str)
                    clean_date = re.sub(r'\.+', '.', clean_date)
                    if len(clean_date.split('.')) == 3 and clean_date not in chart_data['dates']:
                        chart_data['dates'].append(clean_date)
                
                if chart_data['dates']:
                    print(f"Extracted {len(chart_data['dates'])} dates from full chart OCR")
                    chart_data['setup_amounts'] = [None] * len(chart_data['dates'])
                    chart_data['returns'] = [None] * len(chart_data['dates'])
            
        except Exception as e:
            print(f"Error in hover + OCR data collection: {e}")
            import traceback
            traceback.print_exc()
        
        return chart_data
    
    async def parse_top_funds(self, page):
        """Top 펀드 데이터 파싱"""
        top_funds_data = {
            'return_top': [],
            'growth_top': []
        }
        
        # 수익률 TOP 5
        return_funds = await page.query_selector_all('#topFundZone td:nth-child(1) li')
        return_rates = await page.query_selector_all('#topFundZone td:nth-child(2) li')
        
        for i in range(len(return_funds)):
            fund_elem = return_funds[i]
            rate_elem = return_rates[i] if i < len(return_rates) else None
            
            rank = await fund_elem.query_selector('i')
            rank_text = await rank.inner_text() if rank else ''
            
            fund_link = await fund_elem.query_selector('a')
            if fund_link:
                fund_name = await fund_link.inner_text()
                fund_code = await fund_link.get_attribute('data-fund_cd')
                rate_text = await rate_elem.inner_text() if rate_elem else ''
                
                top_funds_data['return_top'].append({
                    'rank': rank_text,
                    'fund_name': fund_name.strip(),
                    'fund_code': fund_code or '',
                    'return_rate': rate_text.strip()
                })
        
        # 설정액증가 TOP 5
        growth_funds = await page.query_selector_all('#topFundZone td:nth-child(3) li')
        growth_amounts = await page.query_selector_all('#topFundZone td:nth-child(4) li')
        
        for i in range(len(growth_funds)):
            fund_elem = growth_funds[i]
            amount_elem = growth_amounts[i] if i < len(growth_amounts) else None
            
            rank = await fund_elem.query_selector('i')
            rank_text = await rank.inner_text() if rank else ''
            
            fund_link = await fund_elem.query_selector('a')
            if fund_link:
                fund_name = await fund_link.inner_text()
                fund_code = await fund_link.get_attribute('data-fund_cd')
                amount_text = await amount_elem.inner_text() if amount_elem else ''
                
                top_funds_data['growth_top'].append({
                    'rank': rank_text,
                    'fund_name': fund_name.strip(),
                    'fund_code': fund_code or '',
                    'growth_amount': amount_text.strip()
                })
        
        return top_funds_data
    
    async def parse_new_funds(self, page):
        """신규 펀드 데이터 파싱"""
        new_funds_data = []
        
        # 신규 펀드가 없는지 확인
        no_data = await page.query_selector('#newFundZone .nodata')
        if no_data:
            return new_funds_data
        
        # 신규 펀드 데이터 가져오기
        rows = await page.query_selector_all('#newFundZone tr')
        for row in rows:
            cols = await row.query_selector_all('td')
            if len(cols) >= 3:
                fund_name = await cols[0].inner_text()
                company = await cols[1].inner_text()
                setup_date = await cols[2].inner_text()
                
                new_funds_data.append({
                    'fund_name': fund_name.strip(),
                    'company': company.strip(),
                    'setup_date': setup_date.strip()
                })
        
        return new_funds_data
    
    async def parse_chart_data_from_svg(self, page):
        """SVG 차트에서 날짜 추출 (X축 레이블)"""
        chart_data = {
            'dates': [],
            'setup_amounts': [],
            'returns': []
        }
        
        try:
            # X축 레이블에서 날짜 추출
            x_axis_texts = await page.query_selector_all('.highcharts-xaxis-labels text')
            for text_elem in x_axis_texts:
                date_text = await text_elem.inner_text()
                if date_text and '.' in date_text:  # 날짜 형식 확인
                    chart_data['dates'].append(date_text.strip())
            
            # Y축 값들 추출 (참고용)
            y_axis_texts = await page.query_selector_all('.highcharts-yaxis-labels text')
            setup_amounts_range = []
            returns_range = []
            
            for i, text_elem in enumerate(y_axis_texts):
                value_text = await text_elem.inner_text()
                if value_text:
                    # 첫 번째 Y축은 설정액, 두 번째 Y축은 수익률
                    value = value_text.replace(',', '').replace('%', '')
                    try:
                        if i < 7:  # 첫 번째 Y축 (설정액)
                            setup_amounts_range.append(float(value))
                        else:  # 두 번째 Y축 (수익률)
                            returns_range.append(float(value))
                    except:
                        pass
            
            print(f"Found {len(chart_data['dates'])} dates from chart")
            print(f"Date range: {chart_data['dates'][0] if chart_data['dates'] else 'N/A'} ~ "
                  f"{chart_data['dates'][-1] if chart_data['dates'] else 'N/A'}")
            
            # 차트의 path 요소에서 실제 데이터 포인트 추정 (복잡한 작업)
            # 현재는 날짜만 추출
            
        except Exception as e:
            print(f"Error parsing SVG chart: {e}")
        
        return chart_data
    
    async def scrape_all_tabs(self):
        """모든 탭의 데이터 수집"""
        all_data = {}
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            try:
                # 페이지 로드
                await page.goto(self.base_url, wait_until='networkidle')
                await page.wait_for_timeout(3000)
                
                # 각 탭 데이터 수집
                tabs = [
                    ('T0370', 'SRI'),
                    ('T0371', 'ESG_주식'),
                    ('T0373', 'ESG_채권')
                ]
                
                for tab_value, tab_name in tabs:
                    data = await self.fetch_tab_data(page, tab_value, tab_name)
                    all_data[tab_name] = data
                    await page.wait_for_timeout(1000)  # 탭 간 대기
                
            except Exception as e:
                print(f"Error during scraping: {e}")
                await self.send_telegram_message(f"❌ ESG 펀드 데이터 수집 중 오류 발생: {str(e)}")
                raise
            finally:
                await browser.close()
        
        return all_data
    
    def to_dataframes(self, all_data):
        """수집된 데이터를 DataFrame으로 변환"""
        dfs = {}
        collection_date = datetime.now().strftime('%Y-%m-%d')
        collection_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        for tab_name, tab_data in all_data.items():
            # 수익률 TOP 5
            if tab_data['top_funds']['return_top']:
                df_key = f'{tab_name}_return_top'
                df = pd.DataFrame(tab_data['top_funds']['return_top'])
                df['tab_type'] = tab_name
                df['collection_date'] = collection_date
                df['collection_time'] = collection_time
                dfs[df_key] = df
            
            # 설정액증가 TOP 5
            if tab_data['top_funds']['growth_top']:
                df_key = f'{tab_name}_growth_top'
                df = pd.DataFrame(tab_data['top_funds']['growth_top'])
                df['tab_type'] = tab_name
                df['collection_date'] = collection_date
                df['collection_time'] = collection_time
                dfs[df_key] = df
            
            # 신규 펀드
            if tab_data['new_funds']:
                df_key = f'{tab_name}_new_funds'
                df = pd.DataFrame(tab_data['new_funds'])
                df['tab_type'] = tab_name
                df['collection_date'] = collection_date
                df['collection_time'] = collection_time
                dfs[df_key] = df
            
            # 일별 차트 데이터 (설정액/수익률)
            if tab_data.get('chart_data') and tab_data['chart_data'].get('dates'):
                df_key = f'{tab_name}_daily_chart'
                
                # 데이터 길이 맞추기
                dates = tab_data['chart_data']['dates']
                setup_amounts = tab_data['chart_data']['setup_amounts']
                returns = tab_data['chart_data']['returns']
                
                # 가장 짧은 길이에 맞추기
                min_length = min(len(dates), len(setup_amounts) if setup_amounts else 0, len(returns) if returns else 0)
                
                if min_length > 0:
                    chart_df = pd.DataFrame({
                        'date': dates[:min_length],
                        'setup_amount': setup_amounts[:min_length] if setup_amounts else [None] * min_length,
                        'return_rate': returns[:min_length] if returns else [None] * min_length
                    })
                    chart_df['tab_type'] = tab_name
                    chart_df['collection_time'] = collection_time
                    dfs[df_key] = chart_df
                    print(f"Created chart dataframe for {tab_name} with {min_length} rows")
        
        return dfs
    
    def save_to_sheets(self, dfs):
        """Google Sheets에 데이터 저장"""
        # 서비스 계정 인증
        scope = ['https://spreadsheets.google.com/feeds',
                 'https://www.googleapis.com/auth/drive']
        
        creds_json = os.environ.get('GOOGLE_SERVICE')
        if not creds_json:
            print("No Google Sheets credentials found")
            print("Looking for GOOGLE_SERVICE environment variable")
            print("Available environment variables:", list(os.environ.keys()))
            return []
        
        try:
            creds_dict = json.loads(creds_json)
            # 서비스 계정 이메일 주소 출력
            service_account_email = creds_dict.get('client_email', 'Unknown')
            print(f"Using service account: {service_account_email}")
            print(f"Please make sure this email has edit access to your Google Sheets")
            
            creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
            client = gspread.authorize(creds)
            
            sheet_id = os.environ.get('KRFUND_SPREADSHEET_ID')
            if not sheet_id:
                print("No Google Sheet ID found")
                print("Available environment variables:", list(os.environ.keys()))
                return []
                
            spreadsheet = client.open_by_key(sheet_id)
            
            # 시트 이름 매핑
            sheet_mapping = {
                'SRI_return_top': 'SRI_수익률TOP5',
                'SRI_growth_top': 'SRI_설정액증가TOP5',
                'SRI_new_funds': 'SRI_신규펀드',
                'SRI_daily_chart': 'SRI_일별차트',
                'ESG_주식_return_top': 'ESG주식_수익률TOP5',
                'ESG_주식_growth_top': 'ESG주식_설정액증가TOP5',
                'ESG_주식_new_funds': 'ESG주식_신규펀드',
                'ESG_주식_daily_chart': 'ESG주식_일별차트',
                'ESG_채권_return_top': 'ESG채권_수익률TOP5',
                'ESG_채권_growth_top': 'ESG채권_설정액증가TOP5',
                'ESG_채권_new_funds': 'ESG채권_신규펀드',
                'ESG_채권_daily_chart': 'ESG채권_일별차트'
            }
            
            updated_sheets = []
            
            for df_key, df in dfs.items():
                sheet_name = sheet_mapping.get(df_key, df_key)
                
                try:
                    # 시트 가져오기 또는 생성
                    try:
                        worksheet = spreadsheet.worksheet(sheet_name)
                    except:
                        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=20)
                    
                    # 기존 데이터 가져오기
                    existing_data = worksheet.get_all_records()
                    
                    if existing_data:
                        existing_df = pd.DataFrame(existing_data)
                        combined_df = pd.concat([existing_df, df], ignore_index=True)
                    else:
                        combined_df = df
                    
                    # 데이터 쓰기
                    worksheet.clear()
                    worksheet.update([combined_df.columns.values.tolist()] + combined_df.values.tolist())
                    
                    updated_sheets.append(sheet_name)
                    print(f"Successfully updated {sheet_name}")
                    
                except Exception as e:
                    print(f"Error updating {sheet_name}: {e}")
                    
            return updated_sheets
            
        except Exception as e:
            print(f"Error in save_to_sheets: {e}")
            return []
    
    def save_backup(self, dfs):
        """로컬 백업 저장"""
        backup_dir = 'data_backup'
        if not os.path.exists(backup_dir):
            os.makedirs(backup_dir)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        saved_files = []
        
        for key, df in dfs.items():
            filename = f'{backup_dir}/esg_fund_{key}_{timestamp}.csv'
            df.to_csv(filename, index=False, encoding='utf-8-sig')
            saved_files.append(filename)
        
        return saved_files
    
    def send_telegram_message(self, message):
        """Telegram 메시지 전송"""
        if not self.telegram_bot_token or not self.telegram_chat_id:
            print("Telegram credentials not found")
            return
        
        try:
            url = f"https://api.telegram.org/bot{self.telegram_bot_token}/sendMessage"
            data = {
                "chat_id": self.telegram_chat_id,
                "text": message,
                "parse_mode": "Markdown"
            }
            response = requests.post(url, data=data)
            response.raise_for_status()
        except Exception as e:
            print(f"Error sending Telegram message: {e}")
    
    async def run(self):
        """전체 프로세스 실행"""
        start_time = time.time()
        print(f"Starting ESG Fund data collection at {datetime.now()}")
        
        try:
            # 1. 모든 탭 데이터 수집
            all_data = await self.scrape_all_tabs()
            
            # 2. DataFrame 변환
            dfs = self.to_dataframes(all_data)
            
            # 3. 데이터 통계
            total_records = sum(len(df) for df in dfs.values())
            
            # 4. Google Sheets 저장
            updated_sheets = self.save_to_sheets(dfs)
            
            # 5. 로컬 백업
            saved_files = self.save_backup(dfs)
            
            # 6. 실행 시간 계산
            execution_time = round(time.time() - start_time, 2)
            
            # 7. 성공 메시지 전송
            sheets_count = len(updated_sheets) if isinstance(updated_sheets, list) else 0
            message = f"""✅ *ESG 펀드 데이터 수집 완료*

📅 수집 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
📊 수집 데이터: {total_records}개 레코드
📁 업데이트 시트: {sheets_count}개
💾 백업 파일: {len(saved_files)}개
⏱️ 실행 시간: {execution_time}초

*수집 항목:*
- SRI 펀드
- ESG 주식형 펀드
- ESG 채권형 펀드

각 항목별 수익률 TOP5, 설정액증가 TOP5, 신규펀드 데이터 수집 완료"""
            
            self.send_telegram_message(message)
            print("Data collection completed successfully")
            
        except Exception as e:
            error_message = f"""❌ *ESG 펀드 데이터 수집 실패*

📅 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
🚫 오류: {str(e)}

관리자에게 확인을 요청하세요."""
            
            self.send_telegram_message(error_message)
            print(f"Data collection failed: {e}")
            raise

if __name__ == "__main__":
    scraper = ESGFundScraper()
    asyncio.run(scraper.run())
