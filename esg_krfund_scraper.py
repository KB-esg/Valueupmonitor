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
from PIL import Image, ImageDraw, ImageEnhance
import pytesseract
import numpy as np
import cv2

class ESGFundScraper:
    def __init__(self):
        self.base_url = "https://www.fundguide.net/hkcenter/esg"
        self.telegram_bot_token = os.environ.get('TELCO_NEWS_TOKEN')
        self.telegram_chat_id = os.environ.get('TELCO_NEWS_TESTER')
        # 환경 변수에서 기간 설정 가져오기 (기본값: 1개월)
        self.collection_period = os.environ.get('COLLECTION_PERIOD', '01')
        self.period_text_map = {
            '01': '1개월',
            '03': '3개월',
            '06': '6개월',
            'YTD': '연초이후',
            '12': '1년',
            '36': '3년',
            '60': '5년'
        }
        print(f"📅 Collection period set to: {self.collection_period} ({self.period_text_map.get(self.collection_period, self.collection_period)})")
        
    async def extract_chart_data_via_javascript(self, page):
        """JavaScript를 통해 Highcharts 데이터 직접 추출"""
        try:
            chart_data = await page.evaluate('''
                () => {
                    // Highcharts 인스턴스 찾기
                    let chartData = {
                        dates: [],
                        setup_amounts: [],
                        returns: []
                    };
                    
                    // 방법 1: Highcharts.charts 배열에서 찾기
                    if (typeof Highcharts !== 'undefined' && Highcharts.charts) {
                        for (let i = 0; i < Highcharts.charts.length; i++) {
                            let chart = Highcharts.charts[i];
                            if (chart && chart.container && chart.container.id === 'lineAreaZone') {
                                console.log('Found chart at index:', i);
                                
                                // X축 카테고리 (날짜)
                                if (chart.xAxis && chart.xAxis[0]) {
                                    // categories가 없으면 tick labels에서 추출
                                    if (chart.xAxis[0].categories) {
                                        chartData.dates = chart.xAxis[0].categories;
                                    } else if (chart.xAxis[0].tickPositions) {
                                        // X축 레이블에서 직접 텍스트 추출
                                        const labels = [];
                                        chart.xAxis[0].ticks && Object.values(chart.xAxis[0].ticks).forEach(tick => {
                                            if (tick.label && tick.label.textStr) {
                                                labels.push(tick.label.textStr);
                                            }
                                        });
                                        chartData.dates = labels;
                                    }
                                }
                                
                                // 시리즈 데이터
                                if (chart.series && chart.series.length > 0) {
                                    chart.series.forEach((series, index) => {
                                        if (series.data && series.data.length > 0) {
                                            const values = series.data.map(point => {
                                                if (point.y !== undefined) return point.y;
                                                if (point.options && point.options.y !== undefined) return point.options.y;
                                                return null;
                                            });
                                            
                                            // 시리즈 이름으로 구분 (없으면 인덱스로)
                                            if (series.name) {
                                                if (series.name.includes('설정액') || series.name.includes('좌')) {
                                                    chartData.setup_amounts = values;
                                                } else if (series.name.includes('수익률') || series.name.includes('우')) {
                                                    chartData.returns = values;
                                                }
                                            } else {
                                                // 이름이 없으면 첫 번째 시리즈를 설정액, 두 번째를 수익률로 가정
                                                if (index === 0) {
                                                    chartData.setup_amounts = values;
                                                } else if (index === 1) {
                                                    chartData.returns = values;
                                                }
                                            }
                                        }
                                    });
                                }
                                
                                // Y축 정보도 추출 (값 범위 파악용)
                                if (chart.yAxis) {
                                    chart.yAxis.forEach((axis, index) => {
                                        console.log(`Y-axis ${index} range:`, axis.min, '-', axis.max);
                                    });
                                }
                                
                                break;
                            }
                        }
                    }
                    
                    // 방법 2: data-highcharts-chart 속성으로 찾기
                    if (chartData.dates.length === 0) {
                        const chartDiv = document.querySelector('#lineAreaZone');
                        if (chartDiv) {
                            const chartIndex = chartDiv.getAttribute('data-highcharts-chart');
                            if (chartIndex && Highcharts.charts[chartIndex]) {
                                const chart = Highcharts.charts[chartIndex];
                                console.log('Found chart via data attribute');
                                
                                // 위와 동일한 로직으로 데이터 추출
                                if (chart.xAxis && chart.xAxis[0] && chart.xAxis[0].categories) {
                                    chartData.dates = chart.xAxis[0].categories;
                                }
                                
                                if (chart.series) {
                                    chart.series.forEach((series, index) => {
                                        if (series.data) {
                                            const values = series.data.map(point => point.y);
                                            if (index === 0) chartData.setup_amounts = values;
                                            else if (index === 1) chartData.returns = values;
                                        }
                                    });
                                }
                            }
                        }
                    }
                    
                    // 방법 3: SVG 요소에서 직접 텍스트 추출 (차선책)
                    if (chartData.dates.length === 0) {
                        const xLabels = document.querySelectorAll('.highcharts-xaxis-labels text');
                        xLabels.forEach(label => {
                            if (label.textContent) {
                                chartData.dates.push(label.textContent);
                            }
                        });
                    }
                    
                    console.log('Extracted data:', chartData);
                    return chartData;
                }
            ''')
            
            if chart_data and chart_data.get('dates'):
                print(f"📊 JavaScript extraction successful!")
                print(f"   Dates: {chart_data['dates']}")
                print(f"   Setup amounts count: {len(chart_data.get('setup_amounts', []))}")
                print(f"   Returns count: {len(chart_data.get('returns', []))}")
                
                # 데이터 검증
                if chart_data.get('setup_amounts'):
                    print(f"   Setup amounts sample: {chart_data['setup_amounts'][:3]}...")
                if chart_data.get('returns'):
                    print(f"   Returns sample: {chart_data['returns'][:3]}...")
                    
                return chart_data
                
        except Exception as e:
            print(f"❌ JavaScript extraction failed: {e}")
            import traceback
            traceback.print_exc()
        
        return None

    async def extract_chart_data(self, page, tab_name):
        """차트 데이터 추출 (JavaScript와 OCR 둘 다 수행하여 비교)"""
        # 1. JavaScript로 직접 추출 시도
        js_data = await self.extract_chart_data_via_javascript(page)
        
        # 2. 이미지 OCR 분석도 수행 (비교용)
        ocr_data = await self.extract_chart_data_with_ocr_analysis(page, tab_name)
        
        # 두 방법 모두의 결과를 반환
        return {
            'js_data': js_data,
            'ocr_data': ocr_data,
            'primary_data': js_data if js_data and js_data.get('dates') else ocr_data
        }
    
    async def extract_chart_data_with_ocr_analysis(self, page, tab_name):
        """차트 이미지 OCR 분석 (백업 방법)"""
        chart_data = {
            'dates': [],
            'setup_amounts': [],
            'returns': []
        }
        
        try:
            # 차트 컨테이너 찾기
            chart_container = await page.query_selector('#lineAreaZone')
            if not chart_container:
                print("❌ Chart container #lineAreaZone not found")
                return chart_data
            
            # 차트 영역 스크린샷
            screenshot_dir = 'chart_analysis'
            if not os.path.exists(screenshot_dir):
                os.makedirs(screenshot_dir)
            
            chart_path = f'{screenshot_dir}/{tab_name}_chart.png'
            await chart_container.screenshot(path=chart_path)
            print(f"📷 Chart screenshot saved: {chart_path}")
            
            # 여기에 OCR 분석 로직 추가 (필요시)
            # ...
            
        except Exception as e:
            print(f"❌ Error in chart image analysis: {e}")
        
        return chart_data
    
    async def parse_top_funds(self, page):
        """Top 펀드 데이터 파싱"""
        top_funds_data = []
        
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
                
                top_funds_data.append({
                    'rank': rank_text,
                    'fund_name': fund_name.strip(),
                    'fund_code': fund_code or '',
                    'value': rate_text.strip(),
                    'type': '수익률'
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
                
                top_funds_data.append({
                    'rank': rank_text,
                    'fund_name': fund_name.strip(),
                    'fund_code': fund_code or '',
                    'value': amount_text.strip(),
                    'type': '설정액증가'
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
    
    async def fetch_tab_data(self, page, tab_value, tab_name):
        """특정 탭의 데이터 가져오기"""
        print(f"🔍 Fetching data for {tab_name}...")
        
        # 탭 클릭
        await page.click(f'button[value="{tab_value}"]')
        await page.wait_for_timeout(3000)  # 데이터 로딩 대기
        
        # 기간 선택 (드롭다운에서 선택)
        try:
            # 먼저 현재 선택된 기간 확인
            current_period = await page.inner_text('#selTerm option[selected]')
            print(f"📅 Current period: {current_period}")
            
            # 원하는 기간이 이미 선택되어 있지 않은 경우에만 변경
            if self.collection_period != '01':  # 기본값이 아닌 경우
                print(f"📅 Changing period to: {self.period_text_map.get(self.collection_period)}")
                
                # select 요소를 직접 조작
                await page.select_option('#selTerm', self.collection_period)
                
                # 선택 후 차트 데이터 로딩 대기
                await page.wait_for_timeout(3000)
                
                # 변경 확인
                new_period = await page.inner_text('#selTerm option[selected]')
                print(f"✅ Period changed to: {new_period}")
        except Exception as e:
            print(f"⚠️ Error changing period: {e}")
            # 오류가 발생해도 계속 진행
        
        # 데이터 추출
        data = {
            'tab_name': tab_name,
            'collection_period': self.collection_period,
            'period_text': self.period_text_map.get(self.collection_period, self.collection_period),
            'top_funds': await self.parse_top_funds(page),
            'new_funds': await self.parse_new_funds(page),
            'chart_data': await self.extract_chart_data(page, tab_name)
        }
        
        return data
    
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
                print(f"❌ Error during scraping: {e}")
                await self.send_telegram_message(f"❌ ESG 펀드 데이터 수집 중 오류 발생: {str(e)}")
                raise
            finally:
                await browser.close()
        
        return all_data
    
    def to_dataframes(self, all_data):
        """수집된 데이터를 DataFrame으로 변환 (통합된 형태)"""
        dfs = {}
        collection_date = datetime.now().strftime('%Y-%m-%d')
        collection_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # 1. TOP5 펀드 데이터 (모든 탭 통합)
        all_top_funds = []
        for tab_name, tab_data in all_data.items():
            for fund in tab_data['top_funds']:
                fund['tab_type'] = tab_name
                fund['collection_date'] = collection_date
                fund['collection_time'] = collection_time
                all_top_funds.append(fund)
        
        if all_top_funds:
            dfs['top_funds'] = pd.DataFrame(all_top_funds)
            print(f"✅ Created unified TOP5 dataframe with {len(all_top_funds)} rows")
        
        # 2. 신규 펀드 데이터 (모든 탭 통합)
        all_new_funds = []
        for tab_name, tab_data in all_data.items():
            for fund in tab_data['new_funds']:
                fund['tab_type'] = tab_name
                fund['collection_date'] = collection_date
                fund['collection_time'] = collection_time
                all_new_funds.append(fund)
        
        if all_new_funds:
            dfs['new_funds'] = pd.DataFrame(all_new_funds)
            print(f"✅ Created unified new funds dataframe with {len(all_new_funds)} rows")
        
        # 3. 일별 차트 데이터 (모든 탭 통합)
        all_chart_data = []
        for tab_name, tab_data in all_data.items():
            chart_data = tab_data.get('chart_data', {})
            if chart_data.get('dates'):
                dates = chart_data['dates']
                setup_amounts = chart_data.get('setup_amounts', [])
                returns = chart_data.get('returns', [])
                
                # 데이터 길이 맞추기
                min_length = min(
                    len(dates), 
                    len(setup_amounts) if setup_amounts else 0, 
                    len(returns) if returns else 0
                )
                
                for i in range(min_length):
                    all_chart_data.append({
                        'date': dates[i],
                        'setup_amount': setup_amounts[i] if i < len(setup_amounts) else None,
                        'return_rate': returns[i] if i < len(returns) else None,
                        'tab_type': tab_name,
                        'collection_time': collection_time
                    })
        
        if all_chart_data:
            dfs['daily_chart'] = pd.DataFrame(all_chart_data)
            print(f"✅ Created unified chart dataframe with {len(all_chart_data)} rows")
        
        return dfs
    
    def save_to_sheets(self, dfs):
        """Google Sheets에 데이터 저장 (통합된 시트)"""
        # 서비스 계정 인증
        scope = ['https://spreadsheets.google.com/feeds',
                 'https://www.googleapis.com/auth/drive']
        
        creds_json = os.environ.get('GOOGLE_SERVICE')
        if not creds_json:
            print("❌ No Google Sheets credentials found")
            return []
        
        try:
            creds_dict = json.loads(creds_json)
            service_account_email = creds_dict.get('client_email', 'Unknown')
            print(f"📧 Using service account: {service_account_email}")
            
            creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
            client = gspread.authorize(creds)
            
            sheet_id = os.environ.get('KRFUND_SPREADSHEET_ID')
            if not sheet_id:
                print("❌ No Google Sheet ID found")
                return []
                
            spreadsheet = client.open_by_key(sheet_id)
            
            # 시트 이름 매핑
            sheet_mapping = {
                'top_funds': 'ESG_TOP5펀드',
                'new_funds': 'ESG_신규펀드',
                'daily_chart': 'ESG_일별차트',
                'chart_comparison': 'ESG_차트비교검증'
            }
            
            updated_sheets = []
            
            for df_key, df in dfs.items():
                sheet_name = sheet_mapping.get(df_key, df_key)
                
                try:
                    # 시트 가져오기 또는 생성
                    try:
                        worksheet = spreadsheet.worksheet(sheet_name)
                    except:
                        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=5000, cols=20)
                    
                    if df_key == 'daily_chart':
                        # 일별 차트는 특별한 처리 (최신 데이터가 위로)
                        existing_data = worksheet.get_all_records()
                        
                        if existing_data:
                            existing_df = pd.DataFrame(existing_data)
                            # 새 데이터와 결합
                            combined_df = pd.concat([df, existing_df], ignore_index=True)
                            # 중복 제거 (날짜와 탭으로)
                            combined_df = combined_df.drop_duplicates(subset=['date', 'tab_type'], keep='first')
                            # 날짜 역순 정렬 (최신이 위로)
                            combined_df = combined_df.sort_values(by=['date', 'tab_type'], ascending=[False, True])
                        else:
                            combined_df = df
                            
                    elif df_key == 'chart_comparison':
                        # 비교 검증 데이터는 매번 새로 쓰기
                        combined_df = df
                        combined_df = combined_df.sort_values(by=['date', 'tab_type', 'method'], 
                                                            ascending=[False, True, True])
                        
                    else:
                        # TOP5와 신규펀드는 기존 로직 유지
                        existing_data = worksheet.get_all_records()
                        
                        if existing_data:
                            existing_df = pd.DataFrame(existing_data)
                            # 중복 제거를 위한 키 설정
                            if df_key == 'top_funds':
                                key_cols = ['collection_date', 'tab_type', 'type', 'rank']
                            elif df_key == 'new_funds':
                                key_cols = ['collection_date', 'tab_type', 'fund_name']
                            
                            # 중복 제거 후 결합
                            combined_df = pd.concat([existing_df, df], ignore_index=True)
                            combined_df = combined_df.drop_duplicates(subset=key_cols, keep='last')
                            # 최신 데이터가 위로 오도록 정렬
                            combined_df = combined_df.sort_values(by=['collection_date', 'tab_type'], 
                                                                ascending=[False, True])
                        else:
                            combined_df = df
                    
                    # 데이터 쓰기
                    worksheet.clear()
                    worksheet.update([combined_df.columns.values.tolist()] + combined_df.values.tolist())
                    
                    updated_sheets.append(sheet_name)
                    print(f"✅ Successfully updated {sheet_name} with {len(combined_df)} rows")
                    
                except Exception as e:
                    print(f"❌ Error updating {sheet_name}: {e}")
                    
            return updated_sheets
            
        except Exception as e:
            print(f"❌ Error in save_to_sheets: {e}")
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
            print(f"💾 Saved backup: {filename}")
        
        return saved_files
    
    def send_telegram_message(self, message):
        """Telegram 메시지 전송"""
        if not self.telegram_bot_token or not self.telegram_chat_id:
            print("❌ Telegram credentials not found")
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
            print(f"❌ Error sending Telegram message: {e}")
    
    async def run(self):
        """전체 프로세스 실행"""
        start_time = time.time()
        print(f"🚀 Starting ESG Fund data collection at {datetime.now()}")
        
        try:
            # 1. 모든 탭 데이터 수집
            all_data = await self.scrape_all_tabs()
            
            # 2. DataFrame 변환 (통합된 형태)
            dfs = self.to_dataframes(all_data)
            
            # 3. 데이터 통계
            total_records = sum(len(df) for df in dfs.values())
            
            # 4. Google Sheets 저장
            updated_sheets = self.save_to_sheets(dfs)
            
            # 5. 로컬 백업
            saved_files = self.save_backup(dfs)
            
            # 6. 실행 시간 계산
            execution_time = round(time.time() - start_time, 2)
            
            # 7. 상세 통계
            stats = {}
            for key, df in dfs.items():
                if key == 'top_funds':
                    stats['TOP5 펀드'] = f"{len(df)}개 (수익률/설정액증가)"
                elif key == 'new_funds':
                    stats['신규 펀드'] = f"{len(df)}개"
                elif key == 'daily_chart':
                    unique_dates = df['date'].nunique() if 'date' in df.columns else 0
                    stats['차트 데이터'] = f"{unique_dates}일치"
            
            # 8. 성공 메시지 전송
            period_text = self.period_text_map.get(self.collection_period, self.collection_period)
            message = f"""✅ *ESG 펀드 데이터 수집 완료*

📅 수집 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
📊 총 레코드: {total_records}개
📁 업데이트 시트: {len(updated_sheets)}개
⏱️ 실행 시간: {execution_time}초
📈 수집 기간: {period_text}

*수집 현황:*
{chr(10).join([f"• {k}: {v}" for k, v in stats.items()])}

*수집 범위:*
• SRI 펀드
• ESG 주식형 펀드
• ESG 채권형 펀드"""
            
            self.send_telegram_message(message)
            print("✅ Data collection completed successfully")
            
        except Exception as e:
            error_message = f"""❌ *ESG 펀드 데이터 수집 실패*

📅 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
🚫 오류: {str(e)}

관리자에게 확인을 요청하세요."""
            
            self.send_telegram_message(error_message)
            print(f"❌ Data collection failed: {e}")
            raise

if __name__ == "__main__":
    scraper = ESGFundScraper()
    asyncio.run(scraper.run())
