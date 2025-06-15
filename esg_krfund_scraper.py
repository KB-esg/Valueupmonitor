import asyncio
from playwright.async_api import async_playwright
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta
import json
import os
import requests
import time
import re
from PIL import Image, ImageDraw, ImageEnhance
import pytesseract
import numpy as np
import cv2
import glob
from tqdm import tqdm

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
                        returns: [],
                        debug_info: {}
                    };
                    
                    // 방법 1: Highcharts.charts 배열에서 찾기
                    if (typeof Highcharts !== 'undefined' && Highcharts.charts) {
                        for (let i = 0; i < Highcharts.charts.length; i++) {
                            let chart = Highcharts.charts[i];
                            if (chart && chart.container && chart.container.id === 'lineAreaZone') {
                                console.log('Found chart at index:', i);
                                chartData.debug_info.chart_found = true;
                                chartData.debug_info.chart_index = i;
                                
                                // X축 카테고리 (날짜)
                                if (chart.xAxis && chart.xAxis[0]) {
                                    // categories가 없으면 tick labels에서 추출
                                    if (chart.xAxis[0].categories && chart.xAxis[0].categories.length > 0) {
                                        chartData.dates = chart.xAxis[0].categories;
                                        chartData.debug_info.date_source = 'categories';
                                    } else if (chart.xAxis[0].tickPositions) {
                                        // X축 레이블에서 직접 텍스트 추출
                                        const labels = [];
                                        chart.xAxis[0].ticks && Object.values(chart.xAxis[0].ticks).forEach(tick => {
                                            if (tick.label && tick.label.textStr) {
                                                labels.push(tick.label.textStr);
                                            }
                                        });
                                        chartData.dates = labels;
                                        chartData.debug_info.date_source = 'tick_labels';
                                    }
                                    
                                    // X축 정보 디버깅
                                    chartData.debug_info.xaxis_min = chart.xAxis[0].min;
                                    chartData.debug_info.xaxis_max = chart.xAxis[0].max;
                                    chartData.debug_info.xaxis_type = chart.xAxis[0].type;
                                }
                                
                                // 시리즈 데이터
                                if (chart.series && chart.series.length > 0) {
                                    chartData.debug_info.series_count = chart.series.length;
                                    chartData.debug_info.series_info = [];
                                    
                                    chart.series.forEach((series, index) => {
                                        const seriesInfo = {
                                            name: series.name,
                                            visible: series.visible,
                                            data_length: series.data ? series.data.length : 0,
                                            type: series.type
                                        };
                                        chartData.debug_info.series_info.push(seriesInfo);
                                        
                                        if (series.visible && series.data && series.data.length > 0) {
                                            const values = series.data.map(point => {
                                                if (point.y !== undefined) return point.y;
                                                if (point.options && point.options.y !== undefined) return point.options.y;
                                                return null;
                                            });
                                            
                                            // 날짜가 없는 경우 데이터 포인트에서 추출 시도
                                            if (chartData.dates.length === 0 && series.data[0].category) {
                                                chartData.dates = series.data.map(point => point.category || '');
                                                chartData.debug_info.date_source = 'series_categories';
                                            }
                                            
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
                                    chartData.debug_info.yaxis_count = chart.yAxis.length;
                                    chartData.debug_info.yaxis_info = [];
                                    chart.yAxis.forEach((axis, index) => {
                                        chartData.debug_info.yaxis_info.push({
                                            index: index,
                                            min: axis.min,
                                            max: axis.max,
                                            title: axis.options.title ? axis.options.title.text : null
                                        });
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
                                chartData.debug_info.chart_found_method = 'data-attribute';
                                
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
                        if (chartData.dates.length > 0) {
                            chartData.debug_info.date_source = 'svg_labels';
                        }
                    }
                    
                    console.log('Extracted data:', chartData);
                    return chartData;
                }
            ''')
            
            if chart_data:
                print(f"📊 JavaScript extraction result:")
                print(f"   Dates: {len(chart_data.get('dates', []))} items")
                print(f"   Setup amounts: {len(chart_data.get('setup_amounts', []))} items")
                print(f"   Returns: {len(chart_data.get('returns', []))} items")
                
                # 디버그 정보 출력
                if chart_data.get('debug_info'):
                    debug = chart_data['debug_info']
                    print(f"   Debug Info:")
                    print(f"     - Chart found: {debug.get('chart_found', False)}")
                    print(f"     - Date source: {debug.get('date_source', 'unknown')}")
                    print(f"     - Series count: {debug.get('series_count', 0)}")
                    if debug.get('series_info'):
                        for i, series in enumerate(debug['series_info']):
                            print(f"     - Series {i}: {series.get('name', 'unnamed')} ({series.get('data_length', 0)} points)")
                
                # 데이터 검증
                if chart_data.get('dates'):
                    print(f"   First 3 dates: {chart_data['dates'][:3]}...")
                    print(f"   Last 3 dates: {chart_data['dates'][-3:]}...")
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
        """차트 이미지 OCR 분석 및 SVG 경로 분석"""
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
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            chart_path = f'{screenshot_dir}/{tab_name}_chart_{timestamp}.png'
            await chart_container.screenshot(path=chart_path)
            print(f"📷 Chart screenshot saved: {chart_path}")
            
            # SVG 경로 데이터 직접 추출 시도
            svg_data = await self.extract_svg_path_data(page)
            if svg_data and svg_data.get('dates'):
                print(f"✅ SVG path analysis successful: {len(svg_data['dates'])} data points")
                return svg_data
            
            # SVG 분석이 실패하면 이미지 OCR 시도
            # 현재는 구현하지 않음 (필요시 추가 가능)
            
        except Exception as e:
            print(f"❌ Error in chart image analysis: {e}")
        
        return chart_data
    
    async def extract_svg_path_data(self, page):
        """SVG path 요소에서 직접 데이터 포인트 추출"""
        try:
            svg_data = await page.evaluate('''
                () => {
                    const result = {
                        dates: [],
                        setup_amounts: [],
                        returns: [],
                        debug_info: {}
                    };
                    
                    // Highcharts SVG 컨테이너 찾기
                    const chartContainer = document.querySelector('#lineAreaZone');
                    if (!chartContainer) return result;
                    
                    const svg = chartContainer.querySelector('svg.highcharts-root');
                    if (!svg) return result;
                    
                    // 모든 시리즈 path 찾기
                    const seriesPaths = svg.querySelectorAll('path.highcharts-graph');
                    result.debug_info.series_count = seriesPaths.length;
                    
                    if (seriesPaths.length === 0) {
                        // 다른 선택자 시도
                        const allPaths = svg.querySelectorAll('path[d]');
                        result.debug_info.total_paths = allPaths.length;
                        
                        // 차트 데이터를 포함하는 path 찾기 (보통 긴 path)
                        allPaths.forEach((path, index) => {
                            const d = path.getAttribute('d');
                            if (d && d.length > 100) { // 충분히 긴 path만
                                const points = parsePathData(d);
                                if (points.length > 10) { // 충분한 데이터 포인트가 있는 경우
                                    result.debug_info[`path_${index}`] = {
                                        points: points.length,
                                        class: path.getAttribute('class'),
                                        stroke: path.getAttribute('stroke')
                                    };
                                }
                            }
                        });
                    }
                    
                    // X축 레이블에서 날짜 추출
                    const xLabels = svg.querySelectorAll('.highcharts-xaxis-labels text');
                    const xLabelData = [];
                    xLabels.forEach(label => {
                        const text = label.textContent;
                        const x = parseFloat(label.getAttribute('x'));
                        if (text && !isNaN(x)) {
                            xLabelData.push({ text, x });
                        }
                    });
                    xLabelData.sort((a, b) => a.x - b.x);
                    result.dates = xLabelData.map(d => d.text);
                    result.debug_info.x_labels_count = result.dates.length;
                    
                    // Path 데이터 파싱 함수
                    function parsePathData(d) {
                        const points = [];
                        const commands = d.match(/[MLHVCSQTAZmlhvcsqtaz][^MLHVCSQTAZmlhvcsqtaz]*/g);
                        if (!commands) return points;
                        
                        let currentX = 0, currentY = 0;
                        commands.forEach(cmd => {
                            const type = cmd[0];
                            const args = cmd.slice(1).trim().split(/[\s,]+/).map(parseFloat);
                            
                            if (type === 'M' || type === 'L') {
                                currentX = args[0];
                                currentY = args[1];
                                points.push({ x: currentX, y: currentY });
                            } else if (type === 'm' || type === 'l') {
                                currentX += args[0];
                                currentY += args[1];
                                points.push({ x: currentX, y: currentY });
                            }
                        });
                        return points;
                    }
                    
                    // 각 시리즈의 path 데이터 추출
                    seriesPaths.forEach((path, index) => {
                        const d = path.getAttribute('d');
                        if (d) {
                            const points = parsePathData(d);
                            result.debug_info[`series_${index}_points`] = points.length;
                            
                            // SVG 좌표를 실제 값으로 변환하기 위해 Y축 범위 필요
                            // 일단 원시 좌표만 저장
                            if (index === 0) {
                                result.debug_info.series_0_sample = points.slice(0, 5);
                            }
                        }
                    });
                    
                    // Highcharts 인스턴스에서 실제 데이터 포인트 수 확인
                    if (typeof Highcharts !== 'undefined' && Highcharts.charts) {
                        for (let chart of Highcharts.charts) {
                            if (chart && chart.container && chart.container.id === 'lineAreaZone') {
                                if (chart.series) {
                                    chart.series.forEach((series, idx) => {
                                        if (series.points) {
                                            result.debug_info[`highcharts_series_${idx}_points`] = series.points.length;
                                            
                                            // 모든 포인트의 날짜와 값 추출
                                            if (series.visible && series.points.length > 0) {
                                                const values = [];
                                                const dates = [];
                                                
                                                series.points.forEach(point => {
                                                    if (point.y !== undefined && point.y !== null) {
                                                        values.push(point.y);
                                                        // x값이나 category에서 날짜 추출
                                                        if (point.category) {
                                                            dates.push(point.category);
                                                        } else if (point.x !== undefined) {
                                                            dates.push(point.x);
                                                        }
                                                    }
                                                });
                                                
                                                if (idx === 0) {
                                                    result.setup_amounts = values;
                                                    if (dates.length > result.dates.length) {
                                                        result.dates = dates;
                                                    }
                                                } else if (idx === 1) {
                                                    result.returns = values;
                                                }
                                                
                                                result.debug_info[`extracted_from_series_${idx}`] = values.length;
                                            }
                                        }
                                    });
                                }
                                break;
                            }
                        }
                    }
                    
                    return result;
                }
            ''')
            
            if svg_data:
                print(f"📊 SVG path analysis result:")
                print(f"   Dates: {len(svg_data.get('dates', []))} items")
                print(f"   Setup amounts: {len(svg_data.get('setup_amounts', []))} items")
                print(f"   Returns: {len(svg_data.get('returns', []))} items")
                
                if svg_data.get('debug_info'):
                    print(f"   Debug Info: {json.dumps(svg_data['debug_info'], indent=2)}")
                
                return svg_data
                
        except Exception as e:
            print(f"❌ SVG path extraction failed: {e}")
            import traceback
            traceback.print_exc()
        
        return None
    
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
        
        try:
            # 신규 펀드가 없는지 확인
            no_data = await page.query_selector('#newFundZone .nodata')
            if no_data:
                print("   ℹ️ No new funds found")
                return new_funds_data
            
            # 신규 펀드 테이블 찾기
            # 테이블 구조가 다를 수 있으므로 여러 방법 시도
            
            # 방법 1: tr 태그 직접 찾기
            rows = await page.query_selector_all('#newFundZone tr')
            
            # 방법 2: tbody 안의 tr 찾기
            if not rows:
                rows = await page.query_selector_all('#newFundZone tbody tr')
            
            # 방법 3: table 안의 모든 tr 찾기
            if not rows:
                rows = await page.query_selector_all('#newFundZone table tr')
            
            print(f"   📝 Found {len(rows)} rows in new funds zone")
            
            for i, row in enumerate(rows):
                # 헤더 행 건너뛰기
                if i == 0:
                    header_text = await row.inner_text()
                    if '펀드명' in header_text or '운용사' in header_text:
                        continue
                
                cols = await row.query_selector_all('td')
                if len(cols) >= 3:
                    fund_name = await cols[0].inner_text()
                    company = await cols[1].inner_text()
                    setup_date = await cols[2].inner_text()
                    
                    # 빈 데이터 건너뛰기
                    if fund_name.strip() and company.strip():
                        new_funds_data.append({
                            'fund_name': fund_name.strip(),
                            'company': company.strip(),
                            'setup_date': setup_date.strip()
                        })
                        print(f"      - New fund: {fund_name.strip()}")
                elif len(cols) > 0:
                    # 컬럼 수가 다른 경우 디버깅
                    print(f"      ⚠️ Row {i} has {len(cols)} columns")
            
            print(f"   ✅ Parsed {len(new_funds_data)} new funds")
            
        except Exception as e:
            print(f"   ❌ Error parsing new funds: {e}")
            import traceback
            traceback.print_exc()
        
        return new_funds_data
    
    async def wait_for_chart_data_complete(self, page, expected_period):
        """차트 데이터가 완전히 로드될 때까지 대기 (AJAX 대응)"""
        print(f"⏳ Waiting for chart data to load completely for {expected_period}...")
        
        # 예상 데이터 포인트 수 계산 (대략적)
        expected_points = {
            '01': 20,      # 1개월: 약 20영업일
            '03': 60,      # 3개월: 약 60영업일
            '06': 120,     # 6개월: 약 120영업일
            'YTD': 150,    # 연초이후: 가변적
            '12': 250,     # 1년: 약 250영업일
            '36': 750,     # 3년: 약 750영업일
            '60': 1250     # 5년: 약 1250영업일
        }
        
        min_expected = expected_points.get(expected_period, 20)
        max_wait_time = 30  # 최대 30초 대기
        check_interval = 0.5  # 0.5초마다 확인
        
        # tqdm 사용 가능 여부 확인
        try:
            pbar = tqdm(total=100, desc="Loading chart data", unit="%")
            use_tqdm = True
        except:
            print("ℹ️ Progress bar not available, using simple logging")
            use_tqdm = False
            pbar = None
        
        try:
            start_time = time.time()
            previous_count = 0
            stable_count = 0
            
            while (time.time() - start_time) < max_wait_time:
                # 현재 데이터 포인트 수 확인
                data_info = await page.evaluate('''
                    () => {
                        let maxPoints = 0;
                        let loadingStatus = 'checking';
                        
                        if (typeof Highcharts !== 'undefined' && Highcharts.charts) {
                            for (let chart of Highcharts.charts) {
                                if (chart && chart.container && chart.container.id === 'lineAreaZone') {
                                    if (chart.series) {
                                        chart.series.forEach(series => {
                                            if (series.visible) {
                                                // 여러 데이터 소스 확인
                                                const counts = [
                                                    series.processedYData ? series.processedYData.length : 0,
                                                    series.points ? series.points.length : 0,
                                                    series.data ? series.data.length : 0
                                                ];
                                                maxPoints = Math.max(maxPoints, ...counts);
                                            }
                                        });
                                    }
                                    
                                    // 로딩 상태 확인
                                    if (chart.showLoading) {
                                        loadingStatus = 'loading';
                                    } else {
                                        loadingStatus = 'complete';
                                    }
                                    break;
                                }
                            }
                        }
                        
                        // AJAX 로딩 인디케이터 확인
                        const loadingIndicators = document.querySelectorAll('.loading, .spinner, .loader');
                        if (loadingIndicators.length > 0) {
                            loadingStatus = 'loading';
                        }
                        
                        return {
                            points: maxPoints,
                            status: loadingStatus
                        };
                    }
                ''')
                
                current_count = data_info['points']
                loading_status = data_info['status']
                
                # 진행률 계산
                progress = min(100, (current_count / min_expected) * 100)
                
                if use_tqdm and pbar:
                    pbar.n = int(progress)
                    pbar.refresh()
                
                # 로그 출력
                if current_count != previous_count:
                    if not use_tqdm:
                        print(f"📊 Current data points: {current_count} (target: >{min_expected})")
                    stable_count = 0
                else:
                    stable_count += 1
                
                # 데이터가 충분히 로드되었고 안정적인 경우
                if current_count >= min_expected and stable_count > 3:
                    print(f"\n✅ Data loading complete: {current_count} points")
                    break
                
                # 로딩이 완료되었고 데이터가 안정적인 경우
                if loading_status == 'complete' and stable_count > 5:
                    print(f"\n✅ Loading complete with {current_count} points")
                    break
                
                previous_count = current_count
                await page.wait_for_timeout(int(check_interval * 1000))
                
            # 최종 대기
            if use_tqdm and pbar:
                pbar.n = 100
                pbar.refresh()
                pbar.close()
            
            # 네트워크 안정화를 위한 추가 대기
            await page.wait_for_timeout(1000)
            
            # 최종 데이터 수 확인
            final_info = await page.evaluate('''
                () => {
                    let result = { points: 0, series_info: [] };
                    
                    if (typeof Highcharts !== 'undefined' && Highcharts.charts) {
                        for (let chart of Highcharts.charts) {
                            if (chart && chart.container && chart.container.id === 'lineAreaZone') {
                                if (chart.series) {
                                    chart.series.forEach((series, idx) => {
                                        if (series.visible) {
                                            const info = {
                                                name: series.name || `Series ${idx}`,
                                                data: series.data ? series.data.length : 0,
                                                points: series.points ? series.points.length : 0,
                                                processedY: series.processedYData ? series.processedYData.length : 0
                                            };
                                            result.series_info.push(info);
                                            result.points = Math.max(result.points, info.processedY, info.points, info.data);
                                        }
                                    });
                                }
                                break;
                            }
                        }
                    }
                    return result;
                }
            ''')
            
            print(f"\n📊 Final data status:")
            print(f"   Total points: {final_info['points']}")
            for series in final_info.get('series_info', []):
                print(f"   - {series['name']}: data={series['data']}, points={series['points']}, processed={series['processedY']}")
                
        except Exception as e:
            print(f"\n❌ Error waiting for data: {e}")
            if pbar:
                pbar.close()
            
    async def fetch_tab_data(self, page, tab_value, tab_name):
        """특정 탭의 데이터 가져오기 (AJAX 로딩 대응)"""
        print(f"\n🔍 Fetching data for {tab_name}...")
        
        # 탭 클릭
        await page.click(f'button[value="{tab_value}"]')
        await page.wait_for_timeout(2000)  # 초기 로딩 대기
        
        # 기간 선택 (드롭다운에서 선택)
        try:
            # 드롭다운이 존재하는지 확인
            select_exists = await page.query_selector('#selTerm')
            if not select_exists:
                print("⚠️ Period selector not found, using default period")
            else:
                # 현재 선택된 기간 확인
                current_period = await page.evaluate('''
                    () => {
                        const select = document.querySelector('#selTerm');
                        return select ? select.value : null;
                    }
                ''')
                print(f"📅 Current period value: {current_period}")
                
                # 원하는 기간 선택
                if self.collection_period != '01' and current_period != self.collection_period:
                    print(f"📅 Changing period to: {self.period_text_map.get(self.collection_period)} ({self.collection_period})")
                    
                    # JavaScript로 직접 선택 (더 안정적)
                    success = await page.evaluate('''
                        (targetValue) => {
                            const select = document.querySelector('#selTerm');
                            if (!select) return false;
                            
                            // 옵션이 존재하는지 확인
                            const option = Array.from(select.options).find(opt => opt.value === targetValue);
                            if (!option) {
                                console.error('Option not found:', targetValue);
                                return false;
                            }
                            
                            // 값 설정
                            select.value = targetValue;
                            
                            // change 이벤트 트리거
                            const event = new Event('change', { bubbles: true });
                            select.dispatchEvent(event);
                            
                            // jQuery가 있다면 jQuery 이벤트도 트리거
                            if (typeof $ !== 'undefined' && $(select).length) {
                                $(select).trigger('change');
                            }
                            
                            return true;
                        }
                    ''', self.collection_period)
                    
                    if not success:
                        print(f"⚠️ Failed to select period via JavaScript, trying alternative method")
                        # 대체 방법: select_option 사용 (타임아웃 짧게)
                        try:
                            await page.select_option('#selTerm', self.collection_period, timeout=5000)
                        except Exception as e:
                            print(f"⚠️ Alternative selection also failed: {e}")
                    
                    # 선택 후 대기
                    await page.wait_for_timeout(1000)
                    
                    # 네트워크 요청 완료 대기
                    try:
                        await page.wait_for_load_state('networkidle', timeout=10000)
                    except:
                        pass  # 타임아웃 무시
                    
                    # 차트 데이터 완전 로드 대기
                    await self.wait_for_chart_data_complete(page, self.collection_period)
                    
                    # 변경 확인
                    new_period = await page.evaluate('''
                        () => {
                            const select = document.querySelector('#selTerm');
                            if (select) {
                                const selectedOption = select.options[select.selectedIndex];
                                return {
                                    value: select.value,
                                    text: selectedOption ? selectedOption.text : null
                                };
                            }
                            return null;
                        }
                    ''')
                    
                    if new_period:
                        print(f"✅ Period changed to: {new_period['text']} (value: {new_period['value']})")
                    
        except Exception as e:
            print(f"⚠️ Error in period selection: {e}")
            import traceback
            traceback.print_exc()
            print("⚠️ Continuing with default period...")
        
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
            
            # 페이지 설정 개선 (AJAX 대응)
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            )
            page = await context.new_page()
            
            # 네트워크 요청 디버깅 활성화
            if os.environ.get('DEBUG_NETWORK', 'false').lower() == 'true':
                page.on('request', lambda request: print(f"📡 Request: {request.url[:80]}..."))
                page.on('response', lambda response: print(f"📥 Response: {response.status} {response.url[:80]}..."))
            
            try:
                # 페이지 로드
                print("📂 Loading ESG fund page...")
                await page.goto(self.base_url, wait_until='networkidle')
                await page.wait_for_timeout(3000)
                
                # JavaScript 에러 확인
                await page.evaluate('''
                    () => {
                        window.addEventListener('error', (e) => {
                            console.error('JS Error:', e.message);
                        });
                    }
                ''')
                
                # 각 탭 데이터 수집
                tabs = [
                    ('T0370', 'SRI'),
                    ('T0371', 'ESG_주식'),
                    ('T0373', 'ESG_채권')
                ]
                
                # 전체 진행 상황 표시
                print(f"\n📊 Collecting data for {len(tabs)} tabs...")
                for i, (tab_value, tab_name) in enumerate(tabs, 1):
                    print(f"\n{'='*50}")
                    print(f"Tab {i}/{len(tabs)}: {tab_name}")
                    print(f"{'='*50}")
                    
                    data = await self.fetch_tab_data(page, tab_value, tab_name)
                    all_data[tab_name] = data
                    
                    # 탭 간 대기 (서버 부하 방지)
                    if i < len(tabs):
                        await page.wait_for_timeout(2000)
                
            except Exception as e:
                print(f"❌ Error during scraping: {e}")
                await self.send_telegram_message(f"❌ ESG 펀드 데이터 수집 중 오류 발생: {str(e)}")
                raise
            finally:
                await context.close()
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
        
        # 3. 일별 차트 데이터 (모든 탭 통합) - 수정된 부분
        all_chart_data = []
        for tab_name, tab_data in all_data.items():
            chart_data_wrapper = tab_data.get('chart_data', {})
            
            # primary_data를 사용 (JavaScript 데이터 우선, 없으면 OCR 데이터)
            chart_data = chart_data_wrapper.get('primary_data', {})
            
            # primary_data가 없으면 js_data 직접 확인
            if not chart_data or not chart_data.get('dates'):
                chart_data = chart_data_wrapper.get('js_data', {})
            
            if chart_data and chart_data.get('dates'):
                dates = chart_data['dates']
                setup_amounts = chart_data.get('setup_amounts', [])
                returns = chart_data.get('returns', [])
                
                print(f"   📊 Processing {tab_name} chart data:")
                print(f"      - Dates: {len(dates)}")
                print(f"      - Setup amounts: {len(setup_amounts)}")
                print(f"      - Returns: {len(returns)}")
                
                # 데이터 길이 맞추기 - 가장 짧은 길이로
                min_length = len(dates)
                if setup_amounts:
                    min_length = min(min_length, len(setup_amounts))
                if returns:
                    min_length = min(min_length, len(returns))
                
                print(f"      - Using {min_length} data points")
                
                for i in range(min_length):
                    # 숫자 데이터 변환 함수
                    def to_numeric(value):
                        if value is None or value == '':
                            return None
                        try:
                            # 문자열인 경우 숫자로 변환
                            if isinstance(value, str):
                                # 쉼표 제거
                                value = value.replace(',', '')
                                # 앞뒤 따옴표 제거
                                value = value.strip("'\"")
                            return float(value)
                        except (ValueError, TypeError):
                            return None
                    
                    all_chart_data.append({
                        'date': dates[i],
                        'setup_amount': to_numeric(setup_amounts[i]) if i < len(setup_amounts) and setup_amounts else None,
                        'return_rate': to_numeric(returns[i]) if i < len(returns) and returns else None,
                        'tab_type': tab_name,
                        'collection_date': collection_date,
                        'collection_time': collection_time,
                        'collection_period': self.collection_period,
                        'period_text': self.period_text_map.get(self.collection_period)
                    })
            else:
                print(f"   ⚠️ No chart data found for {tab_name}")
        
        if all_chart_data:
            dfs['daily_chart'] = pd.DataFrame(all_chart_data)
            print(f"✅ Created unified chart dataframe with {len(all_chart_data)} rows")
            
            # 데이터 샘플 출력 (디버깅용)
            sample_df = dfs['daily_chart'].head(3)
            print(f"   Sample data:")
            print(f"   {sample_df[['date', 'setup_amount', 'return_rate', 'tab_type']].to_string()}")
        
        # 4. 차트 비교 검증 데이터 (JavaScript vs OCR) - 새로 추가
        all_comparison_data = []
        for tab_name, tab_data in all_data.items():
            chart_data_wrapper = tab_data.get('chart_data', {})
            js_data = chart_data_wrapper.get('js_data', {})
            ocr_data = chart_data_wrapper.get('ocr_data', {})
            
            # JavaScript 데이터
            if js_data and js_data.get('dates'):
                for i, date in enumerate(js_data['dates']):
                    all_comparison_data.append({
                        'date': date,
                        'setup_amount': js_data['setup_amounts'][i] if i < len(js_data.get('setup_amounts', [])) else None,
                        'return_rate': js_data['returns'][i] if i < len(js_data.get('returns', [])) else None,
                        'tab_type': tab_name,
                        'method': 'JavaScript',
                        'collection_time': collection_time
                    })
            
            # OCR 데이터 (현재는 비어있을 것임)
            if ocr_data and ocr_data.get('dates'):
                for i, date in enumerate(ocr_data['dates']):
                    all_comparison_data.append({
                        'date': date,
                        'setup_amount': ocr_data['setup_amounts'][i] if i < len(ocr_data.get('setup_amounts', [])) else None,
                        'return_rate': ocr_data['returns'][i] if i < len(ocr_data.get('returns', [])) else None,
                        'tab_type': tab_name,
                        'method': 'OCR',
                        'collection_time': collection_time
                    })
        
        if all_comparison_data:
            dfs['chart_comparison'] = pd.DataFrame(all_comparison_data)
            print(f"✅ Created chart comparison dataframe with {len(all_comparison_data)} rows")
        
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
                print(f"\n📋 Processing {sheet_name} (df_key: {df_key})...")
                
                try:
                    # 시트 가져오기 또는 생성
                    try:
                        worksheet = spreadsheet.worksheet(sheet_name)
                        print(f"   ✅ Found existing sheet: {sheet_name}")
                    except:
                        print(f"   📝 Creating new sheet: {sheet_name}")
                        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=5000, cols=20)
                    
                    if df_key == 'daily_chart':
                        # 일별 차트 - 기존 데이터 보존하면서 새 데이터만 추가
                        try:
                            existing_data = worksheet.get_all_records()
                            print(f"   📊 Existing data in daily chart: {len(existing_data)} rows")
                        except Exception as e:
                            print(f"   ⚠️ Error reading existing data: {e}")
                            existing_data = []
                        
                        if existing_data:
                            existing_df = pd.DataFrame(existing_data)
                            
                            # 기존 데이터에 없는 새 데이터만 필터링
                            merge_keys = ['date', 'tab_type', 'collection_period']
                            
                            # 비교를 위한 키 생성
                            existing_keys = set()
                            for _, row in existing_df.iterrows():
                                key = '|'.join([str(row.get(k, '')) for k in merge_keys])
                                existing_keys.add(key)
                            
                            new_rows_list = []
                            for _, row in df.iterrows():
                                key = '|'.join([str(row.get(k, '')) for k in merge_keys])
                                if key not in existing_keys:
                                    new_rows_list.append(row.to_dict())
                            
                            if new_rows_list:
                                new_rows = pd.DataFrame(new_rows_list)
                                print(f"   📝 Found {len(new_rows)} new rows to add")
                                
                                # 차트 참조 보존 모드 확인
                                preserve_refs = os.environ.get('PRESERVE_CHART_REFS', 'true').lower() == 'true'
                                
                                if preserve_refs:
                                    # 새 데이터를 끝에 추가 (차트 참조 안전)
                                    # 새 데이터를 날짜 내림차순으로 정렬
                                    new_rows = new_rows.sort_values(
                                        by=['date', 'tab_type'], 
                                        ascending=[False, True]
                                    )
                                    
                                    # 새 행 추가 - 숫자 형식 보존
                                    new_values = []
                                    for _, row in new_rows.iterrows():
                                        row_values = []
                                        for col in existing_df.columns:
                                            value = row.get(col, '')
                                            # setup_amount와 return_rate는 숫자로 유지
                                            if col in ['setup_amount', 'return_rate'] and value != '':
                                                row_values.append(value)  # 숫자 그대로
                                            else:
                                                row_values.append(str(value))  # 나머지는 문자열
                                        new_values.append(row_values)
                                    
                                    if new_values:
                                        # USER_ENTERED로 변경하여 Google Sheets가 타입을 자동 인식
                                        worksheet.append_rows(new_values, value_input_option='USER_ENTERED')
                                        print(f"   ✅ Appended {len(new_rows)} new rows (preserving chart references)")
                                        print(f"   ℹ️ Total rows now: {len(existing_data) + len(new_rows)}")
                                else:
                                    # 전체 재정렬 모드
                                    print(f"   🔄 Re-sorting entire dataset")
                                    combined_df = pd.concat([existing_df, new_rows], ignore_index=True)
                                    combined_df = combined_df.sort_values(
                                        by=['date', 'tab_type'], 
                                        ascending=[False, True]
                                    )
                                    
                                    # 전체 데이터 다시 쓰기 - 숫자 형식 보존
                                    worksheet.clear()
                                    
                                    # 헤더
                                    headers = combined_df.columns.values.tolist()
                                    
                                    # 데이터 - 숫자 컬럼은 숫자로 유지
                                    values = [headers]
                                    for _, row in combined_df.iterrows():
                                        row_values = []
                                        for col in combined_df.columns:
                                            value = row[col]
                                            # NaN 처리
                                            if pd.isna(value):
                                                row_values.append('')
                                            # setup_amount와 return_rate는 숫자로
                                            elif col in ['setup_amount', 'return_rate']:
                                                row_values.append(value)  # 숫자 그대로
                                            else:
                                                row_values.append(str(value))
                                        values.append(row_values)
                                    
                                    worksheet.update(values, value_input_option='USER_ENTERED')
                                    print(f"   ✅ Daily chart updated with {len(combined_df)} total rows")
                            else:
                                print(f"   ℹ️ No new data to add")
                        else:
                            # 첫 데이터인 경우
                            print(f"   📝 First time data - creating new sheet content")
                            combined_df = df
                            combined_df = combined_df.sort_values(
                                by=['date', 'tab_type'], 
                                ascending=[False, True]
                            )
                            
                            # 헤더
                            headers = combined_df.columns.values.tolist()
                            
                            # 데이터 - 숫자 컬럼은 숫자로 유지
                            values = [headers]
                            for _, row in combined_df.iterrows():
                                row_values = []
                                for col in combined_df.columns:
                                    value = row[col]
                                    # NaN 처리
                                    if pd.isna(value):
                                        row_values.append('')
                                    # setup_amount와 return_rate는 숫자로
                                    elif col in ['setup_amount', 'return_rate']:
                                        row_values.append(value)  # 숫자 그대로
                                    else:
                                        row_values.append(str(value))
                                values.append(row_values)
                            
                            worksheet.update(values, value_input_option='USER_ENTERED')
                            print(f"   ✅ Created daily chart with {len(combined_df)} rows")
                            
                    elif df_key == 'chart_comparison':
                        # 비교 검증 데이터는 매번 새로 쓰기
                        print(f"   🔄 Updating chart comparison data")
                        combined_df = df
                        combined_df = combined_df.sort_values(
                            by=['date', 'tab_type', 'method'], 
                            ascending=[False, True, True]
                        )
                        
                        # 시트 업데이트
                        worksheet.clear()
                        combined_df = combined_df.fillna('')
                        values = [combined_df.columns.values.tolist()] + combined_df.values.tolist()
                        
                        for i in range(len(values)):
                            for j in range(len(values[i])):
                                values[i][j] = str(values[i][j])
                        
                        worksheet.update(values)
                        print(f"   ✅ Chart comparison updated with {len(combined_df)} rows")
                        
                    else:
                        # TOP5와 신규펀드는 기존 로직 유지
                        print(f"   📊 Processing {df_key} data...")
                        existing_data = worksheet.get_all_records()
                        
                        if existing_data:
                            existing_df = pd.DataFrame(existing_data)
                            # 중복 제거를 위한 키 설정
                            if df_key == 'top_funds':
                                key_cols = ['collection_date', 'tab_type', 'type', 'rank']
                            elif df_key == 'new_funds':
                                key_cols = ['collection_date', 'tab_type', 'fund_name']
                            else:
                                key_cols = list(df.columns)  # 기본값
                            
                            # 중복 제거 후 결합
                            combined_df = pd.concat([df, existing_df], ignore_index=True)
                            combined_df = combined_df.drop_duplicates(subset=key_cols, keep='first')
                            # 최신 데이터가 위로 오도록 정렬
                            combined_df = combined_df.sort_values(
                                by=['collection_date', 'tab_type'], 
                                ascending=[False, True]
                            )
                            print(f"   📊 Combined data: {len(df)} new + {len(existing_df)} existing = {len(combined_df)} total")
                        else:
                            combined_df = df
                            print(f"   📝 First time data: {len(combined_df)} rows")
                        
                        # 데이터 쓰기
                        worksheet.clear()
                        combined_df = combined_df.fillna('')
                        values = [combined_df.columns.values.tolist()] + combined_df.values.tolist()
                        
                        for i in range(len(values)):
                            for j in range(len(values[i])):
                                values[i][j] = str(values[i][j])
                        
                        worksheet.update(values)
                        print(f"   ✅ {sheet_name} updated successfully")
                    
                    updated_sheets.append(sheet_name)
                    
                except Exception as e:
                    print(f"❌ Error updating {sheet_name}: {e}")
                    import traceback
                    traceback.print_exc()
                    
            return updated_sheets
            
        except Exception as e:
            print(f"❌ Error in save_to_sheets: {e}")
            import traceback
            traceback.print_exc()
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
    
    def cleanup_old_files(self):
        """24시간 이상 된 파일 삭제"""
        directories = ['data_backup', 'chart_analysis']
        deleted_count = 0
        
        for directory in directories:
            if os.path.exists(directory):
                now = datetime.now()
                cutoff_time = now - timedelta(hours=24)
                
                # 디렉토리 내 파일 확인
                for filename in os.listdir(directory):
                    filepath = os.path.join(directory, filename)
                    
                    # 파일인 경우만 처리
                    if os.path.isfile(filepath):
                        # 파일 수정 시간 확인
                        file_time = datetime.fromtimestamp(os.path.getmtime(filepath))
                        
                        if file_time < cutoff_time:
                            try:
                                os.remove(filepath)
                                deleted_count += 1
                                print(f"🗑️ Deleted old file: {filepath}")
                            except Exception as e:
                                print(f"❌ Error deleting {filepath}: {e}")
        
        if deleted_count > 0:
            print(f"✅ Cleaned up {deleted_count} old files")
        
        return deleted_count
    
    def calculate_fund_metrics(self, dfs):
        """각 펀드 유형별 설정액 증감률과 주간 수익률 계산"""
        metrics = {}
        
        if 'daily_chart' not in dfs or dfs['daily_chart'].empty:
            return metrics
        
        df = dfs['daily_chart'].copy()
        
        # 날짜를 datetime으로 변환
        df['date'] = pd.to_datetime(df['date'])
        
        # 각 탭별로 계산
        for tab_type in ['SRI', 'ESG_주식', 'ESG_채권']:
            tab_df = df[df['tab_type'] == tab_type].copy()
            
            if tab_df.empty:
                continue
            
            # 날짜순 정렬 (오래된 것부터)
            tab_df = tab_df.sort_values('date')
            
            # 가장 최근 데이터
            latest = tab_df.iloc[-1]
            
            # 1주일 전 데이터 찾기
            one_week_ago = latest['date'] - pd.Timedelta(days=7)
            week_ago_data = tab_df[tab_df['date'] <= one_week_ago]
            
            if not week_ago_data.empty:
                week_ago = week_ago_data.iloc[-1]
                
                # 설정액 증감률 계산
                if pd.notna(latest['setup_amount']) and pd.notna(week_ago['setup_amount']) and week_ago['setup_amount'] != 0:
                    setup_change = ((latest['setup_amount'] - week_ago['setup_amount']) / week_ago['setup_amount']) * 100
                else:
                    setup_change = None
                
                # 주간 수익률 (return_rate의 차이)
                if pd.notna(latest['return_rate']) and pd.notna(week_ago['return_rate']):
                    weekly_return = latest['return_rate'] - week_ago['return_rate']
                else:
                    weekly_return = None
            else:
                setup_change = None
                weekly_return = None
            
            # 탭 이름 매핑
            display_name = {
                'SRI': 'SRI 펀드',
                'ESG_주식': 'ESG 주식형',
                'ESG_채권': 'ESG 채권형'
            }.get(tab_type, tab_type)
            
            metrics[display_name] = {
                'latest_setup_amount': latest['setup_amount'] if pd.notna(latest['setup_amount']) else None,
                'latest_return_rate': latest['return_rate'] if pd.notna(latest['return_rate']) else None,
                'setup_change_pct': setup_change,
                'weekly_return': weekly_return,
                'latest_date': latest['date'].strftime('%Y-%m-%d')
            }
            
            print(f"   📊 {display_name} 지표:")
            print(f"      - 최신 설정액: {latest['setup_amount']:.2f}억원" if pd.notna(latest['setup_amount']) else "      - 최신 설정액: N/A")
            print(f"      - 설정액 증감률: {setup_change:.2f}%" if setup_change is not None else "      - 설정액 증감률: N/A")
            print(f"      - 주간 수익률: {weekly_return:.2f}%" if weekly_return is not None else "      - 주간 수익률: N/A")
        
        return metrics
    
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
    
    def create_summary_html(self, all_data):
        """차트 분석 결과를 HTML로 정리"""
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <title>ESG Fund Chart Analysis - {datetime.now().strftime('%Y-%m-%d %H:%M')}</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                h1, h2 {{ color: #333; }}
                .tab-section {{ margin: 30px 0; padding: 20px; border: 1px solid #ddd; }}
                .chart-image {{ max-width: 100%; margin: 20px 0; }}
                .data-summary {{ background: #f5f5f5; padding: 15px; margin: 10px 0; }}
                .period-info {{ background: #e8f4f8; padding: 10px; margin-bottom: 20px; }}
            </style>
        </head>
        <body>
            <h1>ESG Fund Chart Analysis Report</h1>
            <div class="period-info">
                <strong>Collection Period:</strong> {self.period_text_map.get(self.collection_period)}<br>
                <strong>Generated:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            </div>
        """
        
        for tab_name, tab_data in all_data.items():
            html_content += f"""
            <div class="tab-section">
                <h2>{tab_name}</h2>
                <div class="data-summary">
                    <strong>Top Funds:</strong> {len(tab_data.get('top_funds', []))} items<br>
                    <strong>New Funds:</strong> {len(tab_data.get('new_funds', []))} items<br>
            """
            
            chart_data = tab_data.get('chart_data', {})
            if chart_data:
                primary = chart_data.get('primary_data', {})
                if primary and primary.get('dates'):
                    html_content += f"""
                    <strong>Chart Data Points:</strong> {len(primary.get('dates', []))} dates<br>
                    <strong>Data Source:</strong> {'JavaScript' if chart_data.get('js_data') else 'OCR'}
                    """
            
            html_content += f"""
                </div>
                <img class="chart-image" src="{tab_name}_chart_*.png" alt="{tab_name} Chart">
            </div>
            """
        
        html_content += """
        </body>
        </html>
        """
        
        # HTML 파일 저장
        html_path = 'chart_analysis/analysis_summary.html'
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"📄 Created HTML summary: {html_path}")
        return html_path
    
    async def run(self):
        """전체 프로세스 실행"""
        start_time = time.time()
        print(f"🚀 Starting ESG Fund data collection at {datetime.now()}")
        
        try:
            # 0. 오래된 파일 정리
            deleted_files = self.cleanup_old_files()
            
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
            
            # 6. HTML 요약 생성
            summary_html = self.create_summary_html(all_data)
            
            # 7. 실행 시간 계산
            execution_time = round(time.time() - start_time, 2)
            
            # 8. 상세 통계
            stats = {}
            for key, df in dfs.items():
                if key == 'top_funds':
                    stats['TOP5 펀드'] = f"{len(df)}개 (수익률/설정액증가)"
                elif key == 'new_funds':
                    stats['신규 펀드'] = f"{len(df)}개"
                elif key == 'daily_chart':
                    unique_dates = df['date'].nunique() if 'date' in df.columns else 0
                    stats['차트 데이터'] = f"{unique_dates}일치"
                elif key == 'chart_comparison':
                    stats['비교 검증 데이터'] = f"{len(df)}개"
            
            # 9. 펀드 지표 계산
            fund_metrics = self.calculate_fund_metrics(dfs)
            
            # 10. 성공 메시지 전송
            period_text = self.period_text_map.get(self.collection_period, self.collection_period)
            
            # 기본 메시지
            message = f"""✅ *ESG 펀드 데이터 수집 완료*

📅 수집 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
📊 총 레코드: {total_records}개
📁 업데이트 시트: {len(updated_sheets)}개
🗑️ 정리된 파일: {deleted_files}개
⏱️ 실행 시간: {execution_time}초
📈 수집 기간: {period_text}

*수집 현황:*
{chr(10).join([f"• {k}: {v}" for k, v in stats.items()])}"""

            # 펀드 지표 추가
            if fund_metrics:
                message += "\n\n*📊 주간 펀드 동향:*"
                
                for fund_name, metrics in fund_metrics.items():
                    message += f"\n\n**{fund_name}**"
                    
                    # 설정액 정보
                    if metrics['latest_setup_amount'] is not None:
                        message += f"\n💰 설정액: {metrics['latest_setup_amount']:,.1f}억원"
                        if metrics['setup_change_pct'] is not None:
                            if metrics['setup_change_pct'] > 0:
                                message += f" (📈 +{metrics['setup_change_pct']:.1f}%)"
                            elif metrics['setup_change_pct'] < 0:
                                message += f" (📉 {metrics['setup_change_pct']:.1f}%)"
                            else:
                                message += f" (➡️ 0.0%)"
                    
                    # 수익률 정보
                    if metrics['weekly_return'] is not None:
                        if metrics['weekly_return'] > 0:
                            message += f"\n📊 주간수익률: +{metrics['weekly_return']:.2f}%"
                        else:
                            message += f"\n📊 주간수익률: {metrics['weekly_return']:.2f}%"
                    
                    # 현재 수익률
                    if metrics['latest_return_rate'] is not None:
                        message += f"\n📍 현재수익률: {metrics['latest_return_rate']:.2f}%"
            
            # 신규 펀드 정보 추가
            if 'new_funds' in dfs and not dfs['new_funds'].empty:
                new_funds_df = dfs['new_funds']
                # 오늘 날짜의 신규 펀드만 필터링
                today = datetime.now().strftime('%Y-%m-%d')
                today_new_funds = new_funds_df[new_funds_df['collection_date'] == today]
                
                if not today_new_funds.empty:
                    message += "\n\n*🆕 신규 출시 펀드:*"
                    
                    # 탭 타입별로 그룹화
                    for tab_type in today_new_funds['tab_type'].unique():
                        tab_funds = today_new_funds[today_new_funds['tab_type'] == tab_type]
                        
                        # 탭 이름 표시
                        tab_display = {
                            'SRI': 'SRI',
                            'ESG_주식': 'ESG 주식형',
                            'ESG_채권': 'ESG 채권형'
                        }.get(tab_type, tab_type)
                        
                        message += f"\n\n**[{tab_display}]**"
                        
                        for _, fund in tab_funds.iterrows():
                            message += f"\n• {fund['fund_name']}"
                            message += f"\n  - 운용사: {fund['company']}"
                            message += f"\n  - 설정일: {fund['setup_date']}"
                else:
                    # 이번 주의 신규 펀드 확인 (최근 7일)
                    week_ago = (datetime.now() - pd.Timedelta(days=7)).strftime('%Y-%m-%d')
                    week_new_funds = new_funds_df[new_funds_df['collection_date'] >= week_ago]
                    
                    if not week_new_funds.empty:
                        message += "\n\n*🆕 이번 주 신규 출시 펀드:*"
                        
                        for tab_type in week_new_funds['tab_type'].unique():
                            tab_funds = week_new_funds[week_new_funds['tab_type'] == tab_type]
                            
                            tab_display = {
                                'SRI': 'SRI',
                                'ESG_주식': 'ESG 주식형',
                                'ESG_채권': 'ESG 채권형'
                            }.get(tab_type, tab_type)
                            
                            message += f"\n\n**[{tab_display}]**"
                            
                            # 중복 제거 (펀드명 기준)
                            unique_funds = tab_funds.drop_duplicates(subset=['fund_name'])
                            
                            for _, fund in unique_funds.iterrows():
                                message += f"\n• {fund['fund_name']}"
                                message += f"\n  - 운용사: {fund['company']}"
                                message += f"\n  - 설정일: {fund['setup_date']}"
            
            message += f"\n\n*수집 범위:*\n• SRI 펀드\n• ESG 주식형 펀드\n• ESG 채권형 펀드"
            
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
