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
import base64
from io import BytesIO

class ESGFundScraper:
    def __init__(self):
        self.base_url = "https://www.fundguide.net/hkcenter/esg"
        self.telegram_bot_token = os.environ.get('TELCO_NEWS_TOKEN')
        self.telegram_chat_id = os.environ.get('TELCO_NEWS_TESTER')
        
    def display_image_info(self, image_path, description=""):
        """이미지 정보를 표시하고 확인 방법 안내"""
        try:
            # PIL로 이미지 열기
            img = Image.open(image_path)
            width, height = img.size
            file_size = os.path.getsize(image_path)
            
            print(f"\n📸 {description}: {os.path.basename(image_path)}")
            print(f"   📏 크기: {width}x{height} pixels")
            print(f"   💾 파일 크기: {file_size:,} bytes")
            print(f"   📂 경로: {os.path.abspath(image_path)}")
            
            # GitHub Actions 환경에서는 아티팩트로 저장됨을 안내
            if os.environ.get('GITHUB_ACTIONS'):
                print(f"   ☁️  GitHub Actions에서 실행 중 - 아티팩트에서 확인 가능")
            else:
                print(f"   🖱️  로컬에서 파일을 직접 열어서 확인 가능")
                
            # 이미지 히스토그램 간단 분석 (차트 데이터 유무 확인용)
            img_gray = img.convert('L')
            histogram = img_gray.histogram()
            
            # 밝은 픽셀과 어두운 픽셀 비율로 차트 복잡도 추정
            bright_pixels = sum(histogram[200:])  # 밝은 픽셀
            dark_pixels = sum(histogram[:100])    # 어두운 픽셀
            total_pixels = width * height
            
            bright_ratio = bright_pixels / total_pixels * 100
            dark_ratio = dark_pixels / total_pixels * 100
            
            print(f"   🎨 밝은 영역: {bright_ratio:.1f}% | 어두운 영역: {dark_ratio:.1f}%")
            
            # 차트 라인 추정 (중간 밝기 픽셀)
            line_pixels = sum(histogram[100:200])
            line_ratio = line_pixels / total_pixels * 100
            print(f"   📈 예상 차트 라인 영역: {line_ratio:.1f}%")
            
            if line_ratio > 10:
                print(f"   ✅ 충분한 차트 데이터가 감지됨")
            else:
                print(f"   ⚠️  차트 데이터가 부족할 수 있음")
                
        except Exception as e:
            print(f"❌ Error analyzing image: {e}")
    
    def create_image_summary_html(self, screenshot_dir, tab_name):
        """분석된 이미지들의 HTML 요약 파일 생성"""
        try:
            html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Chart Analysis - {tab_name}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .image-section {{ margin: 20px 0; border: 1px solid #ddd; padding: 15px; }}
        .image-section h3 {{ color: #333; margin-top: 0; }}
        .image-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }}
        .image-item {{ text-align: center; }}
        .image-item img {{ max-width: 100%; height: auto; border: 1px solid #ccc; }}
        .image-item p {{ margin: 10px 0; font-size: 14px; color: #666; }}
        .timestamp {{ color: #888; font-size: 12px; }}
    </style>
</head>
<body>
    <h1>ESG Fund Chart Analysis - {tab_name}</h1>
    <p class="timestamp">Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    
    <div class="image-section">
        <h3>📊 Full Chart</h3>
        <div class="image-grid">
            <div class="image-item">
                <img src="{tab_name}_full_chart.png" alt="Full Chart">
                <p>전체 차트 이미지</p>
            </div>
        </div>
    </div>
    
    <div class="image-section">
        <h3>📏 Axis Analysis</h3>
        <div class="image-grid">
            <div class="image-item">
                <img src="{tab_name}_left_y_axis.png" alt="Left Y Axis">
                <p>왼쪽 Y축 (설정액)</p>
            </div>
            <div class="image-item">
                <img src="{tab_name}_right_y_axis.png" alt="Right Y Axis">
                <p>오른쪽 Y축 (수익률)</p>
            </div>
            <div class="image-item">
                <img src="{tab_name}_x_axis.png" alt="X Axis">
                <p>X축 (날짜)</p>
            </div>
        </div>
    </div>
    
    <div class="image-section">
        <h3>🎯 Chart Area & Line Detection</h3>
        <div class="image-grid">
            <div class="image-item">
                <img src="{tab_name}_chart_area_pil.png" alt="Chart Area">
                <p>순수 차트 영역</p>
            </div>
            <div class="image-item">
                <img src="{tab_name}_blue_mask.png" alt="Blue Mask">
                <p>파란색 라인 마스크 (설정액)</p>
            </div>
            <div class="image-item">
                <img src="{tab_name}_red_mask.png" alt="Red Mask">
                <p>빨간색 라인 마스크 (수익률)</p>
            </div>
        </div>
    </div>
    
    <div class="image-section">
        <h3>📋 Analysis Instructions</h3>
        <ol>
            <li><strong>Full Chart:</strong> 전체 차트의 모습을 확인</li>
            <li><strong>Y-Axis:</strong> 왼쪽은 설정액 범위, 오른쪽은 수익률 범위 확인</li>
            <li><strong>X-Axis:</strong> 날짜 범위 및 OCR 정확도 확인</li>
            <li><strong>Chart Area:</strong> 실제 데이터 라인이 있는 영역</li>
            <li><strong>Color Masks:</strong> 각 라인별 색상 분리가 잘 되었는지 확인</li>
        </ol>
        <p><strong>⚠️ 주의사항:</strong> 마스크에서 라인이 명확하지 않으면 색상 범위 조정이 필요합니다.</p>
    </div>
</body>
</html>
"""
            
            html_path = f'{screenshot_dir}/{tab_name}_analysis_summary.html'
            with open(html_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            print(f"📄 HTML 요약 파일 생성: {html_path}")
            print(f"   🌐 브라우저에서 확인: file://{os.path.abspath(html_path)}")
            
            return html_path
            
        except Exception as e:
            print(f"❌ Error creating HTML summary: {e}")
            return None
    
    async def extract_chart_data_with_ocr_analysis(self, page, tab_name):
        """차트 이미지 OCR과 좌표 분석을 통한 데이터 추출"""
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
            
            print(f"📊 Chart area: x={box['x']}, y={box['y']}, width={box['width']}, height={box['height']}")
            
            # 스크린샷 저장 디렉토리
            screenshot_dir = 'chart_analysis'
            if not os.path.exists(screenshot_dir):
                os.makedirs(screenshot_dir)
            
            # 전체 차트 스크린샷 (Y축 레이블 포함하여 더 넓게)
            chart_screenshot_path = f'{screenshot_dir}/{tab_name}_full_chart.png'
            await page.screenshot(
                path=chart_screenshot_path,
                clip={
                    'x': max(0, box['x'] - 100),  # 왼쪽 Y축 포함
                    'y': max(0, box['y'] - 50),   # 위쪽 여백
                    'width': box['width'] + 200,  # 오른쪽 Y축 포함
                    'height': box['height'] + 100 # 아래쪽 X축 포함
                }
            )
            
            print(f"📷 Chart screenshot saved: {chart_screenshot_path}")
            self.display_image_info(chart_screenshot_path, "전체 차트 스크린샷")
            
            # 이미지 전처리 및 분석
            chart_image = Image.open(chart_screenshot_path)
            chart_data = await self.analyze_chart_image(chart_image, tab_name, screenshot_dir)
            
            # HTML 요약 파일 생성
            self.create_image_summary_html(screenshot_dir, tab_name)
            
        except Exception as e:
            print(f"❌ Error in chart OCR analysis: {e}")
            import traceback
            traceback.print_exc()
        
        return chart_data
    
    async def analyze_chart_image(self, chart_image, tab_name, screenshot_dir):
        """차트 이미지 분석 및 데이터 추출"""
        chart_data = {
            'dates': [],
            'setup_amounts': [],
            'returns': []
        }
        
        try:
            # 이미지를 numpy 배열로 변환
            img_array = np.array(chart_image)
            
            # 1. Y축 값들 추출
            y_axis_values = self.extract_y_axis_values(chart_image, screenshot_dir, tab_name)
            
            # 2. X축 날짜들 추출
            x_axis_dates = self.extract_x_axis_dates(chart_image, screenshot_dir, tab_name)
            
            # 3. 차트 라인 좌표 추출
            line_coordinates = self.extract_chart_lines(chart_image, screenshot_dir, tab_name)
            
            # 4. 좌표와 Y축 값을 이용한 실제 값 계산
            if y_axis_values and line_coordinates and x_axis_dates:
                calculated_data = self.calculate_values_from_coordinates(
                    line_coordinates, y_axis_values, x_axis_dates
                )
                chart_data.update(calculated_data)
            
        except Exception as e:
            print(f"❌ Error analyzing chart image: {e}")
        
        return chart_data
    
    def extract_y_axis_values(self, image, screenshot_dir, tab_name):
        """Y축 값들 추출"""
        y_axis_data = {
            'left_axis': [],  # 설정액 (억원)
            'right_axis': [], # 수익률 (%)
            'left_coords': [],
            'right_coords': []
        }
        
        try:
            width, height = image.size
            
            # 왼쪽 Y축 영역 (설정액)
            left_y_axis = image.crop((0, 0, int(width * 0.15), height))
            left_y_path = f'{screenshot_dir}/{tab_name}_left_y_axis.png'
            left_y_axis.save(left_y_path)
            
            print(f"📊 Left Y-axis cropped and saved: {left_y_path}")
            self.display_image_info(left_y_path, "왼쪽 Y축 (설정액)")
            
            # 오른쪽 Y축 영역 (수익률)
            right_y_axis = image.crop((int(width * 0.85), 0, width, height))
            right_y_path = f'{screenshot_dir}/{tab_name}_right_y_axis.png'
            right_y_axis.save(right_y_path)
            
            print(f"📊 Right Y-axis cropped and saved: {right_y_path}")
            self.display_image_info(right_y_path, "오른쪽 Y축 (수익률)")
            
            # OCR로 Y축 값들 추출
            custom_config = r'--oem 3 --psm 6 -c tessedit_char_whitelist=0123456789.,%'
            
            # 왼쪽 Y축 값들 (설정액)
            left_text = pytesseract.image_to_string(left_y_axis, config=custom_config)
            print(f"🔍 Left Y-axis OCR result: {repr(left_text)}")
            
            left_values = []
            for line in left_text.split('\n'):
                # 숫자 패턴 찾기 (쉼표 포함)
                numbers = re.findall(r'[\d,]+\.?\d*', line.strip())
                for num_str in numbers:
                    try:
                        value = float(num_str.replace(',', ''))
                        if value > 1000:  # 설정액은 보통 큰 수
                            left_values.append(value)
                    except:
                        pass
            
            # 오른쪽 Y축 값들 (수익률)
            right_text = pytesseract.image_to_string(right_y_axis, config=custom_config)
            print(f"🔍 Right Y-axis OCR result: {repr(right_text)}")
            
            right_values = []
            for line in right_text.split('\n'):
                numbers = re.findall(r'[\d.]+', line.strip())
                for num_str in numbers:
                    try:
                        value = float(num_str)
                        if 0 <= value <= 10:  # 수익률은 보통 작은 수
                            right_values.append(value)
                    except:
                        pass
            
            y_axis_data['left_axis'] = sorted(set(left_values), reverse=True)  # 위에서 아래로
            y_axis_data['right_axis'] = sorted(set(right_values), reverse=True)
            
            print(f"📈 Extracted left Y-axis values (설정액): {y_axis_data['left_axis']}")
            print(f"📈 Extracted right Y-axis values (수익률): {y_axis_data['right_axis']}")
            
        except Exception as e:
            print(f"❌ Error extracting Y-axis values: {e}")
        
        return y_axis_data
    
    def extract_x_axis_dates(self, image, screenshot_dir, tab_name):
        """X축 날짜들 추출"""
        dates = []
        
        try:
            width, height = image.size
            
            # X축 영역 (아래쪽)
            x_axis = image.crop((0, int(height * 0.85), width, height))
            x_axis_path = f'{screenshot_dir}/{tab_name}_x_axis.png'
            x_axis.save(x_axis_path)
            
            print(f"📅 X-axis cropped and saved: {x_axis_path}")
            self.display_image_info(x_axis_path, "X축 (날짜)")
            
            # OCR로 날짜 추출
            custom_config = r'--oem 3 --psm 6'
            x_text = pytesseract.image_to_string(x_axis, lang='kor+eng', config=custom_config)
            print(f"🔍 X-axis OCR result: {repr(x_text)}")
            
            # 날짜 패턴 찾기
            date_pattern = r'(\d{4})[.\s]+(\d{1,2})[.\s]+(\d{1,2})'
            date_matches = re.findall(date_pattern, x_text)
            
            for year, month, day in date_matches:
                try:
                    # 날짜 형식 통일
                    formatted_date = f"{year}.{month.zfill(2)}.{day.zfill(2)}"
                    if formatted_date not in dates:
                        dates.append(formatted_date)
                except:
                    pass
            
            print(f"📅 Extracted dates: {dates}")
            
        except Exception as e:
            print(f"❌ Error extracting X-axis dates: {e}")
        
        return dates
    
    def extract_chart_lines(self, image, screenshot_dir, tab_name):
        """차트 라인의 좌표 추출"""
        line_coords = {
            'setup_amount_line': [],  # 설정액 라인 좌표
            'return_rate_line': []    # 수익률 라인 좌표
        }
        
        try:
            # OpenCV로 이미지 처리
            img_cv = cv2.cvtColor(np.array(image), cv2.COLOR_RGB2BGR)
            
            # 차트 영역만 추출 (축 제외)
            height, width = img_cv.shape[:2]
            chart_area = img_cv[
                int(height * 0.1):int(height * 0.8),  # Y 범위
                int(width * 0.15):int(width * 0.85)   # X 범위
            ]
            
            # 차트 영역 저장
            chart_area_path = f'{screenshot_dir}/{tab_name}_chart_area.png'
            cv2.imwrite(chart_area_path, chart_area)
            
            print(f"📊 Chart area extracted: {chart_area_path}")
            
            # PIL로 변환해서 콘솔 표시
            chart_area_pil = Image.fromarray(cv2.cvtColor(chart_area, cv2.COLOR_BGR2RGB))
            chart_area_pil.save(f'{screenshot_dir}/{tab_name}_chart_area_pil.png')
            self.display_image_info(f'{screenshot_dir}/{tab_name}_chart_area_pil.png', "순수 차트 영역")
            
            # 라인 색상별로 추출
            # 파란색 계열 (설정액 - 면적 차트의 라인)
            blue_mask = self.create_color_mask(chart_area, 'blue')
            blue_line_coords = self.extract_line_coordinates(blue_mask, 'blue')
            
            # 빨간색/주황색 계열 (수익률 - 라인 차트)
            red_mask = self.create_color_mask(chart_area, 'red')
            red_line_coords = self.extract_line_coordinates(red_mask, 'red')
            
            # 마스크 이미지 저장
            cv2.imwrite(f'{screenshot_dir}/{tab_name}_blue_mask.png', blue_mask * 255)
            cv2.imwrite(f'{screenshot_dir}/{tab_name}_red_mask.png', red_mask * 255)
            
            print(f"🔵 Blue line coordinates (설정액): {len(blue_line_coords)} points")
            print(f"🔴 Red line coordinates (수익률): {len(red_line_coords)} points")
            
            line_coords['setup_amount_line'] = blue_line_coords
            line_coords['return_rate_line'] = red_line_coords
            
        except Exception as e:
            print(f"❌ Error extracting chart lines: {e}")
        
        return line_coords
    
    def create_color_mask(self, image, color_type):
        """특정 색상의 마스크 생성"""
        hsv = cv2.cvtColor(image, cv2.COLOR_BGR2HSV)
        
        if color_type == 'blue':
            # 파란색 범위
            lower_blue = np.array([100, 50, 50])
            upper_blue = np.array([130, 255, 255])
            mask = cv2.inRange(hsv, lower_blue, upper_blue)
        elif color_type == 'red':
            # 빨간색/주황색 범위
            lower_red1 = np.array([0, 50, 50])
            upper_red1 = np.array([10, 255, 255])
            lower_red2 = np.array([170, 50, 50])
            upper_red2 = np.array([180, 255, 255])
            mask1 = cv2.inRange(hsv, lower_red1, upper_red1)
            mask2 = cv2.inRange(hsv, lower_red2, upper_red2)
            mask = cv2.bitwise_or(mask1, mask2)
        else:
            mask = np.zeros(hsv.shape[:2], dtype=np.uint8)
        
        return mask
    
    def extract_line_coordinates(self, mask, color_name):
        """마스크에서 라인 좌표 추출"""
        coordinates = []
        
        try:
            # 형태학적 연산으로 노이즈 제거
            kernel = np.ones((3, 3), np.uint8)
            mask_clean = cv2.morphologyEx(mask, cv2.MORPH_CLOSE, kernel)
            mask_clean = cv2.morphologyEx(mask_clean, cv2.MORPH_OPEN, kernel)
            
            # 컨투어 찾기
            contours, _ = cv2.findContours(mask_clean, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
            
            if contours:
                # 가장 큰 컨투어 선택 (주요 라인)
                largest_contour = max(contours, key=cv2.contourArea)
                
                # X좌표 순으로 정렬된 포인트들 추출
                points = largest_contour.reshape(-1, 2)
                points = points[points[:, 0].argsort()]  # X좌표로 정렬
                
                # 중복 X좌표 제거하고 평균 Y좌표 계산
                unique_points = {}
                for x, y in points:
                    if x not in unique_points:
                        unique_points[x] = []
                    unique_points[x].append(y)
                
                for x in sorted(unique_points.keys()):
                    avg_y = np.mean(unique_points[x])
                    coordinates.append((x, avg_y))
            
        except Exception as e:
            print(f"❌ Error extracting {color_name} line coordinates: {e}")
        
        return coordinates
    
    def calculate_values_from_coordinates(self, line_coordinates, y_axis_values, x_axis_dates):
        """좌표와 Y축 값을 이용한 실제 값 계산"""
        calculated_data = {
            'dates': [],
            'setup_amounts': [],
            'returns': []
        }
        
        try:
            setup_line = line_coordinates.get('setup_amount_line', [])
            return_line = line_coordinates.get('return_rate_line', [])
            left_y_values = y_axis_values.get('left_axis', [])
            right_y_values = y_axis_values.get('right_axis', [])
            
            if not (setup_line or return_line) or not x_axis_dates:
                print("⚠️ Insufficient data for calculation")
                return calculated_data
            
            print(f"🧮 Calculating values from coordinates...")
            print(f"   Setup line points: {len(setup_line)}")
            print(f"   Return line points: {len(return_line)}")
            print(f"   Available dates: {len(x_axis_dates)}")
            print(f"   Left Y values: {left_y_values}")
            print(f"   Right Y values: {right_y_values}")
            
            # 날짜 기준으로 보간
            for i, date in enumerate(x_axis_dates):
                calculated_data['dates'].append(date)
                
                # X 좌표 비율 계산 (날짜 인덱스 기반)
                x_ratio = i / max(1, len(x_axis_dates) - 1)
                
                # 설정액 계산
                if setup_line and left_y_values:
                    setup_amount = self.interpolate_value_from_line(
                        setup_line, x_ratio, left_y_values, 'setup'
                    )
                    calculated_data['setup_amounts'].append(setup_amount)
                else:
                    calculated_data['setup_amounts'].append(None)
                
                # 수익률 계산
                if return_line and right_y_values:
                    return_rate = self.interpolate_value_from_line(
                        return_line, x_ratio, right_y_values, 'return'
                    )
                    calculated_data['returns'].append(return_rate)
                else:
                    calculated_data['returns'].append(None)
            
            print(f"✅ Calculated {len(calculated_data['dates'])} data points")
            
        except Exception as e:
            print(f"❌ Error calculating values: {e}")
        
        return calculated_data
    
    def interpolate_value_from_line(self, line_coords, x_ratio, y_values, value_type):
        """라인 좌표에서 특정 X 비율에 해당하는 Y값 보간"""
        try:
            if not line_coords or not y_values or len(y_values) < 2:
                return None
            
            # X 좌표를 0-1 비율로 정규화
            x_coords = [coord[0] for coord in line_coords]
            y_coords = [coord[1] for coord in line_coords]
            
            if not x_coords:
                return None
            
            x_min, x_max = min(x_coords), max(x_coords)
            target_x = x_min + (x_max - x_min) * x_ratio
            
            # 가장 가까운 두 점 찾기
            closest_idx = 0
            min_distance = abs(x_coords[0] - target_x)
            
            for i, x_coord in enumerate(x_coords):
                distance = abs(x_coord - target_x)
                if distance < min_distance:
                    min_distance = distance
                    closest_idx = i
            
            # Y 좌표 가져오기
            y_coord = y_coords[closest_idx]
            
            # Y축 값 범위와 비교하여 실제 값 계산
            y_min_value = min(y_values)
            y_max_value = max(y_values)
            
            # Y 좌표를 0-1 비율로 정규화 (이미지에서는 위쪽이 0이므로 반전)
            chart_height = max(y_coords) - min(y_coords) if len(set(y_coords)) > 1 else 1
            y_ratio = 1 - ((y_coord - min(y_coords)) / chart_height)
            
            # 실제 값 계산
            actual_value = y_min_value + (y_max_value - y_min_value) * y_ratio
            
            return round(actual_value, 2)
            
        except Exception as e:
            print(f"❌ Error interpolating {value_type} value: {e}")
            return None
    
    async def fetch_tab_data(self, page, tab_value, tab_name):
        """특정 탭의 데이터 가져오기"""
        print(f"🔍 Fetching data for {tab_name}...")
        
        # 탭 클릭
        await page.click(f'button[value="{tab_value}"]')
        await page.wait_for_timeout(3000)  # 데이터 로딩 대기
        
        # 데이터 추출
        data = {
            'tab_name': tab_name,
            'top_funds': await self.parse_top_funds(page),
            'new_funds': await self.parse_new_funds(page),
            'chart_data': await self.extract_chart_data_with_ocr_analysis(page, tab_name)
        }
        
        return data
    
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
                    print(f"✅ Created chart dataframe for {tab_name} with {min_length} rows")
        
        return dfs
    
    def save_to_sheets(self, dfs):
        """Google Sheets에 데이터 저장"""
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
                    print(f"✅ Successfully updated {sheet_name}")
                    
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

각 항목별 수익률 TOP5, 설정액증가 TOP5, 신규펀드, 일별 차트 데이터 수집 완료"""
            
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
