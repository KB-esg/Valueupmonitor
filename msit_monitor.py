#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re    
import json
import time
import logging
import asyncio
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import numpy as np
import random

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException

from bs4 import BeautifulSoup
import telegram
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('msit_monitor')

# 전역 설정 변수
CONFIG = {
    'landing_url': "https://www.msit.go.kr",
    'stats_url': "https://www.msit.go.kr/bbs/list.do?sCode=user&mPid=74&mId=99",
    'report_types': [
        "이동전화 및 트래픽 통계",
        "이동전화 및 시내전화 번호이동 현황",
        "유선통신서비스 가입 현황",
        "무선통신서비스 가입 현황", 
        "특수부가통신사업자현황",
        "무선데이터 트래픽 통계",
        "유·무선통신서비스 가입 현황 및 무선데이터 트래픽 통계"
    ],
    'telegram_token': os.environ.get('TELCO_NEWS_TOKEN'),
    'chat_id': os.environ.get('TELCO_NEWS_TESTER'),
    'gspread_creds': os.environ.get('MSIT_GSPREAD_ref'),
    'spreadsheet_id': os.environ.get('MSIT_SPREADSHEET_ID'),
    'spreadsheet_name': os.environ.get('SPREADSHEET_NAME', 'MSIT 통신 통계'),
    'ocr_enabled': os.environ.get('OCR_ENABLED', 'true').lower() in ('true', 'yes', '1', 'y')
}

# 임시 디렉토리 설정
TEMP_DIR = Path("./downloads")
TEMP_DIR.mkdir(exist_ok=True)
SCREENSHOTS_DIR = Path("./screenshots")
SCREENSHOTS_DIR.mkdir(exist_ok=True)

def setup_driver():
    """Selenium WebDriver 설정 (향상된 봇 탐지 회피)"""
    options = Options()
    # 비-headless 모드 실행 (GitHub Actions에서 Xvfb 사용 시)
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    
    # 추가 성능 및 안정성 옵션
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-popup-blocking')
    options.add_argument('--disable-web-security')
    options.add_argument('--blink-settings=imagesEnabled=true')  # 이미지 로드 허용
    
    # WebGL 지문을 숨기기 위한 설정
    options.add_argument('--disable-features=WebglDraftExtensions,WebglDecoderExtensions,WebglExtensionForceEnable,WebglImageChromium,WebglOverlays,WebglProgramCacheControl')
    
    # 캐시 비활성화 (항상 새로운 세션처럼 보이도록)
    options.add_argument('--disable-application-cache')
    options.add_argument('--disable-browser-cache')
    
    # 무작위 사용자 데이터 디렉토리 생성 (추적 방지)
    temp_user_data_dir = f"/tmp/chrome-user-data-{int(time.time())}-{random.randint(1000, 9999)}"
    options.add_argument(f'--user-data-dir={temp_user_data_dir}')
    logger.info(f"임시 사용자 데이터 디렉토리 생성: {temp_user_data_dir}")
    
    # 랜덤 User-Agent 설정
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:96.0) Gecko/20100101 Firefox/96.0",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.55 Safari/537.36"
    ]
    selected_ua = random.choice(user_agents)
    options.add_argument(f"user-agent={selected_ua}")
    logger.info(f"선택된 User-Agent: {selected_ua}")
    
    # 자동화 감지 우회를 위한 옵션
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument("--disable-blink-features=AutomationControlled")
    
    # 불필요한 로그 비활성화
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    
    try:
        # webdriver-manager 사용
        from webdriver_manager.chrome import ChromeDriverManager
        service = Service(ChromeDriverManager().install())
        logger.info("ChromeDriverManager를 통한 드라이버 설치 완료")
    except Exception as e:
        logger.error(f"WebDriver 설정 중 오류: {str(e)}")
        # 기본 경로 사용
        if os.path.exists('/usr/bin/chromedriver'):
            service = Service('/usr/bin/chromedriver')
            logger.info("기본 경로 chromedriver 사용")
        else:
            raise Exception("ChromeDriver를 찾을 수 없습니다")
    
    driver = webdriver.Chrome(service=service, options=options)
    
    # 페이지 로드 타임아웃 증가
    driver.set_page_load_timeout(90)
    
    # Selenium Stealth 적용 (있는 경우)
    try:
        from selenium_stealth import stealth
        stealth(driver,
            languages=["ko-KR", "ko", "en-US", "en"],
            vendor="Google Inc.",
            platform="Win32",
            webgl_vendor="Intel Inc.",
            renderer="Intel Iris OpenGL Engine",
            fix_hairline=True)
        logger.info("Selenium Stealth 적용 완료")
    except ImportError:
        logger.warning("selenium-stealth 라이브러리를 찾을 수 없습니다. 기본 모드로 계속합니다.")
    
    # 추가 스텔스 설정
    try:
        # 웹드라이버 탐지 방지를 위한 JavaScript 실행
        driver.execute_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        logger.info("추가 스텔스 설정 적용 완료")
    except Exception as js_err:
        logger.warning(f"추가 스텔스 설정 적용 중 오류: {str(js_err)}")
    
    return driver

def setup_gspread_client():
    """Google Sheets 클라이언트 초기화"""
    if not CONFIG['gspread_creds']:
        return None
    
    try:
        # 환경 변수에서 자격 증명 파싱
        creds_dict = json.loads(CONFIG['gspread_creds'])
        
        # 임시 파일에 자격 증명 저장
        temp_creds_path = TEMP_DIR / "temp_creds.json"
        with open(temp_creds_path, 'w') as f:
            json.dump(creds_dict, f)
        
        # gspread 클라이언트 설정
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        credentials = ServiceAccountCredentials.from_json_keyfile_name(str(temp_creds_path), scope)
        client = gspread.authorize(credentials)
        
        # 임시 파일 삭제
        os.unlink(temp_creds_path)
        
        # 스프레드시트 접근 테스트
        if CONFIG['spreadsheet_id']:
            try:
                test_sheet = client.open_by_key(CONFIG['spreadsheet_id'])
                logger.info(f"Google Sheets API 연결 확인: {test_sheet.title}")
            except gspread.exceptions.APIError as e:
                if "PERMISSION_DENIED" in str(e):
                    logger.error(f"Google Sheets 권한 오류: {str(e)}")
                else:
                    logger.warning(f"Google Sheets API 오류: {str(e)}")
            except Exception as e:
                logger.warning(f"스프레드시트 접근 테스트 중 오류: {str(e)}")
        
        return client
    except json.JSONDecodeError:
        logger.error("Google Sheets 자격 증명 JSON 파싱 오류")
        return None
    except Exception as e:
        logger.error(f"Google Sheets 클라이언트 초기화 중 오류: {str(e)}")
        return None

def is_telecom_stats_post(title):
    """게시물이 통신 통계 보고서인지 확인"""
    if not title:
        return False
        
    # "(YYYY년 MM월말 기준)" 형식의 날짜 패턴 확인 (더 유연한 정규식)
    date_pattern = r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)'
    has_date_pattern = re.search(date_pattern, title) is not None
    
    if not has_date_pattern:
        return False
    
    # 제목에 보고서 유형이 포함되어 있는지 확인
    contains_report_type = any(report_type in title for report_type in CONFIG['report_types'])
    
    return contains_report_type

def extract_post_id(item):
    """BeautifulSoup 항목에서 게시물 ID 추출"""
    try:
        # 여러 가능한 선택자 시도
        link_elem = item.find('a')
        if not link_elem:
            return None
            
        # onclick 속성에서 ID 추출 시도
        onclick_attr = link_elem.get('onclick', '')
        match = re.search(r"fn_detail\((\d+)\)", onclick_attr)
        if match:
            return match.group(1)
            
        return None
    except Exception as e:
        logger.error(f"게시물 ID 추출 중 에러: {str(e)}")
        return None

def get_post_url(post_id):
    """게시물 ID로부터 URL 생성"""
    if not post_id:
        return None
    return f"https://www.msit.go.kr/bbs/view.do?sCode=user&mId=99&mPid=74&nttSeqNo={post_id}"

def is_in_date_range(date_str, days=4):
    """게시물 날짜가 지정된 범위 내에 있는지 확인"""
    try:
        # 날짜 문자열 정규화
        date_str = date_str.replace(',', ' ').strip()
        
        # 다양한 날짜 형식 시도
        post_date = None
        date_formats = [
            '%Y. %m. %d',  # "YYYY. MM. DD" 형식
            '%b %d %Y',    # "MMM DD YYYY" 형식
            '%Y-%m-%d',    # "YYYY-MM-DD" 형식
            '%Y/%m/%d',    # "YYYY/MM/DD" 형식
            '%d %b %Y',    # "DD MMM YYYY" 형식
        ]
        
        for date_format in date_formats:
            try:
                post_date = datetime.strptime(date_str, date_format).date()
                break
            except ValueError:
                continue
        
        # 정규식으로 시도
        if not post_date:
            match = re.search(r'(\d{4})[.\-\s/]+(\d{1,2})[.\-\s/]+(\d{1,2})', date_str)
            if match:
                year, month, day = map(int, match.groups())
                try:
                    post_date = datetime(year, month, day).date()
                except ValueError:
                    logger.warning(f"날짜 값이 유효하지 않음: {year}-{month}-{day}")
                    return True  # 오류 발생 시 포함으로 처리
        
        if not post_date:
            logger.warning(f"알 수 없는 날짜 형식: {date_str}")
            return True  # 알 수 없는 경우 포함으로 처리
        
        # 날짜 범위 계산 (한국 시간대)
        korea_tz = datetime.now() + timedelta(hours=9)  # UTC에서 KST로
        days_ago = (korea_tz - timedelta(days=days)).date()
        
        logger.info(f"게시물 날짜 확인: {post_date} vs {days_ago} ({days}일 전, 한국 시간 기준)")
        return post_date >= days_ago
        
    except Exception as e:
        logger.error(f"날짜 파싱 에러: {str(e)}")
        return True  # 오류 발생 시 기본적으로 포함

def has_next_page(driver):
    """다음 페이지가 있는지 확인"""
    try:
        # 여러 가능한 선택자 시도
        selectors = [
            "a.page-link[aria-current='page']",
            "a.on[href*='pageIndex']",
            ".pagination .active a"
        ]
        
        current_page = None
        for selector in selectors:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            if elements:
                try:
                    current_page = int(elements[0].text)
                    break
                except ValueError:
                    continue
        
        if not current_page:
            logger.warning("현재 페이지 번호를 찾을 수 없음")
            return False
            
        # 다음 페이지 링크 확인
        next_page_selectors = [
            f"a.page-link[href*='pageIndex={current_page + 1}']",
            f"a[href*='pageIndex={current_page + 1}']",
            ".pagination a.next"
        ]
        
        for selector in next_page_selectors:
            next_page_links = driver.find_elements(By.CSS_SELECTOR, selector)
            if next_page_links:
                return True
                
        return False
        
    except Exception as e:
        logger.error(f"다음 페이지 확인 중 에러: {str(e)}")
        return False

def go_to_next_page(driver):
    """다음 페이지로 이동"""
    try:
        # 현재 페이지 번호 확인
        selectors = [
            "a.page-link[aria-current='page']",
            "a.on[href*='pageIndex']",
            ".pagination .active a"
        ]
        
        current_page = None
        current_page_element = None
        
        for selector in selectors:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            if elements:
                try:
                    current_page = int(elements[0].text)
                    current_page_element = elements[0]
                    break
                except ValueError:
                    continue
        
        if not current_page:
            logger.warning("현재 페이지 번호를 찾을 수 없음")
            return False
            
        # 다음 페이지 링크 확인 및 클릭
        next_page_selectors = [
            f"a.page-link[href*='pageIndex={current_page + 1}']",
            f"a[href*='pageIndex={current_page + 1}']",
            ".pagination a.next"
        ]
        
        for selector in next_page_selectors:
            next_page_links = driver.find_elements(By.CSS_SELECTOR, selector)
            if next_page_links:
                # 현재 페이지 콘텐츠 저장
                try:
                    current_content = driver.find_element(By.CSS_SELECTOR, "div.board_list").get_attribute("innerHTML")
                except:
                    current_content = ""
                
                # 다음 페이지 클릭
                next_page_links[0].click()
                
                # 페이지 변경 대기
                wait = WebDriverWait(driver, 10)
                try:
                    # 콘텐츠 변경 대기
                    if current_content:
                        wait.until(lambda d: d.find_element(By.CSS_SELECTOR, "div.board_list").get_attribute("innerHTML") != current_content)
                    else:
                        # 다른 방식으로 페이지 변경 감지
                        wait.until(EC.staleness_of(current_page_element))
                    
                    logger.info(f"페이지 {current_page}에서 {current_page+1}로 이동 성공")
                    return True
                except TimeoutException:
                    logger.warning("페이지 콘텐츠 변경 감지 실패, 로딩 대기 중...")
                    time.sleep(3)  # 추가 대기
                    
                    # 페이지 번호 변경 확인
                    for selector in selectors:
                        new_page_elements = driver.find_elements(By.CSS_SELECTOR, selector)
                        if new_page_elements and new_page_elements[0].text != str(current_page):
                            logger.info(f"페이지 번호 변경 감지: {current_page} → {new_page_elements[0].text}")
                            return True
                    
                    logger.warning("페이지 변경 확인 실패")
                    return False
        
        logger.warning("다음 페이지 링크를 찾을 수 없음")
        return False
        
    except Exception as e:
        logger.error(f"다음 페이지 이동 중 에러: {str(e)}")
        return False

def take_screenshot(driver, name, crop_area=None):
    """특정 페이지의 스크린샷 저장 (선택적 영역 잘라내기)"""
    try:
        screenshots_dir = Path("./screenshots")
        screenshots_dir.mkdir(exist_ok=True)
        
        screenshot_path = f"screenshots/{name}_{int(time.time())}.png"
        driver.save_screenshot(screenshot_path)
        
        # 특정 영역만 잘라내기 (OCR 개선용)
        if crop_area and CONFIG['ocr_enabled']:
            try:
                from PIL import Image
                
                # 전체 스크린샷 로드
                img = Image.open(screenshot_path)
                
                # 특정 영역 잘라내기 (crop_area는 (x1, y1, x2, y2) 형식)
                if isinstance(crop_area, tuple) and len(crop_area) == 4:
                    cropped_img = img.crop(crop_area)
                    
                    # 크롭된 이미지 저장 (원본 이름에 _crop 추가)
                    crop_path = screenshot_path.replace('.png', '_crop.png')
                    cropped_img.save(crop_path)
                    logger.info(f"크롭된 스크린샷 저장: {crop_path}")
                    
                    # 크롭된 이미지 경로 반환
                    return crop_path
                # 요소 기준 잘라내기 (crop_area는 WebElement)
                elif hasattr(crop_area, 'location') and hasattr(crop_area, 'size'):
                    element = crop_area
                    location = element.location
                    size = element.size
                    
                    left = location['x']
                    top = location['y']
                    right = location['x'] + size['width']
                    bottom = location['y'] + size['height']
                    
                    cropped_img = img.crop((left, top, right, bottom))
                    
                    # 크롭된 이미지 저장
                    crop_path = screenshot_path.replace('.png', '_element_crop.png')
                    cropped_img.save(crop_path)
                    logger.info(f"요소 기준 크롭된 스크린샷 저장: {crop_path}")
                    
                    # 크롭된 이미지 경로 반환
                    return crop_path
            except ImportError:
                logger.warning("PIL 라이브러리가 설치되지 않아 크롭 불가")
            except Exception as crop_err:
                logger.warning(f"이미지 크롭 중 오류: {str(crop_err)}")
        
        logger.info(f"스크린샷 저장: {screenshot_path}")
        return screenshot_path
    except Exception as e:
        logger.error(f"스크린샷 저장 중 오류: {str(e)}")
        return None

def extract_data_from_screenshot(screenshot_path):
    """스크린샷에서 표 형태의 데이터를 추출하는 함수
    
    Args:
        screenshot_path (str): 스크린샷 파일 경로
        
    Returns:
        list: 추출된 데이터프레임 목록
    """
    import os
    import numpy as np
    import pandas as pd
    import cv2
    import pytesseract
    from PIL import Image, ImageEnhance, ImageFilter
    import logging
    
    logger = logging.getLogger('msit_monitor')
    
    try:
        logger.info(f"이미지 파일에서 표 데이터 추출 시작: {screenshot_path}")
        
        # 이미지 로드 및 전처리
        image = cv2.imread(screenshot_path)
        if image is None:
            logger.error(f"이미지를 로드할 수 없습니다: {screenshot_path}")
            return []
        
        # 그레이스케일 변환
        gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        
        # 이미지 향상 (대비 증가)
        _, thresh = cv2.threshold(gray, 150, 255, cv2.THRESH_BINARY_INV)
        
        # 노이즈 제거
        kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (3, 3))
        opening = cv2.morphologyEx(thresh, cv2.MORPH_OPEN, kernel, iterations=1)
        
        # 테이블 경계 감지를 위한 전처리
        dilated = cv2.dilate(opening, kernel, iterations=3)
        
        # 표를 구성하는 선 감지
        # 수직선 감지
        vertical_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (1, np.array(gray).shape[0] // 50))
        vertical_lines = cv2.erode(dilated, vertical_kernel, iterations=3)
        vertical_lines = cv2.dilate(vertical_lines, vertical_kernel, iterations=5)
        
        # 수평선 감지
        horizontal_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (np.array(gray).shape[1] // 50, 1))
        horizontal_lines = cv2.erode(dilated, horizontal_kernel, iterations=3)
        horizontal_lines = cv2.dilate(horizontal_lines, horizontal_kernel, iterations=5)
        
        # 수직선과 수평선 병합
        table_mask = cv2.bitwise_or(vertical_lines, horizontal_lines)
        
        # 셀 경계 찾기
        contours, _ = cv2.findContours(table_mask, cv2.RETR_TREE, cv2.CHAIN_APPROX_SIMPLE)
        
        # 처리된 이미지 저장 (디버깅용)
        cv2.imwrite(f"{screenshot_path}_processed.png", table_mask)
        
        # 테이블 구조가 없는 경우 일반 OCR 시도
        if len(contours) < 10:  # 충분한 셀이 없는 경우
            logger.info("표 구조를 감지하지 못했습니다. 일반 OCR 진행...")
            return extract_text_without_table_structure(screenshot_path)
        
        # 감지된 셀을 정렬하여 테이블 구조 복원
        # 먼저 충분히 큰 셀만 필터링
        min_cell_area = (image.shape[0] * image.shape[1]) / 1000  # 이미지 크기에 비례한 최소 셀 크기
        cell_contours = [cnt for cnt in contours if cv2.contourArea(cnt) > min_cell_area]
        
        # 셀이 충분하지 않으면 일반 OCR 시도
        if len(cell_contours) < 5:
            logger.info(f"감지된 셀이 너무 적습니다 ({len(cell_contours)}). 일반 OCR 진행...")
            return extract_text_without_table_structure(screenshot_path)
        
        # 셀의 바운딩 박스 추출 및 정렬
        bounding_boxes = []
        for cnt in cell_contours:
            x, y, w, h = cv2.boundingRect(cnt)
            bounding_boxes.append((x, y, w, h))
        
        # 셀 위치에 따라 행과 열로 그룹화
        # 첫 번째 단계: y 좌표로 행 그룹화
        y_tolerance = image.shape[0] // 40  # 높이의 2.5% 이내면 같은 행으로 간주
        rows = []
        bounding_boxes.sort(key=lambda b: b[1])  # y 좌표로 정렬
        
        current_row = [bounding_boxes[0]]
        current_y = bounding_boxes[0][1]
        
        for box in bounding_boxes[1:]:
            if abs(box[1] - current_y) <= y_tolerance:
                current_row.append(box)
            else:
                rows.append(current_row)
                current_row = [box]
                current_y = box[1]
        
        if current_row:
            rows.append(current_row)
        
        # 각 행 내에서 셀을 x 좌표로 정렬
        for i in range(len(rows)):
            rows[i].sort(key=lambda b: b[0])
        
        # 행과 열 수 결정
        if not rows:
            logger.warning("행 그룹화 실패")
            return extract_text_without_table_structure(screenshot_path)
        
        num_rows = len(rows)
        num_cols = max(len(row) for row in rows)
        
        logger.info(f"감지된 표 구조: {num_rows} 행 x {num_cols} 열")
        
        # OCR로 각 셀의 텍스트 추출
        table_data = []
        for i, row in enumerate(rows):
            row_data = [''] * num_cols  # 빈 셀로 초기화
            
            for j, (x, y, w, h) in enumerate(row):
                if j >= num_cols:  # 열 인덱스가 범위를 벗어나는 경우 건너뛰기
                    continue
                    
                # 이미지에서 셀 영역 추출
                cell_image = gray[y:y+h, x:x+w]
                
                # 셀 이미지가 너무 작으면 건너뛰기
                if cell_image.size == 0 or w < 10 or h < 10:
                    continue
                
                # 셀 이미지 강화 
                _, cell_thresh = cv2.threshold(cell_image, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
                
                # Tesseract OCR 구성 (숫자 및 텍스트를 모두 포함하는 페이지 분할 모드)
                custom_config = r'-c preserve_interword_spaces=1 --oem 1 --psm 6'
                
                # 셀 내용이 주로 숫자인 경우 특수 구성
                if j > 0:  # 첫 번째 열이 아닌 경우 (보통 헤더는 텍스트, 값은 숫자)
                    custom_config = r'-c preserve_interword_spaces=1 --oem 1 --psm 6 -c tessedit_char_whitelist="0123456789,.-% "' 
                
                # OCR 실행
                text = pytesseract.image_to_string(cell_thresh, lang='kor+eng', config=custom_config).strip()
                
                # 공백 및 개행 정리
                text = ' '.join(text.split())
                
                # 추출된 텍스트가 있으면 저장
                if text:
                    row_data[j] = text
            
            table_data.append(row_data)
        
        # Pandas DataFrame 생성
        df = pd.DataFrame(table_data)
        
        # 첫 번째 행이 헤더인지 확인
        if len(table_data) > 1:
            # 데이터 정제
            df = df.replace(r'^\s*$', '', regex=True)  # 공백 셀 정리
            
            # 첫 행이 헤더인지 확인 (모든 값이 있고 숫자가 아닌 경우)
            first_row = df.iloc[0].fillna('')
            if all(first_row) and not any(cell.replace(',', '').replace('.', '').isdigit() for cell in first_row if cell):
                # 첫 행을 헤더로 설정
                headers = first_row.tolist()
                df = df.iloc[1:].reset_index(drop=True)
                df.columns = headers
        
        # 빈 열 제거
        df = df.loc[:, df.notna().any()]
        
        # 결과 저장
        logger.info(f"표 데이터 추출 완료: {df.shape[0]}행 {df.shape[1]}열")
        return [df]
    
    except Exception as e:
        logger.error(f"표 데이터 추출 중 오류: {str(e)}")
        # 오류 발생 시 일반 OCR 시도
        return extract_text_without_table_structure(screenshot_path)



def extract_text_without_table_structure(screenshot_path):
    """
    Improved function to extract text without relying on table structure detection.
    Uses general OCR approach and attempts to organize into a tabular format.
    
    Args:
        screenshot_path: Path to the screenshot file
        
    Returns:
        pandas.DataFrame: DataFrame with extracted text
    """
    try:
        logger.info(f"Starting general OCR extraction from: {screenshot_path}")
        
        # Load and enhance the image
        image = Image.open(screenshot_path)
        
        # Enhance the image
        enhancer = ImageEnhance.Contrast(image)
        enhanced_image = enhancer.enhance(2.0)  # Increase contrast
        
        # Apply OCR with custom configuration
        custom_config = r'-l kor+eng --oem 1 --psm 6'
        text = pytesseract.image_to_string(enhanced_image, config=custom_config)
        
        logger.info(f"Extracted text length: {len(text)}")
        
        # Split into lines
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        
        # Detect potential table structure
        table_data = []
        
        # Try several strategies to determine table structure
        
        # Strategy 1: Split by consistent whitespace patterns
        for line in lines:
            # Check for tab characters
            if '\t' in line:
                parts = [part.strip() for part in line.split('\t')]
                if len(parts) >= 2:
                    table_data.append(parts)
                    continue
            
            # Check for multiple space characters (likely column separators)
            parts = re.split(r'\s{2,}', line)
            if len(parts) >= 2:
                table_data.append(parts)
                continue
            
            # Check for common separators
            for sep in ['|', ';', ',']:
                if sep in line:
                    parts = [part.strip() for part in line.split(sep)]
                    if len(parts) >= 2:
                        table_data.append(parts)
                        break
            
            # If no pattern identified, add as single-column row
            if not table_data or table_data[-1] != [line]:
                table_data.append([line])
        
        # If we didn't find any tabular structure, convert to single-column format
        if not table_data or max(len(row) for row in table_data) < 2:
            logger.info("No clear tabular structure found, creating single-column format")
            table_data = [[line] for line in lines]
        
        # Normalize table structure
        max_cols = max(len(row) for row in table_data)
        normalized_data = []
        
        for row in table_data:
            # Add empty cells to make row length consistent
            normalized_row = row + [''] * (max_cols - len(row))
            normalized_data.append(normalized_row)
        
        # Create DataFrame
        if normalized_data:
            # First row is header
            header = normalized_data[0]
            
            # Clean headers
            clean_headers = []
            for h in header:
                h_str = str(h).strip()
                if not h_str:
                    h_str = f"Column_{len(clean_headers)}"
                clean_headers.append(h_str)
            
            # Create DataFrame
            if len(normalized_data) > 1:
                df = pd.DataFrame(normalized_data[1:], columns=clean_headers)
            else:
                # Only header row, create empty DataFrame with these columns
                df = pd.DataFrame(columns=clean_headers)
            
            # Clean data
            df = df.replace(r'^\s*', '', regex=True)
            
            # Remove empty rows and columns
            df = df.replace('', np.nan)
            df = df.dropna(how='all').reset_index(drop=True)
            df = df.loc[:, ~df.isna().all()]
            df = df.fillna('')  # Convert NaN back to empty string
            
            # Remove duplicate rows
            df = df.drop_duplicates().reset_index(drop=True)
            
            return df
        else:
            # Create a minimal DataFrame with information about the failure
            logger.warning("Failed to extract any structured data")
            return pd.DataFrame({'OCR_Text': [line.strip() for line in lines if line.strip()]})
    
    except Exception as e:
        logger.error(f"Error in general OCR extraction: {str(e)}")
        # Return minimal DataFrame with error info
        return pd.DataFrame({'Error': [f"OCR extraction failed: {str(e)}"]})
        

def execute_javascript(driver, script, async_script=False, max_retries=3, description=""):
    """JavaScript 실행 유틸리티 메서드 (오류 처리 및 재시도 포함)"""
    retry_count = 0
    last_error = None
    
    while retry_count < max_retries:
        try:
            if async_script:
                return driver.execute_async_script(script)
            else:
                return driver.execute_script(script)
        except Exception as e:
            retry_count += 1
            last_error = e
            logger.warning(f"{description} JavaScript 실행 실패 (시도 {retry_count}/{max_retries}): {str(e)}")
            time.sleep(1)  # 재시도 전 대기
    
    logger.error(f"{description} JavaScript 최대 재시도 횟수 초과: {str(last_error)}")
    return None

def reset_browser_context(driver, delete_cookies=True, navigate_to_blank=True):
    """브라우저 컨텍스트 초기화 (쿠키 삭제 및 빈 페이지로 이동)"""
    try:
        # 쿠키 삭제
        if delete_cookies:
            driver.delete_all_cookies()
            logger.info("모든 쿠키 삭제 완료")
            
        # 빈 페이지로 이동
        if navigate_to_blank:
            driver.get("about:blank")
            logger.info("빈 페이지로 이동 완료")
            
        # 로컬 스토리지 및 세션 스토리지 클리어
        try:
            driver.execute_script("localStorage.clear(); sessionStorage.clear();")
            logger.info("로컬 스토리지 및 세션 스토리지 클리어")
        except Exception as js_err:
            logger.warning(f"스토리지 클리어 중 오류: {str(js_err)}")
            
        return True
    except Exception as e:
        logger.error(f"브라우저 컨텍스트 초기화 실패: {str(e)}")
        return False

def parse_page(driver, page_num=1, days_range=4):
    """현재 페이지에서 관련 게시물 파싱 (개선된 버전)"""
    all_posts = []
    telecom_stats_posts = []
    continue_search = True
    
    try:
        # 다양한 로딩 지표로 페이지 로드 대기
        selectors = [
            (By.CLASS_NAME, "board_list"),
            (By.CSS_SELECTOR, ".board_list .toggle"),
            (By.CSS_SELECTOR, "table.board_list tr")
        ]
        
        loaded = False
        for by_type, selector in selectors:
            try:
                WebDriverWait(driver, 15).until(  # 대기 시간 증가
                    EC.presence_of_element_located((by_type, selector))
                )
                loaded = True
                logger.info(f"페이지 로드 감지됨: {selector}")
                break
            except TimeoutException:
                continue
        
        if not loaded:
            logger.error("페이지 로드 시간 초과")
            return [], [], False
        
        # 페이지가 로드되면 약간의 지연 시간 추가
        time.sleep(3)  # 더 긴 지연 시간
        
        # 스크롤을 천천히 내려 모든 요소 로드
        try:
            # 스크롤을 부드럽게 내리기
            execute_javascript(driver, """
                function smoothScroll() {
                    const height = document.body.scrollHeight;
                    const step = Math.floor(height / 10);
                    let i = 0;
                    const timer = setInterval(function() {
                        window.scrollBy(0, step);
                        i++;
                        if (i >= 10) clearInterval(timer);
                    }, 100);
                }
                smoothScroll();
            """, description="페이지 스크롤")
            time.sleep(2)  # 스크롤 완료 대기
            
            # 페이지 맨 위로 돌아가기
            driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(1)
        except Exception as scroll_err:
            logger.warning(f"스크롤 중 오류: {str(scroll_err)}")
        
        # 페이지 소스 저장 (디버깅용)
        try:
            with open(f'page_{page_num}_source.html', 'w', encoding='utf-8') as f:
                f.write(driver.page_source)
            logger.info(f"현재 페이지 소스 저장 완료: page_{page_num}_source.html")
        except Exception as save_err:
            logger.warning(f"페이지 소스 저장 중 오류: {str(save_err)}")
        
        # 스크린샷 저장
        take_screenshot(driver, f"parsed_page_{page_num}")
        
        # BeautifulSoup으로 파싱
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # 게시물 선택자 (다양한 사이트 레이아웃 지원)
        post_selectors = [
            "div.toggle:not(.thead)",
            "table.board_list tr:not(.thead)",
            ".board_list li",
            ".board_list .post-item"
        ]
        
        posts = []
        for selector in post_selectors:
            posts = soup.select(selector)
            if posts:
                logger.info(f"{len(posts)}개 게시물 항목 발견 (선택자: {selector})")
                break
        
        if not posts:
            # DOM에서 직접 시도
            try:
                logger.warning("BeautifulSoup으로 게시물을 찾을 수 없음, Selenium으로 직접 시도")
                direct_posts = []
                for selector in post_selectors:
                    direct_posts = driver.find_elements(By.CSS_SELECTOR, selector)
                    if direct_posts:
                        logger.info(f"Selenium으로 {len(direct_posts)}개 게시물 항목 발견 (선택자: {selector})")
                        break
                        
                if direct_posts:
                    # Selenium 요소를 사용하여 정보 추출
                    for item in direct_posts:
                        try:
                            # 헤더 행 건너뛰기
                            if 'thead' in item.get_attribute('class') or item.tag_name == 'th':
                                continue
                                
                            # 날짜 추출
                            date_sel = [".date", "div.date", "td.date", ".post-date"]
                            date_elem = None
                            for sel in date_sel:
                                try:
                                    date_elem = item.find_element(By.CSS_SELECTOR, sel)
                                    if date_elem:
                                        break
                                except:
                                    continue
                                    
                            if not date_elem:
                                continue
                                
                            date_str = date_elem.text.strip()
                            if not date_str or date_str == '등록일':
                                continue
                                
                            logger.info(f"날짜 문자열 발견: {date_str}")
                            
                            # 게시물이 날짜 범위 내에 있는지 확인
                            if not is_in_date_range(date_str, days=days_range):
                                logger.info(f"날짜 범위 밖의 게시물: {date_str}, 이후 게시물 검색 중단")
                                continue_search = False
                                break
                                
                            # 제목 추출
                            title_sel = ["p.title", ".title", "td.title", ".subject a", "a.nttInfoBtn"]
                            title_elem = None
                            for sel in title_sel:
                                try:
                                    title_elem = item.find_element(By.CSS_SELECTOR, sel)
                                    if title_elem:
                                        break
                                except:
                                    continue
                                    
                            if not title_elem:
                                continue
                                
                            title = title_elem.text.strip()
                            
                            # 제목 요소의 href 또는 클릭 속성에서 ID 추출
                            post_id = None
                            onclick = title_elem.get_attribute('onclick')
                            if onclick:
                                match = re.search(r"fn_detail\((\d+)\)", onclick)
                                if match:
                                    post_id = match.group(1)
                                    
                            if not post_id:
                                # 부모 요소 또는 조상 요소에서 ID 추출 시도
                                parent = item
                                for _ in range(3):  # 최대 3단계 상위까지 확인
                                    parent_onclick = parent.get_attribute('onclick')
                                    if parent_onclick and 'fn_detail' in parent_onclick:
                                        match = re.search(r"fn_detail\((\d+)\)", parent_onclick)
                                        if match:
                                            post_id = match.group(1)
                                            break
                                    try:
                                        parent = parent.find_element(By.XPATH, "..")
                                    except:
                                        break
                                        
                            # 게시물 URL 생성
                            post_url = get_post_url(post_id) if post_id else None
                            
                            # 부서 정보 추출
                            dept_sel = ["dd[id*='td_CHRG_DEPT_NM']", ".dept", "td.dept", ".department"]
                            dept_elem = None
                            for sel in dept_sel:
                                try:
                                    dept_elem = item.find_element(By.CSS_SELECTOR, sel)
                                    if dept_elem:
                                        break
                                except:
                                    continue
                                    
                            dept_text = dept_elem.text.strip() if dept_elem else "부서 정보 없음"
                            
                            # 게시물 정보 딕셔너리 생성
                            post_info = {
                                'title': title,
                                'date': date_str,
                                'department': dept_text,
                                'url': post_url,
                                'post_id': post_id
                            }
                            
                            # 모든 게시물 리스트에 추가
                            all_posts.append(post_info)
                            
                            # 통신 통계 게시물인지 확인
                            if is_telecom_stats_post(title):
                                logger.info(f"통신 통계 게시물 발견: {title}")
                                telecom_stats_posts.append(post_info)
                                
                        except Exception as direct_err:
                            logger.error(f"직접 추출 중 오류: {str(direct_err)}")
                            continue
                else:
                    logger.warning("게시물을 찾을 수 없음")
                    return [], [], False
            except Exception as direct_attempt_err:
                logger.error(f"직접 파싱 시도 중 오류: {str(direct_attempt_err)}")
                return [], [], False
        else:
            # BeautifulSoup으로 찾은 게시물 처리
            for item in posts:
                try:
                    # 헤더 행 건너뛰기
                    if 'thead' in item.get('class', []) or item.name == 'th':
                        continue
                    
                    # 날짜 정보 추출 (여러 선택자 시도)
                    date_selectors = [
                        "div.date[aria-label='등록일']",
                        "div.date",
                        ".date",
                        "td.date",
                        ".post-date"
                    ]
                    
                    date_elem = None
                    for selector in date_selectors:
                        date_elem = item.select_one(selector)
                        if date_elem:
                            break
                            
                    if not date_elem:
                        logger.debug("날짜 요소를 찾을 수 없음, 건너뜀")
                        continue
                        
                    date_str = date_elem.text.strip()
                    if not date_str or date_str == '등록일':
                        continue
                    
                    logger.info(f"날짜 문자열 발견: {date_str}")
                    
                    # 게시물이 날짜 범위 내에 있는지 확인
                    if not is_in_date_range(date_str, days=days_range):
                        logger.info(f"날짜 범위 밖의 게시물: {date_str}, 이후 게시물 검색 중단")
                        continue_search = False
                        break
                    
                    # 제목 및 게시물 ID 추출
                    title_selectors = [
                        "p.title",
                        ".title",
                        "td.title",
                        ".subject a",
                        "a.nttInfoBtn"
                    ]
                    
                    title_elem = None
                    for selector in title_selectors:
                        title_elem = item.select_one(selector)
                        if title_elem:
                            break
                            
                    if not title_elem:
                        logger.debug("제목 요소를 찾을 수 없음, 건너뜀")
                        continue
                        
                    title = title_elem.text.strip()
                    post_id = extract_post_id(item)
                    post_url = get_post_url(post_id)
                    
                    # 부서 정보 추출 (여러 선택자 시도)
                    dept_selectors = [
                        "dd[id*='td_CHRG_DEPT_NM']",
                        ".dept",
                        "td.dept",
                        ".department"
                    ]
                    
                    dept_elem = None
                    for selector in dept_selectors:
                        dept_elem = item.select_one(selector)
                        if dept_elem:
                            break
                            
                    dept_text = dept_elem.text.strip() if dept_elem else "부서 정보 없음"
                    
                    # 게시물 정보 딕셔너리 생성
                    post_info = {
                        'title': title,
                        'date': date_str,
                        'department': dept_text,
                        'url': post_url,
                        'post_id': post_id
                    }
                    
                    # 모든 게시물 리스트에 추가
                    all_posts.append(post_info)
                    
                    # 통신 통계 게시물인지 확인
                    if is_telecom_stats_post(title):
                        logger.info(f"통신 통계 게시물 발견: {title}")
                        telecom_stats_posts.append(post_info)
                        
                except Exception as e:
                    logger.error(f"게시물 파싱 중 에러: {str(e)}")
                    continue
        
        return all_posts, telecom_stats_posts, continue_search
        
    except Exception as e:
        logger.error(f"페이지 파싱 중 에러: {str(e)}")
        return [], [], False

def find_view_link_params(driver, post):
    """게시물에서 바로보기 링크 파라미터 찾기 (클릭 방식 우선)"""
    if not post.get('post_id'):
        logger.error(f"게시물 접근 불가 {post['title']} - post_id 누락")
        return None
    
    logger.info(f"게시물 열기: {post['title']}")
    
    # 현재 URL 저장
    current_url = driver.current_url
    
    # 게시물 목록 페이지로 돌아가기
    try:
        driver.get(CONFIG['stats_url'])
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "board_list"))
        )
        time.sleep(2)  # 추가 대기
    except Exception as e:
        logger.error(f"게시물 목록 페이지 접근 실패: {str(e)}")
        return direct_access_view_link_params(driver, post)
    
    # 최대 재시도 횟수
    max_retries = 3
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            # 제목으로 게시물 링크 찾기
            xpath_selectors = [
                f"//p[contains(@class, 'title') and contains(text(), '{post['title'][:20]}')]",
                f"//a[contains(text(), '{post['title'][:20]}')]",
                f"//div[contains(@class, 'toggle') and contains(., '{post['title'][:20]}')]"
            ]
            
            post_link = None
            for selector in xpath_selectors:
                try:
                    elements = driver.find_elements(By.XPATH, selector)
                    if elements:
                        post_link = elements[0]
                        logger.info(f"게시물 링크 발견 (선택자: {selector})")
                        break
                except Exception as find_err:
                    logger.warning(f"선택자로 게시물 찾기 실패: {selector}")
                    continue
            
            if not post_link:
                logger.warning(f"게시물 링크를 찾을 수 없음: {post['title']}")
                
                if attempt < max_retries - 1:
                    logger.info(f"재시도 중... ({attempt+1}/{max_retries})")
                    time.sleep(retry_delay)
                    continue
                else:
                    # 직접 URL 접근 방식으로 대체
                    logger.info("클릭 방식 실패, 직접 URL 접근 방식으로 대체")
                    return direct_access_view_link_params(driver, post)
            
            # 스크린샷 저장 (클릭 전)
            take_screenshot(driver, f"before_click_{post['post_id']}")
            
            # 링크 클릭하여 상세 페이지로 이동
            logger.info(f"게시물 링크 클릭 시도: {post['title']}")
            
            # JavaScript로 클릭 시도 (더 신뢰성 있는 방법)
            try:
                driver.execute_script("arguments[0].click();", post_link)
                logger.info("JavaScript를 통한 클릭 실행")
            except Exception as js_click_err:
                logger.warning(f"JavaScript 클릭 실패: {str(js_click_err)}")
                # 일반 클릭 시도
                post_link.click()
                logger.info("일반 클릭 실행")
            
            # 페이지 로드 대기
            try:
                WebDriverWait(driver, 15).until(
                    lambda d: d.current_url != CONFIG['stats_url']
                )
                logger.info(f"페이지 URL 변경 감지됨: {driver.current_url}")
                time.sleep(3)  # 추가 대기
            except TimeoutException:
                logger.warning("URL 변경 감지 실패")
            
            # 상세 페이지 대기
            wait_elements = [
                (By.CLASS_NAME, "view_head"),
                (By.CLASS_NAME, "view_cont"),
                (By.CSS_SELECTOR, ".bbs_wrap .view"),
                (By.XPATH, "//div[contains(@class, 'view')]")
            ]
            
            element_found = False
            for by_type, selector in wait_elements:
                try:
                    WebDriverWait(driver, 15).until(
                        EC.presence_of_element_located((by_type, selector))
                    )
                    logger.info(f"상세 페이지 로드 완료: {selector} 요소 발견")
                    element_found = True
                    break
                except TimeoutException:
                    continue
            
            if not element_found:
                logger.warning("상세 페이지 로드 실패")
                if attempt < max_retries - 1:
                    continue
                else:
                    # AJAX 방식 시도
                    logger.info("AJAX 방식으로 접근 시도")
                    ajax_result = try_ajax_access(driver, post)
                    if ajax_result:
                        return ajax_result
                    
                    # 직접 URL 접근 방식으로 대체
                    return direct_access_view_link_params(driver, post)
            
            # 스크린샷 저장
            take_screenshot(driver, f"post_view_clicked_{post['post_id']}")
            
            # 바로보기 링크 찾기 (확장된 선택자)
            try:
                # 여러 선택자로 바로보기 링크 찾기
                view_links = []
                
                # 1. 일반적인 '바로보기' 링크
                view_links = driver.find_elements(By.CSS_SELECTOR, "a.view[title='새창 열림']")
                
                # 2. onclick 속성으로 찾기
                if not view_links:
                    all_links = driver.find_elements(By.TAG_NAME, "a")
                    view_links = [link for link in all_links if 'getExtension_path' in (link.get_attribute('onclick') or '')]
                
                # 3. 텍스트로 찾기
                if not view_links:
                    all_links = driver.find_elements(By.TAG_NAME, "a")
                    view_links = [link for link in all_links if '바로보기' in (link.text or '')]
                
                # 4. class 속성으로 찾기
                if not view_links:
                    view_links = driver.find_elements(By.CSS_SELECTOR, "a.attach-file, a.file_link, a.download")
                
                # 5. 제목에 포함된 키워드로 관련 링크 찾기
                if not view_links and '통계' in post['title']:
                    all_links = driver.find_elements(By.TAG_NAME, "a")
                    view_links = [link for link in all_links if 
                                any(ext in (link.get_attribute('href') or '')  
                                   for ext in ['.xls', '.xlsx', '.pdf', '.hwp'])]
                
                if view_links:
                    view_link = view_links[0]
                    onclick_attr = view_link.get_attribute('onclick')
                    href_attr = view_link.get_attribute('href')
                    
                    logger.info(f"바로보기 링크 발견, onclick: {onclick_attr}, href: {href_attr}")
                    
                    # getExtension_path('49234', '1') 형식에서 매개변수 추출
                    if onclick_attr and 'getExtension_path' in onclick_attr:
                        match = re.search(r"getExtension_path\s*\(\s*['\"]([\d]+)['\"]?\s*,\s*['\"]([\d]+)['\"]", onclick_attr)
                        if match:
                            atch_file_no = match.group(1)
                            file_ord = match.group(2)
                            
                            # 날짜 정보 추출
                            date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post['title'])
                            if date_match:
                                year = int(date_match.group(1))
                                month = int(date_match.group(2))
                                
                                return {
                                    'atch_file_no': atch_file_no,
                                    'file_ord': file_ord,
                                    'date': {'year': year, 'month': month},
                                    'post_info': post
                                }
                            
                            return {
                                'atch_file_no': atch_file_no,
                                'file_ord': file_ord,
                                'post_info': post
                            }
                    # 직접 다운로드 URL인 경우 처리
                    elif href_attr and any(ext in href_attr for ext in ['.xls', '.xlsx', '.pdf', '.hwp']):
                        logger.info(f"직접 다운로드 링크 발견: {href_attr}")
                        
                        # 날짜 정보 추출
                        date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post['title'])
                        if date_match:
                            year = int(date_match.group(1))
                            month = int(date_match.group(2))
                            
                            return {
                                'download_url': href_attr,
                                'date': {'year': year, 'month': month},
                                'post_info': post
                            }
                
                # 바로보기 링크를 찾을 수 없는 경우
                logger.warning(f"바로보기 링크를 찾을 수 없음: {post['title']}")
                
                # 게시물 내용 추출 시도
                try:
                    # 다양한 선택자로 내용 찾기
                    content_selectors = [
                        "div.view_cont", 
                        ".view_content", 
                        ".bbs_content",
                        ".bbs_detail_content",
                        "div[class*='view'] div[class*='cont']"
                    ]
                    
                    content = ""
                    for selector in content_selectors:
                        try:
                            content_elem = driver.find_element(By.CSS_SELECTOR, selector)
                            content = content_elem.text if content_elem else ""
                            if content.strip():
                                logger.info(f"게시물 내용 추출 성공 (길이: {len(content)})")
                                break
                        except:
                            continue
                    
                    # 날짜 정보 추출
                    date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post['title'])
                    if date_match:
                        year = int(date_match.group(1))
                        month = int(date_match.group(2))
                        
                        return {
                            'content': content if content else "내용 없음",
                            'date': {'year': year, 'month': month},
                            'post_info': post
                        }
                    
                except Exception as content_err:
                    logger.warning(f"게시물 내용 추출 중 오류: {str(content_err)}")
                
                # 날짜 정보 추출
                date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post['title'])
                if date_match:
                    year = int(date_match.group(1))
                    month = int(date_match.group(2))
                    
                    return {
                        'content': "내용 없음",
                        'date': {'year': year, 'month': month},
                        'post_info': post
                    }
                
                return None
                
            except Exception as e:
                logger.error(f"바로보기 링크 파라미터 추출 중 오류: {str(e)}")
                
                # 오류 발생 시에도 날짜 정보 추출 시도
                date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post['title'])
                if date_match:
                    year = int(date_match.group(1))
                    month = int(date_match.group(2))
                    
                    return {
                        'content': f"오류 발생: {str(e)}",
                        'date': {'year': year, 'month': month},
                        'post_info': post
                    }
                    
                return None
                
        except TimeoutException:
            if attempt < max_retries - 1:
                logger.warning(f"페이지 로드 타임아웃, 재시도 {attempt+1}/{max_retries}")
                time.sleep(retry_delay * 2)  # 대기 시간 증가
            else:
                logger.error(f"{max_retries}번 시도 후 페이지 로드 실패")
                
                # AJAX 방식 시도
                ajax_result = try_ajax_access(driver, post)
                if ajax_result:
                    return ajax_result
                    
                # 날짜 정보 추출 시도
                date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post['title'])
                if date_match:
                    year = int(date_match.group(1))
                    month = int(date_match.group(2))
                    
                    return {
                        'content': "페이지 로드 타임아웃",
                        'date': {'year': year, 'month': month},
                        'post_info': post
                    }
                
                return None
        except Exception as e:
            logger.error(f"게시물 상세 정보 접근 중 오류: {str(e)}")
            
            # 오류 발생 시에도 날짜 정보 추출 시도
            date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post['title'])
            if date_match:
                year = int(date_match.group(1))
                month = int(date_match.group(2))
                
                return {
                    'content': f"접근 오류: {str(e)}",
                    'date': {'year': year, 'month': month},
                    'post_info': post
                }
            
            return None
    
    return None

def try_ajax_access(driver, post):
    """AJAX 방식으로 게시물 데이터 접근 시도"""
    if not post.get('post_id'):
        logger.error(f"AJAX 접근 불가 {post['title']} - post_id 누락")
        return None
        
    try:
        logger.info(f"AJAX 방식으로 게시물 데이터 접근 시도: {post['title']}")
        
        # AJAX 요청 실행
        script = f"""
            return new Promise((resolve, reject) => {{
                const xhr = new XMLHttpRequest();
                xhr.open('GET', '/bbs/ajaxView.do?sCode=user&mId=99&mPid=74&nttSeqNo={post['post_id']}', true);
                xhr.setRequestHeader('Content-Type', 'application/json');
                xhr.onload = function() {{
                    if (this.status >= 200 && this.status < 300) {{
                        resolve(xhr.responseText);
                    }} else {{
                        reject(xhr.statusText);
                    }}
                }};
                xhr.onerror = function() {{
                    reject(xhr.statusText);
                }};
                xhr.send();
            }});
        """
        
        try:
            result = driver.execute_async_script(script)
            logger.info(f"AJAX 호출 결과: {result[:100] if result else '결과 없음'}...")  # 처음 100자만 로깅
            
            if not result:
                logger.warning("AJAX 호출 결과가 없습니다")
                return None
                
            # JSON 파싱 및 데이터 추출
            try:
                data = json.loads(result)
                logger.info(f"AJAX 데이터 파싱 성공: {str(data)[:200]}")
                
                # 날짜 정보 추출
                date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post['title'])
                if date_match:
                    year = int(date_match.group(1))
                    month = int(date_match.group(2))
                    
                    return {
                        'ajax_data': data,
                        'date': {'year': year, 'month': month},
                        'post_info': post
                    }
                
                return {
                    'ajax_data': data,
                    'post_info': post
                }
            except json.JSONDecodeError:
                logger.warning("AJAX 응답을 JSON으로 파싱할 수 없습니다")
                
                # 날짜 정보 추출
                date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post['title'])
                if date_match:
                    year = int(date_match.group(1))
                    month = int(date_match.group(2))
                    
                    return {
                        'content': result[:1000],  # 긴 내용은 제한
                        'date': {'year': year, 'month': month},
                        'post_info': post
                    }
                
        except Exception as script_err:
            logger.warning(f"AJAX 스크립트 실행 오류: {str(script_err)}")
            
        # 대체 AJAX 엔드포인트 시도
        try:
            alternate_script = f"""
                return new Promise((resolve, reject) => {{
                    const xhr = new XMLHttpRequest();
                    xhr.open('GET', '/bbs/getPost.do?nttSeqNo={post['post_id']}', true);
                    xhr.setRequestHeader('Content-Type', 'application/json');
                    xhr.onload = function() {{
                        if (this.status >= 200 && this.status < 300) {{
                            resolve(xhr.responseText);
                        }} else {{
                            reject(xhr.statusText);
                        }}
                    }};
                    xhr.onerror = function() {{
                        reject(xhr.statusText);
                    }};
                    xhr.send();
                }});
            """
            
            result = driver.execute_async_script(alternate_script)
            logger.info(f"대체 AJAX 호출 결과: {result[:100] if result else '결과 없음'}...")
            
            if result:
                # 날짜 정보 추출
                date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post['title'])
                if date_match:
                    year = int(date_match.group(1))
                    month = int(date_match.group(2))
                    
                    return {
                        'content': result[:1000],  # 긴 내용은 제한
                        'date': {'year': year, 'month': month},
                        'post_info': post
                    }
        except Exception as alt_err:
            logger.warning(f"대체 AJAX 시도 오류: {str(alt_err)}")
            
        return None
        
    except Exception as e:
        logger.error(f"AJAX 접근 시도 중 오류: {str(e)}")
        return None
        
def create_placeholder_dataframe(post_info):
    """데이터 추출 실패 시 기본 데이터프레임 생성"""
    try:
        # 날짜 정보 추출
        date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post_info['title'])
        if date_match:
            year = date_match.group(1)
            month = date_match.group(2)
            
            # 보고서 유형 결정
            report_type = determine_report_type(post_info['title'])
            
            # 기본 데이터프레임 생성
            df = pd.DataFrame({
                '구분': [f'{year}년 {month}월 통계'],
                '값': ['데이터를 추출할 수 없습니다'],
                '비고': [f'{post_info["title"]} - 접근 오류'],
                '링크': [post_info.get('url', '링크 없음')]
            })
            
            logger.info(f"플레이스홀더 데이터프레임 생성: {year}년 {month}월 {report_type}")
            return df
            
        return pd.DataFrame({
            '구분': ['알 수 없음'],
            '값': ['데이터 추출 실패'],
            '비고': [f'게시물: {post_info["title"]} - 날짜 정보 없음']
        })  # 날짜 정보가 없으면 최소 정보 포함
        
    except Exception as e:
        logger.error(f"플레이스홀더 데이터프레임 생성 중 오류: {str(e)}")
        # 오류 발생 시도 최소한의 정보 포함
        return pd.DataFrame({
            '구분': ['오류 발생'],
            '업데이트 상태': ['데이터프레임 생성 실패'],
            '비고': [f'오류: {str(e)}']
        })



def access_iframe_with_ocr_fallback(driver, file_params):
    """
    Enhanced function to access iframe content with robust OCR fallback.
    Tries multiple approaches to extract data from document viewer and uses optimized OCR
    as a last resort.
    
    Args:
        driver: Selenium WebDriver instance
        file_params: Dictionary containing file parameters
        
    Returns:
        dict: Dictionary of sheet names to pandas DataFrames
    """
    # First try direct iframe access
    sheets_data = access_iframe_direct(driver, file_params)
    
    # If successful, return the data
    if sheets_data and any(not df.empty for df in sheets_data.values()):
        logger.info(f"Direct iframe access successful: {len(sheets_data)} sheets extracted")
        
        # Validate data quality
        valid_sheets = {}
        for sheet_name, df in sheets_data.items():
            if df is not None and not df.empty:
                # Basic quality check: ensure there's reasonable data
                if df.shape[0] >= 2 and df.shape[1] >= 2:
                    valid_sheets[sheet_name] = df
                    logger.info(f"Sheet {sheet_name} validated: {df.shape[0]} rows, {df.shape[1]} columns")
                else:
                    logger.warning(f"Sheet {sheet_name} appears to have insufficient data: {df.shape}")
        
        # If we have valid sheets, return them
        if valid_sheets:
            return valid_sheets
        else:
            logger.warning("No valid sheets found in direct iframe access results")
    else:
        logger.warning("Direct iframe access failed or returned empty data")
    
    # OCR fallback is disabled, return None
    if not CONFIG['ocr_enabled']:
        logger.info("OCR fallback disabled, skipping")
        return None
    
    # OCR fallback
    logger.info("Falling back to OCR-based extraction")
    
    try:
        # Take multiple screenshots for different parts of the page
        all_sheets = {}
        
        # Take full page screenshot
        full_page_screenshot = f"ocr_full_page_{int(time.time())}.png"
        driver.save_screenshot(full_page_screenshot)
        logger.info(f"Captured full page screenshot: {full_page_screenshot}")
        
        # Try to identify the main content area for targeted OCR
        content_areas = find_content_areas(driver)
        
        if content_areas:
            logger.info(f"Found {len(content_areas)} potential content areas for targeted OCR")
            
            for i, area in enumerate(content_areas):
                try:
                    # Take screenshot of this specific area
                    area_screenshot = capture_element_screenshot(driver, area, f"content_area_{i+1}")
                    
                    if area_screenshot:
                        # Extract data using enhanced OCR
                        ocr_data_list = extract_data_from_screenshot(area_screenshot)
                        
                        if ocr_data_list and any(not df.empty for df in ocr_data_list):
                            logger.info(f"Successfully extracted data from content area {i+1}")
                            for j, df in enumerate(ocr_data_list):
                                if df is not None and not df.empty:
                                    sheet_name = f"OCR_Area{i+1}_Table{j+1}"
                                    all_sheets[sheet_name] = df
                                    logger.info(f"Added {sheet_name}: {df.shape[0]} rows, {df.shape[1]} columns")
                except Exception as area_err:
                    logger.error(f"Error processing content area {i+1}: {str(area_err)}")
        
        # If we still don't have data, try full page OCR
        if not all_sheets:
            logger.info("No data from targeted areas, trying full page OCR")
            
            ocr_data_list = extract_data_from_screenshot(full_page_screenshot)
            
            if ocr_data_list:
                for i, df in enumerate(ocr_data_list):
                    if df is not None and not df.empty:
                        sheet_name = f"OCR_FullPage_Table{i+1}"
                        all_sheets[sheet_name] = df
                        logger.info(f"Added {sheet_name} from full page OCR: {df.shape[0]} rows, {df.shape[1]} columns")
        
        # If we have sheets, return them
        if all_sheets:
            return all_sheets
        
        # Final attempt - try text-only OCR
        logger.info("Table structure detection failed, trying text-only OCR")
        df = extract_text_without_table_structure(full_page_screenshot)
        
        if df is not None and not df.empty:
            all_sheets["OCR_TextOnly"] = df
            logger.info(f"Added text-only OCR data: {df.shape[0]} rows, {df.shape[1]} columns")
            return all_sheets
        
        logger.warning("All OCR attempts failed")
        return None
        
    except Exception as e:
        logger.error(f"Error in OCR fallback process: {str(e)}")
        return None

def access_iframe_direct(driver, file_params):
    """
    Enhanced function to access iframe content and extract data with better handling of
    complex viewer structures and multiple retry strategies.
    
    Args:
        driver: Selenium WebDriver instance
        file_params: Dictionary containing file parameters (atch_file_no, file_ord)
        
    Returns:
        dict: Dictionary of sheet names to pandas DataFrames, or None if extraction fails
    """
    if not file_params or not file_params.get('atch_file_no') or not file_params.get('file_ord'):
        logger.error("File parameters missing or incomplete")
        return None
    
    atch_file_no = file_params['atch_file_no']
    file_ord = file_params['file_ord']
    
    # Construct the view URL
    view_url = f"https://www.msit.go.kr/bbs/documentView.do?atchFileNo={atch_file_no}&fileOrdr={file_ord}"
    logger.info(f"Accessing document view URL: {view_url}")
    
    # Maximum retries for the entire process
    max_retries = 3
    all_sheets = {}
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Document access attempt {attempt+1}/{max_retries}")
            
            # Navigate to the document view page
            driver.get(view_url)
            
            # Take screenshot for debugging
            screenshot_path = f"document_view_{atch_file_no}_{file_ord}_attempt_{attempt+1}.png"
            driver.save_screenshot(screenshot_path)
            logger.info(f"Saved document view screenshot: {screenshot_path}")
            
            # Check for system maintenance page
            if "시스템 점검 안내" in driver.page_source:
                logger.warning("System maintenance page detected")
                if attempt < max_retries - 1:
                    time.sleep(5)  # Wait before retry
                    continue
                else:
                    logger.error("System under maintenance. Cannot access document.")
                    return None
            
            # Check for access denied or error page
            error_indicators = [
                "접근이 거부되었습니다", 
                "Access Denied",
                "권한이 없습니다",
                "Error", 
                "오류가 발생했습니다"
            ]
            
            if any(indicator in driver.page_source for indicator in error_indicators):
                logger.warning("Access denied or error page detected")
                if attempt < max_retries - 1:
                    # Try a different approach - reset cookies and session
                    reset_browser_context(driver)
                    time.sleep(3)
                    continue
                else:
                    logger.error("Access denied after multiple attempts")
                    return None
            
            # Wait for page to load and stabilize
            time.sleep(5)  # Initial wait
            
            # First, check if we landed in a document viewer system
            current_url = driver.current_url
            logger.info(f"Current URL after navigation: {current_url}")
            
            # Check if we're in a document viewer system
            in_doc_viewer = ('SynapDocViewServer' in current_url or 
                            'doc.msit.go.kr' in current_url or
                            'viewer' in current_url.lower())
            
            # Handle window/tab management
            original_window = driver.current_window_handle
            if len(driver.window_handles) > 1:
                logger.info(f"Multiple windows detected: {len(driver.window_handles)}")
                
                # Switch to the new window/tab
                for handle in driver.window_handles:
                    if handle != original_window:
                        driver.switch_to.window(handle)
                        logger.info(f"Switched to new window: {driver.current_url}")
                        break
            
            # Different strategies based on the type of page we landed on
            if in_doc_viewer:
                logger.info("Document viewer detected - attempting viewer extraction")
                sheets_data = extract_from_document_viewer(driver)
                if sheets_data:
                    return sheets_data
            else:
                logger.info("Standard HTML page detected - attempting direct extraction")
                # Try different extraction strategies for regular HTML pages
                
                # Strategy 1: Look for embedded tables directly
                try:
                    tables = pd.read_html(driver.page_source)
                    if tables:
                        logger.info(f"Found {len(tables)} tables directly in the page")
                        # Process and return the tables
                        for i, table in enumerate(tables):
                            if not table.empty:
                                sheet_name = f"Table_{i+1}"
                                all_sheets[sheet_name] = clean_dataframe(table)
                                logger.info(f"Extracted table {i+1} with {table.shape[0]} rows, {table.shape[1]} columns")
                        
                        if all_sheets:
                            return all_sheets
                except Exception as table_err:
                    logger.warning(f"Direct table extraction failed: {str(table_err)}")
                
                # Strategy 2: Look for iframes in the main page
                try:
                    iframes = driver.find_elements(By.TAG_NAME, "iframe")
                    if iframes:
                        logger.info(f"Found {len(iframes)} iframes in the page")
                        for i, iframe in enumerate(iframes):
                            try:
                                logger.info(f"Switching to iframe {i+1}/{len(iframes)}")
                                driver.switch_to.frame(iframe)
                                
                                # Capture iframe content
                                iframe_html = driver.page_source
                                
                                # Extract tables from the iframe content
                                iframe_df = extract_table_from_html(iframe_html)
                                
                                # Switch back to main content
                                driver.switch_to.default_content()
                                
                                if iframe_df is not None and not iframe_df.empty:
                                    sheet_name = f"Frame_{i+1}"
                                    all_sheets[sheet_name] = iframe_df
                                    logger.info(f"Extracted data from iframe {i+1}: {iframe_df.shape[0]} rows, {iframe_df.shape[1]} columns")
                            except Exception as iframe_err:
                                logger.warning(f"Error accessing iframe {i+1}: {str(iframe_err)}")
                                # Switch back to main content on error
                                try:
                                    driver.switch_to.default_content()
                                except:
                                    pass
                        
                        if all_sheets:
                            return all_sheets
                except Exception as frame_err:
                    logger.warning(f"Iframe extraction failed: {str(frame_err)}")
            
            # If we reach here, try a more aggressive javascript-based approach
            logger.info("Attempting JavaScript-based extraction")
            js_extracted = extract_with_javascript(driver)
            if js_extracted:
                return js_extracted
            
            # Final fallback: OCR
            if attempt == max_retries - 1 and CONFIG['ocr_enabled']:
                logger.info("All direct extraction methods failed, falling back to OCR")
                return None  # We'll let the caller handle OCR fallback
                
        except Exception as e:
            logger.error(f"Error during document access attempt {attempt+1}: {str(e)}")
            
            # Try to recover
            try:
                # Reset browser state
                reset_browser_context(driver, delete_cookies=False)
                
                # Switch back to default content if we might be stuck in a frame
                try:
                    driver.switch_to.default_content()
                except:
                    pass
                    
                # Switch back to original window if needed
                if original_window in driver.window_handles:
                    driver.switch_to.window(original_window)
            except:
                pass
                
            if attempt < max_retries - 1:
                time.sleep(3)  # Wait before retry
                continue
    
    return all_sheets if all_sheets else None



def find_content_areas(driver):
    """
    Find potential content areas in the page for targeted OCR.
    
    Args:
        driver: Selenium WebDriver instance
        
    Returns:
        list: List of WebElement objects representing potential content areas
    """
    try:
        content_areas = []
        
        # Try to find the main content area first
        content_selectors = [
            "div.view_cont",
            "div.content",
            "div.main-content",
            "div#content",
            "div.table-container",
            "div[class*='content']",
            "div[class*='view']",
            "div[id*='content']",
            "div[id*='view']"
        ]
        
        for selector in content_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if elements:
                    content_areas.extend(elements)
                    logger.info(f"Found {len(elements)} content areas with selector: {selector}")
            except:
                continue
        
        # Look for tables directly
        try:
            tables = driver.find_elements(By.TAG_NAME, "table")
            if tables:
                content_areas.extend(tables)
                logger.info(f"Found {len(tables)} tables")
        except:
            pass
        
        # Look for iframes that might contain content
        try:
            iframes = driver.find_elements(By.TAG_NAME, "iframe")
            if iframes:
                for iframe in iframes:
                    try:
                        # Try to switch to iframe
                        driver.switch_to.frame(iframe)
                        
                        # Look for content inside iframe
                        inner_tables = driver.find_elements(By.TAG_NAME, "table")
                        if inner_tables:
                            # Capture screenshot of iframe body
                            content_areas.append(driver.find_element(By.TAG_NAME, "body"))
                            logger.info(f"Found content in iframe")
                            
                        # Switch back to main content
                        driver.switch_to.default_content()
                    except:
                        # Switch back on error
                        try:
                            driver.switch_to.default_content()
                        except:
                            pass
        except:
            # Ensure we're back in the main content
            try:
                driver.switch_to.default_content()
            except:
                pass
        
        # If no specific content areas found, use the body
        if not content_areas:
            try:
                body = driver.find_element(By.TAG_NAME, "body")
                content_areas.append(body)
                logger.info("No specific content areas found, using full page body")
            except:
                pass
        
        return content_areas
        
    except Exception as e:
        logger.error(f"Error finding content areas: {str(e)}")
        return []



def capture_element_screenshot(driver, element, name_prefix):
    """
    Capture a screenshot of a specific element.
    
    Args:
        driver: Selenium WebDriver instance
        element: WebElement to capture
        name_prefix: Prefix for the screenshot filename
        
    Returns:
        str: Path to the screenshot file, or None if capture failed
    """
    try:
        # Get element location and size
        location = element.location
        size = element.size
        
        # Take full page screenshot
        temp_screenshot = f"temp_{name_prefix}_{int(time.time())}.png"
        driver.save_screenshot(temp_screenshot)
        
        # Crop to element
        from PIL import Image
        
        # Open the screenshot
        img = Image.open(temp_screenshot)
        
        # Calculate element boundaries
        left = location['x']
        top = location['y']
        right = location['x'] + size['width']
        bottom = location['y'] + size['height']
        
        # Ensure coordinates are within image bounds
        img_width, img_height = img.size
        left = max(0, left)
        top = max(0, top)
        right = min(img_width, right)
        bottom = min(img_height, bottom)
        
        # Crop the image
        if left < right and top < bottom:
            cropped_img = img.crop((left, top, right, bottom))
            
            # Save the cropped image
            element_screenshot = f"{name_prefix}_{int(time.time())}.png"
            cropped_img.save(element_screenshot)
            
            # Clean up temporary screenshot
            try:
                os.remove(temp_screenshot)
            except:
                pass
                
            logger.info(f"Captured element screenshot: {element_screenshot}")
            return element_screenshot
        else:
            logger.warning(f"Invalid crop dimensions: ({left}, {top}, {right}, {bottom})")
            return temp_screenshot
            
    except Exception as e:
        logger.error(f"Error capturing element screenshot: {str(e)}")
        return None

def extract_from_document_viewer(driver):
    """
    Extract data specifically from document viewer interface.
    
    Args:
        driver: Selenium WebDriver instance
        
    Returns:
        dict: Dictionary of sheet names to pandas DataFrames, or None if extraction fails
    """
    all_sheets = {}
    
    try:
        # Wait for viewer to initialize
        time.sleep(5)  # Initial wait for viewer to load
        
        # Take screenshot for debugging
        screenshot_path = f"document_viewer_{int(time.time())}.png"
        driver.save_screenshot(screenshot_path)
        
        # Look for sheet tabs - different viewers have different structures
        sheet_tabs = None
        sheet_selectors = [
            ".sheet-list__sheet-tab",  # Common in Synap
            ".tab-item",
            "ul.tabs > li",
            ".nav-tabs > li",
            "[role='tab']"
        ]
        
        for selector in sheet_selectors:
            try:
                tabs = driver.find_elements(By.CSS_SELECTOR, selector)
                if tabs:
                    sheet_tabs = tabs
                    logger.info(f"Found {len(tabs)} sheet tabs with selector: {selector}")
                    break
            except:
                continue
        
        # Process multiple sheets if found
        if sheet_tabs and len(sheet_tabs) > 0:
            for i, tab in enumerate(sheet_tabs):
                sheet_name = tab.text.strip() if tab.text.strip() else f"Sheet_{i+1}"
                logger.info(f"Processing sheet {i+1}/{len(sheet_tabs)}: {sheet_name}")
                
                # Click on tab if not the first one (first is usually selected by default)
                if i > 0:
                    try:
                        tab.click()
                        time.sleep(3)  # Wait for sheet content to load
                    except Exception as click_err:
                        logger.warning(f"Failed to click sheet tab {i+1}: {str(click_err)}")
                        continue
                
                # Try different iframe selectors
                iframe_found = False
                iframe_selectors = ["#innerWrap", "#viewerFrame", "iframe", "[id*='frame']"]
                
                for selector in iframe_selectors:
                    try:
                        iframe = WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                        )
                        driver.switch_to.frame(iframe)
                        iframe_found = True
                        logger.info(f"Switched to iframe with selector: {selector}")
                        break
                    except:
                        continue
                
                if not iframe_found:
                    logger.warning(f"No iframe found for sheet {sheet_name}, taking screenshot for OCR")
                    # Capture screenshot for this sheet for potential OCR
                    driver.save_screenshot(f"sheet_{sheet_name}_{int(time.time())}.png")
                    continue
                
                # Get iframe content
                try:
                    iframe_html = driver.page_source
                    df = extract_table_from_html(iframe_html)
                    
                    # Switch back to main document
                    driver.switch_to.default_content()
                    
                    if df is not None and not df.empty:
                        all_sheets[sheet_name] = df
                        logger.info(f"Successfully extracted data from sheet '{sheet_name}': {df.shape[0]} rows, {df.shape[1]} columns")
                    else:
                        logger.warning(f"No table data found in sheet '{sheet_name}'")
                except Exception as content_err:
                    logger.error(f"Error extracting content from sheet '{sheet_name}': {str(content_err)}")
                    # Try to switch back to main document on error
                    try:
                        driver.switch_to.default_content()
                    except:
                        pass
        else:
            # No sheet tabs found, try to extract from a single sheet/iframe
            logger.info("No sheet tabs found, attempting single sheet extraction")
            
            # Try different iframe selectors
            iframe_found = False
            iframe_selectors = ["#innerWrap", "#viewerFrame", "iframe", "[id*='frame']"]
            
            for selector in iframe_selectors:
                try:
                    iframe = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )
                    driver.switch_to.frame(iframe)
                    iframe_found = True
                    logger.info(f"Switched to iframe with selector: {selector}")
                    break
                except:
                    continue
            
            if iframe_found:
                try:
                    iframe_html = driver.page_source
                    df = extract_table_from_html(iframe_html)
                    
                    # Switch back to main document
                    driver.switch_to.default_content()
                    
                    if df is not None and not df.empty:
                        all_sheets["Main_Sheet"] = df
                        logger.info(f"Successfully extracted data from single sheet: {df.shape[0]} rows, {df.shape[1]} columns")
                    else:
                        logger.warning("No table data found in single sheet")
                except Exception as content_err:
                    logger.error(f"Error extracting content from single sheet: {str(content_err)}")
                    try:
                        driver.switch_to.default_content()
                    except:
                        pass
            else:
                logger.warning("No iframe found, taking screenshot for OCR")
                driver.save_screenshot(f"single_sheet_{int(time.time())}.png")
        
        return all_sheets if all_sheets else None
        
    except Exception as e:
        logger.error(f"Error in document viewer extraction: {str(e)}")
        try:
            driver.switch_to.default_content()
        except:
            pass
        return None

def extract_with_javascript(driver):
    """
    Use JavaScript to extract table data from the page.
    This can sometimes access data that's not easily accessible through the DOM.
    
    Args:
        driver: Selenium WebDriver instance
        
    Returns:
        dict: Dictionary of sheet names to pandas DataFrames, or None if extraction fails
    """
    try:
        logger.info("Attempting data extraction using JavaScript")
        
        # Script to extract tables from the page
        js_script = """
        function extractTables() {
            // Extract data from standard tables
            const tables = document.querySelectorAll('table');
            const tableData = [];
            
            for (let i = 0; i < tables.length; i++) {
                const table = tables[i];
                const rows = table.querySelectorAll('tr');
                const tableRows = [];
                
                for (let j = 0; j < rows.length; j++) {
                    const row = rows[j];
                    const cells = row.querySelectorAll('th, td');
                    const rowData = [];
                    
                    for (let k = 0; k < cells.length; k++) {
                        const cell = cells[k];
                        // Handle colspan and rowspan
                        const colspan = parseInt(cell.getAttribute('colspan')) || 1;
                        
                        // Add the cell content (repeat for colspan)
                        const cellContent = cell.textContent.trim();
                        for (let c = 0; c < colspan; c++) {
                            rowData.push(cellContent);
                        }
                    }
                    
                    if (rowData.length > 0) {
                        tableRows.push(rowData);
                    }
                }
                
                if (tableRows.length > 0) {
                    tableData.push({
                        id: 'table_' + i,
                        data: tableRows
                    });
                }
            }
            
            // Also try to find div-based tables with grid styling
            const gridContainers = document.querySelectorAll('.grid, [class*="grid"], [role="grid"]');
            for (let i = 0; i < gridContainers.length; i++) {
                const grid = gridContainers[i];
                const gridRows = grid.querySelectorAll('.row, [class*="row"], [role="row"]');
                const gridTableRows = [];
                
                for (let j = 0; j < gridRows.length; j++) {
                    const row = gridRows[j];
                    const cells = row.querySelectorAll('.cell, [class*="cell"], [role="cell"]');
                    const rowData = [];
                    
                    for (let k = 0; k < cells.length; k++) {
                        rowData.push(cells[k].textContent.trim());
                    }
                    
                    if (rowData.length > 0) {
                        gridTableRows.push(rowData);
                    }
                }
                
                if (gridTableRows.length > 0) {
                    tableData.push({
                        id: 'grid_' + i,
                        data: gridTableRows
                    });
                }
            }
            
            return tableData;
        }
        return extractTables();
        """
        
        # Execute the script and get results
        result = driver.execute_script(js_script)
        
        if not result or len(result) == 0:
            logger.warning("No tables found using JavaScript extraction")
            return None
            
        logger.info(f"JavaScript extraction found {len(result)} tables/grids")
        
        # Convert the JavaScript results to DataFrames
        all_sheets = {}
        
        for table_obj in result:
            table_id = table_obj.get('id', f"Table_{len(all_sheets)}")
            table_data = table_obj.get('data', [])
            
            if not table_data or len(table_data) < 2:  # Need at least header + one data row
                logger.warning(f"Table {table_id} has insufficient data, skipping")
                continue
                
            # First row is header
            header = table_data[0]
            data_rows = table_data[1:]
            
            # Clean and normalize headers
            clean_headers = []
            header_counts = {}
            
            for h in header:
                h_str = str(h).strip()
                if not h_str:  # Replace empty headers
                    h_str = f"Column_{len(clean_headers)}"
                    
                if h_str in header_counts:
                    header_counts[h_str] += 1
                    clean_headers.append(f"{h_str}_{header_counts[h_str]}")
                else:
                    header_counts[h_str] = 0
                    clean_headers.append(h_str)
            
            # Create DataFrame
            try:
                df = pd.DataFrame(data_rows, columns=clean_headers)
                df = clean_dataframe(df)  # Apply additional cleaning
                
                if not df.empty:
                    all_sheets[table_id] = df
                    logger.info(f"Created DataFrame for {table_id}: {df.shape[0]} rows, {df.shape[1]} columns")
            except Exception as df_err:
                logger.warning(f"Error creating DataFrame for {table_id}: {str(df_err)}")
        
        return all_sheets if all_sheets else None
        
    except Exception as e:
        logger.error(f"JavaScript extraction error: {str(e)}")
        return None


def clean_dataframe(df):
    """
    Clean and normalize a DataFrame.
    
    Args:
        df: pandas DataFrame to clean
        
    Returns:
        pandas DataFrame: Cleaned DataFrame
    """
    if df is None or df.empty:
        return df
        
    try:
        # Remove completely empty rows and columns
        df = df.replace('', np.nan)
        df = df.dropna(how='all').reset_index(drop=True)
        df = df.loc[:, ~df.isna().all()]
        
        # Fill NaN back with empty strings for text processing
        df = df.fillna('')
        
        # Attempt numeric conversion for likely numeric columns
        for col in df.columns:
            # Skip columns that are clearly not numeric (long text)
            if df[col].astype(str).str.len().mean() > 20:
                continue
                
            # Clean strings that might be numeric
            cleaned = df[col].astype(str).str.replace(',', '').str.replace('%', '')
            
            # Check if column looks numeric
            if cleaned.str.match(r'^-?\d+\.?\d*).mean() > 0.7:  # If >70% match numeric pattern
                try:
                    df[col] = pd.to_numeric(cleaned, errors='coerce')
                    df[col] = df[col].fillna(0)  # Replace NaN from failed conversions
                except:
                    pass  # Keep as string if conversion fails
        
        # Remove duplicate rows
        df = df.drop_duplicates().reset_index(drop=True)
        
        return df
    except Exception as e:
        logger.warning(f"Error cleaning DataFrame: {str(e)}")
        return df


def extract_data_from_screenshot(screenshot_path):
    """
    Enhanced function to extract tabular data from screenshots with better image preprocessing
    and table structure recognition.
    
    Args:
        screenshot_path (str): Path to the screenshot file
        
    Returns:
        list: List of extracted DataFrames
    """
    try:
        import cv2
        import numpy as np
        import pandas as pd
        import pytesseract
        from PIL import Image, ImageEnhance
        
        logger.info(f"Starting enhanced OCR extraction from: {screenshot_path}")
        
        # Load image
        image = cv2.imread(screenshot_path)
        if image is None:
            logger.error(f"Failed to load image: {screenshot_path}")
            return []
        
        # Save original dimensions for reference
        original_height, original_width = image.shape[:2]
        logger.info(f"Original image dimensions: {original_width}x{original_height}")
        
        # Image preprocessing pipeline for better OCR
        processed_images = preprocess_image_for_ocr(image)
        
        # Table structure detection
        tables_info = detect_table_structure(processed_images['table_structure'])
        
        all_dataframes = []
        
        # If table structure is detected
        if tables_info and len(tables_info) > 0:
            logger.info(f"Detected {len(tables_info)} table structures")
            
            for i, table_info in enumerate(tables_info):
                logger.info(f"Processing table {i+1}/{len(tables_info)}")
                
                # Extract cells based on detected structure
                cells_data = extract_cells_from_table(processed_images['ocr_ready'], table_info)
                
                if cells_data and len(cells_data) > 0:
                    # Convert to DataFrame
                    df = create_dataframe_from_cells(cells_data)
                    if df is not None and not df.empty:
                        all_dataframes.append(df)
                        logger.info(f"Successfully created DataFrame from table {i+1}: {df.shape[0]} rows, {df.shape[1]} columns")
                    else:
                        logger.warning(f"Failed to create DataFrame from table {i+1}")
        
        # If no tables were detected or extracted successfully, fall back to general OCR
        if not all_dataframes:
            logger.info("No tables detected or extraction failed, falling back to general OCR")
            df = extract_text_without_table_structure(screenshot_path)
            if df is not None and not df.empty:
                all_dataframes.append(df)
        
        # Save debug images
        cv2.imwrite(f"{screenshot_path}_processed_binary.png", processed_images['binary'])
        cv2.imwrite(f"{screenshot_path}_processed_lines.png", processed_images['lines'])
        
        return all_dataframes
        
    except Exception as e:
        logger.error(f"Error in enhanced OCR extraction: {str(e)}")
        # Try basic extraction as fallback
        try:
            return [extract_text_without_table_structure(screenshot_path)]
        except:
            logger.error("Basic OCR extraction also failed")
            return []



def create_dataframe_from_cells(cell_data):
    """
    Create a pandas DataFrame from extracted cell data.
    
    Args:
        cell_data: 2D list of cell text
        
    Returns:
        pandas.DataFrame: DataFrame created from cell data
    """
    try:
        if not cell_data or len(cell_data) < 2:  # Need at least header and one data row
            logger.warning("Insufficient cell data for DataFrame creation")
            return None
            
        # First row as header
        header = cell_data[0]
        data_rows = cell_data[1:]
        
        # Clean headers
        clean_headers = []
        header_counts = {}
        
        for h in header:
            h_str = str(h).strip()
            if not h_str:  # Replace empty headers
                h_str = f"Column_{len(clean_headers)}"
                
            if h_str in header_counts:
                header_counts[h_str] += 1
                clean_headers.append(f"{h_str}_{header_counts[h_str]}")
            else:
                header_counts[h_str] = 0
                clean_headers.append(h_str)
        
        # Create DataFrame
        df = pd.DataFrame(data_rows, columns=clean_headers)
        
        # Clean data
        df = df.replace(r'^\s*$', '', regex=True)  # Replace whitespace-only cells
        
        # Try to convert numeric columns
        for col in df.columns:
            # Skip columns that are clearly not numeric (long text)
            if df[col].astype(str).str.len().mean() > 20:
                continue
                
            # Clean strings that might be numeric
            cleaned = df[col].astype(str).str.replace(',', '').str.replace('%', '')
            
            # Check if column looks numeric
            try:
                is_numeric = pd.to_numeric(cleaned, errors='coerce').notna().mean() > 0.7
                if is_numeric:
                    df[col] = pd.to_numeric(cleaned, errors='coerce')
            except:
                pass  # Keep as string if conversion fails
        
        # Remove empty rows and columns
        df = df.replace('', np.nan)
        df = df.dropna(how='all').reset_index(drop=True)
        df = df.loc[:, ~df.isna().all()]
        df = df.fillna('')  # Convert NaN back to empty string
        
        # Remove duplicate rows
        df = df.drop_duplicates().reset_index(drop=True)
        
        return df
        
    except Exception as e:
        logger.error(f"Error creating DataFrame from cells: {str(e)}")
        return None

def preprocess_image_for_ocr(image):
    """
    Apply multiple preprocessing techniques to optimize image for both table structure detection
    and text recognition.
    
    Args:
        image: OpenCV image (numpy array)
        
    Returns:
        dict: Dictionary of processed images for different purposes
    """
    try:
        # Make a copy to avoid modifying original
        original = image.copy()
        
        # Convert to grayscale if not already
        if len(image.shape) == 3:
            gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        else:
            gray = image.copy()
        
        # Create results dictionary
        result = {
            'original': original,
            'grayscale': gray
        }
        
        # Resize if image is very large (for faster processing)
        height, width = gray.shape
        max_dimension = 2000  # Maximum allowed dimension
        
        if max(height, width) > max_dimension:
            scale_factor = max_dimension / max(height, width)
            new_width = int(width * scale_factor)
            new_height = int(height * scale_factor)
            gray = cv2.resize(gray, (new_width, new_height))
            logger.info(f"Resized image from {width}x{height} to {new_width}x{new_height}")
            result['resized'] = gray
        
        # Apply noise reduction
        gray_denoised = cv2.fastNlMeansDenoising(gray, None, h=10, templateWindowSize=7, searchWindowSize=21)
        result['denoised'] = gray_denoised
        
        # Create different binary versions with various thresholds
        
        # 1. Adaptive threshold (good for varying lighting conditions)
        binary_adaptive = cv2.adaptiveThreshold(
            gray_denoised, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY_INV, 11, 2
        )
        result['binary_adaptive'] = binary_adaptive
        
        # 2. Otsu's threshold (good for bimodal images)
        _, binary_otsu = cv2.threshold(gray_denoised, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)
        result['binary_otsu'] = binary_otsu
        
        # 3. Standard binary threshold
        _, binary = cv2.threshold(gray_denoised, 150, 255, cv2.THRESH_BINARY_INV)
        result['binary'] = binary
        
        # Create a combined binary image optimized for line detection
        binary_for_lines = cv2.bitwise_or(binary_adaptive, binary_otsu)
        
        # Morphological operations to enhance lines
        kernel_line = cv2.getStructuringElement(cv2.MORPH_RECT, (3, 3))
        binary_line_enhanced = cv2.morphologyEx(binary_for_lines, cv2.MORPH_OPEN, kernel_line, iterations=1)
        binary_line_enhanced = cv2.dilate(binary_line_enhanced, kernel_line, iterations=1)
        result['line_enhanced'] = binary_line_enhanced
        
        # Detect horizontal and vertical lines
        # Vertical lines
        vertical_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (1, height // 30))
        vertical_lines = cv2.erode(binary_line_enhanced, vertical_kernel, iterations=3)
        vertical_lines = cv2.dilate(vertical_lines, vertical_kernel, iterations=3)
        result['vertical_lines'] = vertical_lines
        
        # Horizontal lines
        horizontal_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (width // 30, 1))
        horizontal_lines = cv2.erode(binary_line_enhanced, horizontal_kernel, iterations=3)
        horizontal_lines = cv2.dilate(horizontal_lines, horizontal_kernel, iterations=3)
        result['horizontal_lines'] = horizontal_lines
        
        # Combine lines
        lines = cv2.bitwise_or(vertical_lines, horizontal_lines)
        result['lines'] = lines
        
        # Create image for table structure detection
        table_structure = lines.copy()
        result['table_structure'] = table_structure
        
        # Create image optimized for OCR (remove lines to improve text recognition)
        # Invert binary for better OCR
        _, binary_inv = cv2.threshold(gray_denoised, 150, 255, cv2.THRESH_BINARY)
        
        # Dilate lines slightly to ensure they cover text that touches them
        dilated_lines = cv2.dilate(lines, np.ones((3, 3), np.uint8), iterations=1)
        
        # Remove lines from the inverted binary image
        ocr_ready = cv2.bitwise_and(binary_inv, binary_inv, mask=cv2.bitwise_not(dilated_lines))
        result['ocr_ready'] = ocr_ready
        
        return result
        
    except Exception as e:
        logger.error(f"Error in image preprocessing: {str(e)}")
        # Return simple preprocessing as fallback
        if len(image.shape) == 3:
            gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        else:
            gray = image.copy()
            
        _, binary = cv2.threshold(gray, 150, 255, cv2.THRESH_BINARY_INV)
        
        return {
            'original': image,
            'grayscale': gray,
            'binary': binary,
            'ocr_ready': gray,
            'table_structure': binary,
            'lines': binary
        }


def detect_table_structure(image):
    """
    Detect table structure from the preprocessed image.
    
    Args:
        image: Binary image optimized for table structure detection
        
    Returns:
        list: List of detected tables with their structure information
    """
    try:
        # Find contours
        contours, hierarchy = cv2.findContours(image, cv2.RETR_CCOMP, cv2.CHAIN_APPROX_SIMPLE)
        
        if not contours:
            logger.warning("No contours found for table detection")
            return []
            
        # Get image dimensions
        height, width = image.shape
        min_area = (width * height) / 1000  # Minimum area threshold
        
        # Filter contours by area and shape
        cell_contours = []
        for cnt in contours:
            area = cv2.contourArea(cnt)
            if area < min_area:
                continue
                
            x, y, w, h = cv2.boundingRect(cnt)
            # Filter out contours that are too small or too large
            if w < 10 or h < 10 or w > width * 0.9 or h > height * 0.9:
                continue
                
            cell_contours.append((x, y, w, h))
        
        if not cell_contours:
            logger.warning("No suitable cell contours found")
            return []
            
        # Sort by y-coordinate to group into rows
        cell_contours.sort(key=lambda c: c[1])
        
        # Group contours into rows based on y-coordinate
        y_tolerance = height // 40  # Adjust based on image size
        rows = []
        current_row = [cell_contours[0]]
        current_y = cell_contours[0][1]
        
        for cell in cell_contours[1:]:
            y = cell[1]
            if abs(y - current_y) <= y_tolerance:
                current_row.append(cell)
            else:
                rows.append(current_row)
                current_row = [cell]
                current_y = y
                
        if current_row:
            rows.append(current_row)
            
        # Sort each row by x-coordinate
        for i in range(len(rows)):
            rows[i].sort(key=lambda c: c[0])
            
        # Count rows and columns
        if not rows:
            logger.warning("No rows detected in table structure")
            return []
            
        num_rows = len(rows)
        num_cols = max(len(row) for row in rows)
        
        logger.info(f"Detected table structure: {num_rows} rows × {num_cols} columns")
        
        # Check if this looks like a valid table (at least 2 rows and 2 columns)
        if num_rows < 2 or num_cols < 2:
            logger.warning(f"Structure too small to be a proper table: {num_rows} rows × {num_cols} columns")
            return []
            
        # Return table structure
        return [{
            'rows': rows,
            'num_rows': num_rows,
            'num_cols': num_cols
        }]
        
    except Exception as e:
        logger.error(f"Error detecting table structure: {str(e)}")
        return []



def extract_cells_from_table(image, table_info):
    """
    Extract cell content from the detected table structure.
    
    Args:
        image: Image optimized for OCR
        table_info: Dictionary with table structure information
        
    Returns:
        list: 2D list with extracted cell text
    """
    try:
        rows = table_info['rows']
        num_rows = table_info['num_rows']
        num_cols = table_info['num_cols']
        
        # Create tesseract configuration for different cell types
        # Header configuration (optimized for text)
        header_config = r'-c preserve_interword_spaces=1 --psm 6'
        
        # Data configuration (optimized for numbers and short text)
        data_config = r'-c preserve_interword_spaces=1 --psm 6'
        
        # Configuration for cells likely containing numbers
        number_config = r'-c preserve_interword_spaces=1 --psm 7 -c tessedit_char_whitelist="0123456789,.-%() "'
        
        cell_data = []
        
        # Process each row
        for row_idx, row in enumerate(rows):
            row_data = [''] * num_cols  # Initialize with empty strings
            
            # Process each cell in the row
            for col_idx, (x, y, w, h) in enumerate(row):
                if col_idx >= num_cols:
                    continue  # Skip if column index exceeds the determined number of columns
                    
                # Extract cell region from the image
                cell_img = image[y:y+h, x:x+w]
                
                if cell_img.size == 0:
                    continue  # Skip empty cells
                    
                # Determine which OCR configuration to use
                if row_idx == 0:
                    config = header_config  # First row is likely header
                elif col_idx == 0:
                    config = header_config  # First column often contains text labels
                elif w < 100:
                    config = number_config  # Narrow columns often contain numbers
                else:
                    config = data_config  # Default configuration
                
                # Improve contrast for OCR
                # Create a PIL Image for contrast enhancement
                pil_img = Image.fromarray(cell_img)
                enhancer = ImageEnhance.Contrast(pil_img)
                enhanced_img = enhancer.enhance(2.0)  # Increase contrast
                
                # Apply OCR using appropriate configuration
                try:
                    text = pytesseract.image_to_string(
                        enhanced_img, lang='kor+eng', config=config
                    ).strip()
                    
                    # Clean the text
                    text = ' '.join(text.split())  # Remove excessive whitespace
                    
                    # Store in the row data
                    if text:
                        row_data[col_idx] = text
                except Exception as ocr_err:
                    logger.warning(f"OCR error for cell at row {row_idx}, col {col_idx}: {str(ocr_err)}")
            
            # Add row to cell data if it contains some text
            if any(cell for cell in row_data):
                cell_data.append(row_data)
        
        return cell_data
        
    except Exception as e:
        logger.error(f"Error extracting cells from table: {str(e)}")
        return []























def extract_table_from_html(html_content):
    """
    Enhanced function to extract table data from HTML content with better handling of
    colspan, rowspan, and complex table structures.
    
    Args:
        html_content (str): The HTML content containing tables
        
    Returns:
        pd.DataFrame or None: The extracted table as a DataFrame, or None if extraction fails
    """
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Look for tables with different approaches
        tables = []
        selectors = [
            'table', 
            'div.table', 
            '.grid-table',
            'div[role="table"]', 
            '.tableData',
            '.data-table'
        ]
        
        for selector in selectors:
            found_tables = soup.select(selector)
            if found_tables:
                tables.extend(found_tables)
                logger.info(f"Found {len(found_tables)} tables with selector: {selector}")
        
        if not tables:
            logger.warning("No tables found in HTML content")
            
            # Try looking for structured data that might be a table but not in <table> tags
            grid_elements = soup.select('div.grid, div.row, div.cell, div[class*="grid"], div[class*="row"], div[class*="cell"]')
            if grid_elements:
                logger.info(f"Found {len(grid_elements)} grid-like elements, attempting to reconstruct table")
                # This would require a custom parser for grid layouts
                # For now, we'll try pandas read_html as fallback
            
            # Try pandas read_html as a fallback
            try:
                logger.info("Attempting to use pandas read_html as fallback")
                tables_df = pd.read_html(html_content, flavor='bs4')
                if tables_df and len(tables_df) > 0:
                    largest_df = max(tables_df, key=lambda df: df.size)
                    logger.info(f"Successfully extracted table with pandas: {largest_df.shape}")
                    return largest_df
            except Exception as pandas_err:
                logger.warning(f"pandas read_html failed: {str(pandas_err)}")
            
            return None
        
        # Parse each table and select the best one
        parsed_tables = []
        
        for table_idx, table in enumerate(tables):
            logger.info(f"Processing table {table_idx+1}/{len(tables)}")
            
            # First, try to determine if this is a data table or a layout table
            # Data tables typically have th elements or regular structure
            if not table.find_all(['th', 'td']):
                logger.info(f"Table {table_idx+1} appears to be a layout table without cells, skipping")
                continue
            
            # Check if table has reasonable data density
            cells = table.find_all(['td', 'th'])
            empty_cells = sum(1 for cell in cells if not cell.get_text(strip=True))
            if cells and empty_cells / len(cells) > 0.7:  # More than 70% empty
                logger.info(f"Table {table_idx+1} appears to be mostly empty ({empty_cells}/{len(cells)} empty cells), skipping")
                continue
                
            # Advanced table parsing with better rowspan/colspan handling
            try:
                table_data = parse_complex_table(table)
                if table_data and len(table_data) > 0:
                    # Check if table has a good amount of data
                    row_count = len(table_data)
                    col_count = max(len(row) for row in table_data) if table_data else 0
                    data_density = sum(len(str(cell).strip()) for row in table_data for cell in row) / (row_count * col_count) if row_count * col_count > 0 else 0
                    
                    logger.info(f"Table {table_idx+1}: {row_count} rows, {col_count} columns, data density: {data_density:.2f}")
                    parsed_tables.append((row_count, col_count, data_density, table_data))
            except Exception as parse_err:
                logger.warning(f"Error parsing table {table_idx+1}: {str(parse_err)}")
        
        if not parsed_tables:
            logger.warning("No valid tables were successfully parsed")
            return None
        
        # Select best table based on combined criteria: rows, columns, and data density
        # We normalize and weight each factor
        best_table = None
        best_score = -1
        
        for row_count, col_count, data_density, table_data in parsed_tables:
            # Simple scoring: prefer tables with more rows, more columns, and higher data density
            # Adjust weights as needed
            row_weight, col_weight, density_weight = 0.4, 0.3, 0.3
            max_rows = max(t[0] for t in parsed_tables)
            max_cols = max(t[1] for t in parsed_tables)
            max_density = max(t[2] for t in parsed_tables)
            
            # Normalize scores
            row_score = row_count / max_rows if max_rows > 0 else 0
            col_score = col_count / max_cols if max_cols > 0 else 0
            density_score = data_density / max_density if max_density > 0 else 0
            
            total_score = (row_weight * row_score) + (col_weight * col_score) + (density_weight * density_score)
            
            if total_score > best_score:
                best_score = total_score
                best_table = table_data
        
        if not best_table or len(best_table) < 2:  # Need at least header + 1 data row
            logger.warning("No suitable table data found or table too small")
            return None
        
        # Process the best table into a DataFrame
        # First row is assumed to be header
        header = best_table[0]
        data_rows = best_table[1:]
        
        # Clean and normalize data rows
        clean_data = []
        for row in data_rows:
            # Skip entirely empty rows
            if not any(cell.strip() if isinstance(cell, str) else cell for cell in row):
                continue
                
            # Normalize row length to match header length
            if len(row) < len(header):
                row = row + [""] * (len(header) - len(row))
            elif len(row) > len(header):
                row = row[:len(header)]
                
            clean_data.append(row)
        
        # Handle duplicate headers by adding suffixes
        unique_headers = []
        header_counts = {}
        
        for h in header:
            h_str = str(h).strip()
            if not h_str:  # Replace empty headers
                h_str = f"Column_{len(unique_headers)}"
                
            if h_str in header_counts:
                header_counts[h_str] += 1
                unique_headers.append(f"{h_str}_{header_counts[h_str]}")
            else:
                header_counts[h_str] = 0
                unique_headers.append(h_str)
        
        # Create DataFrame
        df = pd.DataFrame(clean_data, columns=unique_headers)
        
        # Post-process: clean data types, remove duplicate rows
        for col in df.columns:
            # Try to convert numerical columns
            try:
                # Check if column looks like it contains numbers
                # First, clean the strings (remove commas, etc)
                if df[col].dtype == 'object':
                    cleaned = df[col].astype(str).str.replace(',', '').str.replace('%', '')
                    # If more than 50% of non-empty values can be converted to float, try conversion
                    non_empty = cleaned[cleaned != '']
                    if len(non_empty) > 0:
                        convertible = sum(is_numeric(val) for val in non_empty) / len(non_empty)
                        if convertible > 0.5:
                            df[col] = pd.to_numeric(cleaned, errors='coerce')
            except Exception as type_err:
                logger.debug(f"Type conversion failed for column {col}: {str(type_err)}")
        
        # Remove completely empty rows and columns
        df = df.dropna(how='all').reset_index(drop=True)
        df = df.loc[:, ~df.isna().all()]
        
        # Remove duplicate rows
        df = df.drop_duplicates().reset_index(drop=True)
        
        logger.info(f"Successfully created DataFrame from table: {df.shape[0]} rows, {df.shape[1]} columns")
        return df
        
    except Exception as e:
        logger.error(f"Error extracting table from HTML: {str(e)}")
        return None

def parse_complex_table(table):
    """
    Advanced table parser that handles rowspan and colspan attributes.
    
    Args:
        table: BeautifulSoup table element
        
    Returns:
        list: 2D list containing table data with merged cells properly handled
    """
    rows = table.find_all('tr')
    if not rows:
        return []
        
    # First, determine the total columns by examining all rows
    max_cols = 0
    for row in rows:
        cells = row.find_all(['td', 'th'])
        # Count total columns considering colspan
        cols = sum(int(cell.get('colspan', 1)) for cell in cells)
        max_cols = max(max_cols, cols)
    
    # Initialize the 2D grid with empty strings
    # We don't know exact dimensions yet due to rowspan, so we'll extend as needed
    grid = []
    
    # Keep track of cells extended by rowspan
    rowspan_tracker = [0] * max_cols  # How many more rows each column extends down
    
    for row_idx, row in enumerate(rows):
        # Initialize new row in the grid
        if row_idx >= len(grid):
            grid.append([''] * max_cols)
        
        # Decrement rowspan tracker and carry values down
        for col_idx in range(max_cols):
            if rowspan_tracker[col_idx] > 0:
                # This cell is covered by a rowspan from above
                rowspan_tracker[col_idx] -= 1
                # If we're adding a new row due to rowspan, extend the grid
                if row_idx + rowspan_tracker[col_idx] >= len(grid):
                    grid.append([''] * max_cols)
                # Get value from the cell above
                if row_idx > 0:
                    grid[row_idx][col_idx] = grid[row_idx-1][col_idx]
        
        # Process cells in this row
        cells = row.find_all(['td', 'th'])
        col_idx = 0
        
        for cell in cells:
            # Skip columns that are already filled due to rowspan
            while col_idx < max_cols and rowspan_tracker[col_idx] > 0:
                col_idx += 1
            
            # If we've run out of columns, break
            if col_idx >= max_cols:
                break
            
            # Get cell content
            content = cell.get_text(strip=True)
            
            # Get rowspan and colspan
            try:
                rowspan = int(cell.get('rowspan', 1))
            except ValueError:
                rowspan = 1
                
            try:
                colspan = int(cell.get('colspan', 1))
            except ValueError:
                colspan = 1
            
            # Fill in the current cell and cells to the right (colspan)
            for c in range(colspan):
                if col_idx + c < max_cols:
                    grid[row_idx][col_idx + c] = content
            
            # Update rowspan tracker for future rows
            if rowspan > 1:
                for c in range(colspan):
                    if col_idx + c < max_cols:
                        rowspan_tracker[col_idx + c] = rowspan - 1
                
                # Ensure grid has enough rows for the rowspan
                while len(grid) < row_idx + rowspan:
                    grid.append([''] * max_cols)
                
                # Fill in cells below (rowspan)
                for r in range(1, rowspan):
                    for c in range(colspan):
                        if row_idx + r < len(grid) and col_idx + c < max_cols:
                            grid[row_idx + r][col_idx + c] = content
            
            # Move to the next column, considering colspan
            col_idx += colspan
    
    return grid

def is_numeric(value):
    """
    Check if a string value can be converted to a number.
    
    Args:
        value: The string value to check
        
    Returns:
        bool: True if the value can be converted to a number, False otherwise
    """
    if not value or not isinstance(value, str):
        return False
        
    # Strip whitespace and handle empty strings
    value = value.strip()
    if not value:
        return False
        
    # Try to convert to float
    try:
        float(value)
        return True
    except ValueError:
        # Handle special cases like percentages, thousands separators
        try:
            # Remove common non-numeric characters
            cleaned = value.replace(',', '').replace('%', '').replace(' ', '')
            float(cleaned)
            return True
        except ValueError:
            return False

def create_placeholder_dataframe(post_info):
    """데이터 추출 실패 시 기본 데이터프레임 생성"""
    try:
        # 날짜 정보 추출
        date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post_info['title'])
        if date_match:
            year = date_match.group(1)
            month = date_match.group(2)
            
            # 보고서 유형 결정
            report_type = determine_report_type(post_info['title'])
            
            # 기본 데이터프레임 생성
            df = pd.DataFrame({
                '구분': [f'{year}년 {month}월 통계'],
                '값': ['데이터를 추출할 수 없습니다'],
                '비고': [f'{post_info["title"]} - 접근 오류'],
                '링크': [post_info.get('url', '링크 없음')]
            })
            
            logger.info(f"플레이스홀더 데이터프레임 생성: {year}년 {month}월 {report_type}")
            return df
            
        return pd.DataFrame({
            '구분': ['알 수 없음'],
            '값': ['데이터 추출 실패'],
            '비고': [f'게시물: {post_info["title"]} - 날짜 정보 없음']
        })  # 날짜 정보가 없으면 최소 정보 포함
        
    except Exception as e:
        logger.error(f"플레이스홀더 데이터프레임 생성 중 오류: {str(e)}")
        # 오류 발생 시도 최소한의 정보 포함
        return pd.DataFrame({
            '구분': ['오류 발생'],
            '업데이트 상태': ['데이터프레임 생성 실패'],
            '비고': [f'오류: {str(e)}']
        })


def determine_report_type(title):
    """게시물 제목에서 보고서 유형 결정"""
    for report_type in CONFIG['report_types']:
        if report_type in title:
            return report_type
            
    # 부분 매칭 시도
    for report_type in CONFIG['report_types']:
        # 주요 키워드 추출
        keywords = report_type.split()
        if any(keyword in title for keyword in keywords if len(keyword) > 1):
            return report_type
            
    return "기타 통신 통계"


def update_google_sheets(client, data):
    """
    Enhanced function to update Google Sheets with better error handling,
    rate limiting protection, and data validation.
    
    Args:
        client: gspread client instance
        data: Dictionary containing data and metadata to update
        
    Returns:
        bool: True if update was successful, False otherwise
    """
    if not client or not data:
        logger.error("Google Sheets update failed: client or data missing")
        return False
    
    try:
        # Extract information
        post_info = data['post_info']
        
        # Get date information
        if 'date' in data:
            year = data['date']['year']
            month = data['date']['month']
        else:
            # Extract date from title (enhanced regex)
            date_match = re.search(r'\(\s*(\d{4})년\s*(\d{1,2})월말\s*기준\)', post_info['title'])
            if not date_match:
                logger.error(f"Failed to extract date from title: {post_info['title']}")
                return False
                
            year = int(date_match.group(1))
            month = int(date_match.group(2))
        
        # Format date string
        date_str = f"{year}년 {month}월"
        report_type = determine_report_type(post_info['title'])
        
        logger.info(f"Updating Google Sheets for: {date_str} - {report_type}")
        
        # Open spreadsheet with retry logic
        spreadsheet = open_spreadsheet_with_retry(client)
        if not spreadsheet:
            logger.error("Failed to open spreadsheet after multiple attempts")
            return False
        
        # Process the data
        if 'sheets' in data:
            # Multiple sheets
            return update_multiple_sheets(spreadsheet, data['sheets'], date_str, report_type)
        elif 'dataframe' in data:
            # Single dataframe
            return update_single_sheet(spreadsheet, report_type, data['dataframe'], date_str)
        else:
            logger.error("No data to update: neither 'sheets' nor 'dataframe' found in data")
            return False
    
    except Exception as e:
        logger.error(f"Error updating Google Sheets: {str(e)}")
        return False

def update_single_sheet(spreadsheet, sheet_name, df, date_str):
    """단일 시트 업데이트"""
    try:
        # API 속도 제한 방지를 위한 지연 시간
        time.sleep(1)
        
        # 워크시트 찾기 또는 생성
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
            logger.info(f"기존 워크시트 찾음: {sheet_name}")
        except gspread.exceptions.WorksheetNotFound:
            # 새 워크시트 생성
            worksheet = spreadsheet.add_worksheet(title=sheet_name, rows="1000", cols="50")
            logger.info(f"새 워크시트 생성: {sheet_name}")
            
            # 헤더 행 설정
            worksheet.update_cell(1, 1, "항목")
            time.sleep(1)  # API 속도 제한 방지
        
        # 날짜 열 확인
        headers = worksheet.row_values(1)
        
        # 빈 헤더 채우기
        if not headers or len(headers) == 0:
            worksheet.update_cell(1, 1, "항목")
            headers = ["항목"]
            time.sleep(1)  # API 속도 제한 방지
        
        if date_str in headers:
            col_idx = headers.index(date_str) + 1
            logger.info(f"'{date_str}' 열이 이미 위치 {col_idx}에 존재합니다")
        else:
            # 새 날짜 열 추가
            col_idx = len(headers) + 1
            worksheet.update_cell(1, col_idx, date_str)
            logger.info(f"위치 {col_idx}에 새 열 '{date_str}' 추가")
            time.sleep(1)  # API 속도 제한 방지
        
        # 데이터프레임 형식 검증 및 정리
        if df.shape[1] < 2:
            logger.warning(f"데이터프레임 열이 부족합니다: {df.shape[1]} 열. 최소 2열 필요")
            
            # 최소 열 추가
            if df.shape[1] == 1:
                col_name = df.columns[0]
                df['값'] = df[col_name]
            else:
                # 데이터프레임이 비어있는 경우
                df = pd.DataFrame({
                    '항목': ['데이터 없음'],
                    '값': ['업데이트 실패']
                })
        
        # 데이터프레임으로 시트 업데이트 (배치 처리)
        update_sheet_from_dataframe(worksheet, df, col_idx)
        
        logger.info(f"워크시트 '{sheet_name}'에 '{date_str}' 데이터 업데이트 완료")
        return True
        
    except gspread.exceptions.APIError as api_err:
        if "RESOURCE_EXHAUSTED" in str(api_err) or "RATE_LIMIT_EXCEEDED" in str(api_err):
            logger.warning(f"Google Sheets API 속도 제한 발생: {str(api_err)}")
            logger.info("대기 후 재시도 중...")
            time.sleep(5)  # 더 긴 대기 시간
            
            # 간소화된 방식으로 재시도
            try:
                # 워크시트 찾기 또는 생성
                try:
                    worksheet = spreadsheet.worksheet(sheet_name)
                except gspread.exceptions.WorksheetNotFound:
                    worksheet = spreadsheet.add_worksheet(title=sheet_name, rows="1000", cols="20")
                    worksheet.update_cell(1, 1, "항목")
                
                # 날짜 열 위치 결정 (단순화)
                headers = worksheet.row_values(1)
                if date_str in headers:
                    col_idx = headers.index(date_str) + 1
                else:
                    col_idx = len(headers) + 1
                    worksheet.update_cell(1, col_idx, date_str)
                
                # 최소한의 데이터만 업데이트
                if df.shape[0] > 0:
                    first_col_name = df.columns[0]
                    items = df[first_col_name].astype(str).tolist()[:10]  # 처음 10개 항목
                    values = ["업데이트 성공"] * len(items)
                    
                    for i, (item, value) in enumerate(zip(items, values)):
                        row_idx = i + 2  # 헤더 행 이후
                        worksheet.update_cell(row_idx, 1, item)
                        worksheet.update_cell(row_idx, col_idx, value)
                        time.sleep(1)  # 더 긴 지연 시간
                
                logger.info(f"제한된 데이터로 워크시트 '{sheet_name}' 업데이트 완료")
                return True
                
            except Exception as retry_err:
                logger.error(f"재시도 중 오류: {str(retry_err)}")
                return False
        else:
            logger.error(f"Google Sheets API 오류: {str(api_err)}")
            return False
            
    except Exception as e:
        logger.error(f"시트 '{sheet_name}' 업데이트 중 오류: {str(e)}")
        return False


def update_multiple_sheets(spreadsheet, sheets_data, date_str, report_type):
    """
    Update multiple sheets in the spreadsheet.
    
    Args:
        spreadsheet: gspread Spreadsheet object
        sheets_data: Dictionary mapping sheet names to DataFrames
        date_str: Date string for the column header
        report_type: Type of report
        
    Returns:
        bool: True if update was successful, False otherwise
    """
    if not sheets_data:
        logger.error("No sheets data to update")
        return False
        
    success_count = 0
    total_sheets = len(sheets_data)
    
    # Update sheet metadata
    try:
        # Add metadata in a special sheet
        metadata_sheet = ensure_metadata_sheet(spreadsheet)
        if metadata_sheet:
            update_metadata_sheet(metadata_sheet, {
                'last_update': datetime.now().isoformat(),
                'report_type': report_type,
                'date': date_str,
                'sheets': list(sheets_data.keys())
            })
    except Exception as meta_err:
        logger.warning(f"Failed to update metadata: {str(meta_err)}")
    
    # Process each sheet
    for sheet_name, df in sheets_data.items():
        try:
            # Skip empty dataframes
            if df is None or df.empty:
                logger.warning(f"Skipping empty dataframe for sheet: {sheet_name}")
                continue
                
            # Clean sheet name to be valid
            clean_sheet_name = clean_sheet_name_for_gsheets(sheet_name)
            
            # Check data quality
            df = validate_and_clean_dataframe(df)
            if df.empty:
                logger.warning(f"Dataframe for sheet {clean_sheet_name} is empty after cleaning")
                continue
                
            # Update the sheet
            success = update_single_sheet_with_retry(spreadsheet, clean_sheet_name, df, date_str)
            if success:
                success_count += 1
                logger.info(f"Successfully updated sheet: {clean_sheet_name}")
            else:
                logger.warning(f"Failed to update sheet: {clean_sheet_name}")
        
        except Exception as sheet_err:
            logger.error(f"Error updating sheet {sheet_name}: {str(sheet_err)}")
    
    logger.info(f"Updated {success_count}/{total_sheets} sheets")
    return success_count > 0

def update_metadata_sheet(metadata_sheet, metadata):
    """
    Update the metadata sheet with the provided information.
    
    Args:
        metadata_sheet: gspread Worksheet object
        metadata: Dictionary of metadata to update
        
    Returns:
        bool: True if update was successful, False otherwise
    """
    try:
        # Get existing keys
        try:
            keys = metadata_sheet.col_values(1)[1:]  # Skip header
        except:
            keys = []
            
        # Current timestamp
        timestamp = datetime.now().isoformat()
        
        # Build updates
        updates = []
        
        for key, value in metadata.items():
            # Format the value if it's not a string
            if isinstance(value, (list, dict)):
                value = json.dumps(value)
            else:
                value = str(value)
                
            # Find or create row for this key
            if key in keys:
                row_idx = keys.index(key) + 2  # +2 for header and 0-index
            else:
                # Add new key at the end
                row_idx = len(keys) + 2
                keys.append(key)
                
                # Add key to first column
                updates.append({
                    'range': f'A{row_idx}',
                    'values': [[key]]
                })
            
            # Update value and timestamp
            updates.append({
                'range': f'B{row_idx}:C{row_idx}',
                'values': [[value, timestamp]]
            })
        
        # Execute the updates
        if updates:
            for update in updates:
                try:
                    metadata_sheet.batch_update([update])
                    time.sleep(1)  # Avoid rate limits
                except Exception as update_err:
                    logger.warning(f"Metadata update failed: {str(update_err)}")
            
            logger.info(f"Updated {len(metadata)} metadata entries")
            return True
        else:
            logger.warning("No metadata to update")
            return True
            
    except Exception as e:
        logger.error(f"Error updating metadata: {str(e)}")
        return False

def ensure_metadata_sheet(spreadsheet):
    """
    Ensure that a metadata sheet exists in the spreadsheet.
    
    Args:
        spreadsheet: gspread Spreadsheet object
        
    Returns:
        gspread Worksheet object or None if failed
    """
    try:
        # Try to get existing metadata sheet
        try:
            metadata_sheet = spreadsheet.worksheet("__metadata__")
            return metadata_sheet
        except gspread.exceptions.WorksheetNotFound:
            # Create new metadata sheet
            metadata_sheet = spreadsheet.add_worksheet(title="__metadata__", rows=100, cols=10)
            
            # Initialize metadata sheet
            headers = ["key", "value", "updated_at"]
            metadata_sheet.update('A1:C1', [headers])
            
            return metadata_sheet
            
    except Exception as e:
        logger.error(f"Error ensuring metadata sheet: {str(e)}")
        return None
    
def clean_sheet_name_for_gsheets(sheet_name):
    """
    Clean sheet name to be valid for Google Sheets.
    
    Args:
        sheet_name: Original sheet name
        
    Returns:
        str: Cleaned sheet name
    """
    # Replace invalid characters
    clean_name = re.sub(r'[\\/*\[\]:]', '_', str(sheet_name))
    
    # Limit length (Google Sheets has a 100 character limit)
    if len(clean_name) > 100:
        clean_name = clean_name[:97] + '...'
        
    # Ensure it's not empty
    if not clean_name:
        clean_name = 'Sheet'
        
    return clean_name



def validate_and_clean_dataframe(df):
    """
    Validate and clean a DataFrame before updating Google Sheets.
    
    Args:
        df: pandas DataFrame to clean
        
    Returns:
        pandas.DataFrame: Cleaned DataFrame
    """
    try:
        if df is None or df.empty:
            return pd.DataFrame()
            
        # Make a copy to avoid modifying the original
        df_clean = df.copy()
        
        # Replace NaN values with empty strings
        df_clean = df_clean.fillna('')
        
        # Convert all values to strings for consistency
        for col in df_clean.columns:
            df_clean[col] = df_clean[col].astype(str)
            
            # Clean up formatting for numeric values
            # If the column looks numeric, format it nicely
            if df_clean[col].str.replace(',', '').str.replace('.', '').str.isdigit().mean() > 0.7:
                try:
                    numeric_values = pd.to_numeric(df_clean[col].str.replace(',', ''))
                    # Format large numbers with commas
                    df_clean[col] = numeric_values.apply(lambda x: f"{x:,}" if abs(x) >= 1000 else str(x))
                except:
                    pass
        
        # Remove rows that are completely empty
        df_clean = df_clean.replace('', np.nan)
        df_clean = df_clean.dropna(how='all').reset_index(drop=True)
        
        # Remove columns that are completely empty
        df_clean = df_clean.loc[:, ~df_clean.isna().all()]
        
        # Convert NaN back to empty strings
        df_clean = df_clean.fillna('')
        
        # Clean column headers
        df_clean.columns = [str(col).strip() for col in df_clean.columns]
        
        # Handle duplicate column names
        if len(df_clean.columns) != len(set(df_clean.columns)):
            new_columns = []
            seen = {}
            for col in df_clean.columns:
                if col in seen:
                    seen[col] += 1
                    new_columns.append(f"{col}_{seen[col]}")
                else:
                    seen[col] = 0
                    new_columns.append(col)
            df_clean.columns = new_columns
        
        return df_clean
        
    except Exception as e:
        logger.error(f"Error validating DataFrame: {str(e)}")
        return pd.DataFrame()  # Return empty DataFrame on error


def open_spreadsheet_with_retry(client, max_retries=3, retry_delay=2):
    """
    Open Google Spreadsheet with retry logic.
    
    Args:
        client: gspread client instance
        max_retries: Maximum number of retry attempts
        retry_delay: Delay between retries (seconds)
        
    Returns:
        Spreadsheet object or None if failed
    """
    spreadsheet = None
    retry_count = 0
    
    while retry_count < max_retries and not spreadsheet:
        try:
            # Try by ID first
            if CONFIG['spreadsheet_id']:
                try:
                    spreadsheet = client.open_by_key(CONFIG['spreadsheet_id'])
                    logger.info(f"Successfully opened spreadsheet by ID: {CONFIG['spreadsheet_id']}")
                    return spreadsheet
                except Exception as id_err:
                    logger.warning(f"Failed to open spreadsheet by ID: {str(id_err)}")
            
            # Try by name
            try:
                spreadsheet = client.open(CONFIG['spreadsheet_name'])
                logger.info(f"Successfully opened spreadsheet by name: {CONFIG['spreadsheet_name']}")
                return spreadsheet
            except gspread.exceptions.SpreadsheetNotFound:
                # Create new spreadsheet
                logger.info(f"Spreadsheet not found, creating new one: {CONFIG['spreadsheet_name']}")
                spreadsheet = client.create(CONFIG['spreadsheet_name'])
                # Update config with new ID
                CONFIG['spreadsheet_id'] = spreadsheet.id
                logger.info(f"Created new spreadsheet with ID: {spreadsheet.id}")
                return spreadsheet
                
        except gspread.exceptions.APIError as api_err:
            retry_count += 1
            
            if "RESOURCE_EXHAUSTED" in str(api_err) or "RATE_LIMIT_EXCEEDED" in str(api_err):
                wait_time = 2 ** retry_count  # Exponential backoff
                logger.warning(f"API rate limit hit, waiting {wait_time} seconds before retry")
                time.sleep(wait_time)
            else:
                logger.error(f"Google Sheets API error: {str(api_err)}")
                if retry_count < max_retries:
                    time.sleep(retry_delay)
                else:
                    return None
                    
        except Exception as e:
            logger.error(f"Error opening spreadsheet: {str(e)}")
            retry_count += 1
            if retry_count < max_retries:
                time.sleep(retry_delay)
            else:
                return None
    
    return None

def update_sheet_from_dataframe(worksheet, df, col_idx):
    """데이터프레임으로 워크시트 업데이트 (배치 처리)"""
    try:
        # 기존 항목 (첫 번째 열) 가져오기
        existing_items = worksheet.col_values(1)[1:]  # 헤더 제외
        
        if df.shape[0] > 0:
            # 데이터프레임에서 항목과 값 추출
            # 첫 번째 열은 항목, 두 번째 열은 값으로 가정
            if df.shape[1] >= 2:
                df = df.fillna('')  # NaN 값 처리
                item_col = df.columns[0]
                value_col = df.columns[1]
                
                new_items = df[item_col].astype(str).tolist()
                values = df[value_col].astype(str).tolist()
                
                # 배치 업데이트 준비
                cell_updates = []
                new_rows = []
                
                for i, (item, value) in enumerate(zip(new_items, values)):
                    if item and item.strip():  # 빈 항목 제외
                        # 항목이 이미 존재하는지 확인
                        if item in existing_items:
                            row_idx = existing_items.index(item) + 2  # 헤더와, 0-인덱스 보정
                        else:
                            # 새 항목은 끝에 추가
                            row_idx = len(existing_items) + 2
                            new_rows.append(item)  # 새 행 추적
                            
                            # 항목 업데이트
                            cell_updates.append({
                                'range': f'A{row_idx}',
                                'values': [[item]]
                            })
                            existing_items.append(item)
                        
                        # 값 업데이트
                        value_to_update = "" if pd.isna(value) else value
                        cell_updates.append({
                            'range': f'{chr(64 + col_idx)}{row_idx}',
                            'values': [[value_to_update]]
                        })
                
                # 일괄 업데이트 실행 (API 호출 제한 방지를 위한 분할)
                if cell_updates:
                    batch_size = 10  # 한 번에 처리할 업데이트 수
                    for i in range(0, len(cell_updates), batch_size):
                        batch = cell_updates[i:i+batch_size]
                        try:
                            worksheet.batch_update(batch)
                            logger.info(f"일괄 업데이트 {i+1}~{min(i+batch_size, len(cell_updates))} 완료")
                            time.sleep(2)  # API 속도 제한 방지
                        except gspread.exceptions.APIError as api_err:
                            if "RESOURCE_EXHAUSTED" in str(api_err) or "RATE_LIMIT_EXCEEDED" in str(api_err):
                                logger.warning(f"API 속도 제한 발생: {str(api_err)}")
                                time.sleep(10)  # 더 긴 대기
                                # 더 작은 배치로 재시도
                                for update in batch:
                                    try:
                                        worksheet.batch_update([update])
                                        time.sleep(3)
                                    except Exception as single_err:
                                        logger.error(f"단일 업데이트 실패: {str(single_err)}")
                            else:
                                logger.error(f"일괄 업데이트 실패: {str(api_err)}")
                    
                    logger.info(f"{len(cell_updates)}개 셀 업데이트 완료 (새 항목: {len(new_rows)}개)")
                    
        return True
        
    except Exception as e:
        logger.error(f"데이터프레임으로 워크시트 업데이트 중 오류: {str(e)}")
        return False


def update_single_sheet_with_retry(spreadsheet, sheet_name, df, date_str, max_retries=3):
    """
    Update a single sheet with retry logic.
    
    Args:
        spreadsheet: gspread Spreadsheet object
        sheet_name: Name of the sheet to update
        df: pandas DataFrame with data
        date_str: Date string for the column header
        max_retries: Maximum number of retry attempts
        
    Returns:
        bool: True if update was successful, False otherwise
    """
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            # Get or create worksheet
            try:
                worksheet = spreadsheet.worksheet(sheet_name)
                logger.info(f"Found existing worksheet: {sheet_name}")
            except gspread.exceptions.WorksheetNotFound:
                # Create new worksheet
                worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=50)
                logger.info(f"Created new worksheet: {sheet_name}")
                
                # Set header for new worksheet
                worksheet.update_cell(1, 1, "항목")
                time.sleep(1)  # Avoid rate limiting
            
            # Get existing headers
            headers = worksheet.row_values(1)
            
            # Ensure we have at least one header
            if not headers or len(headers) == 0:
                worksheet.update_cell(1, 1, "항목")
                headers = ["항목"]
                time.sleep(1)
            
            # Find or add date column
            if date_str in headers:
                col_idx = headers.index(date_str) + 1
                logger.info(f"Found existing column for {date_str} at position {col_idx}")
            else:
                # Add new date column
                col_idx = len(headers) + 1
                worksheet.update_cell(1, col_idx, date_str)
                logger.info(f"Added new column for {date_str} at position {col_idx}")
                time.sleep(1)
            
            # Update data using optimized batched updates
            success = update_sheet_data_batched(worksheet, df, col_idx)
            
            if success:
                return True
            else:
                retry_count += 1
                logger.warning(f"Failed to update sheet data, retrying ({retry_count}/{max_retries})")
                time.sleep(2 ** retry_count)  # Exponential backoff
                
        except gspread.exceptions.APIError as api_err:
            retry_count += 1
            
            if "RESOURCE_EXHAUSTED" in str(api_err) or "RATE_LIMIT_EXCEEDED" in str(api_err):
                wait_time = 5 + (2 ** retry_count)  # Exponential backoff with base delay
                logger.warning(f"API rate limit hit, waiting {wait_time} seconds")
                time.sleep(wait_time)
            else:
                logger.error(f"Google Sheets API error: {str(api_err)}")
                if retry_count < max_retries:
                    time.sleep(2)
                else:
                    return False
        
        except Exception as e:
            logger.error(f"Error updating sheet {sheet_name}: {str(e)}")
            retry_count += 1
            if retry_count < max_retries:
                time.sleep(2)
            else:
                return False
    
    return False



def update_sheet_data_batched(worksheet, df, date_col_idx, batch_size=10):
    """
    Update sheet data using optimized batched updates.
    
    Args:
        worksheet: gspread Worksheet object
        df: pandas DataFrame with data
        date_col_idx: Column index for the date column
        batch_size: Number of cells to update in each batch
        
    Returns:
        bool: True if update was successful, False otherwise
    """
    try:
        # Get existing items (first column)
        existing_items = worksheet.col_values(1)[1:]  # Skip header
        
        # Prepare updates
        cell_updates = []
        
        # First column is used as the key
        if df.shape[1] >= 1:
            key_col = df.columns[0]
            value_col = df.columns[1] if df.shape[1] >= 2 else key_col
            
            # Get items and values
            items = df[key_col].astype(str).tolist()
            values = df[value_col].astype(str).tolist()
            
            # Build update operations
            for i, (item, value) in enumerate(zip(items, values)):
                if not item or not item.strip():
                    continue  # Skip empty items
                    
                # Find row index for this item
                if item in existing_items:
                    row_idx = existing_items.index(item) + 2  # +2 for header and 0-index
                else:
                    # Add new item at the end
                    row_idx = len(existing_items) + 2
                    existing_items.append(item)
                    
                    # Add item to first column
                    cell_updates.append({
                        'range': f'A{row_idx}',
                        'values': [[item]]
                    })
                
                # Add value to date column
                cell_updates.append({
                    'range': f'{chr(64 + date_col_idx)}{row_idx}',
                    'values': [[value]]
                })
        
        # Execute batched updates
        if cell_updates:
            success = execute_batched_updates(worksheet, cell_updates, batch_size)
            if success:
                logger.info(f"Successfully updated {len(cell_updates)} cells")
                return True
            else:
                logger.warning("Batched updates failed")
                return False
        else:
            logger.warning("No cell updates to perform")
            return True  # No updates needed is still a "success"
            
    except Exception as e:
        logger.error(f"Error in batched sheet update: {str(e)}")
        return False


def execute_batched_updates(worksheet, updates, batch_size):
    """
    Execute batched updates with rate limit handling.
    
    Args:
        worksheet: gspread Worksheet object
        updates: List of update operations
        batch_size: Number of operations per batch
        
    Returns:
        bool: True if all updates were successful
    """
    try:
        for i in range(0, len(updates), batch_size):
            batch = updates[i:i+batch_size]
            
            try:
                worksheet.batch_update(batch)
                logger.info(f"Batch update {i+1}-{i+len(batch)} succeeded ({len(batch)} operations)")
                
                # Add delay to avoid rate limits
                time.sleep(1 + (len(batch) // 5))  # Adjust delay based on batch size
                
            except gspread.exceptions.APIError as api_err:
                if "RESOURCE_EXHAUSTED" in str(api_err) or "RATE_LIMIT_EXCEEDED" in str(api_err):
                    logger.warning(f"Rate limit hit during batch {i+1}-{i+len(batch)}, switching to individual updates")
                    
                    # Fall back to individual updates with longer delays
                    for update in batch:
                        try:
                            worksheet.batch_update([update])
                            time.sleep(2)  # Longer delay for individual updates
                        except Exception as single_err:
                            logger.error(f"Individual update failed: {str(single_err)}")
                else:
                    logger.error(f"Batch update API error: {str(api_err)}")
                    return False
                    
        return True
        
    except Exception as e:
        logger.error(f"Error executing batched updates: {str(e)}")
        return False


async def send_telegram_message(posts, data_updates=None):
    """텔레그램으로 알림 메시지 전송"""
    if not posts and not data_updates:
        logger.info("알림을 보낼 내용이 없습니다")
        return
        
    try:
        # 텔레그램 봇 초기화
        bot = telegram.Bot(token=CONFIG['telegram_token'])
        
        message = "📊 *MSIT 통신 통계 모니터링*\n\n"
        
        # 새 게시물 정보 추가
        if posts:
            message += "📱 *새로운 통신 관련 게시물*\n\n"
            
            # 최대 5개 게시물만 표시 (너무 길지 않도록)
            displayed_posts = posts[:5]
            for post in displayed_posts:
                message += f"📅 {post['date']}\n"
                message += f"📑 {post['title']}\n"
                message += f"🏢 {post['department']}\n"
                if post.get('url'):
                    message += f"🔗 [게시물 링크]({post['url']})\n"
                message += "\n"
            
            # 추가 게시물이 있는 경우 표시
            if len(posts) > 5:
                message += f"_...외 {len(posts) - 5}개 게시물_\n\n"
        
        # 데이터 업데이트 정보 추가
        if data_updates:
            message += "📊 *Google Sheets 데이터 업데이트*\n\n"
            
            # 최대 10개 업데이트만 표시
            displayed_updates = data_updates[:10]
            for update in displayed_updates:
                post_info = update['post_info']
                
                # 날짜 정보 추출
                if 'date' in update:
                    year = update['date']['year']
                    month = update['date']['month']
                else:
                    date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post_info['title'])
                    if date_match:
                        year = date_match.group(1)
                        month = date_match.group(2)
                    else:
                        year = "알 수 없음"
                        month = "알 수 없음"
                
                date_str = f"{year}년 {month}월"
                report_type = determine_report_type(post_info['title'])
                
                message += f"📅 *{date_str}*\n"
                message += f"📑 {report_type}\n"
                
                # 시트 정보 표시 (있는 경우)
                if 'sheets' in update:
                    sheet_names = list(update['sheets'].keys())
                    if len(sheet_names) <= 3:
                        message += f"📗 시트: {', '.join(sheet_names)}\n"
                    else:
                        message += f"📗 시트: {len(sheet_names)}개 업데이트됨\n"
                
                message += "✅ 업데이트 완료\n\n"
            
            # 추가 업데이트가 있는 경우 표시
            if len(data_updates) > 10:
                message += f"_...외 {len(data_updates) - 10}개 업데이트_\n\n"
        
        # 스프레드시트 정보 추가
        if data_updates and CONFIG['spreadsheet_id']:
            spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{CONFIG['spreadsheet_id']}"
            message += f"📋 [스프레드시트 보기]({spreadsheet_url})\n\n"
        
        # 현재 시간 추가
        kr_time = datetime.now() + timedelta(hours=9)
        message += f"🕒 *업데이트 시간: {kr_time.strftime('%Y-%m-%d %H:%M')} (KST)*"
        
        # 메시지 분할 (텔레그램 제한)
        max_length = 4000
        if len(message) > max_length:
            chunks = [message[i:i+max_length] for i in range(0, len(message), max_length)]
            for i, chunk in enumerate(chunks):
                # 첫 번째가 아닌 메시지에 헤더 추가
                if i > 0:
                    chunk = "📊 *MSIT 통신 통계 모니터링 (계속)...*\n\n" + chunk
                
                chat_id = int(CONFIG['chat_id'])
                await bot.send_message(
                    chat_id=chat_id,
                    text=chunk,
                    parse_mode='Markdown'
                )
                time.sleep(1)  # 메시지 사이 지연
            
            logger.info(f"텔레그램 메시지 {len(chunks)}개 청크로 분할 전송 완료")
        else:
            # 단일 메시지 전송
            chat_id = int(CONFIG['chat_id'])
            await bot.send_message(
                chat_id=chat_id,
                text=message,
                parse_mode='Markdown'
            )
            logger.info("텔레그램 메시지 전송 성공")
        
    except Exception as e:
        logger.error(f"텔레그램 메시지 전송 중 오류: {str(e)}")
        
        # 단순화된 메시지로 재시도
        try:
            simple_msg = f"⚠️ MSIT 통신 통계 알림: {len(posts) if posts else 0}개 새 게시물, {len(data_updates) if data_updates else 0}개 업데이트"
            await bot.send_message(
                chat_id=int(CONFIG['chat_id']),
                text=simple_msg
            )
            logger.info("단순화된 텔레그램 메시지 전송 성공")
        except Exception as simple_err:
            logger.error(f"단순화된 텔레그램 메시지 전송 중 오류: {str(simple_err)}")


async def run_monitor(days_range=4, check_sheets=True):
    """
    Enhanced monitoring function with improved document processing flow.
    
    Args:
        days_range: Number of days to look back for new posts
        check_sheets: Whether to update Google Sheets
    
    Returns:
        None
    """
    driver = None
    gs_client = None
    
    try:
        # Start time recording
        start_time = time.time()
        logger.info(f"=== MSIT 통신 통계 모니터링 시작 (days_range={days_range}, check_sheets={check_sheets}) ===")

        # Create screenshot directory
        screenshots_dir = Path("./screenshots")
        screenshots_dir.mkdir(exist_ok=True)

        # Initialize WebDriver with enhanced stealth settings
        driver = setup_driver()
        logger.info("WebDriver 초기화 완료")
        
        # Initialize Google Sheets client
        if check_sheets and CONFIG['gspread_creds']:
            gs_client = setup_gspread_client()
            if gs_client:
                logger.info("Google Sheets 클라이언트 초기화 완료")
            else:
                logger.warning("Google Sheets 클라이언트 초기화 실패")
        
        # Enhance WebDriver stealth settings
        try:
            # Reset User-Agent
            user_agents = [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:96.0) Gecko/20100101 Firefox/96.0"
            ]
            selected_ua = random.choice(user_agents)
            execute_javascript(driver, f'Object.defineProperty(navigator, "userAgent", {{get: function() {{return "{selected_ua}";}}}});', description="User-Agent 재설정")
            logger.info(f"JavaScript로 User-Agent 재설정: {selected_ua}")
            
            # Avoid webdriver detection
            execute_javascript(driver, """
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                if (window.navigator.permissions) {
                    window.navigator.permissions.query = (parameters) => {
                        return Promise.resolve({state: 'prompt', onchange: null});
                    };
                }
            """, description="웹드라이버 감지 회피")
        except Exception as setup_err:
            logger.warning(f"웹드라이버 강화 설정 중 오류: {str(setup_err)}")
        
        # Access landing page
        try:
            # Navigate to landing page
            landing_url = CONFIG['landing_url']
            driver.get(landing_url)
            
            # Wait for page to load
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.ID, "skip_nav"))
            )
            logger.info("랜딩 페이지 접속 완료 - 쿠키 및 세션 정보 획득")
            
            # Random delay (simulate natural user behavior)
            time.sleep(random.uniform(2, 4))
            
            # Save screenshot
            driver.save_screenshot("landing_page.png")
            logger.info("랜딩 페이지 스크린샷 저장 완료")
            
            # Simulate scrolling
            execute_javascript(driver, """
                function smoothScroll() {
                    const height = document.body.scrollHeight;
                    const step = Math.floor(height / 10);
                    let i = 0;
                    const timer = setInterval(function() {
                        window.scrollBy(0, step);
                        i++;
                        if (i >= 10) clearInterval(timer);
                    }, 100);
                }
                smoothScroll();
            """, description="랜딩 페이지 스크롤")
            time.sleep(2)
            
            # Find and click statistics link
            stats_link_selectors = [
                "a[href*='mId=99'][href*='mPid=74']",
                "//a[contains(text(), '통계정보')]",
                "//a[contains(text(), '통계')]"
            ]
            
            stats_link_found = False
            for selector in stats_link_selectors:
                try:
                    if selector.startswith("//"):
                        # XPath selector
                        links = driver.find_elements(By.XPATH, selector)
                    else:
                        # CSS selector
                        links = driver.find_elements(By.CSS_SELECTOR, selector)
                    
                    if links:
                        stats_link = links[0]
                        logger.info(f"통계정보 링크 발견 (선택자: {selector}), 클릭 시도")
                        
                        # Screenshot (before click)
                        driver.save_screenshot("before_stats_click.png")
                        
                        # Click using JavaScript (more reliable)
                        driver.execute_script("arguments[0].click();", stats_link)
                        
                        # Wait for URL change
                        WebDriverWait(driver, 15).until(
                            lambda d: '/bbs/list.do' in d.current_url
                        )
                        stats_link_found = True
                        logger.info(f"통계정보 페이지로 이동 완료: {driver.current_url}")
                        break
                except Exception as link_err:
                    logger.warning(f"통계정보 링크 클릭 실패 (선택자: {selector}): {str(link_err)}")
            
            if not stats_link_found:
                logger.warning("통계정보 링크를 찾을 수 없음, 직접 URL로 접속")
                driver.get(CONFIG['stats_url'])
                
                # Wait for page to load
                try:
                    WebDriverWait(driver, 15).until(
                        EC.presence_of_element_located((By.CLASS_NAME, "board_list"))
                    )
                    logger.info("통계정보 페이지 직접 접속 성공")
                except TimeoutException:
                    logger.warning("통계정보 페이지 로드 시간 초과, 계속 진행")
            
        except Exception as e:
            logger.error(f"랜딩 또는 통계정보 버튼 클릭 중 오류 발생, fallback으로 직접 접속: {str(e)}")
            
            # Reset browser context
            reset_browser_context(driver)
            
            # Access stats page directly
            driver.get(CONFIG['stats_url'])
            
            try:
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "board_list"))
                )
                logger.info("통계정보 페이지 직접 접속 성공 (오류 후 재시도)")
            except TimeoutException:
                logger.warning("통계정보 페이지 로드 시간 초과 (오류 후 재시도), 계속 진행")
        
        logger.info("MSIT 웹사이트 접근 완료")
        
        # Save screenshot (for debugging)
        try:
            driver.save_screenshot("stats_page.png")
            logger.info("통계정보 페이지 스크린샷 저장 완료")
        except Exception as ss_err:
            logger.warning(f"스크린샷 저장 중 오류: {str(ss_err)}")
        
        # Track all posts and telecom statistics posts
        all_posts = []
        telecom_stats_posts = []
        continue_search = True
        page_num = 1
        
        # Parse pages
        while continue_search:
            logger.info(f"페이지 {page_num} 파싱 중...")
            
            # Check page load state
            try:
                # Verify page is properly loaded
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "board_list"))
                )
            except TimeoutException:
                logger.warning(f"페이지 {page_num} 로드 시간 초과, 새로고침 시도")
                driver.refresh()
                time.sleep(3)
                
                try:
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CLASS_NAME, "board_list"))
                    )
                    logger.info("새로고침 후 페이지 로드 성공")
                except TimeoutException:
                    logger.error("새로고침 후에도 페이지 로드 실패, 다음 단계로 진행")
            
            page_posts, stats_posts, should_continue = parse_page(driver, page_num, days_range=days_range)
            all_posts.extend(page_posts)
            telecom_stats_posts.extend(stats_posts)
            
            logger.info(f"페이지 {page_num} 파싱 결과: {len(page_posts)}개 게시물, {len(stats_posts)}개 통신 통계")
            
            if not should_continue:
                logger.info(f"날짜 범위 밖의 게시물 발견. 검색 중단")
                break
                
            if has_next_page(driver):
                if go_to_next_page(driver):
                    page_num += 1
                else:
                    logger.warning(f"페이지 {page_num}에서 다음 페이지로 이동 실패")
                    break
            else:
                logger.info(f"마지막 페이지 ({page_num}) 도달")
                break
        
        # Process telecom statistics posts with enhanced extraction
        data_updates = []
        
        if gs_client and telecom_stats_posts and check_sheets:
            logger.info(f"{len(telecom_stats_posts)}개 통신 통계 게시물 처리 중")
            
            for i, post in enumerate(telecom_stats_posts):
                try:
                    logger.info(f"게시물 {i+1}/{len(telecom_stats_posts)} 처리 중: {post['title']}")
                    
                    # Extract view link parameters with enhanced error handling
                    file_params = find_view_link_params(driver, post)
                    
                    if not file_params:
                        logger.warning(f"바로보기 링크 파라미터 추출 실패: {post['title']}")
                        continue
                    
                    # ENHANCED: Detailed logging of extraction parameters
                    param_log = {k: v for k, v in file_params.items() if k != 'post_info'}
                    logger.info(f"Document extraction parameters: {json.dumps(param_log, default=str)}")
                    
                    # Direct access with OCR fallback - CORE IMPROVEMENT
                    if 'atch_file_no' in file_params and 'file_ord' in file_params:
                        # ENHANCED: Clear logging of extraction process
                        logger.info(f"Starting document extraction via iframe with OCR fallback")
                        
                        # Try to extract data with enhanced iframe access and OCR fallback
                        sheets_data = access_iframe_with_ocr_fallback(driver, file_params)
                        
                        if sheets_data:
                            # Log detailed information about extracted data
                            sheet_names = list(sheets_data.keys())
                            sheet_sizes = {name: sheets_data[name].shape for name in sheet_names}
                            logger.info(f"Successful data extraction: {len(sheet_names)} sheets extracted")
                            logger.info(f"Sheet dimensions: {sheet_sizes}")
                            
                            # Prepare update data
                            update_data = {
                                'sheets': sheets_data,
                                'post_info': post
                            }
                            
                            if 'date' in file_params:
                                update_data['date'] = file_params['date']
                            
                            # Update Google Sheets with enhanced update function
                            success = update_google_sheets(gs_client, update_data)
                            if success:
                                logger.info(f"Google Sheets 업데이트 성공: {post['title']}")
                                data_updates.append(update_data)
                            else:
                                logger.warning(f"Google Sheets 업데이트 실패: {post['title']}")
                        else:
                            logger.warning(f"iframe에서 데이터 추출 실패: {post['title']}")
                            
                            # ENHANCED: Create a better placeholder with diagnostic info
                            placeholder_df = create_improved_placeholder_dataframe(post, file_params)
                            
                            if not placeholder_df.empty:
                                update_data = {
                                    'dataframe': placeholder_df,
                                    'post_info': post
                                }
                                
                                if 'date' in file_params:
                                    update_data['date'] = file_params['date']
                                
                                success = update_google_sheets(gs_client, update_data)
                                if success:
                                    logger.info(f"대체 데이터로 업데이트 성공: {post['title']}")
                                    data_updates.append(update_data)
                    
                    # Handle content-only case
                    elif 'content' in file_params:
                        logger.info(f"게시물 내용으로 처리 중: {post['title']}")
                        
                        # Create placeholder dataframe
                        placeholder_df = create_improved_placeholder_dataframe(post, file_params)
                        if not placeholder_df.empty:
                            update_data = {
                                'dataframe': placeholder_df,
                                'post_info': post
                            }
                            
                            if 'date' in file_params:
                                update_data['date'] = file_params['date']
                            
                            success = update_google_sheets(gs_client, update_data)
                            if success:
                                logger.info(f"내용 기반 데이터로 업데이트 성공: {post['title']}")
                                data_updates.append(update_data)
                    
                    # Handle AJAX data case
                    elif 'ajax_data' in file_params:
                        logger.info(f"AJAX 데이터로 처리 중: {post['title']}")
                        
                        # Create placeholder dataframe with AJAX info
                        placeholder_df = create_improved_placeholder_dataframe(post, file_params)
                        placeholder_df['AJAX 데이터'] = ['있음']
                        
                        if not placeholder_df.empty:
                            update_data = {
                                'dataframe': placeholder_df,
                                'post_info': post
                            }
                            
                            if 'date' in file_params:
                                update_data['date'] = file_params['date']
                            
                            success = update_google_sheets(gs_client, update_data)
                            if success:
                                logger.info(f"AJAX 데이터로 업데이트 성공: {post['title']}")
                                data_updates.append(update_data)
                    
                    # Handle direct download URL case
                    elif 'download_url' in file_params:
                        logger.info(f"직접 다운로드 URL 처리 중: {post['title']}")
                        
                        # Create placeholder with URL info
                        placeholder_df = create_improved_placeholder_dataframe(post, file_params)
                        
                        if not placeholder_df.empty:
                            update_data = {
                                'dataframe': placeholder_df,
                                'post_info': post
                            }
                            
                            if 'date' in file_params:
                                update_data['date'] = file_params['date']
                            
                            success = update_google_sheets(gs_client, update_data)
                            if success:
                                logger.info(f"URL 정보로 업데이트 성공: {post['title']}")
                                data_updates.append(update_data)
                    
                    # Prevent rate limiting
                    time.sleep(2)
                
                except Exception as e:
                    logger.error(f"게시물 처리 중 오류: {str(e)}")
                    
                    # ENHANCED: More robust error recovery
                    try:
                        # Reset browser context without losing cookies
                        reset_browser_context(driver, delete_cookies=False)
                        logger.info("게시물 처리 오류 후 브라우저 컨텍스트 초기화")
                        
                        # Return to statistics page
                        driver.get(CONFIG['stats_url'])
                        WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.CLASS_NAME, "board_list"))
                        )
                        logger.info("통계 페이지로 복귀 성공")
                    except Exception as recovery_err:
                        logger.error(f"오류 복구 실패: {str(recovery_err)}")
                        
                        # Take screenshot for debugging
                        try:
                            driver.save_screenshot(f"error_recovery_{int(time.time())}.png")
                        except:
                            pass
                        
                        # Try reloading browser if recovery fails repeatedly
                        if ':error_count' not in CONFIG:
                            CONFIG[':error_count'] = 1
                        else:
                            CONFIG[':error_count'] += 1
                            
                        if CONFIG[':error_count'] >= 3:
                            logger.warning("Repeated errors, attempting browser reset")
                            try:
                                driver.quit()
                                driver = setup_driver()
                                driver.get(CONFIG['stats_url'])
                                CONFIG[':error_count'] = 0
                            except Exception as reset_err:
                                logger.error(f"Browser reset failed: {str(reset_err)}")
        
        # Calculate end time and execution time
        end_time = time.time()
        execution_time = end_time - start_time
        logger.info(f"실행 시간: {execution_time:.2f}초")
        
        # Send Telegram notification
        if all_posts or data_updates:
            await send_telegram_message(all_posts, data_updates)
            logger.info(f"알림 전송 완료: {len(all_posts)}개 게시물, {len(data_updates)}개 업데이트")
        else:
            logger.info(f"최근 {days_range}일 내 새 게시물이 없습니다")
            
            # Send "no results" notification (optional)
            if days_range > 7:  # Only for longer-range searches
                bot = telegram.Bot(token=CONFIG['telegram_token'])
                await bot.send_message(
                    chat_id=int(CONFIG['chat_id']),
                    text=f"📊 MSIT 통신 통계 모니터링: 최근 {days_range}일 내 새 게시물이 없습니다. ({datetime.now().strftime('%Y-%m-%d %H:%M')})"
                )
    
    except Exception as e:
        logger.error(f"모니터링 중 오류 발생: {str(e)}")
        
        try:
            # Save error screenshot
            if driver:
                try:
                    driver.save_screenshot("error_screenshot.png")
                    logger.info("오류 발생 시점 스크린샷 저장 완료")
                except Exception as ss_err:
                    logger.error(f"오류 스크린샷 저장 실패: {str(ss_err)}")
            
            # Send error notification
            bot = telegram.Bot(token=CONFIG['telegram_token'])
            error_post = {
                'title': f"모니터링 오류: {str(e)}",
                'date': datetime.now().strftime('%Y. %m. %d'),
                'department': 'System Error'
            }
            await send_telegram_message([error_post])
            logger.info("오류 알림 전송 완료")
        except Exception as telegram_err:
            logger.error(f"오류 알림 전송 중 추가 오류: {str(telegram_err)}")
    
    finally:
        # Clean up resources
        if driver:
            driver.quit()
            logger.info("WebDriver 종료")
        
        logger.info("=== MSIT 통신 통계 모니터링 종료 ===")


def create_improved_placeholder_dataframe(post_info, file_params=None):
    """
    Enhanced function to create more informative placeholder DataFrames.
    
    Args:
        post_info: Dictionary with post information
        file_params: Optional dictionary with file parameters
        
    Returns:
        pandas.DataFrame: Placeholder DataFrame with diagnostic information
    """
    try:
        # Extract date information
        date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post_info['title'])
        
        year = date_match.group(1) if date_match else "Unknown"
        month = date_match.group(2) if date_match else "Unknown"
        
        # Get report type
        report_type = determine_report_type(post_info['title'])
        
        # Determine extraction status
        extraction_status = "데이터 추출 실패"
        
        # Add diagnostic information if available
        diagnostic_info = "알 수 없는 오류"
        
        if file_params:
            if 'atch_file_no' in file_params and 'file_ord' in file_params:
                extraction_status = "문서 뷰어 접근 실패"
                diagnostic_info = f"atch_file_no={file_params['atch_file_no']}, file_ord={file_params['file_ord']}"
            elif 'content' in file_params:
                extraction_status = "HTML 콘텐츠 분석 실패"
                content_preview = file_params['content'][:100] + "..." if len(file_params['content']) > 100 else file_params['content']
                diagnostic_info = f"Content preview: {content_preview}"
            elif 'ajax_data' in file_params:
                extraction_status = "AJAX 데이터 분석 실패"
                diagnostic_info = "AJAX response received but could not be parsed"
            elif 'download_url' in file_params:
                extraction_status = "직접 다운로드 URL 접근 실패"
                diagnostic_info = f"URL: {file_params['download_url']}"
        
        # Create DataFrame with improved diagnostic information
        df = pd.DataFrame({
            '구분': [f'{year}년 {month}월 통계'],
            '보고서 유형': [report_type],
            '상태': [extraction_status],
            '진단 정보': [diagnostic_info],
            '링크': [post_info.get('url', '링크 없음')],
            '추출 시도 시간': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
        })
        
        logger.info(f"Enhanced placeholder DataFrame created: {year}년 {month}월 {report_type}")
        return df
            
    except Exception as e:
        logger.error(f"Placeholder DataFrame creation error: {str(e)}")
        # Create minimal DataFrame with error info
        return pd.DataFrame({
            '구분': ['오류 발생'],
            '업데이트 상태': ['데이터프레임 생성 실패'],
            '비고': [f'오류: {str(e)}']
        })


async def main():
    """메인 함수: 환경 변수 처리 및 모니터링 실행"""
    # 환경 변수 가져오기 (향상된 버전)
    try:
        days_range = int(os.environ.get('DAYS_RANGE', '4'))
    except ValueError:
        logger.warning("잘못된 DAYS_RANGE 형식. 기본값 4일 사용")
        days_range = 4
        
    check_sheets_str = os.environ.get('CHECK_SHEETS', 'true').lower()
    check_sheets = check_sheets_str in ('true', 'yes', '1', 'y')
    
    spreadsheet_name = os.environ.get('SPREADSHEET_NAME', 'MSIT 통신 통계')
    
    # OCR 설정 확인
    ocr_enabled_str = os.environ.get('OCR_ENABLED', 'true').lower()
    CONFIG['ocr_enabled'] = ocr_enabled_str in ('true', 'yes', '1', 'y')
    
    # 환경 설정 로그
    logger.info(f"MSIT 모니터 시작 - days_range={days_range}, check_sheets={check_sheets}, ocr_enabled={CONFIG['ocr_enabled']}")
    logger.info(f"스프레드시트 이름: {spreadsheet_name}")
    
    # 전역 설정 업데이트
    CONFIG['spreadsheet_name'] = spreadsheet_name
    
    # OCR 라이브러리 확인
    if CONFIG['ocr_enabled']:
        try:
            import pytesseract
            from PIL import Image, ImageEnhance, ImageFilter
            import cv2
            logger.info("OCR 관련 라이브러리 로드 성공")
            
            # Tesseract 가용성 확인
            try:
                pytesseract.get_tesseract_version()
                logger.info("Tesseract OCR 설치 확인됨")
            except Exception as tess_err:
                logger.warning(f"Tesseract OCR 설치 확인 실패: {str(tess_err)}")
                CONFIG['ocr_enabled'] = False
                logger.warning("OCR 기능 비활성화")
        except ImportError as import_err:
            logger.warning(f"OCR 라이브러리 가져오기 실패: {str(import_err)}")
            CONFIG['ocr_enabled'] = False
            logger.warning("OCR 기능 비활성화")
    
    # 모니터링 실행
    try:
        await run_monitor(days_range=days_range, check_sheets=check_sheets)
    except Exception as e:
        logging.error(f"메인 함수 오류: {str(e)}", exc_info=True)
        
        # 치명적 오류 시 텔레그램 알림 시도
        try:
            bot = telegram.Bot(token=CONFIG['telegram_token'])
            await bot.send_message(
                chat_id=int(CONFIG['chat_id']),
                text=f"⚠️ *MSIT 모니터링 치명적 오류*\n\n{str(e)}\n\n시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                parse_mode='Markdown'
            )
        except Exception as telegram_err:
            logger.error(f"텔레그램 메시지 전송 중 추가 오류: {str(telegram_err)}")


if __name__ == "__main__":
    asyncio.run(main())
