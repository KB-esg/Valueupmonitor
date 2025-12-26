"""
KRX 종목코드 조회 모듈
회사명으로 종목코드(6자리)를 조회

사용법:
    from stock_code_mapper import StockCodeMapper
    
    mapper = StockCodeMapper()
    code = mapper.get_code("삼성전자")  # "005930"
"""

import requests
from typing import Optional, Dict
from datetime import datetime


def log(message: str):
    """타임스탬프와 함께 로그 출력"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)


class StockCodeMapper:
    """회사명 → 종목코드 매핑"""
    
    # KRX 상장종목 조회 API
    KRX_API_URL = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
    
    def __init__(self):
        """초기화 - KRX에서 전체 종목 목록 로드"""
        self._cache: Dict[str, str] = {}  # 회사명 → 종목코드
        self._cache_by_code: Dict[str, str] = {}  # 종목코드 → 회사명
        self._loaded = False
    
    def _load_stock_list(self) -> bool:
        """KRX에서 상장종목 목록 로드"""
        if self._loaded:
            return True
        
        # 방법 1: pykrx 라이브러리 시도
        if self._load_via_pykrx():
            return True
        
        # 방법 2: KRX API 직접 호출
        if self._load_via_krx_api():
            return True
        
        log("  종목 목록 로드 실패 - 모든 방법 실패")
        return False
    
    def _load_via_pykrx(self) -> bool:
        """pykrx 라이브러리로 종목 목록 로드"""
        try:
            from pykrx import stock
            
            # 오늘 날짜 기준
            today = datetime.now().strftime("%Y%m%d")
            
            # KOSPI
            kospi_tickers = stock.get_market_ticker_list(today, market="KOSPI")
            for ticker in kospi_tickers:
                name = stock.get_market_ticker_name(ticker)
                if name:
                    self._cache[name] = ticker
                    self._cache[self._normalize_name(name)] = ticker
                    self._cache_by_code[ticker] = name
            
            # KOSDAQ
            kosdaq_tickers = stock.get_market_ticker_list(today, market="KOSDAQ")
            for ticker in kosdaq_tickers:
                name = stock.get_market_ticker_name(ticker)
                if name:
                    self._cache[name] = ticker
                    self._cache[self._normalize_name(name)] = ticker
                    self._cache_by_code[ticker] = name
            
            if self._cache_by_code:
                self._loaded = True
                log(f"  pykrx로 {len(self._cache_by_code)}개 종목 로드")
                return True
            return False
            
        except ImportError:
            log("  pykrx 미설치, KRX API로 시도")
            return False
        except Exception as e:
            log(f"  pykrx 오류: {e}")
            return False
    
    def _load_via_krx_api(self) -> bool:
        """KRX API로 종목 목록 로드"""
        try:
            markets = [
                ("STK", "KOSPI"),
                ("KSQ", "KOSDAQ"),
            ]
            
            for mkt_code, mkt_name in markets:
                payload = {
                    "bld": "dbms/MDC/STAT/standard/MDCSTAT01901",
                    "locale": "ko_KR",
                    "mktId": mkt_code,
                    "share": "1",
                    "csvxls_is498": "false",
                }
                
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Referer": "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd",
                }
                
                response = requests.post(
                    self.KRX_API_URL,
                    data=payload,
                    headers=headers,
                    timeout=30
                )
                
                if response.status_code == 200:
                    data = response.json()
                    items = data.get("OutBlock_1", [])
                    
                    for item in items:
                        종목코드 = item.get("ISU_SRT_CD", "")
                        회사명 = item.get("ISU_ABBRV", "")
                        
                        if 종목코드 and 회사명:
                            normalized_name = self._normalize_name(회사명)
                            self._cache[normalized_name] = 종목코드
                            self._cache[회사명] = 종목코드
                            self._cache_by_code[종목코드] = 회사명
                    
                    log(f"  {mkt_name} 종목 {len(items)}개 로드")
            
            if self._cache_by_code:
                self._loaded = True
                log(f"  KRX API로 총 {len(self._cache_by_code)}개 종목 캐시됨")
                return True
            return False
            
        except Exception as e:
            log(f"  KRX API 오류: {e}")
            return False
    
    def _normalize_name(self, name: str) -> str:
        """회사명 정규화 - 검색 정확도 향상"""
        import re
        # 공백, 특수문자 제거
        normalized = re.sub(r'[\s\(\)\[\]\.·\-]', '', name)
        return normalized.strip()
    
    def get_code(self, company_name: str) -> Optional[str]:
        """
        회사명으로 종목코드 조회
        
        Args:
            company_name: 회사명 (예: "삼성전자", "한미반도체")
            
        Returns:
            종목코드 (6자리) 또는 None
        """
        if not self._loaded:
            self._load_stock_list()
        
        if not company_name:
            return None
        
        # 정확히 일치
        if company_name in self._cache:
            return self._cache[company_name]
        
        # 정규화된 이름으로 검색
        normalized = self._normalize_name(company_name)
        if normalized in self._cache:
            return self._cache[normalized]
        
        # 부분 일치 검색 (회사명이 포함된 경우)
        for cached_name, code in self._cache.items():
            if normalized in self._normalize_name(cached_name):
                return code
            if self._normalize_name(cached_name) in normalized:
                return code
        
        return None
    
    def get_name(self, stock_code: str) -> Optional[str]:
        """
        종목코드로 회사명 조회
        
        Args:
            stock_code: 종목코드 (6자리)
            
        Returns:
            회사명 또는 None
        """
        if not self._loaded:
            self._load_stock_list()
        
        return self._cache_by_code.get(stock_code)
    
    def get_code_bulk(self, company_names: list) -> Dict[str, Optional[str]]:
        """
        여러 회사명의 종목코드 일괄 조회
        
        Args:
            company_names: 회사명 리스트
            
        Returns:
            {회사명: 종목코드} 딕셔너리
        """
        if not self._loaded:
            self._load_stock_list()
        
        result = {}
        for name in company_names:
            result[name] = self.get_code(name)
        return result


# 싱글톤 인스턴스 (재사용)
_mapper_instance: Optional[StockCodeMapper] = None


def get_stock_code(company_name: str) -> Optional[str]:
    """
    간편 함수: 회사명으로 종목코드 조회
    
    Args:
        company_name: 회사명
        
    Returns:
        종목코드 (6자리) 또는 None
    """
    global _mapper_instance
    if _mapper_instance is None:
        _mapper_instance = StockCodeMapper()
    return _mapper_instance.get_code(company_name)


def main():
    """테스트"""
    mapper = StockCodeMapper()
    
    test_companies = [
        "삼성전자",
        "한미반도체",
        "현대차",
        "감성코퍼레이션",
        "HD현대마린솔루션",
        "카카오",
        "NAVER",
    ]
    
    log("종목코드 조회 테스트:")
    for company in test_companies:
        code = mapper.get_code(company)
        log(f"  {company} → {code or '(없음)'}")


if __name__ == "__main__":
    main()
