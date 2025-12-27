"""
Gemini API 분석기
밸류업 PDF를 Framework 기반으로 분석
새로운 google-genai 패키지 사용

분석 방식:
1. PDF 직접 전달 (우선) - Gemini의 멀티모달 기능 활용
2. 텍스트 전달 (fallback) - PDF 직접 전달 실패 시
"""

import os
import sys
import json
import re
from typing import Dict, List, Optional, Any
from datetime import datetime

# 새로운 google-genai 패키지
try:
    from google import genai
    from google.genai import types
    HAS_GENAI = True
except ImportError:
    HAS_GENAI = False

from framework_loader import Framework, FrameworkItem

sys.stdout.reconfigure(line_buffering=True)


def log(message: str):
    """타임스탬프와 함께 로그 출력"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)
    sys.stdout.flush()
    sys.stderr.flush()


class GeminiAnalyzer:
    """Gemini API를 사용한 밸류업 분석기"""
    
    # 모델 설정
    DEFAULT_MODEL = "gemini-2.0-flash-lite"
    
    # 분석 결과 템플릿
    RESULT_TEMPLATE = {
        "level": 0,  # 0: 언급없음, 1: 정성적, 2: 정량적
        "current_value": None,
        "target_value": None,
        "target_year": None,
        "note": ""
    }
    
    def __init__(self, api_key: Optional[str] = None, model_name: Optional[str] = None):
        """
        초기화
        
        Args:
            api_key: Gemini API 키 (기본값: GEM_ANALYTIC 환경변수)
            model_name: 모델명 (기본값: gemini-1.5-flash)
        """
        self.api_key = api_key or os.environ.get('GEM_ANALYTIC')
        self.model_name = model_name or self.DEFAULT_MODEL
        self.client = None
        self.last_analysis_method = None  # 마지막 분석 방식 기록
        
        if not HAS_GENAI:
            log("[ERROR] google-genai 패키지가 설치되지 않았습니다.")
            return
        
        if not self.api_key:
            log("[ERROR] Gemini API 키가 설정되지 않았습니다.")
            return
        
        # 클라이언트 초기화
        try:
            self.client = genai.Client(api_key=self.api_key)
            log(f"Gemini 클라이언트 초기화 완료: {self.model_name}")
        except Exception as e:
            log(f"[ERROR] Gemini 클라이언트 초기화 실패: {e}")
    
    @property
    def model(self):
        """하위 호환성을 위한 model 속성"""
        return self.client
    
    def _build_system_prompt(self, framework: Framework) -> str:
        """
        시스템 프롬프트 생성
        
        Args:
            framework: 분석 프레임워크
            
        Returns:
            시스템 프롬프트 텍스트
        """
        prompt = """당신은 한국 상장기업의 '기업가치 제고 계획(밸류업)' 공시를 분석하는 전문가입니다.
주어진 PDF 텍스트를 분석하여 프레임워크에 정의된 각 항목별로 정보를 추출해주세요.

## 추출 규칙

1. **level (필수)**
   - 0: 해당 항목에 대한 언급이 전혀 없음
   - 1: 정성적 언급만 있음 (방향/계획만, 예: "배당 확대 예정", "수익성 개선 노력")
   - 2: 정량적 수치가 제시됨 (예: "배당성향 40% 목표", "ROE 15% 달성")

2. **current_value**
   - 현재 수치 (보고서 기준연도 또는 최근 실적)
   - 수치가 언급되지 않으면 null

3. **target_value**
   - 목표 수치
   - 수치가 언급되지 않으면 null

4. **target_year**
   - 목표 달성 연도 (예: 2025, 2027)
   - 연도가 언급되지 않으면 null

5. **note**
   - 관련 문장 인용 또는 요약 (50자 이내)
   - 언급이 없으면 빈 문자열

## 주의사항
- 금액 단위는 억원으로 통일 (1조원 = 10000억원)
- 비율은 % 단위로 통일
- 불확실한 정보는 추측하지 말고 null로 표시
- Core 항목(is_core=true)은 반드시 분석 시도

"""
        # 프레임워크 항목 추가
        prompt += "\n## 분석 항목\n\n"
        
        for item in framework.items:
            core_mark = "[CORE]" if item.is_core else ""
            prompt += f"### {item.item_id}: {item.item_name} ({item.item_name_en}) {core_mark}\n"
            prompt += f"- 영역: {item.area_name} > {item.category_name}\n"
            prompt += f"- 단위: {item.unit}\n"
            prompt += f"- 설명: {item.description}\n"
            if item.extraction_keywords:
                prompt += f"- 키워드: {', '.join(item.extraction_keywords)}\n"
            prompt += "\n"
        
        return prompt
    
    def _build_user_prompt_for_pdf(self, company_name: str, framework: Framework) -> str:
        """
        PDF 직접 전달용 사용자 프롬프트 생성
        
        Args:
            company_name: 회사명
            framework: 분석 프레임워크
            
        Returns:
            사용자 프롬프트 텍스트
        """
        item_ids = framework.get_item_ids()
        
        prompt = f"""## 분석 대상
- 회사명: {company_name}

첨부된 PDF 문서를 분석하여 아래 JSON 형식으로 응답해주세요.
각 항목에 대해 level, current_value, target_value, target_year, note를 분석해주세요.

```json
{{
  "company_name": "{company_name}",
  "analysis_items": {{
"""
        
        for i, item_id in enumerate(item_ids):
            comma = "," if i < len(item_ids) - 1 else ""
            prompt += f"""    "{item_id}": {{
      "level": 0,
      "current_value": null,
      "target_value": null,
      "target_year": null,
      "note": ""
    }}{comma}
"""
        
        prompt += """  },
  "summary": {
    "total_items_mentioned": 0,
    "core_items_mentioned": 0,
    "key_highlights": []
  }
}
```

위 형식을 정확히 따라 JSON으로만 응답해주세요.
"""
        
        return prompt
    
    def _build_user_prompt_for_text(self, pdf_text: str, company_name: str, framework: Framework) -> str:
        """
        텍스트 전달용 사용자 프롬프트 생성
        
        Args:
            pdf_text: PDF 추출 텍스트
            company_name: 회사명
            framework: 분석 프레임워크
            
        Returns:
            사용자 프롬프트 텍스트
        """
        item_ids = framework.get_item_ids()
        
        prompt = f"""## 분석 대상
- 회사명: {company_name}

## PDF 내용
```
{pdf_text[:30000]}  
```

## 응답 형식
아래 JSON 형식으로 응답하세요. 각 항목에 대해 level, current_value, target_value, target_year, note를 분석해주세요.

```json
{{
  "company_name": "{company_name}",
  "analysis_items": {{
"""
        
        for i, item_id in enumerate(item_ids):
            comma = "," if i < len(item_ids) - 1 else ""
            prompt += f"""    "{item_id}": {{
      "level": 0,
      "current_value": null,
      "target_value": null,
      "target_year": null,
      "note": ""
    }}{comma}
"""
        
        prompt += """  },
  "summary": {
    "total_items_mentioned": 0,
    "core_items_mentioned": 0,
    "key_highlights": []
  }
}
```

위 형식을 정확히 따라 JSON으로만 응답해주세요.
"""
        
        return prompt
    
    def analyze(
        self, 
        company_name: str, 
        framework: Framework,
        pdf_bytes: Optional[bytes] = None,
        pdf_text: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        밸류업 PDF 분석
        
        분석 방식:
        1. pdf_bytes가 있으면 PDF 직접 전달 시도
        2. 실패하거나 pdf_bytes가 없으면 pdf_text로 fallback
        
        Args:
            company_name: 회사명
            framework: 분석 프레임워크
            pdf_bytes: PDF 바이너리 데이터 (Optional)
            pdf_text: PDF 추출 텍스트 (Optional, fallback용)
            
        Returns:
            분석 결과 딕셔너리 또는 None
        """
        import sys
        sys.stdout.flush()
        
        if not self.client:
            log("  [ERROR] Gemini 클라이언트가 초기화되지 않았습니다.")
            sys.stdout.flush()
            return None
        
        if not pdf_bytes and not pdf_text:
            log("  [ERROR] PDF 데이터 또는 텍스트가 필요합니다.")
            sys.stdout.flush()
            return None
        
        result = None
        
        # 1. PDF 직접 전달 시도
        if pdf_bytes:
            log(f"  [방식1] PDF 직접 전달 시도 ({len(pdf_bytes):,} bytes)...")
            sys.stdout.flush()
            
            result = self._analyze_with_pdf(pdf_bytes, company_name, framework)
            sys.stdout.flush()
            
            if result:
                self.last_analysis_method = "PDF_DIRECT"
                log("  ✓ PDF 직접 전달 성공!")
                sys.stdout.flush()
            else:
                log("  ✗ PDF 직접 전달 실패, 텍스트 방식으로 전환...")
                sys.stdout.flush()
        
        # 2. 텍스트 전달 (fallback)
        if not result and pdf_text:
            log(f"  [방식2] 텍스트 전달 시도 ({len(pdf_text):,} 글자)...")
            sys.stdout.flush()
            
            result = self._analyze_with_text(pdf_text, company_name, framework)
            sys.stdout.flush()
            
            if result:
                self.last_analysis_method = "TEXT_FALLBACK"
                log("  ✓ 텍스트 전달 성공!")
                sys.stdout.flush()
            else:
                log("  ✗ 텍스트 전달도 실패")
                sys.stdout.flush()
        
        # 결과 통계 출력
        if result:
            items_mentioned = sum(
                1 for item_id, data in result.get('analysis_items', {}).items()
                if data.get('level', 0) > 0
            )
            core_mentioned = sum(
                1 for item in framework.core_items
                if result.get('analysis_items', {}).get(item.item_id, {}).get('level', 0) > 0
            )
            
            log(f"  분석 완료 [{self.last_analysis_method}]: "
                f"{items_mentioned}개 항목 언급, {core_mentioned}개 Core 항목")
            sys.stdout.flush()
        
        return result
    
    def _analyze_with_pdf(
        self, 
        pdf_bytes: bytes, 
        company_name: str, 
        framework: Framework,
        max_retries: int = 3
    ) -> Optional[Dict[str, Any]]:
        """
        PDF 직접 전달로 분석 (Retry 로직 포함)
        
        Args:
            pdf_bytes: PDF 바이너리 데이터
            company_name: 회사명
            framework: 분석 프레임워크
            max_retries: 최대 재시도 횟수
            
        Returns:
            분석 결과 또는 None
        """
        import time
        
        # 시스템 프롬프트
        system_prompt = self._build_system_prompt(framework)
        
        # PDF용 사용자 프롬프트
        user_prompt = self._build_user_prompt_for_pdf(company_name, framework)
        
        # PDF Part 생성
        log("    → PDF Part 생성 중...")
        pdf_part = types.Part.from_bytes(
            data=pdf_bytes,
            mime_type="application/pdf"
        )
        
        # 전체 프롬프트 = 시스템 + PDF + 사용자 질문
        full_prompt = f"{system_prompt}\n\n---\n\n{user_prompt}"
        
        for attempt in range(max_retries):
            try:
                # API 호출 (PDF + 텍스트)
                log(f"    → Gemini API 호출 중... (시도 {attempt + 1}/{max_retries})")
                response = self.client.models.generate_content(
                    model=self.model_name,
                    contents=[pdf_part, full_prompt],
                    config=types.GenerateContentConfig(
                        temperature=0.1,
                        top_p=0.95,
                        top_k=40,
                        max_output_tokens=8192,
                        response_mime_type="application/json"
                    )
                )
                
                if not response:
                    log("    → Gemini 응답 객체가 None입니다.")
                    return None
                
                if not response.text:
                    log("    → Gemini 응답 텍스트가 비어있습니다.")
                    if hasattr(response, 'candidates'):
                        log(f"    → candidates: {response.candidates}")
                    return None
                
                log(f"    → 응답 수신 완료: {len(response.text):,}자")
                return self._parse_response(response.text)
                
            except Exception as e:
                error_str = str(e)
                
                # Rate Limit 오류 처리
                if '429' in error_str or 'RESOURCE_EXHAUSTED' in error_str:
                    # retryDelay 파싱 시도
                    wait_time = self._parse_retry_delay(error_str)
                    
                    if attempt < max_retries - 1:
                        log(f"    → Rate Limit 발생, {wait_time}초 대기 후 재시도...")
                        time.sleep(wait_time)
                        continue
                    else:
                        log(f"    → Rate Limit: 최대 재시도 횟수({max_retries}) 초과")
                        return None
                else:
                    log(f"    → PDF 직접 전달 오류: {type(e).__name__}: {e}")
                    import traceback
                    log(f"    → 스택 트레이스: {traceback.format_exc()[:500]}")
                    return None
        
        return None
    
    def _parse_retry_delay(self, error_str: str) -> int:
        """
        오류 메시지에서 retryDelay 파싱
        
        Args:
            error_str: 오류 메시지 문자열
            
        Returns:
            대기 시간 (초), 기본값 30초
        """
        import re
        
        # "retryDelay': '23s'" 또는 "Please retry in 23.575660186s" 패턴 찾기
        patterns = [
            r"retryDelay['\"]?\s*[:=]\s*['\"]?(\d+(?:\.\d+)?)",
            r"retry in (\d+(?:\.\d+)?)",
            r"(\d+(?:\.\d+)?)s"
        ]
        
        for pattern in patterns:
            match = re.search(pattern, error_str, re.IGNORECASE)
            if match:
                delay = float(match.group(1))
                # 최소 5초, 최대 60초
                return max(5, min(int(delay) + 2, 60))
        
        # 기본 대기 시간
        return 30
    
    def _analyze_with_text(
        self, 
        pdf_text: str, 
        company_name: str, 
        framework: Framework,
        max_retries: int = 3
    ) -> Optional[Dict[str, Any]]:
        """
        텍스트로 분석 (fallback, Retry 로직 포함)
        
        Args:
            pdf_text: PDF 추출 텍스트
            company_name: 회사명
            framework: 분석 프레임워크
            max_retries: 최대 재시도 횟수
            
        Returns:
            분석 결과 또는 None
        """
        import time
        
        if not pdf_text or len(pdf_text) < 100:
            log(f"    → 텍스트가 너무 짧습니다. (길이: {len(pdf_text) if pdf_text else 0}자)")
            return None
        
        # 프롬프트 생성
        system_prompt = self._build_system_prompt(framework)
        user_prompt = self._build_user_prompt_for_text(pdf_text, company_name, framework)
        
        # 전체 프롬프트
        full_prompt = f"{system_prompt}\n\n---\n\n{user_prompt}"
        
        log(f"    → 프롬프트 길이: {len(full_prompt):,}자")
        
        for attempt in range(max_retries):
            try:
                log(f"    → Gemini API 호출 중... (시도 {attempt + 1}/{max_retries})")
                
                # API 호출
                response = self.client.models.generate_content(
                    model=self.model_name,
                    contents=full_prompt,
                    config=types.GenerateContentConfig(
                        temperature=0.1,
                        top_p=0.95,
                        top_k=40,
                        max_output_tokens=8192,
                        response_mime_type="application/json"
                    )
                )
                
                if not response:
                    log("    → Gemini 응답 객체가 None입니다.")
                    return None
                    
                if not response.text:
                    log("    → Gemini 응답 텍스트가 비어있습니다.")
                    return None
                
                log(f"    → 응답 수신 완료: {len(response.text):,}자")
                return self._parse_response(response.text)
                
            except Exception as e:
                error_str = str(e)
                
                # Rate Limit 오류 처리
                if '429' in error_str or 'RESOURCE_EXHAUSTED' in error_str:
                    wait_time = self._parse_retry_delay(error_str)
                    
                    if attempt < max_retries - 1:
                        log(f"    → Rate Limit 발생, {wait_time}초 대기 후 재시도...")
                        time.sleep(wait_time)
                        continue
                    else:
                        log(f"    → Rate Limit: 최대 재시도 횟수({max_retries}) 초과")
                        return None
                else:
                    log(f"    → 텍스트 분석 오류: {type(e).__name__}: {e}")
                    import traceback
                    log(f"    → 스택 트레이스: {traceback.format_exc()[:500]}")
                    return None
        
        return None
    
    def _parse_response(self, response_text: str) -> Optional[Dict[str, Any]]:
        """
        Gemini 응답 파싱
        
        Args:
            response_text: Gemini API 응답 텍스트
            
        Returns:
            파싱된 JSON 또는 None
        """
        try:
            # 먼저 직접 파싱 시도
            result = json.loads(response_text)
            return result
        except json.JSONDecodeError:
            pass
        
        # JSON 블록 추출 시도
        json_patterns = [
            r'```json\s*([\s\S]*?)\s*```',
            r'```\s*([\s\S]*?)\s*```',
            r'\{[\s\S]*\}'
        ]
        
        for pattern in json_patterns:
            match = re.search(pattern, response_text)
            if match:
                try:
                    json_str = match.group(1) if '```' in pattern else match.group(0)
                    result = json.loads(json_str)
                    return result
                except json.JSONDecodeError:
                    continue
        
        log("[ERROR] JSON 파싱 실패")
        log(f"  응답 미리보기: {response_text[:500]}")
        return None
    
    def format_result_for_sheet(
        self, 
        result: Dict[str, Any], 
        framework: Framework
    ) -> Dict[str, Any]:
        """
        분석 결과를 시트 저장용 형식으로 변환
        
        Args:
            result: Gemini 분석 결과
            framework: 프레임워크
            
        Returns:
            시트 저장용 딕셔너리
        """
        sheet_data = {
            'company_name': result.get('company_name', ''),
            'analysis_timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'items': {}
        }
        
        analysis_items = result.get('analysis_items', {})
        
        for item in framework.items:
            item_data = analysis_items.get(item.item_id, self.RESULT_TEMPLATE.copy())
            
            sheet_data['items'][item.item_id] = {
                'item_name': item.item_name,
                'area_name': item.area_name,
                'category_name': item.category_name,
                'is_core': item.is_core,
                'level': item_data.get('level', 0),
                'current_value': item_data.get('current_value'),
                'target_value': item_data.get('target_value'),
                'target_year': item_data.get('target_year'),
                'note': item_data.get('note', '')
            }
        
        # 요약 정보
        sheet_data['summary'] = result.get('summary', {})
        
        return sheet_data


def main():
    """테스트용 메인 함수"""
    # 프레임워크 샘플
    from framework_loader import FrameworkLoader
    
    sample_records = [
        {'section': 'GUIDE', 'description': 'level 0: 해당 항목 언급 없음'},
        {'section': 'GUIDE', 'description': 'level 1: 정성적 언급'},
        {'section': 'GUIDE', 'description': 'level 2: 정량적 수치 제시'},
        {
            'section': 'ITEM',
            'area_id': 'A1', 'area_name': '자본효율',
            'category_id': 'C01', 'category_name': '수익성',
            'item_id': 'I01', 'item_name': 'ROE', 'item_name_en': 'roe',
            'unit': '%', 'is_core': 1.0, 'data_type': 'numeric',
            'description': '자기자본이익률',
            'extraction_keywords': 'ROE;자기자본이익률'
        },
        {
            'section': 'ITEM',
            'area_id': 'A2', 'area_name': '주주환원',
            'category_id': 'C05', 'category_name': '배당',
            'item_id': 'I13', 'item_name': '배당성향', 'item_name_en': 'payout_ratio',
            'unit': '%', 'is_core': 1.0, 'data_type': 'numeric',
            'description': '순이익 대비 배당총액 비율',
            'extraction_keywords': '배당성향;Payout Ratio'
        }
    ]
    
    loader = FrameworkLoader()
    framework = loader.load_from_records(sample_records)
    
    # 분석기 테스트
    analyzer = GeminiAnalyzer()
    
    if analyzer.client:
        # 샘플 텍스트로 테스트
        sample_text = """
        기업가치 제고 계획
        
        당사는 2027년까지 ROE 15% 달성을 목표로 합니다.
        현재 ROE는 8%입니다.
        
        주주환원 정책으로 배당성향을 현재 30%에서 2025년까지 40%로 확대할 예정입니다.
        """
        
        result = analyzer.analyze(sample_text, "테스트기업", framework)
        
        if result:
            print("\n=== 분석 결과 ===")
            print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
