"""
Claude API 분석기
밸류업 PDF를 Framework 기반으로 분석
Anthropic Claude Haiku 모델 사용

분석 방식:
1. PDF 직접 전달 (우선) - Claude의 문서 이해 기능 활용
2. 텍스트 전달 (fallback) - PDF 분석 실패 시
"""

import os
import sys
import json
import re
import base64
import time
from typing import Dict, List, Optional, Any
from datetime import datetime

# Anthropic 패키지
try:
    import anthropic
    HAS_ANTHROPIC = True
except ImportError:
    HAS_ANTHROPIC = False

from framework_loader import Framework, FrameworkItem

sys.stdout.reconfigure(line_buffering=True)


def log(message: str):
    """타임스탬프와 함께 로그 출력"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)
    sys.stdout.flush()
    sys.stderr.flush()


class ClaudeAnalyzer:
    """Claude API를 사용한 밸류업 분석기"""
    
    # 모델 설정
    DEFAULT_MODEL = "claude-3-5-haiku-20241022"
    
    # 분석 결과 템플릿 (확장 버전)
    RESULT_TEMPLATE = {
        "level": 0,  # 0: 언급없음, 1: 정성적, 2: 정량적
        "current_value": None,        # 현재값 (누적 또는 최신 실적)
        "mid_target_min": None,       # 중기 목표 최소 (1~2년 내)
        "mid_target_max": None,       # 중기 목표 최대
        "mid_target_year": None,      # 중기 목표 연도
        "long_target_min": None,      # 장기 목표 최소 (3년 이상)
        "long_target_max": None,      # 장기 목표 최대
        "long_target_year": None,     # 장기 목표 연도
        "progress_summary": "",       # 이행동향 (진행상황 요약)
        "action_plan": "",            # 목표달성방안 (계획/전략)
        "note": ""                    # 비고
    }
    
    def __init__(self, api_key: Optional[str] = None, model_name: Optional[str] = None):
        """
        초기화
        
        Args:
            api_key: Anthropic API 키 (기본값: ANTHROPIC_API_KEY 환경변수)
            model_name: 모델명 (기본값: claude-3-5-haiku-20241022)
        """
        self.api_key = api_key or os.environ.get('ANT_ANALYTIC')
        self.model_name = model_name or self.DEFAULT_MODEL
        self.client = None
        self.last_analysis_method = None  # 마지막 분석 방식 기록
        
        if not HAS_ANTHROPIC:
            log("[ERROR] anthropic 패키지가 설치되지 않았습니다.")
            log("  pip install anthropic 명령으로 설치해주세요.")
            return
        
        if not self.api_key:
            log("[ERROR] Anthropic API 키가 설정되지 않았습니다.")
            log("  ANT_ANALYTIC 환경변수를 설정해주세요.")
            return
        
        # 클라이언트 초기화
        try:
            # API 키 확인용 로그 (앞 8자만 출력)
            key_prefix = self.api_key[:8] if len(self.api_key) > 8 else "???"
            log(f"Claude API 키 확인: {key_prefix}...")
            
            self.client = anthropic.Anthropic(api_key=self.api_key)
            log(f"Claude 클라이언트 초기화 완료: {self.model_name}")
        except Exception as e:
            log(f"[ERROR] Claude 클라이언트 초기화 실패: {e}")
    
    def _build_system_prompt(self, framework: Framework) -> str:
        """
        시스템 프롬프트 생성
        
        Args:
            framework: 분석 프레임워크
            
        Returns:
            시스템 프롬프트 텍스트
        """
        prompt = """당신은 한국 상장기업의 '기업가치 제고 계획(밸류업)' 공시를 분석하는 전문가입니다.
주어진 PDF 텍스트를 분석하여 프레임워크에 정의된 각 항목별로 상세 정보를 추출해주세요.

## 추출 규칙

1. **level (필수)**
   - 0: 해당 항목에 대한 언급이 전혀 없음
   - 1: 정성적 언급만 있음 (방향/계획만)
   - 2: 정량적 수치가 제시됨

2. **current_value (현재값)**
   - 현재 실적 또는 누적 이행 현황
   - 예: ROE 3%, 자사주 소각 308만주 (누적), 배당성향 76%
   - 수치가 없으면 null

3. **중기 목표 (1~2년 내)**
   - mid_target_min: 중기 목표 최소값
   - mid_target_max: 중기 목표 최대값 (범위가 아니면 min과 동일)
   - mid_target_year: 중기 목표 연도 (예: 2025, 2026)

4. **장기 목표 (3년 이상)**
   - long_target_min: 장기 목표 최소값
   - long_target_max: 장기 목표 최대값
   - long_target_year: 장기 목표 연도 (예: 2027, 2030)

5. **progress_summary (이행동향)**
   - 현재까지의 진행상황을 간결하게 요약 (100자 이내)
   - 예: "24년 저점으로 25년부터 개선세 진입", "25.4월 5만주, 25.9월 303만주 소각 완료"

6. **action_plan (목표달성방안)**
   - 향후 계획/전략을 블릿 포인트로 정리 (150자 이내)
   - 예: "• A,B,C 영역 투자성과 창출\\n• 계열사 경쟁력 강화\\n• 광화문빌딩 매각대금 활용"

7. **note (비고)**
   - 특이사항이나 추가 설명 (50자 이내)

## 주의사항
- 금액 단위: 억원 (1조원 = 10000억원), 주식수는 주 단위
- 비율: % 단위 (기호 제외, 숫자만)
- 목표가 단일값이면 min = max로 동일하게 입력
- 중기/장기 구분이 불명확하면 문맥상 판단 (보통 1~2년=중기, 3년 이상=장기)
- Core 항목(is_core=true)은 반드시 분석 시도
- 응답은 반드시 JSON 형식으로만

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
    
    def _build_user_prompt(self, pdf_text: str, company_name: str, framework: Framework) -> str:
        """
        사용자 프롬프트 생성
        
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
각 항목에 대해 상세 분석 결과를 JSON으로 응답하세요.

**필드 설명:**
- current_value: 현재 실적/누적 이행 현황 (예: ROE 3, 자사주 소각 308만주)
- mid_target_min/max: 중기(1~2년) 목표 범위, mid_target_year: 중기 목표연도
- long_target_min/max: 장기(3년+) 목표 범위, long_target_year: 장기 목표연도
- progress_summary: 이행동향 요약 (예: "24년 저점으로 개선세 진입")
- action_plan: 목표달성방안 (예: "• ABC 영역 투자\\n• 자본효율화")

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
      "mid_target_min": null,
      "mid_target_max": null,
      "mid_target_year": null,
      "long_target_min": null,
      "long_target_max": null,
      "long_target_year": null,
      "progress_summary": "",
      "action_plan": "",
      "note": ""
    }}{comma}
"""
        
        prompt += """  },
  "summary": {
    "total_items_mentioned": 0,
    "core_items_mentioned": 0,
    "key_highlights": []
  },
  "special_notes": []
}
```

**special_notes 예시:** 광화문빌딩 매각(4000억)처럼 특별 이벤트가 있으면 별도 배열로 기록:
```json
"special_notes": [
  {
    "title": "광화문빌딩 매각",
    "amount": "4000억원",
    "date": "2025-12-31",
    "usage": "• 미래투자 (설비/R&D/M&A)\\n• 주주환원 재원",
    "status": "확정 (활용방안 검토중)"
  }
]
```

위 형식을 정확히 따라 JSON으로만 응답해주세요.
"""
        
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

첨부된 PDF 문서를 분석하여 JSON 형식으로 응답해주세요.

**필드 설명:**
- current_value: 현재 실적/누적 이행 현황 (예: ROE 3, 자사주 소각 308만주)
- mid_target_min/max: 중기(1~2년) 목표 범위, mid_target_year: 중기 목표연도
- long_target_min/max: 장기(3년+) 목표 범위, long_target_year: 장기 목표연도
- progress_summary: 이행동향 요약 (예: "24년 저점으로 개선세 진입")
- action_plan: 목표달성방안 (예: "• ABC 영역 투자\\n• 자본효율화")

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
      "mid_target_min": null,
      "mid_target_max": null,
      "mid_target_year": null,
      "long_target_min": null,
      "long_target_max": null,
      "long_target_year": null,
      "progress_summary": "",
      "action_plan": "",
      "note": ""
    }}{comma}
"""
        
        prompt += """  },
  "summary": {
    "total_items_mentioned": 0,
    "core_items_mentioned": 0,
    "key_highlights": []
  },
  "special_notes": []
}
```

**special_notes 예시:** 광화문빌딩 매각(4000억)처럼 특별 이벤트가 있으면 별도 배열로 기록:
```json
"special_notes": [
  {
    "title": "광화문빌딩 매각",
    "amount": "4000억원",
    "date": "2025-12-31",
    "usage": "• 미래투자 (설비/R&D/M&A)\\n• 주주환원 재원",
    "status": "확정 (활용방안 검토중)"
  }
]
```

위 형식을 정확히 따라 JSON으로만 응답해주세요.
"""
        
        return prompt
    
    def analyze(
        self, 
        pdf_bytes: Optional[bytes] = None,
        pdf_text: Optional[str] = None,
        company_name: str = "Unknown",
        framework: Optional[Framework] = None
    ) -> Optional[Dict[str, Any]]:
        """
        밸류업 공시 분석
        
        Args:
            pdf_bytes: PDF 바이너리 데이터 (선택)
            pdf_text: PDF 추출 텍스트 (선택)
            company_name: 회사명
            framework: 분석 프레임워크
            
        Returns:
            분석 결과 딕셔너리 또는 None
        """
        sys.stdout.flush()
        
        if not self.client:
            log("  [ERROR] Claude 클라이언트가 초기화되지 않았습니다.")
            sys.stdout.flush()
            return None
        
        if not pdf_bytes and not pdf_text:
            log("  [ERROR] PDF 데이터 또는 텍스트가 필요합니다.")
            sys.stdout.flush()
            return None
        
        result = None
        
        # 1. PDF 직접 전달 우선 시도 (Claude의 문서 이해 기능 활용)
        if pdf_bytes:
            log(f"  [방식1] PDF 직접 전달 시도 ({len(pdf_bytes):,} bytes)...")
            sys.stdout.flush()
            
            result = self._analyze_with_pdf(pdf_bytes, company_name, framework, max_retries=3)
            sys.stdout.flush()
            
            if result:
                self.last_analysis_method = "PDF_DIRECT"
                log("  ✓ PDF 직접 전달 성공!")
                sys.stdout.flush()
            else:
                log("  ✗ PDF 직접 전달 실패, 텍스트 분석으로 전환...")
                sys.stdout.flush()
        
        # 2. 텍스트 분석 (PDF 실패 시 또는 PDF 없을 때)
        if not result and pdf_text and len(pdf_text) >= 500:
            log(f"  [방식2] 텍스트 전달 시도 ({len(pdf_text):,} 글자)...")
            sys.stdout.flush()
            
            result = self._analyze_with_text(pdf_text, company_name, framework, max_retries=3)
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
                1 for item in framework.items
                if item.is_core and result.get('analysis_items', {}).get(item.item_id, {}).get('level', 0) > 0
            )
            
            log(f"  분석 완료 [{self.last_analysis_method}]: "
                f"{items_mentioned}개 항목 언급, {core_mentioned}개 Core 항목")
            sys.stdout.flush()
        
        return result
    
    def _analyze_with_text(
        self, 
        pdf_text: str, 
        company_name: str, 
        framework: Framework,
        max_retries: int = 3
    ) -> Optional[Dict[str, Any]]:
        """
        텍스트로 분석 (Retry 로직 포함)
        
        Args:
            pdf_text: PDF 추출 텍스트
            company_name: 회사명
            framework: 분석 프레임워크
            max_retries: 최대 재시도 횟수
            
        Returns:
            분석 결과 또는 None
        """
        system_prompt = self._build_system_prompt(framework)
        user_prompt = self._build_user_prompt(pdf_text, company_name, framework)
        
        log(f"    → 프롬프트 길이: {len(user_prompt):,}자")
        
        for attempt in range(max_retries):
            try:
                log(f"    → Claude API 호출 중... (시도 {attempt + 1}/{max_retries})")
                
                response = self.client.messages.create(
                    model=self.model_name,
                    max_tokens=8192,
                    system=system_prompt,
                    messages=[
                        {"role": "user", "content": user_prompt}
                    ]
                )
                
                if not response or not response.content:
                    log("    → Claude 응답이 비어있습니다.")
                    return None
                
                response_text = response.content[0].text
                log(f"    → 응답 수신 완료: {len(response_text):,}자")
                
                return self._parse_response(response_text)
                
            except anthropic.RateLimitError as e:
                error_str = str(e)
                # 오류 상세 메시지 출력 (처음 1회만)
                if attempt == 0:
                    log(f"    → Rate Limit 오류 상세: {error_str[:300]}...")
                
                wait_time = self._parse_retry_delay(error_str)
                
                if attempt < max_retries - 1:
                    log(f"    → Rate Limit 발생, {wait_time}초 대기 후 재시도...")
                    time.sleep(wait_time)
                    continue
                else:
                    log(f"    → Rate Limit: 최대 재시도 횟수({max_retries}) 초과")
                    return None
                    
            except anthropic.APIError as e:
                log(f"    → Claude API 오류: {type(e).__name__}: {e}")
                return None
                
            except Exception as e:
                log(f"    → 텍스트 분석 오류: {type(e).__name__}: {e}")
                import traceback
                log(f"    → 스택 트레이스: {traceback.format_exc()[:500]}")
                return None
        
        return None
    
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
        system_prompt = self._build_system_prompt(framework)
        user_prompt = self._build_user_prompt_for_pdf(company_name, framework)
        
        # PDF를 base64로 인코딩
        log("    → PDF Base64 인코딩 중...")
        pdf_base64 = base64.standard_b64encode(pdf_bytes).decode('utf-8')
        
        for attempt in range(max_retries):
            try:
                log(f"    → Claude API 호출 중... (시도 {attempt + 1}/{max_retries})")
                
                response = self.client.messages.create(
                    model=self.model_name,
                    max_tokens=8192,
                    system=system_prompt,
                    messages=[
                        {
                            "role": "user",
                            "content": [
                                {
                                    "type": "document",
                                    "source": {
                                        "type": "base64",
                                        "media_type": "application/pdf",
                                        "data": pdf_base64
                                    }
                                },
                                {
                                    "type": "text",
                                    "text": user_prompt
                                }
                            ]
                        }
                    ]
                )
                
                if not response or not response.content:
                    log("    → Claude 응답이 비어있습니다.")
                    return None
                
                response_text = response.content[0].text
                log(f"    → 응답 수신 완료: {len(response_text):,}자")
                
                return self._parse_response(response_text)
                
            except anthropic.RateLimitError as e:
                error_str = str(e)
                if attempt == 0:
                    log(f"    → Rate Limit 오류 상세: {error_str[:300]}...")
                
                wait_time = self._parse_retry_delay(error_str)
                
                if attempt < max_retries - 1:
                    log(f"    → Rate Limit 발생, {wait_time}초 대기 후 재시도...")
                    time.sleep(wait_time)
                    continue
                else:
                    log(f"    → Rate Limit: 최대 재시도 횟수({max_retries}) 초과")
                    return None
                    
            except anthropic.APIError as e:
                log(f"    → Claude API 오류: {type(e).__name__}: {e}")
                return None
                
            except Exception as e:
                log(f"    → PDF 직접 전달 오류: {type(e).__name__}: {e}")
                import traceback
                log(f"    → 스택 트레이스: {traceback.format_exc()[:500]}")
                return None
        
        return None
    
    def _parse_retry_delay(self, error_str: str) -> int:
        """
        오류 메시지에서 retryDelay 파싱
        
        Args:
            error_str: 오류 메시지
            
        Returns:
            대기 시간 (초)
        """
        patterns = [
            r"retry.?after['\"]?\s*[:=]\s*['\"]?(\d+(?:\.\d+)?)",
            r"(\d+(?:\.\d+)?)\s*second",
            r"retryDelay['\"]?\s*[:=]\s*['\"]?(\d+(?:\.\d+)?)",
        ]
        
        for pattern in patterns:
            match = re.search(pattern, error_str, re.IGNORECASE)
            if match:
                delay = float(match.group(1))
                return max(5, min(int(delay) + 2, 60))
        
        return 30
    
    def _parse_response(self, response_text: str) -> Optional[Dict[str, Any]]:
        """
        Claude 응답 파싱
        
        Args:
            response_text: Claude API 응답 텍스트
            
        Returns:
            파싱된 JSON 또는 None
        """
        try:
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
            result: Claude 분석 결과
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
            
            # 하위 호환: 이전 버전 target_min/max 또는 target_value
            mid_target_min = item_data.get('mid_target_min')
            mid_target_max = item_data.get('mid_target_max')
            long_target_min = item_data.get('long_target_min')
            long_target_max = item_data.get('long_target_max')
            
            # 이전 버전 호환: target_min/max만 있는 경우 → 장기 목표로 매핑
            if mid_target_min is None and long_target_min is None:
                old_target_min = item_data.get('target_min')
                old_target_max = item_data.get('target_max')
                old_target_year = item_data.get('target_year')
                
                if old_target_min is None and old_target_max is None:
                    # target_value만 있는 아주 오래된 버전
                    old_target = item_data.get('target_value')
                    if old_target is not None:
                        long_target_min = old_target
                        long_target_max = old_target
                else:
                    long_target_min = old_target_min
                    long_target_max = old_target_max
                
                # target_year도 장기로 매핑
                if item_data.get('long_target_year') is None and old_target_year:
                    item_data['long_target_year'] = old_target_year
            
            sheet_data['items'][item.item_id] = {
                'item_name': item.item_name,
                'is_core': item.is_core,
                'level': item_data.get('level', 0),
                'current_value': item_data.get('current_value'),
                'mid_target_min': mid_target_min,
                'mid_target_max': mid_target_max,
                'mid_target_year': item_data.get('mid_target_year'),
                'long_target_min': long_target_min,
                'long_target_max': long_target_max,
                'long_target_year': item_data.get('long_target_year'),
                'progress_summary': item_data.get('progress_summary', ''),
                'action_plan': item_data.get('action_plan', ''),
                'note': item_data.get('note', '')
            }
        
        # 요약 정보
        summary = result.get('summary', {})
        sheet_data['summary'] = {
            'total_items_mentioned': summary.get('total_items_mentioned', 0),
            'core_items_mentioned': summary.get('core_items_mentioned', 0),
            'key_highlights': summary.get('key_highlights', [])
        }
        
        # 특기사항
        sheet_data['special_notes'] = result.get('special_notes', [])
        
        return sheet_data


def main():
    """테스트용 메인 함수"""
    analyzer = ClaudeAnalyzer()
    
    if not analyzer.client:
        log("Claude 클라이언트 초기화 실패")
        return
    
    # 프레임워크 로드
    from framework_loader import load_framework
    framework = load_framework()
    
    if not framework:
        log("프레임워크 로드 실패")
        return
    
    log(f"프레임워크 로드 완료: {len(framework.items)}개 항목")
    
    # 테스트 텍스트
    test_text = """
    당사는 기업가치 제고를 위해 2027년까지 ROE 15% 달성을 목표로 하고 있습니다.
    현재 ROE는 8.5% 수준이며, 배당성향을 40%까지 확대할 계획입니다.
    또한 자사주 매입을 통해 주주환원을 강화하겠습니다.
    """
    
    result = analyzer.analyze(
        pdf_text=test_text,
        company_name="테스트기업",
        framework=framework
    )
    
    if result:
        log("분석 성공!")
        log(json.dumps(result, ensure_ascii=False, indent=2)[:1000])
    else:
        log("분석 실패")


if __name__ == "__main__":
    main()
