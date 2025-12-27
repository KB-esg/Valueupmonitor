"""
Gemini API 분석기
밸류업 PDF를 Framework 기반으로 분석
"""

import os
import sys
import json
import re
from typing import Dict, List, Optional, Any
from datetime import datetime

try:
    import google.generativeai as genai
    HAS_GENAI = True
except ImportError:
    HAS_GENAI = False

from framework_loader import Framework, FrameworkItem

sys.stdout.reconfigure(line_buffering=True)


def log(message: str):
    """타임스탬프와 함께 로그 출력"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)


class GeminiAnalyzer:
    """Gemini API를 사용한 밸류업 분석기"""
    
    # 모델 설정
    DEFAULT_MODEL = "gemini-2.0-flash"
    
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
            model_name: 모델명 (기본값: gemini-2.0-flash)
        """
        self.api_key = api_key or os.environ.get('GEM_ANALYTIC')
        self.model_name = model_name or self.DEFAULT_MODEL
        self.model = None
        
        if not HAS_GENAI:
            log("[ERROR] google-generativeai 패키지가 설치되지 않았습니다.")
            return
        
        if not self.api_key:
            log("[ERROR] Gemini API 키가 설정되지 않았습니다.")
            return
        
        # API 초기화
        genai.configure(api_key=self.api_key)
        
        # 모델 설정
        generation_config = {
            "temperature": 0.1,  # 낮은 temperature로 일관된 결과
            "top_p": 0.95,
            "top_k": 40,
            "max_output_tokens": 8192,
            "response_mime_type": "application/json"
        }
        
        self.model = genai.GenerativeModel(
            model_name=self.model_name,
            generation_config=generation_config
        )
        
        log(f"Gemini 모델 초기화 완료: {self.model_name}")
    
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
        # 항목 ID 리스트
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
        
        # 각 항목별 템플릿 추가
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
        pdf_text: str, 
        company_name: str, 
        framework: Framework
    ) -> Optional[Dict[str, Any]]:
        """
        밸류업 PDF 분석
        
        Args:
            pdf_text: PDF 추출 텍스트
            company_name: 회사명
            framework: 분석 프레임워크
            
        Returns:
            분석 결과 딕셔너리 또는 None
        """
        if not self.model:
            log("[ERROR] Gemini 모델이 초기화되지 않았습니다.")
            return None
        
        if not pdf_text or len(pdf_text) < 100:
            log("[ERROR] PDF 텍스트가 너무 짧습니다.")
            return None
        
        try:
            # 프롬프트 생성
            system_prompt = self._build_system_prompt(framework)
            user_prompt = self._build_user_prompt(pdf_text, company_name, framework)
            
            # Gemini API 호출
            full_prompt = f"{system_prompt}\n\n---\n\n{user_prompt}"
            
            log(f"  Gemini API 호출 중... (텍스트 길이: {len(pdf_text):,}자)")
            
            response = self.model.generate_content(full_prompt)
            
            if not response or not response.text:
                log("[ERROR] Gemini 응답이 비어있습니다.")
                return None
            
            # JSON 파싱
            result = self._parse_response(response.text)
            
            if result:
                # 통계 계산
                items_mentioned = sum(
                    1 for item_id, data in result.get('analysis_items', {}).items()
                    if data.get('level', 0) > 0
                )
                core_mentioned = sum(
                    1 for item in framework.core_items
                    if result.get('analysis_items', {}).get(item.item_id, {}).get('level', 0) > 0
                )
                
                log(f"  분석 완료: {items_mentioned}개 항목 언급, {core_mentioned}개 Core 항목 언급")
            
            return result
            
        except Exception as e:
            log(f"[ERROR] 분석 중 오류: {e}")
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
    
    if analyzer.model:
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
