"""
Framework 시트 로더
밸류업 분석 프레임워크를 로드하고 구조화
"""

import sys
from typing import List, Dict, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime

sys.stdout.reconfigure(line_buffering=True)


def log(message: str):
    """타임스탬프와 함께 로그 출력"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)


@dataclass
class FrameworkItem:
    """프레임워크 항목"""
    area_id: str
    area_name: str
    category_id: str
    category_name: str
    item_id: str
    item_name: str
    item_name_en: str
    unit: str
    is_core: bool
    data_type: str
    description: str
    extraction_keywords: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """딕셔너리로 변환"""
        return {
            'area_id': self.area_id,
            'area_name': self.area_name,
            'category_id': self.category_id,
            'category_name': self.category_name,
            'item_id': self.item_id,
            'item_name': self.item_name,
            'item_name_en': self.item_name_en,
            'unit': self.unit,
            'is_core': self.is_core,
            'data_type': self.data_type,
            'description': self.description,
            'extraction_keywords': self.extraction_keywords
        }


@dataclass
class Framework:
    """밸류업 분석 프레임워크"""
    version: str = ""
    last_modified: str = ""
    items: List[FrameworkItem] = field(default_factory=list)
    extraction_rules: List[str] = field(default_factory=list)
    
    @property
    def core_items(self) -> List[FrameworkItem]:
        """Core 항목만 반환"""
        return [item for item in self.items if item.is_core]
    
    @property
    def all_items(self) -> List[FrameworkItem]:
        """전체 항목 반환"""
        return self.items
    
    def get_items_by_area(self, area_id: str) -> List[FrameworkItem]:
        """영역별 항목 반환"""
        return [item for item in self.items if item.area_id == area_id]
    
    def get_item_by_id(self, item_id: str) -> Optional[FrameworkItem]:
        """항목 ID로 검색"""
        for item in self.items:
            if item.item_id == item_id:
                return item
        return None
    
    def to_prompt_text(self, include_non_core: bool = True) -> str:
        """LLM 프롬프트용 텍스트 생성"""
        lines = []
        lines.append("# 밸류업 분석 프레임워크")
        lines.append("")
        lines.append("## 추출 규칙")
        for rule in self.extraction_rules:
            lines.append(f"- {rule}")
        lines.append("")
        
        # 영역별로 그룹화
        areas = {}
        for item in self.items:
            if item.area_id not in areas:
                areas[item.area_id] = {
                    'name': item.area_name,
                    'categories': {}
                }
            if item.category_id not in areas[item.area_id]['categories']:
                areas[item.area_id]['categories'][item.category_id] = {
                    'name': item.category_name,
                    'items': []
                }
            if include_non_core or item.is_core:
                areas[item.area_id]['categories'][item.category_id]['items'].append(item)
        
        # 텍스트 생성
        for area_id, area_data in sorted(areas.items()):
            lines.append(f"## {area_id}. {area_data['name']}")
            for cat_id, cat_data in sorted(area_data['categories'].items()):
                if not cat_data['items']:
                    continue
                lines.append(f"### {cat_id}. {cat_data['name']}")
                for item in cat_data['items']:
                    core_mark = "[CORE]" if item.is_core else ""
                    lines.append(f"- {item.item_id} {item.item_name} ({item.item_name_en}) {core_mark}")
                    lines.append(f"  - 단위: {item.unit}, 타입: {item.data_type}")
                    lines.append(f"  - 설명: {item.description}")
                    if item.extraction_keywords:
                        lines.append(f"  - 키워드: {', '.join(item.extraction_keywords)}")
        
        return "\n".join(lines)
    
    def get_item_ids(self) -> List[str]:
        """전체 항목 ID 리스트 반환"""
        return [item.item_id for item in self.items]


class FrameworkLoader:
    """Framework 시트에서 프레임워크 로드"""
    
    def __init__(self):
        self.framework: Optional[Framework] = None
    
    def load_from_records(self, records: List[Dict]) -> Framework:
        """
        시트 레코드에서 프레임워크 로드
        
        Args:
            records: gspread get_all_records() 결과
            
        Returns:
            Framework 객체
        """
        framework = Framework()
        
        for record in records:
            section = str(record.get('section', '')).strip()
            
            if section == 'META':
                # 메타 정보 파싱
                desc = str(record.get('description', ''))
                if '버전:' in desc:
                    framework.version = desc.split(':')[-1].strip()
                elif '최종수정일:' in desc:
                    framework.last_modified = desc.split(':')[-1].strip()
            
            elif section == 'GUIDE':
                # 추출 규칙 파싱
                desc = str(record.get('description', '')).strip()
                if desc and not desc.startswith('---'):
                    framework.extraction_rules.append(desc)
            
            elif section == 'ITEM':
                # 항목 파싱
                try:
                    # 키워드 파싱
                    keywords_str = str(record.get('extraction_keywords', ''))
                    keywords = [k.strip() for k in keywords_str.split(';') if k.strip()]
                    
                    # is_core 파싱 (1.0 또는 1 또는 True)
                    is_core_val = record.get('is_core', 0)
                    is_core = bool(is_core_val) if isinstance(is_core_val, (int, float)) else str(is_core_val).lower() in ('true', '1', '1.0')
                    
                    item = FrameworkItem(
                        area_id=str(record.get('area_id', '')).strip(),
                        area_name=str(record.get('area_name', '')).strip(),
                        category_id=str(record.get('category_id', '')).strip(),
                        category_name=str(record.get('category_name', '')).strip(),
                        item_id=str(record.get('item_id', '')).strip(),
                        item_name=str(record.get('item_name', '')).strip(),
                        item_name_en=str(record.get('item_name_en', '')).strip(),
                        unit=str(record.get('unit', '')).strip(),
                        is_core=is_core,
                        data_type=str(record.get('data_type', '')).strip(),
                        description=str(record.get('description', '')).strip(),
                        extraction_keywords=keywords
                    )
                    
                    if item.item_id:  # 유효한 항목만 추가
                        framework.items.append(item)
                        
                except Exception as e:
                    log(f"  [WARN] 항목 파싱 오류: {e}")
                    continue
        
        self.framework = framework
        log(f"프레임워크 로드 완료: {len(framework.items)}개 항목, {len(framework.core_items)}개 Core 항목")
        
        return framework


def main():
    """테스트용 메인 함수"""
    # 샘플 데이터로 테스트
    sample_records = [
        {'section': 'META', 'description': '프레임워크 버전: v2.1'},
        {'section': 'META', 'description': '최종수정일: 2024-12-26'},
        {'section': 'GUIDE', 'description': 'level 0: 해당 항목 언급 없음'},
        {'section': 'GUIDE', 'description': 'level 1: 정성적 언급 (방향/계획만)'},
        {'section': 'GUIDE', 'description': 'level 2: 정량적 수치 제시'},
        {
            'section': 'ITEM',
            'area_id': 'A1',
            'area_name': '자본효율',
            'category_id': 'C01',
            'category_name': '수익성',
            'item_id': 'I01',
            'item_name': 'ROE',
            'item_name_en': 'roe',
            'unit': '%',
            'is_core': 1.0,
            'data_type': 'numeric',
            'description': '자기자본이익률',
            'extraction_keywords': 'ROE;자기자본이익률;Return on Equity'
        }
    ]
    
    loader = FrameworkLoader()
    framework = loader.load_from_records(sample_records)
    
    print(f"\n버전: {framework.version}")
    print(f"수정일: {framework.last_modified}")
    print(f"추출 규칙: {framework.extraction_rules}")
    print(f"\n프롬프트 텍스트:\n{framework.to_prompt_text()}")


if __name__ == "__main__":
    main()
