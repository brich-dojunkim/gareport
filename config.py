"""
설정 관리 모듈
GA4 연결 정보 및 분석 파라미터 관리
"""

import json
import os
from typing import Dict, Any

class Config:
    """설정 관리 클래스"""
    
    def __init__(self, config_path: str = "config.json"):
        """
        Args:
            config_path: 설정 파일 경로
        """
        self.config_path = config_path
        self._load_config()
    
    def _load_config(self):
        """설정 파일 로드"""
        if os.path.exists(self.config_path):
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config_data = json.load(f)
        else:
            # 기본 설정으로 파일 생성
            config_data = self._get_default_config()
            self._save_config(config_data)
        
        # 설정값들을 속성으로 설정
        self._set_attributes(config_data)
    
    def _get_default_config(self) -> Dict[str, Any]:
        """기본 설정값 반환"""
        return {
            # GA4 연결 정보
            "ga4": {
                "credentials_path": "/Users/brich/secure_keys/b-flow-407402-f835e3a78bf7.json",
                "property_id": "302932513"
            },

            # 퍼널 단계 정의
            "funnel_stages": {
                "awareness": {
                    "name": "AWARENESS",
                    "description": "첫 접촉 및 인지 단계",
                    "key_events": ["session_start", "first_visit"],
                    "key_pages": ["/", "/skill-guide/one-click"]
                },
                "interest": {
                    "name": "INTEREST", 
                    "description": "관심 및 참여 단계",
                    "key_events": ["user_engagement", "visit_blog"],
                    "key_pages": ["/posts/*", "/skill-guide/*"]
                },
                "consideration": {
                    "name": "CONSIDERATION",
                    "description": "서비스 검토 단계", 
                    "key_events": ["page_view"],
                    "key_pages": ["/fee-information", "/providers", "/service-plan/*"]
                },
                "conversion": {
                    "name": "CONVERSION",
                    "description": "전환 완료 단계",
                    "key_events": ["회원가입2"],
                    "key_pages": ["/provider-join"]
                }
            },
            
            # 분석 파라미터
            "analysis": {
                "min_engagement_time": 15,  # 최소 참여 시간 (초)
                "session_timeout": 1800,    # 세션 타임아웃 (초)
                "conversion_window": 7,     # 전환 윈도우 (일)
                "min_sample_size": 30       # 최소 샘플 사이즈
            },
            
            # 페이지 카테고리 매핑
            "page_categories": {
                "homepage": ["/"],
                "authentication": ["/login", "/id-login", "/provider-join"],
                "content": ["/posts/*", "/skill-guide/*", "/p-posts/*"],
                "service_info": ["/fee-information", "/providers", "/service-plan/*"],
                "core_features": ["/order/*", "/products/*", "/distribution/*"],
                "support": ["/p-posts/qna", "/skill-guide/*"]
            },
            
            # 트래픽 소스 그룹핑
            "traffic_groups": {
                "paid_search": ["google/cpc", "naver/cpc", "naver/cpc2"],
                "organic_search": ["google/organic", "naver/organic", "daum/organic"],
                "social_media": ["blog/sns", "instagram/*", "youtube/*"],
                "referral": ["*referral", "blog/*"],
                "direct": ["(direct)/(none)", "(not set)/(not set)"],
                "email": ["stibee/*"]
            },
            
            # 리포트 설정
            "reporting": {
                "output_formats": ["json", "excel", "html"],
                "include_charts": True,
                "detailed_breakdowns": True,
                "optimization_recommendations": True
            }
        }
    
    def _save_config(self, config_data: Dict[str, Any]):
        """설정을 파일로 저장"""
        with open(self.config_path, 'w', encoding='utf-8') as f:
            json.dump(config_data, f, ensure_ascii=False, indent=2)
    
    def _set_attributes(self, config_data: Dict[str, Any]):
        """설정값들을 클래스 속성으로 설정"""
        # GA4 연결 정보
        self.credentials_path = config_data['ga4']['credentials_path']
        self.property_id = config_data['ga4']['property_id']
        
        # 퍼널 단계 설정
        self.funnel_stages = config_data['funnel_stages']
        
        # 분석 파라미터
        self.min_engagement_time = config_data['analysis']['min_engagement_time']
        self.session_timeout = config_data['analysis']['session_timeout']
        self.conversion_window = config_data['analysis']['conversion_window']
        self.min_sample_size = config_data['analysis']['min_sample_size']
        
        # 페이지 카테고리
        self.page_categories = config_data['page_categories']
        
        # 트래픽 그룹
        self.traffic_groups = config_data['traffic_groups']
        
        # 리포트 설정
        self.reporting = config_data['reporting']
    
    def get_stage_config(self, stage_name: str) -> Dict[str, Any]:
        """특정 퍼널 단계 설정 반환"""
        return self.funnel_stages.get(stage_name.lower(), {})
    
    def get_page_category(self, page_path: str) -> str:
        """페이지 경로로 카테고리 반환"""
        for category, patterns in self.page_categories.items():
            for pattern in patterns:
                if pattern.endswith('*'):
                    # 와일드카드 매칭
                    prefix = pattern[:-1]
                    if page_path.startswith(prefix):
                        return category
                else:
                    # 정확한 매칭
                    if page_path == pattern:
                        return category
        return 'other'
    
    def get_traffic_group(self, source: str, medium: str) -> str:
        """트래픽 소스/미디엄으로 그룹 반환"""
        source_medium = f"{source}/{medium}"
        
        for group, patterns in self.traffic_groups.items():
            for pattern in patterns:
                if pattern.endswith('*'):
                    # 와일드카드 매칭
                    prefix = pattern[:-1]
                    if source_medium.startswith(prefix):
                        return group
                else:
                    # 정확한 매칭
                    if source_medium == pattern:
                        return group
        return 'other'
    
    def update_config(self, key_path: str, value: Any):
        """설정값 업데이트 및 저장"""
        # 현재 설정 로드
        with open(self.config_path, 'r', encoding='utf-8') as f:
            config_data = json.load(f)
        
        # 중첩된 키 경로 처리 (예: "analysis.min_engagement_time")
        keys = key_path.split('.')
        current = config_data
        for key in keys[:-1]:
            current = current[key]
        current[keys[-1]] = value
        
        # 파일 저장 및 속성 업데이트
        self._save_config(config_data)
        self._set_attributes(config_data)
        
        print(f"설정 업데이트됨: {key_path} = {value}")

# 사용 예시
if __name__ == "__main__":
    # 설정 초기화
    config = Config()
    
    # 설정값 확인
    print(f"Property ID: {config.property_id}")
    print(f"최소 참여 시간: {config.min_engagement_time}초")
    
    # 페이지 카테고리 테스트
    print(f"/posts/739 카테고리: {config.get_page_category('/posts/739')}")
    print(f"/ 카테고리: {config.get_page_category('/')}")
    
    # 트래픽 그룹 테스트
    print(f"google/cpc 그룹: {config.get_traffic_group('google', 'cpc')}")
    print(f"(direct)/(none) 그룹: {config.get_traffic_group('(direct)', '(none)')}")