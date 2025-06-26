import json
from datetime import datetime
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange, Dimension, Metric, RunReportRequest, OrderBy
)
import pandas as pd
from google.oauth2 import service_account

class WebsiteKnowledgeGenerator:
    def __init__(self, credentials_path, property_id):
        """웹사이트 지식 JSON 생성기"""
        self.property_id = property_id
        
        # GA4 클라이언트 초기화
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=['https://www.googleapis.com/auth/analytics.readonly']
        )
        self.client = BetaAnalyticsDataClient(credentials=credentials)
    
    def generate_knowledge_json(self, filename="website_knowledge.json"):
        """웹사이트 지식을 JSON 파일로 생성"""
        print("🔍 웹사이트 지식 수집 시작...")
        
        knowledge = {
            "generated_at": datetime.now().isoformat(),
            "property_id": self.property_id,
            "website_structure": {},
            "pages": {},
            "traffic_sources": {},
            "events": {},
            "user_journey": {},
            "business_insights": {}
        }
        
        # 1. 페이지 구조 수집
        print("  📄 페이지 구조 수집 중...")
        pages_data = self._get_all_pages()
        knowledge["pages"] = self._analyze_pages(pages_data)
        knowledge["website_structure"] = self._analyze_website_structure(pages_data)
        
        # 2. 트래픽 소스 수집
        print("  🚀 트래픽 소스 수집 중...")
        traffic_data = self._get_traffic_sources()
        knowledge["traffic_sources"] = self._analyze_traffic_sources(traffic_data)
        
        # 3. 이벤트 수집
        print("  🎯 이벤트 수집 중...")
        events_data = self._get_events()
        knowledge["events"] = self._analyze_events(events_data)
        
        # 4. 사용자 여정 수집
        print("  👥 사용자 여정 수집 중...")
        landing_data = self._get_landing_pages()
        knowledge["user_journey"] = self._analyze_user_journey(landing_data)
        
        # 5. 비즈니스 인사이트 생성
        print("  💡 비즈니스 인사이트 생성 중...")
        knowledge["business_insights"] = self._generate_business_insights(
            pages_data, traffic_data, events_data, landing_data
        )
        
        # JSON 파일 저장
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(knowledge, f, ensure_ascii=False, indent=2)
        
        print(f"✅ 지식 JSON 생성 완료: {filename}")
        self._print_summary(knowledge)
        
        return knowledge
    
    def _get_all_pages(self, days=90):
        """모든 페이지 데이터 수집"""
        request = RunReportRequest(
            property=f"properties/{self.property_id}",
            dimensions=[
                Dimension(name="pagePath"),
                Dimension(name="pageTitle"),
            ],
            metrics=[
                Metric(name="screenPageViews"),
                Metric(name="activeUsers"),
                Metric(name="sessions"),
                Metric(name="engagementRate"),
                Metric(name="averageSessionDuration"),
            ],
            date_ranges=[DateRange(start_date=f"{days}daysAgo", end_date="today")],
            order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="screenPageViews"), desc=True)],
            limit=1000
        )
        response = self.client.run_report(request=request)
        return self._response_to_dict_list(response)
    
    def _get_traffic_sources(self, days=30):
        """트래픽 소스 데이터 수집"""
        request = RunReportRequest(
            property=f"properties/{self.property_id}",
            dimensions=[
                Dimension(name="sessionSource"),
                Dimension(name="sessionMedium"),
                Dimension(name="pagePath"),
            ],
            metrics=[
                Metric(name="sessions"),
                Metric(name="activeUsers"),
                Metric(name="engagementRate"),
            ],
            date_ranges=[DateRange(start_date=f"{days}daysAgo", end_date="today")],
            order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="sessions"), desc=True)],
            limit=500
        )
        response = self.client.run_report(request=request)
        return self._response_to_dict_list(response)
    
    def _get_events(self, days=30):
        """이벤트 데이터 수집"""
        request = RunReportRequest(
            property=f"properties/{self.property_id}",
            dimensions=[
                Dimension(name="eventName"),
                Dimension(name="pagePath"),
            ],
            metrics=[
                Metric(name="eventCount"),
                Metric(name="activeUsers"),
            ],
            date_ranges=[DateRange(start_date=f"{days}daysAgo", end_date="today")],
            order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="eventCount"), desc=True)],
            limit=200
        )
        response = self.client.run_report(request=request)
        return self._response_to_dict_list(response)
    
    def _get_landing_pages(self, days=30):
        """진입 페이지 데이터 수집"""
        request = RunReportRequest(
            property=f"properties/{self.property_id}",
            dimensions=[
                Dimension(name="landingPage"),
            ],
            metrics=[
                Metric(name="sessions"),
                Metric(name="bounceRate"),
                Metric(name="engagementRate"),
            ],
            date_ranges=[DateRange(start_date=f"{days}daysAgo", end_date="today")],
            order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="sessions"), desc=True)],
            limit=100
        )
        response = self.client.run_report(request=request)
        return self._response_to_dict_list(response)
    
    def _response_to_dict_list(self, response):
        """GA4 응답을 딕셔너리 리스트로 변환"""
        dimension_headers = [header.name for header in response.dimension_headers]
        metric_headers = [header.name for header in response.metric_headers]
        
        data = []
        for row in response.rows:
            row_data = {}
            
            # 차원 데이터
            for i, dimension_value in enumerate(row.dimension_values):
                row_data[dimension_headers[i]] = dimension_value.value
            
            # 측정항목 데이터
            for i, metric_value in enumerate(row.metric_values):
                value = metric_value.value
                try:
                    value = float(value)
                except ValueError:
                    pass
                row_data[metric_headers[i]] = value
            
            data.append(row_data)
        
        return data
    
    def _analyze_pages(self, pages_data):
        """페이지 분석"""
        return {
            "total_pages": len(pages_data),
            "top_pages": pages_data[:20],  # 상위 20개 페이지
            "page_categories": self._categorize_pages(pages_data),
            "performance_summary": {
                "total_pageviews": sum(p.get('screenPageViews', 0) for p in pages_data),
                "total_users": sum(p.get('activeUsers', 0) for p in pages_data),
                "avg_engagement": sum(p.get('engagementRate', 0) for p in pages_data) / len(pages_data) if pages_data else 0
            }
        }
    
    def _categorize_pages(self, pages_data):
        """페이지 카테고리 분류"""
        categories = {}
        
        for page in pages_data:
            path = page.get('pagePath', '')
            category = self._get_page_category(path)
            
            if category not in categories:
                categories[category] = {
                    "count": 0,
                    "total_views": 0,
                    "examples": []
                }
            
            categories[category]["count"] += 1
            categories[category]["total_views"] += page.get('screenPageViews', 0)
            
            if len(categories[category]["examples"]) < 3:
                categories[category]["examples"].append({
                    "path": path,
                    "title": page.get('pageTitle', ''),
                    "views": page.get('screenPageViews', 0)
                })
        
        return categories
    
    def _get_page_category(self, path):
        """페이지 경로로 카테고리 판단"""
        if path == '/':
            return 'homepage'
        elif 'login' in path:
            return 'authentication'
        elif 'order' in path:
            return 'order_management'
        elif 'product' in path:
            return 'product_management'
        elif 'distribution' in path:
            return 'distribution'
        elif any(keyword in path for keyword in ['qna', 'support', 'help', 'guide']):
            return 'support'
        elif any(keyword in path for keyword in ['post', 'blog', 'news']):
            return 'content'
        else:
            return 'other'
    
    def _analyze_website_structure(self, pages_data):
        """웹사이트 구조 분석"""
        structure = {
            "sections": {},
            "depth_analysis": {},
            "url_patterns": []
        }
        
        # 섹션별 분석
        for page in pages_data:
            path = page.get('pagePath', '')
            parts = [p for p in path.split('/') if p]
            
            if parts:
                section = parts[0]
                if section not in structure["sections"]:
                    structure["sections"][section] = {
                        "pages": 0,
                        "total_views": 0,
                        "subsections": set()
                    }
                
                structure["sections"][section]["pages"] += 1
                structure["sections"][section]["total_views"] += page.get('screenPageViews', 0)
                
                if len(parts) > 1:
                    structure["sections"][section]["subsections"].add(parts[1])
        
        # set을 list로 변환 (JSON 직렬화 때문)
        for section in structure["sections"]:
            structure["sections"][section]["subsections"] = list(structure["sections"][section]["subsections"])
        
        return structure
    
    def _analyze_traffic_sources(self, traffic_data):
        """트래픽 소스 분석"""
        sources = {}
        
        for record in traffic_data:
            source = record.get('sessionSource', 'unknown')
            medium = record.get('sessionMedium', 'unknown')
            key = f"{source}/{medium}"
            
            if key not in sources:
                sources[key] = {
                    "sessions": 0,
                    "users": 0,
                    "pages": []
                }
            
            sources[key]["sessions"] += record.get('sessions', 0)
            sources[key]["users"] += record.get('activeUsers', 0)
            
            page_path = record.get('pagePath', '')
            if page_path and page_path not in sources[key]["pages"]:
                sources[key]["pages"].append(page_path)
        
        # 상위 트래픽 소스만 저장
        top_sources = dict(sorted(sources.items(), key=lambda x: x[1]['sessions'], reverse=True)[:20])
        
        return {
            "top_sources": top_sources,
            "summary": {
                "total_sources": len(sources),
                "top_source": max(sources.items(), key=lambda x: x[1]['sessions'])[0] if sources else None
            }
        }
    
    def _analyze_events(self, events_data):
        """이벤트 분석"""
        events = {}
        conversion_events = []
        
        for record in events_data:
            event_name = record.get('eventName', '')
            
            if event_name not in events:
                events[event_name] = {
                    "total_count": 0,
                    "total_users": 0,
                    "pages": []
                }
            
            events[event_name]["total_count"] += record.get('eventCount', 0)
            events[event_name]["total_users"] += record.get('activeUsers', 0)
            
            page_path = record.get('pagePath', '')
            if page_path:
                events[event_name]["pages"].append(page_path)
            
            # 전환 이벤트 식별
            if any(keyword in event_name.lower() for keyword in ['회원가입', '구매', '신청', 'signup', 'purchase']):
                conversion_events.append({
                    "event_name": event_name,
                    "count": record.get('eventCount', 0),
                    "page": page_path
                })
        
        return {
            "all_events": events,
            "conversion_events": conversion_events,
            "top_events": dict(sorted(events.items(), key=lambda x: x[1]['total_count'], reverse=True)[:10])
        }
    
    def _analyze_user_journey(self, landing_data):
        """사용자 여정 분석"""
        return {
            "top_entry_points": landing_data[:10],
            "high_bounce_pages": [
                page for page in landing_data 
                if page.get('bounceRate', 0) > 70
            ][:10],
            "best_engagement_pages": sorted(
                landing_data, 
                key=lambda x: x.get('engagementRate', 0), 
                reverse=True
            )[:10]
        }
    
    def _generate_business_insights(self, pages_data, traffic_data, events_data, landing_data):
        """비즈니스 인사이트 생성"""
        return {
            "business_type": self._identify_business_type(pages_data),
            "key_functions": self._identify_key_functions(pages_data, events_data),
            "user_behavior_patterns": self._analyze_user_behavior(traffic_data, landing_data),
            "conversion_opportunities": self._identify_conversion_opportunities(events_data),
            "optimization_suggestions": self._generate_optimization_suggestions(pages_data, landing_data)
        }
    
    def _identify_business_type(self, pages_data):
        """비즈니스 타입 식별"""
        page_paths = [p.get('pagePath', '') for p in pages_data]
        
        if any('order' in path for path in page_paths):
            if any('distribution' in path for path in page_paths):
                return "B2B_distribution_platform"
            else:
                return "ecommerce_platform"
        elif any('product' in path for path in page_paths):
            return "product_management_platform"
        else:
            return "general_business_platform"
    
    def _identify_key_functions(self, pages_data, events_data):
        """핵심 기능 식별"""
        page_categories = self._categorize_pages(pages_data)
        event_names = [e.get('eventName', '') for e in events_data]
        
        return {
            "primary_functions": list(page_categories.keys()),
            "key_events": list(set(event_names))[:10],
            "most_used_section": max(page_categories.items(), key=lambda x: x[1]['total_views'])[0] if page_categories else None
        }
    
    def _analyze_user_behavior(self, traffic_data, landing_data):
        """사용자 행동 패턴 분석"""
        return {
            "primary_traffic_source": "direct" if any("direct" in t.get('sessionSource', '') for t in traffic_data) else "search",
            "avg_bounce_rate": sum(l.get('bounceRate', 0) for l in landing_data) / len(landing_data) if landing_data else 0,
            "engagement_pattern": "high" if sum(l.get('engagementRate', 0) for l in landing_data) / len(landing_data) > 30 else "moderate"
        }
    
    def _identify_conversion_opportunities(self, events_data):
        """전환 기회 식별"""
        conversion_events = [e for e in events_data if any(keyword in e.get('eventName', '').lower() for keyword in ['회원가입', '구매', '신청'])]
        
        return {
            "has_signup_tracking": any('회원가입' in e.get('eventName', '') for e in events_data),
            "conversion_events_count": len(conversion_events),
            "conversion_pages": list(set(e.get('pagePath', '') for e in conversion_events))
        }
    
    def _generate_optimization_suggestions(self, pages_data, landing_data):
        """최적화 제안 생성"""
        high_traffic_pages = [p for p in pages_data if p.get('screenPageViews', 0) > 1000]
        high_bounce_pages = [l for l in landing_data if l.get('bounceRate', 0) > 70]
        
        return {
            "high_traffic_pages_count": len(high_traffic_pages),
            "high_bounce_pages_count": len(high_bounce_pages),
            "optimization_priority": "bounce_rate" if len(high_bounce_pages) > 5 else "traffic_conversion"
        }
    
    def _print_summary(self, knowledge):
        """생성된 지식 요약 출력"""
        print("\n📊 생성된 지식 요약")
        print("="*50)
        print(f"📄 총 페이지: {knowledge['pages']['total_pages']:,}개")
        print(f"🏗️ 웹사이트 섹션: {len(knowledge['website_structure']['sections'])}개")
        print(f"🚀 트래픽 소스: {knowledge['traffic_sources']['summary']['total_sources']}개")
        print(f"🎯 이벤트 종류: {len(knowledge['events']['all_events'])}개")
        print(f"💼 비즈니스 타입: {knowledge['business_insights']['business_type']}")
        print(f"🔍 전환 추적: {'있음' if knowledge['business_insights']['conversion_opportunities']['has_signup_tracking'] else '없음'}")

# 실행
if __name__ == "__main__":
    # 설정
    CREDENTIALS_PATH = "/Users/brich/Downloads/gareport/b-flow-407402-f835e3a78bf7.json"
    PROPERTY_ID = "302932513"
    
    # 지식 생성기 초기화 및 실행
    generator = WebsiteKnowledgeGenerator(CREDENTIALS_PATH, PROPERTY_ID)
    knowledge = generator.generate_knowledge_json("website_knowledge.json")
    
    print("\n🎉 완료! 'website_knowledge.json' 파일을 Claude에게 업로드하세요.")
    print("그러면 자연어 요청에 맞는 GA4 분석 코드를 생성해드릴 수 있습니다!")