#!/usr/bin/env python3
"""
GA4 기본 클라이언트 모듈
Google Analytics 4 API 연결 및 기본 데이터 수집 기능 제공
다양한 분석 모듈에서 공통으로 사용
"""

import pandas as pd
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest, FilterExpression, Filter
from google.oauth2 import service_account

class GA4Client:
    """GA4 기본 클라이언트"""
    
    def __init__(self, credentials_path: str, property_id: str):
        self.property_id = property_id
        self.client = self._initialize_client(credentials_path)
    
    def _initialize_client(self, credentials_path: str):
        """GA4 클라이언트 초기화"""
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=['https://www.googleapis.com/auth/analytics.readonly']
        )
        return BetaAnalyticsDataClient(credentials=credentials)
    
    def get_report(self, dimensions: list, metrics: list, start_date: str, end_date: str, 
                   filters: FilterExpression = None, limit: int = 1000) -> pd.DataFrame:
        """범용 리포트 수집 메서드"""
        request = RunReportRequest(
            property=f"properties/{self.property_id}",
            dimensions=[Dimension(name=dim) for dim in dimensions],
            metrics=[Metric(name=metric) for metric in metrics],
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            limit=limit
        )
        
        if filters:
            request.dimension_filter = filters
        
        response = self.client.run_report(request=request)
        return self._response_to_dataframe(response)
    
    def get_session_traffic(self, start_date: str, end_date: str) -> pd.DataFrame:
        """세션 기준 트래픽 데이터 수집"""
        return self.get_report(
            dimensions=["sessionSource", "sessionMedium"],
            metrics=["activeUsers", "sessions", "screenPageViews"],
            start_date=start_date,
            end_date=end_date
        )
    
    def get_page_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        """페이지별 데이터 수집"""
        return self.get_report(
            dimensions=["pagePath", "sessionSource", "sessionMedium"],
            metrics=["screenPageViews", "activeUsers", "sessions"],
            start_date=start_date,
            end_date=end_date,
            limit=2000
        )
    
    def get_event_data(self, event_name: str, start_date: str, end_date: str) -> pd.DataFrame:
        """특정 이벤트 데이터 수집"""
        event_filter = self.create_event_filter(event_name)
        return self.get_report(
            dimensions=["sessionSource", "sessionMedium"],
            metrics=["eventCount"],
            start_date=start_date,
            end_date=end_date,
            filters=event_filter,
            limit=100
        )
    
    def get_user_behavior(self, start_date: str, end_date: str) -> pd.DataFrame:
        """사용자 행동 데이터 수집"""
        return self.get_report(
            dimensions=["sessionSource", "sessionMedium", "deviceCategory"],
            metrics=["activeUsers", "sessions", "averageSessionDuration", "bounceRate"],
            start_date=start_date,
            end_date=end_date
        )
    
    def get_ecommerce_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        """이커머스 데이터 수집"""
        return self.get_report(
            dimensions=["sessionSource", "sessionMedium"],
            metrics=["purchaseRevenue", "transactions", "averagePurchaseRevenue"],
            start_date=start_date,
            end_date=end_date
        )
    
    def create_event_filter(self, event_name: str) -> FilterExpression:
        """이벤트 필터 생성"""
        return FilterExpression(
            filter=Filter(
                field_name="eventName",
                string_filter=Filter.StringFilter(
                    match_type=Filter.StringFilter.MatchType.EXACT,
                    value=event_name
                )
            )
        )
    
    def create_page_filter(self, page_path: str, match_type: str = "EXACT") -> FilterExpression:
        """페이지 필터 생성"""
        match_types = {
            "EXACT": Filter.StringFilter.MatchType.EXACT,
            "CONTAINS": Filter.StringFilter.MatchType.CONTAINS,
            "BEGINS_WITH": Filter.StringFilter.MatchType.BEGINS_WITH
        }
        
        return FilterExpression(
            filter=Filter(
                field_name="pagePath",
                string_filter=Filter.StringFilter(
                    match_type=match_types.get(match_type, Filter.StringFilter.MatchType.EXACT),
                    value=page_path
                )
            )
        )
    
    def _response_to_dataframe(self, response) -> pd.DataFrame:
        """GA4 응답을 DataFrame으로 변환"""
        if not response.rows:
            return pd.DataFrame()
        
        dimension_headers = [header.name for header in response.dimension_headers]
        metric_headers = [header.name for header in response.metric_headers]
        
        data = []
        for row in response.rows:
            row_data = {}
            
            for i, dimension_value in enumerate(row.dimension_values):
                row_data[dimension_headers[i]] = dimension_value.value
            
            for i, metric_value in enumerate(row.metric_values):
                try:
                    if '.' not in metric_value.value:
                        row_data[metric_headers[i]] = int(metric_value.value)
                    else:
                        row_data[metric_headers[i]] = float(metric_value.value)
                except ValueError:
                    row_data[metric_headers[i]] = metric_value.value
            
            data.append(row_data)
        
        return pd.DataFrame(data)