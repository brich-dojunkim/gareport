"""
GA4 데이터 수집 모듈
퍼널 분석에 필요한 모든 데이터를 GA4에서 수집
"""

import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange, Dimension, Metric, RunReportRequest, 
    OrderBy, FilterExpression, Filter
)
from google.oauth2 import service_account

class GA4DataLoader:
    """GA4 데이터 수집 클래스"""
    
    def __init__(self, credentials_path: str, property_id: str):
        """
        Args:
            credentials_path: GA4 서비스 계정 키 파일 경로
            property_id: GA4 속성 ID
        """
        self.property_id = property_id
        self.client = self._initialize_client(credentials_path)
    
    def _initialize_client(self, credentials_path: str) -> BetaAnalyticsDataClient:
        """GA4 클라이언트 초기화"""
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=['https://www.googleapis.com/auth/analytics.readonly']
        )
        return BetaAnalyticsDataClient(credentials=credentials)
    
    def get_funnel_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        퍼널 분석용 기본 데이터 수집
        
        Args:
            start_date: 시작일 (YYYY-MM-DD)
            end_date: 종료일 (YYYY-MM-DD)
            
        Returns:
            DataFrame with columns: [user_pseudo_id, ga_session_id, event_name, 
                                   page_location, session_source, session_medium, 
                                   event_timestamp, engagement_time_msec]
        """
        print(f"  📥 기본 퍼널 데이터 수집: {start_date} ~ {end_date}")
        
        request = RunReportRequest(
            property=f"properties/{self.property_id}",
            dimensions=[
                Dimension(name="userPseudoId"),
                Dimension(name="sessionId"), 
                Dimension(name="eventName"),
                Dimension(name="pagePath"),
                Dimension(name="sessionSource"),
                Dimension(name="sessionMedium"),
                Dimension(name="deviceCategory"),
                Dimension(name="country")
            ],
            metrics=[
                Metric(name="eventCount"),
                Metric(name="engagementTimeMsec"),
                Metric(name="screenPageViews")
            ],
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            order_bys=[
                OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name="userPseudoId")),
                OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name="eventTimestamp"))
            ],
            limit=100000  # 대용량 데이터 처리
        )
        
        response = self.client.run_report(request=request)
        df = self._response_to_dataframe(response)
        
        print(f"  ✅ 수집 완료: {len(df):,}개 이벤트")
        return df
    
    def get_page_sequences(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        사용자별 페이지 시퀀스 데이터 수집
        
        Returns:
            DataFrame with page navigation sequences per user
        """
        print(f"  📥 페이지 시퀀스 데이터 수집: {start_date} ~ {end_date}")
        
        request = RunReportRequest(
            property=f"properties/{self.property_id}",
            dimensions=[
                Dimension(name="userPseudoId"),
                Dimension(name="sessionId"),
                Dimension(name="pagePath"),
                Dimension(name="eventTimestamp")
            ],
            metrics=[
                Metric(name="screenPageViews")
            ],
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimension_filter=FilterExpression(
                filter=Filter(
                    field_name="eventName",
                    string_filter=Filter.StringFilter(
                        match_type=Filter.StringFilter.MatchType.EXACT,
                        value="page_view"
                    )
                )
            ),
            order_bys=[
                OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name="userPseudoId")),
                OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name="eventTimestamp"))
            ],
            limit=50000
        )
        
        response = self.client.run_report(request=request)
        df = self._response_to_dataframe(response)
        
        print(f"  ✅ 수집 완료: {len(df):,}개 페이지뷰")
        return df
    
    def get_conversion_events(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        전환 이벤트 상세 데이터 수집
        
        Returns:
            DataFrame with conversion event details
        """
        print(f"  📥 전환 이벤트 데이터 수집: {start_date} ~ {end_date}")
        
        request = RunReportRequest(
            property=f"properties/{self.property_id}",
            dimensions=[
                Dimension(name="userPseudoId"),
                Dimension(name="sessionId"),
                Dimension(name="eventName"),
                Dimension(name="pagePath"),
                Dimension(name="sessionSource"),
                Dimension(name="sessionMedium"),
                Dimension(name="eventTimestamp")
            ],
            metrics=[
                Metric(name="eventCount")
            ],
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimension_filter=FilterExpression(
                filter=Filter(
                    field_name="eventName",
                    string_filter=Filter.StringFilter(
                        match_type=Filter.StringFilter.MatchType.EXACT,
                        value="회원가입2"
                    )
                )
            ),
            order_bys=[
                OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name="eventTimestamp"))
            ],
            limit=10000
        )
        
        response = self.client.run_report(request=request)
        df = self._response_to_dataframe(response)
        
        print(f"  ✅ 수집 완료: {len(df):,}개 전환")
        return df
    
    def get_traffic_source_details(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        트래픽 소스별 상세 성과 데이터 수집
        
        Returns:
            DataFrame with traffic source performance details
        """
        print(f"  📥 트래픽 소스 상세 데이터 수집: {start_date} ~ {end_date}")
        
        request = RunReportRequest(
            property=f"properties/{self.property_id}",
            dimensions=[
                Dimension(name="sessionSource"),
                Dimension(name="sessionMedium"),
                Dimension(name="firstUserCampaignName"),
                Dimension(name="landingPage")
            ],
            metrics=[
                Metric(name="activeUsers"),
                Metric(name="newUsers"),
                Metric(name="sessions"),
                Metric(name="engagementRate"),
                Metric(name="bounceRate"),
                Metric(name="averageSessionDuration"),
                Metric(name="screenPageViewsPerSession")
            ],
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            order_bys=[
                OrderBy(metric=OrderBy.MetricOrderBy(metric_name="activeUsers"), desc=True)
            ],
            limit=500
        )
        
        response = self.client.run_report(request=request)
        df = self._response_to_dataframe(response)
        
        print(f"  ✅ 수집 완료: {len(df):,}개 소스")
        return df
    
    def get_content_performance(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        콘텐츠별 성과 데이터 수집 (블로그, 가이드 등)
        
        Returns:
            DataFrame with content performance metrics
        """
        print(f"  📥 콘텐츠 성과 데이터 수집: {start_date} ~ {end_date}")
        
        request = RunReportRequest(
            property=f"properties/{self.property_id}",
            dimensions=[
                Dimension(name="pagePath"),
                Dimension(name="pageTitle")
            ],
            metrics=[
                Metric(name="screenPageViews"),
                Metric(name="activeUsers"),
                Metric(name="averageSessionDuration"),
                Metric(name="engagementRate")
            ],
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimension_filter=FilterExpression(
                or_group=FilterExpression.FilterExpressionList(
                    expressions=[
                        FilterExpression(
                            filter=Filter(
                                field_name="pagePath",
                                string_filter=Filter.StringFilter(
                                    match_type=Filter.StringFilter.MatchType.CONTAINS,
                                    value="/posts/"
                                )
                            )
                        ),
                        FilterExpression(
                            filter=Filter(
                                field_name="pagePath", 
                                string_filter=Filter.StringFilter(
                                    match_type=Filter.StringFilter.MatchType.CONTAINS,
                                    value="/skill-guide/"
                                )
                            )
                        )
                    ]
                )
            ),
            order_bys=[
                OrderBy(metric=OrderBy.MetricOrderBy(metric_name="screenPageViews"), desc=True)
            ],
            limit=200
        )
        
        response = self.client.run_report(request=request)
        df = self._response_to_dataframe(response)
        
        print(f"  ✅ 수집 완료: {len(df):,}개 콘텐츠")
        return df
    
    def _response_to_dataframe(self, response) -> pd.DataFrame:
        """GA4 응답을 pandas DataFrame으로 변환"""
        if not response.rows:
            return pd.DataFrame()
        
        # 헤더 정보 추출
        dimension_headers = [header.name for header in response.dimension_headers]
        metric_headers = [header.name for header in response.metric_headers]
        
        # 데이터 변환
        data = []
        for row in response.rows:
            row_data = {}
            
            # 차원 데이터
            for i, dimension_value in enumerate(row.dimension_values):
                row_data[dimension_headers[i]] = dimension_value.value
            
            # 측정항목 데이터 (숫자 변환)
            for i, metric_value in enumerate(row.metric_values):
                try:
                    # 정수로 변환 시도
                    if '.' not in metric_value.value:
                        row_data[metric_headers[i]] = int(metric_value.value)
                    else:
                        row_data[metric_headers[i]] = float(metric_value.value)
                except ValueError:
                    row_data[metric_headers[i]] = metric_value.value
            
            data.append(row_data)
        
        df = pd.DataFrame(data)
        
        # 데이터 타입 최적화
        return self._optimize_dataframe(df)
    
    def _optimize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """DataFrame 최적화 (메모리 효율성)"""
        if df.empty:
            return df
        
        # 문자열 컬럼은 category로 변환 (메모리 절약)
        string_columns = df.select_dtypes(include=['object']).columns
        for col in string_columns:
            if df[col].nunique() / len(df) < 0.5:  # 고유값이 50% 미만인 경우
                df[col] = df[col].astype('category')
        
        return df
    
    def get_data_summary(self, start_date: str, end_date: str) -> Dict:
        """
        데이터 수집 요약 정보 반환
        
        Returns:
            데이터 수집 요약 딕셔너리
        """
        print(f"📊 데이터 요약 정보 수집: {start_date} ~ {end_date}")
        
        # 기본 통계
        basic_stats = self.get_funnel_data(start_date, end_date)
        conversions = self.get_conversion_events(start_date, end_date)
        
        summary = {
            'period': f"{start_date} ~ {end_date}",
            'total_events': len(basic_stats),
            'unique_users': basic_stats['userPseudoId'].nunique() if not basic_stats.empty else 0,
            'total_sessions': basic_stats['sessionId'].nunique() if not basic_stats.empty else 0,
            'total_conversions': len(conversions),
            'conversion_rate': len(conversions) / basic_stats['userPseudoId'].nunique() * 100 if not basic_stats.empty else 0,
            'data_collection_time': datetime.now().isoformat()
        }
        
        print(f"  📈 요약: 사용자 {summary['unique_users']:,}명, 세션 {summary['total_sessions']:,}개, 전환 {summary['total_conversions']:,}건")
        
        return summary

# 사용 예시
if __name__ == "__main__":
    # 데이터 로더 초기화
    loader = GA4DataLoader(
        credentials_path="/Users/brich/secure_keys/b-flow-407402-f835e3a78bf7.json",
        property_id="302932513"
    )
    
    # 최근 30일 데이터 수집
    from datetime import datetime, timedelta
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    
    # 각종 데이터 수집
    funnel_data = loader.get_funnel_data(start_date, end_date)
    conversions = loader.get_conversion_events(start_date, end_date)
    summary = loader.get_data_summary(start_date, end_date)
    
    print("데이터 수집 완료!")