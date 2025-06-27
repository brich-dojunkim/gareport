#!/usr/bin/env python3
"""
현실적인 B-flow 퍼널 분석기
올바른 어트리뷰션과 현실적인 전환율 계산
"""

import pandas as pd
from datetime import datetime, timedelta
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from google.oauth2 import service_account

class RealisticFunnelAnalyzer:
    """현실적인 퍼널 분석기"""
    
    def __init__(self, credentials_path: str, property_id: str):
        self.property_id = property_id
        self.client = self._initialize_client(credentials_path)
        
        # 퍼널 단계 정의
        self.funnel_steps = {
            'AWARENESS': ['/', '/skill-guide/one-click'],
            'INTEREST': ['/posts/', '/skill-guide/'],
            'CONSIDERATION': ['/fee-information', '/providers', '/service-plan'],
        }
    
    def _initialize_client(self, credentials_path: str):
        """GA4 클라이언트 초기화"""
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=['https://www.googleapis.com/auth/analytics.readonly']
        )
        return BetaAnalyticsDataClient(credentials=credentials)
    
    def get_session_based_traffic(self, start_date: str, end_date: str) -> pd.DataFrame:
        """세션 기준 트래픽 데이터 수집 (현실적인 어트리뷰션)"""
        print(f"📊 세션 기준 트래픽 데이터 수집: {start_date} ~ {end_date}")
        
        request = RunReportRequest(
            property=f"properties/{self.property_id}",
            dimensions=[
                Dimension(name="sessionSource"),
                Dimension(name="sessionMedium")
            ],
            metrics=[
                Metric(name="activeUsers"),
                Metric(name="sessions"),
                Metric(name="screenPageViews")
            ],
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            limit=1000
        )
        
        response = self.client.run_report(request=request)
        df = self._response_to_dataframe(response)
        
        if not df.empty:
            df['source_medium'] = df['sessionSource'] + ' / ' + df['sessionMedium']
            df = df.rename(columns={
                'sessionSource': 'source',
                'sessionMedium': 'medium'
            })
        
        return df
    
    def get_conversion_events_same_period(self, start_date: str, end_date: str) -> pd.DataFrame:
        """같은 기간 전환 이벤트 수집 (동일한 어트리뷰션)"""
        print("📈 같은 기간 전환 이벤트 수집...")
        
        request = RunReportRequest(
            property=f"properties/{self.property_id}",
            dimensions=[
                Dimension(name="sessionSource"),
                Dimension(name="sessionMedium")
            ],
            metrics=[
                Metric(name="eventCount")
            ],
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimension_filter=self._create_event_filter("회원가입2"),
            limit=100
        )
        
        response = self.client.run_report(request=request)
        df = self._response_to_dataframe(response)
        
        if not df.empty:
            df['source_medium'] = df['sessionSource'] + ' / ' + df['sessionMedium']
            df = df.rename(columns={
                'sessionSource': 'source',
                'sessionMedium': 'medium',
                'eventCount': 'conversions'
            })
        
        return df
    
    def get_page_data_session_based(self, start_date: str, end_date: str) -> pd.DataFrame:
        """세션 기준 페이지 데이터 수집"""
        print(f"📄 세션 기준 페이지 데이터 수집: {start_date} ~ {end_date}")
        
        request = RunReportRequest(
            property=f"properties/{self.property_id}",
            dimensions=[
                Dimension(name="pagePath"),
                Dimension(name="sessionSource"),
                Dimension(name="sessionMedium")
            ],
            metrics=[
                Metric(name="screenPageViews"),
                Metric(name="activeUsers"),
                Metric(name="sessions")
            ],
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            limit=2000
        )
        
        response = self.client.run_report(request=request)
        df = self._response_to_dataframe(response)
        
        if not df.empty:
            df['source_medium'] = df['sessionSource'] + ' / ' + df['sessionMedium']
            df = df.rename(columns={
                'sessionSource': 'source',
                'sessionMedium': 'medium'
            })
        
        return df
    
    def analyze_funnel(self, start_date: str, end_date: str) -> dict:
        """현실적인 퍼널 분석 실행"""
        print(f"🚀 현실적인 퍼널 분석 시작: {start_date} ~ {end_date}")
        
        # 1. 세션 기준 트래픽 데이터 (같은 기간)
        traffic_data = self.get_session_based_traffic(start_date, end_date)
        if traffic_data.empty:
            print("❌ 트래픽 데이터가 없습니다")
            return {}
        
        # 2. 같은 기간 전환 데이터 (동일한 어트리뷰션)
        conversion_data = self.get_conversion_events_same_period(start_date, end_date)
        
        # 3. 페이지별 데이터
        page_data = self.get_page_data_session_based(start_date, end_date)
        
        # 4. 현실적인 전환율 계산
        source_summary = self._calculate_realistic_conversion_rates(traffic_data, conversion_data)
        
        # 5. 퍼널 단계별 분석
        funnel_details = self._analyze_funnel_steps(page_data, conversion_data)
        
        # 6. 단계별 요약
        step_summary = self._create_step_summary(funnel_details)
        
        # 7. 데이터 설명 생성
        data_explanation = self._create_data_explanation(start_date, end_date)
        
        return {
            'data_explanation': data_explanation,
            'source_summary': source_summary,
            'step_summary': step_summary,
            'funnel_details': funnel_details
        }
    
    def _calculate_realistic_conversion_rates(self, traffic_data: pd.DataFrame, 
                                            conversion_data: pd.DataFrame) -> pd.DataFrame:
        """현실적인 전환율 계산"""
        print("  📊 현실적인 전환율 계산...")
        
        # 전환 데이터를 딕셔너리로 변환
        conversion_dict = {}
        if not conversion_data.empty:
            for _, row in conversion_data.iterrows():
                conversion_dict[row['source_medium']] = row['conversions']
        
        # 트래픽 데이터에 전환 수 매칭
        results = []
        for _, row in traffic_data.iterrows():
            source_medium = row['source_medium']
            conversions = conversion_dict.get(source_medium, 0)
            
            # 현실적인 전환율 계산 (세션 기준)
            conversion_rate = (conversions / row['sessions'] * 100) if row['sessions'] > 0 else 0
            
            # 데이터 신뢰도 평가
            reliability = self._assess_reliability(row['activeUsers'], row['sessions'], conversions, conversion_rate)
            
            results.append({
                'source_medium': source_medium,
                'source': row['source'],
                'medium': row['medium'],
                'users': row['activeUsers'],
                'sessions': row['sessions'],
                'page_views': row['screenPageViews'],
                'conversions': conversions,
                'conversion_rate': round(conversion_rate, 2),
                'reliability': reliability
            })
        
        result_df = pd.DataFrame(results)
        result_df = result_df.sort_values('conversion_rate', ascending=False)
        
        return result_df
    
    def _assess_reliability(self, users: int, sessions: int, conversions: int, rate: float) -> str:
        """데이터 신뢰도 평가"""
        if conversions == 0:
            return "전환 없음"
        elif rate > 50:
            return "⚠️ 비정상적 - 검토 필요"
        elif sessions < 10:
            return "표본 부족"
        elif sessions >= 100 and rate <= 20:
            return "신뢰도 높음"
        elif sessions >= 30 and rate <= 30:
            return "신뢰도 보통"
        else:
            return "신뢰도 낮음"
    
    def _create_data_explanation(self, start_date: str, end_date: str) -> dict:
        """데이터 설명 생성"""
        return {
            'title': 'B-flow 퍼널 분석 리포트',
            'period': f'{start_date} ~ {end_date}',
            'attribution_model': 'sessionSource/sessionMedium (세션 기준)',
            'explanations': {
                'users': '해당 소스에서 유입된 고유 사용자 수',
                'sessions': '해당 소스에서 발생한 총 세션(방문) 수',
                'page_views': '해당 소스에서 발생한 총 페이지뷰 수',
                'conversions': '해당 소스에서 발생한 "회원가입2" 이벤트 수',
                'conversion_rate': '전환율 = (전환수 ÷ 세션수) × 100',
                'reliability': '데이터 신뢰도 (표본 크기와 전환율 기준)',
                'funnel_steps': {
                    'AWARENESS': '인지 단계 - 홈페이지(/) 및 원클릭 가이드 페이지',
                    'INTEREST': '관심 단계 - 블로그 포스트(/posts/) 및 스킬 가이드(/skill-guide/) 페이지',
                    'CONSIDERATION': '검토 단계 - 요금정보, 제공업체, 서비스 플랜 페이지',
                    'CONVERSION': '전환 단계 - "회원가입2" 이벤트 발생'
                }
            },
            'notes': [
                '전환율은 세션 기준으로 계산됩니다 (현실적인 지표)',
                '같은 기간 내 유입과 전환을 비교하여 정확성을 높였습니다',
                '신뢰도 높음: 100+ 세션, 20% 이하 전환율',
                '신뢰도 보통: 30+ 세션, 30% 이하 전환율',
                '비정상적: 50% 이상 전환율 (데이터 검토 필요)'
            ]
        }
    
    def _analyze_funnel_steps(self, page_data: pd.DataFrame, 
                            conversion_data: pd.DataFrame) -> pd.DataFrame:
        """퍼널 단계별 분석"""
        print("  🔄 퍼널 단계별 분석...")
        
        funnel_results = []
        
        # 페이지 기반 단계들
        for step_name, pages in self.funnel_steps.items():
            step_data = self._filter_pages_for_step(page_data, pages)
            
            if not step_data.empty:
                source_summary = step_data.groupby(['source', 'medium', 'source_medium']).agg({
                    'screenPageViews': 'sum',
                    'activeUsers': 'sum',
                    'sessions': 'sum'
                }).reset_index()
                
                for _, row in source_summary.iterrows():
                    funnel_results.append({
                        'funnel_step': step_name,
                        'source': row['source'],
                        'medium': row['medium'],
                        'source_medium': row['source_medium'],
                        'page_views': row['screenPageViews'],
                        'users': row['activeUsers'],
                        'sessions': row['sessions'],
                        'conversions': 0
                    })
        
        # 전환 단계
        if not conversion_data.empty:
            for _, row in conversion_data.iterrows():
                funnel_results.append({
                    'funnel_step': 'CONVERSION',
                    'source': row['source'],
                    'medium': row['medium'],
                    'source_medium': row['source_medium'],
                    'page_views': 0,
                    'users': 0,
                    'sessions': 0,
                    'conversions': row['conversions']
                })
        
        return pd.DataFrame(funnel_results) if funnel_results else pd.DataFrame()
    
    def _create_step_summary(self, funnel_details: pd.DataFrame) -> pd.DataFrame:
        """단계별 요약 생성"""
        if funnel_details.empty:
            return pd.DataFrame()
        
        step_summary = funnel_details.groupby('funnel_step').agg({
            'page_views': 'sum',
            'users': 'sum',
            'sessions': 'sum',
            'conversions': 'sum'
        }).reset_index()
        
        # 단계 순서 정렬
        step_order = ['AWARENESS', 'INTEREST', 'CONSIDERATION', 'CONVERSION']
        step_summary['step_order'] = step_summary['funnel_step'].apply(
            lambda x: step_order.index(x) if x in step_order else 999
        )
        step_summary = step_summary.sort_values('step_order').drop('step_order', axis=1)
        
        return step_summary
    
    def _filter_pages_for_step(self, page_data: pd.DataFrame, pages: list) -> pd.DataFrame:
        """특정 퍼널 단계에 해당하는 페이지 필터링"""
        if page_data.empty:
            return pd.DataFrame()
        
        filtered_data = pd.DataFrame()
        
        for page in pages:
            if page.endswith('/'):
                page_filter = page_data[page_data['pagePath'].str.contains(page, na=False)]
            else:
                page_filter = page_data[page_data['pagePath'] == page]
            
            if not page_filter.empty:
                filtered_data = pd.concat([filtered_data, page_filter], ignore_index=True)
        
        return filtered_data
    
    def save_to_excel(self, analysis_result: dict, filename: str = None):
        """Excel 파일로 저장 (설명 포함)"""
        if not analysis_result:
            print("❌ 저장할 데이터가 없습니다")
            return None
        
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'realistic_funnel_analysis_{timestamp}.xlsx'
        
        print(f"💾 Excel 저장: {filename}")
        
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            current_row = 0
            
            # 1. 데이터 설명 (최상단)
            if 'data_explanation' in analysis_result:
                explanation = analysis_result['data_explanation']
                
                # 제목과 기본 정보
                title_df = pd.DataFrame({
                    'A': [
                        f"=== {explanation['title']} ===",
                        f"분석 기간: {explanation['period']}",
                        f"어트리뷰션 모델: {explanation['attribution_model']}",
                        "",
                        "=== 지표 설명 ===",
                        f"• users: {explanation['explanations']['users']}",
                        f"• sessions: {explanation['explanations']['sessions']}",
                        f"• page_views: {explanation['explanations']['page_views']}",
                        f"• conversions: {explanation['explanations']['conversions']}",
                        f"• conversion_rate: {explanation['explanations']['conversion_rate']}",
                        f"• reliability: {explanation['explanations']['reliability']}",
                        "",
                        "=== 퍼널 단계 정의 ===",
                        f"• AWARENESS: {explanation['explanations']['funnel_steps']['AWARENESS']}",
                        f"• INTEREST: {explanation['explanations']['funnel_steps']['INTEREST']}",
                        f"• CONSIDERATION: {explanation['explanations']['funnel_steps']['CONSIDERATION']}",
                        f"• CONVERSION: {explanation['explanations']['funnel_steps']['CONVERSION']}",
                        "",
                        "=== 주의사항 ===",
                    ] + [f"• {note}" for note in explanation['notes']] + ["", ""]
                })
                
                title_df.to_excel(writer, sheet_name='퍼널분석결과', 
                                startrow=current_row, index=False, header=False)
                current_row += len(title_df) + 2
            
            # 2. 소스별 전환율 요약 (신뢰도 포함)
            if 'source_summary' in analysis_result and not analysis_result['source_summary'].empty:
                title_df = pd.DataFrame({'A': ['=== 소스별 전환율 요약 (현실적인 계산) ===']})
                title_df.to_excel(writer, sheet_name='퍼널분석결과', 
                                startrow=current_row, index=False, header=False)
                current_row += 2
                
                analysis_result['source_summary'].to_excel(
                    writer, sheet_name='퍼널분석결과', 
                    startrow=current_row, index=False
                )
                current_row += len(analysis_result['source_summary']) + 3
            
            # 3. 단계별 요약
            if 'step_summary' in analysis_result and not analysis_result['step_summary'].empty:
                title_df = pd.DataFrame({'A': ['=== 퍼널 단계별 요약 ===']})
                title_df.to_excel(writer, sheet_name='퍼널분석결과', 
                                startrow=current_row, index=False, header=False)
                current_row += 2
                
                analysis_result['step_summary'].to_excel(
                    writer, sheet_name='퍼널분석결과', 
                    startrow=current_row, index=False
                )
                current_row += len(analysis_result['step_summary']) + 3
            
            # 4. 상세 데이터
            if 'funnel_details' in analysis_result and not analysis_result['funnel_details'].empty:
                title_df = pd.DataFrame({'A': ['=== 단계별 상세 데이터 ===']})
                title_df.to_excel(writer, sheet_name='퍼널분석결과', 
                                startrow=current_row, index=False, header=False)
                current_row += 2
                
                analysis_result['funnel_details'].to_excel(
                    writer, sheet_name='퍼널분석결과', 
                    startrow=current_row, index=False
                )
        
        print(f"✅ 저장 완료: {filename}")
        return filename
    
    def print_summary(self, analysis_result: dict):
        """콘솔에 요약 출력 (설명 포함)"""
        if not analysis_result:
            print("❌ 출력할 데이터가 없습니다")
            return
        
        # 데이터 설명 출력
        if 'data_explanation' in analysis_result:
            explanation = analysis_result['data_explanation']
            print("\n" + "="*80)
            print(f"📊 {explanation['title']}")
            print(f"📅 분석 기간: {explanation['period']}")
            print(f"🔍 어트리뷰션 모델: {explanation['attribution_model']}")
            print("="*80)
        
        print("\n🔍 === 신뢰할 수 있는 소스 (30+ 세션) ===")
        if 'source_summary' in analysis_result and not analysis_result['source_summary'].empty:
            df = analysis_result['source_summary']
            
            # 신뢰할 수 있는 데이터만 표시
            reliable_data = df[
                (df['sessions'] >= 30) & 
                (df['reliability'].isin(['신뢰도 높음', '신뢰도 보통']))
            ]
            
            if not reliable_data.empty:
                print(reliable_data[['source_medium', 'sessions', 'conversions', 'conversion_rate', 'reliability']].head(10).to_string(index=False))
            else:
                print("신뢰할 수 있는 데이터가 없습니다 (30+ 세션)")
            
            print(f"\n📈 === 전체 상위 15개 소스 ===")
            print(df[['source_medium', 'sessions', 'conversions', 'conversion_rate', 'reliability']].head(15).to_string(index=False))
            
            # 비정상적인 데이터 경고
            problematic_data = df[df['reliability'].str.contains('비정상적')]
            if not problematic_data.empty:
                print(f"\n⚠️ === 검토가 필요한 데이터 ===")
                print(problematic_data[['source_medium', 'sessions', 'conversions', 'conversion_rate', 'reliability']].to_string(index=False))
        
        print("\n" + "="*50)
        print("📈 === 퍼널 단계별 요약 ===")
        print("="*50)
        
        if 'step_summary' in analysis_result and not analysis_result['step_summary'].empty:
            print(analysis_result['step_summary'].to_string(index=False))
        
        print("\n🔍 === 주요 인사이트 ===")
        if 'source_summary' in analysis_result and not analysis_result['source_summary'].empty:
            df = analysis_result['source_summary']
            total_sessions = df['sessions'].sum()
            total_conversions = df['conversions'].sum()
            overall_rate = (total_conversions / total_sessions * 100) if total_sessions > 0 else 0
            
            print(f"• 전체 세션: {total_sessions:,}개")
            print(f"• 전체 전환: {total_conversions:,}건")
            print(f"• 전체 전환율: {overall_rate:.2f}% (세션 기준)")
            
            # 신뢰할 수 있는 최고 성과 소스
            reliable_data = df[df['reliability'].isin(['신뢰도 높음', '신뢰도 보통'])]
            if len(reliable_data) > 0:
                best_source = reliable_data.iloc[0]
                print(f"• 최고 전환율 (신뢰도 있음): {best_source['source_medium']} ({best_source['conversion_rate']:.2f}%)")
    
    def _create_event_filter(self, event_name: str):
        """이벤트 필터 생성"""
        from google.analytics.data_v1beta.types import FilterExpression, Filter
        
        return FilterExpression(
            filter=Filter(
                field_name="eventName",
                string_filter=Filter.StringFilter(
                    match_type=Filter.StringFilter.MatchType.EXACT,
                    value=event_name
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

def main():
    """메인 실행 함수"""
    # 설정
    CREDENTIALS_PATH = "/Users/brich/secure_keys/b-flow-407402-f835e3a78bf7.json"
    PROPERTY_ID = "302932513"
    
    # 분석 기간 (최근 7일)
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    
    try:
        # 현실적인 분석기 초기화
        analyzer = RealisticFunnelAnalyzer(CREDENTIALS_PATH, PROPERTY_ID)
        
        # 퍼널 분석 실행
        analysis_result = analyzer.analyze_funnel(start_date, end_date)
        
        if analysis_result:
            # 콘솔에 요약 출력
            analyzer.print_summary(analysis_result)
            
            # Excel 저장
            filename = analyzer.save_to_excel(analysis_result)
            print(f"\n🎉 분석 완료! 결과: {filename}")
        else:
            print("❌ 분석할 데이터가 없습니다")
    
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()