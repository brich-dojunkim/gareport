#!/usr/bin/env python3
"""
전환율 분석 모듈
소스별 전환율 계산 및 분석
"""

import pandas as pd
from datetime import datetime, timedelta
from ga4_client import GA4Client

class ConversionAnalyzer:
    """전환율 분석기"""
    
    def __init__(self, ga4_client: GA4Client):
        self.client = ga4_client
    
    def analyze(self, start_date: str, end_date: str, event_name: str = "회원가입2") -> dict:
        """전환율 분석 실행"""
        print(f"🚀 전환율 분석 시작: {start_date} ~ {end_date}")
        
        # 1. 트래픽 데이터 수집
        traffic_data = self.client.get_session_traffic(start_date, end_date)
        if traffic_data.empty:
            print("❌ 트래픽 데이터가 없습니다")
            return {}
        
        # 2. 전환 데이터 수집
        conversion_data = self.client.get_event_data(event_name, start_date, end_date)
        
        # 3. 전환율 계산
        result = self._calculate_conversion_rates(traffic_data, conversion_data)
        
        return {
            'analysis_info': {
                'title': 'B-flow 전환율 분석',
                'period': f'{start_date} ~ {end_date}',
                'event_name': event_name,
                'total_sessions': result['sessions'].sum(),
                'total_conversions': result['conversions'].sum(),
                'overall_rate': round((result['conversions'].sum() / result['sessions'].sum() * 100), 2) if result['sessions'].sum() > 0 else 0
            },
            'conversion_summary': result
        }
    
    def _calculate_conversion_rates(self, traffic_data: pd.DataFrame, conversion_data: pd.DataFrame) -> pd.DataFrame:
        """전환율 계산"""
        # source_medium 컬럼 생성
        traffic_data['source_medium'] = traffic_data['sessionSource'] + ' / ' + traffic_data['sessionMedium']
        
        # 전환 데이터 처리
        conversion_dict = {}
        if not conversion_data.empty:
            conversion_data['source_medium'] = conversion_data['sessionSource'] + ' / ' + conversion_data['sessionMedium']
            for _, row in conversion_data.iterrows():
                conversion_dict[row['source_medium']] = row['eventCount']
        
        # 결과 계산
        results = []
        for _, row in traffic_data.iterrows():
            conversions = conversion_dict.get(row['source_medium'], 0)
            conversion_rate = (conversions / row['sessions'] * 100) if row['sessions'] > 0 else 0
            
            results.append({
                'source_medium': row['source_medium'],
                'source': row['sessionSource'],
                'medium': row['sessionMedium'],
                'users': row['activeUsers'],
                'sessions': row['sessions'],
                'page_views': row['screenPageViews'],
                'conversions': conversions,
                'conversion_rate': round(conversion_rate, 2),
                'reliability': self._assess_reliability(row['sessions'], conversions, conversion_rate)
            })
        
        return pd.DataFrame(results).sort_values('conversion_rate', ascending=False)
    
    def _assess_reliability(self, sessions: int, conversions: int, rate: float) -> str:
        """데이터 신뢰도 평가"""
        if conversions == 0:
            return "전환 없음"
        elif rate > 50:
            return "⚠️ 비정상적"
        elif sessions >= 100 and rate <= 20:
            return "신뢰도 높음"
        elif sessions >= 30 and rate <= 30:
            return "신뢰도 보통"
        else:
            return "신뢰도 낮음"
    
    def save_to_excel(self, result: dict, filename: str = None) -> str:
        """Excel 저장"""
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'conversion_analysis_{timestamp}.xlsx'
        
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # 분석 정보
            info_df = pd.DataFrame({
                'A': [
                    f"=== {result['analysis_info']['title']} ===",
                    f"분석 기간: {result['analysis_info']['period']}",
                    f"이벤트명: {result['analysis_info']['event_name']}",
                    f"전체 세션: {result['analysis_info']['total_sessions']:,}",
                    f"전체 전환: {result['analysis_info']['total_conversions']:,}",
                    f"전체 전환율: {result['analysis_info']['overall_rate']}%",
                    "", "=== 소스별 전환율 요약 ==="
                ]
            })
            info_df.to_excel(writer, sheet_name='전환율분석', startrow=0, index=False, header=False)
            
            # 전환율 데이터
            result['conversion_summary'].to_excel(writer, sheet_name='전환율분석', startrow=len(info_df)+1, index=False)
        
        print(f"✅ 저장 완료: {filename}")
        return filename
    
    def print_summary(self, result: dict):
        """요약 출력"""
        info = result['analysis_info']
        df = result['conversion_summary']
        
        print(f"\n📊 {info['title']}")
        print(f"📅 {info['period']} | 이벤트: {info['event_name']}")
        print(f"🎯 전체: {info['total_sessions']:,}세션 → {info['total_conversions']:,}전환 ({info['overall_rate']}%)")
        print("\n" + "="*60)
        
        # 신뢰도 높은 데이터
        reliable = df[(df['sessions'] >= 30) & df['reliability'].isin(['신뢰도 높음', '신뢰도 보통'])]
        if not reliable.empty:
            print("🔍 신뢰할 수 있는 소스 (30+ 세션)")
            print(reliable[['source_medium', 'sessions', 'conversions', 'conversion_rate']].head(10).to_string(index=False))
        
        print(f"\n📈 전체 상위 15개 소스")
        print(df[['source_medium', 'sessions', 'conversions', 'conversion_rate', 'reliability']].head(15).to_string(index=False))

def main():
    """실행 예시"""
    # GA4 클라이언트 설정
    CREDENTIALS_PATH = "/Users/brich/secure_keys/b-flow-407402-f835e3a78bf7.json"
    PROPERTY_ID = "302932513"
    
    # 분석 기간
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    
    try:
        # GA4 클라이언트 초기화
        ga4_client = GA4Client(CREDENTIALS_PATH, PROPERTY_ID)
        
        # 전환율 분석기 초기화
        analyzer = ConversionAnalyzer(ga4_client)
        
        # 분석 실행
        result = analyzer.analyze(start_date, end_date)
        
        if result:
            analyzer.print_summary(result)
            analyzer.save_to_excel(result)
        
    except Exception as e:
        print(f"❌ 오류: {e}")

if __name__ == "__main__":
    main()