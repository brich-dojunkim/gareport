"""
전환율 계산 모듈
퍼널 단계별 전환율 및 다양한 조합별 성과 측정
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from config import Config

class ConversionCalculator:
    """전환율 계산 클래스"""
    
    def __init__(self, config: Config = None):
        """
        Args:
            config: 설정 객체
        """
        self.config = config or Config()
    
    def calculate_stage_conversions(self, classified_df: pd.DataFrame, 
                                  conversion_events: pd.DataFrame) -> Dict:
        """
        퍼널 단계별 전환율 계산
        
        Args:
            classified_df: 퍼널 단계가 분류된 데이터프레임
            conversion_events: 전환 이벤트 데이터프레임
            
        Returns:
            단계별 전환율 딕셔너리
        """
        print("📈 퍼널 단계별 전환율 계산...")
        
        if classified_df.empty:
            return {'error': '분류된 데이터가 없습니다'}
        
        # 사용자별 퍼널 진행 상황 파악
        user_progression = self._get_user_progression(classified_df)
        
        # 단계별 사용자 수 계산
        stage_counts = {
            'awareness': len(user_progression[user_progression['reached_awareness']]),
            'interest': len(user_progression[user_progression['reached_interest']]),
            'consideration': len(user_progression[user_progression['reached_consideration']]),
            'conversion': len(user_progression[user_progression['reached_conversion']])
        }
        
        # 전환율 계산
        total_users = len(user_progression)
        conversion_rates = {
            'total_users': total_users,
            'stage_counts': stage_counts,
            'stage_rates': {
                'awareness_rate': stage_counts['awareness'] / total_users * 100,
                'interest_rate': stage_counts['interest'] / total_users * 100,
                'consideration_rate': stage_counts['consideration'] / total_users * 100,
                'conversion_rate': stage_counts['conversion'] / total_users * 100
            },
            'step_conversions': {
                'awareness_to_interest': (stage_counts['interest'] / stage_counts['awareness'] * 100) if stage_counts['awareness'] > 0 else 0,
                'interest_to_consideration': (stage_counts['consideration'] / stage_counts['interest'] * 100) if stage_counts['interest'] > 0 else 0,
                'consideration_to_conversion': (stage_counts['conversion'] / stage_counts['consideration'] * 100) if stage_counts['consideration'] > 0 else 0
            },
            'overall_conversion': stage_counts['conversion'] / total_users * 100
        }
        
        print(f"  📊 전체 사용자: {total_users:,}명")
        print(f"  🎯 전체 전환율: {conversion_rates['overall_conversion']:.1f}%")
        print(f"  📈 단계별 전환율:")
        for step, rate in conversion_rates['step_conversions'].items():
            print(f"    - {step}: {rate:.1f}%")
        
        return conversion_rates
    
    def calculate_source_conversions(self, classified_df: pd.DataFrame) -> pd.DataFrame:
        """
        트래픽 소스별 전환율 계산
        
        Returns:
            소스별 전환율 데이터프레임
        """
        print("🚀 트래픽 소스별 전환율 계산...")
        
        source_conversions = []
        
        # 사용자별 첫 번째 소스와 전환 여부 매핑
        user_sources = classified_df.groupby('userPseudoId').agg({
            'sessionSource': 'first',
            'sessionMedium': 'first',
            'funnel_stage': lambda x: 'CONVERSION' in x.values
        }).rename(columns={'funnel_stage': 'converted'})
        
        # 소스+미디엄 조합별 집계
        source_groups = user_sources.groupby(['sessionSource', 'sessionMedium'])
        
        for (source, medium), group in source_groups:
            total_users = len(group)
            conversions = group['converted'].sum()
            conversion_rate = conversions / total_users * 100
            
            # 트래픽 그룹 분류
            traffic_group = self.config.get_traffic_group(source, medium)
            
            source_conversions.append({
                'source': source,
                'medium': medium,
                'source_medium': f"{source} / {medium}",
                'traffic_group': traffic_group,
                'total_users': total_users,
                'conversions': conversions,
                'conversion_rate': conversion_rate,
                'traffic_quality': self._assess_traffic_quality(conversion_rate, total_users)
            })
        
        source_df = pd.DataFrame(source_conversions)
        source_df = source_df.sort_values('conversion_rate', ascending=False)
        
        print(f"  📊 분석된 소스: {len(source_df)}개")
        print(f"  🏆 최고 전환율: {source_df.iloc[0]['source_medium']} ({source_df.iloc[0]['conversion_rate']:.1f}%)")
        
        return source_df
    
    def calculate_content_conversions(self, classified_df: pd.DataFrame) -> pd.DataFrame:
        """
        콘텐츠별 전환율 계산
        
        Returns:
            콘텐츠별 전환율 데이터프레임
        """
        print("📝 콘텐츠별 전환율 계산...")
        
        content_conversions = []
        
        # 콘텐츠 페이지 필터링
        content_events = classified_df[
            (classified_df['pagePath'].str.contains('/posts/', na=False)) |
            (classified_df['pagePath'].str.contains('/skill-guide/', na=False))
        ].copy()
        
        if content_events.empty:
            print("  ⚠️ 콘텐츠 페이지 이벤트가 없습니다")
            return pd.DataFrame()
        
        # 사용자별 콘텐츠 참여와 전환 여부 매핑
        user_content = content_events.groupby(['userPseudoId', 'pagePath']).size().reset_index(name='interactions')
        
        # 전환한 사용자 목록
        converted_users = set(
            classified_df[classified_df['funnel_stage'] == 'CONVERSION']['userPseudoId'].unique()
        )
        
        # 콘텐츠별 집계
        for page_path, page_group in user_content.groupby('pagePath'):
            total_users = page_group['userPseudoId'].nunique()
            conversions = sum(1 for user in page_group['userPseudoId'].unique() if user in converted_users)
            conversion_rate = conversions / total_users * 100 if total_users > 0 else 0
            
            # 콘텐츠 타입 분류
            content_type = self._classify_content_type(page_path)
            
            content_conversions.append({
                'page_path': page_path,
                'content_type': content_type,
                'total_users': total_users,
                'conversions': conversions,
                'conversion_rate': conversion_rate,
                'avg_interactions': page_group['interactions'].mean(),
                'content_effectiveness': self._assess_content_effectiveness(conversion_rate, total_users)
            })
        
        content_df = pd.DataFrame(content_conversions)
        content_df = content_df.sort_values('conversion_rate', ascending=False)
        
        print(f"  📊 분석된 콘텐츠: {len(content_df)}개")
        if not content_df.empty:
            print(f"  🏆 최고 전환율: {content_df.iloc[0]['page_path']} ({content_df.iloc[0]['conversion_rate']:.1f}%)")
        
        return content_df
    
    def calculate_device_conversions(self, classified_df: pd.DataFrame) -> pd.DataFrame:
        """
        디바이스별 전환율 계산
        
        Returns:
            디바이스별 전환율 데이터프레임
        """
        print("📱 디바이스별 전환율 계산...")
        
        if 'deviceCategory' not in classified_df.columns:
            print("  ⚠️ 디바이스 카테고리 정보가 없습니다")
            return pd.DataFrame()
        
        device_conversions = []
        
        # 사용자별 주요 디바이스와 전환 여부 매핑
        user_devices = classified_df.groupby('userPseudoId').agg({
            'deviceCategory': lambda x: x.mode().iloc[0] if not x.mode().empty else 'unknown',
            'funnel_stage': lambda x: 'CONVERSION' in x.values
        }).rename(columns={'funnel_stage': 'converted'})
        
        # 디바이스별 집계
        for device, group in user_devices.groupby('deviceCategory'):
            total_users = len(group)
            conversions = group['converted'].sum()
            conversion_rate = conversions / total_users * 100
            
            device_conversions.append({
                'device_category': device,
                'total_users': total_users,
                'conversions': conversions,
                'conversion_rate': conversion_rate,
                'user_share': total_users / len(user_devices) * 100
            })
        
        device_df = pd.DataFrame(device_conversions)
        device_df = device_df.sort_values('conversion_rate', ascending=False)
        
        print(f"  📊 분석된 디바이스: {len(device_df)}개")
        if not device_df.empty:
            print(f"  🏆 최고 전환율: {device_df.iloc[0]['device_category']} ({device_df.iloc[0]['conversion_rate']:.1f}%)")
        
        return device_df
    
    def calculate_time_based_conversions(self, classified_df: pd.DataFrame) -> Dict:
        """
        시간대별 전환율 계산
        
        Returns:
            시간대별 전환율 딕셔너리
        """
        print("⏰ 시간대별 전환율 계산...")
        
        if 'eventTimestamp' not in classified_df.columns:
            print("  ⚠️ 타임스탬프 정보가 없습니다")
            return {}
        
        # 타임스탬프를 datetime으로 변환
        classified_df = classified_df.copy()
        classified_df['hour'] = pd.to_datetime(classified_df['eventTimestamp']).dt.hour
        classified_df['day_of_week'] = pd.to_datetime(classified_df['eventTimestamp']).dt.day_name()
        
        # 사용자별 주요 활동 시간과 전환 여부
        user_times = classified_df.groupby('userPseudoId').agg({
            'hour': lambda x: x.mode().iloc[0] if not x.mode().empty else 12,
            'day_of_week': lambda x: x.mode().iloc[0] if not x.mode().empty else 'Monday',
            'funnel_stage': lambda x: 'CONVERSION' in x.values
        }).rename(columns={'funnel_stage': 'converted'})
        
        # 시간대별 분석
        hourly_conversions = []
        for hour in range(24):
            hour_users = user_times[user_times['hour'] == hour]
            if len(hour_users) > 0:
                conversion_rate = hour_users['converted'].sum() / len(hour_users) * 100
                hourly_conversions.append({
                    'hour': hour,
                    'total_users': len(hour_users),
                    'conversions': hour_users['converted'].sum(),
                    'conversion_rate': conversion_rate
                })
        
        # 요일별 분석
        daily_conversions = []
        for day in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']:
            day_users = user_times[user_times['day_of_week'] == day]
            if len(day_users) > 0:
                conversion_rate = day_users['converted'].sum() / len(day_users) * 100
                daily_conversions.append({
                    'day_of_week': day,
                    'total_users': len(day_users),
                    'conversions': day_users['converted'].sum(),
                    'conversion_rate': conversion_rate
                })
        
        time_analysis = {
            'hourly_conversions': hourly_conversions,
            'daily_conversions': daily_conversions
        }
        
        # 최고 성과 시간대 출력
        if hourly_conversions:
            best_hour = max(hourly_conversions, key=lambda x: x['conversion_rate'])
            print(f"  🏆 최고 전환 시간대: {best_hour['hour']}시 ({best_hour['conversion_rate']:.1f}%)")
        
        if daily_conversions:
            best_day = max(daily_conversions, key=lambda x: x['conversion_rate'])
            print(f"  🏆 최고 전환 요일: {best_day['day_of_week']} ({best_day['conversion_rate']:.1f}%)")
        
        return time_analysis
    
    def _get_user_progression(self, classified_df: pd.DataFrame) -> pd.DataFrame:
        """사용자별 퍼널 진행 상황 파악"""
        user_progressions = []
        
        for user_id, user_events in classified_df.groupby('userPseudoId'):
            stages = user_events['funnel_stage'].unique()
            
            progression = {
                'user_id': user_id,
                'reached_awareness': 'AWARENESS' in stages,
                'reached_interest': 'INTEREST' in stages,
                'reached_consideration': 'CONSIDERATION' in stages,
                'reached_conversion': 'CONVERSION' in stages,
                'total_events': len(user_events),
                'unique_pages': user_events['pagePath'].nunique(),
                'first_source': user_events.iloc[0]['sessionSource'],
                'first_medium': user_events.iloc[0]['sessionMedium']
            }
            
            user_progressions.append(progression)
        
        return pd.DataFrame(user_progressions)
    
    def _assess_traffic_quality(self, conversion_rate: float, volume: int) -> str:
        """트래픽 품질 평가"""
        if conversion_rate >= 20 and volume >= 50:
            return 'Premium'
        elif conversion_rate >= 10 and volume >= 100:
            return 'High'
        elif conversion_rate >= 5 and volume >= 200:
            return 'Medium'
        elif conversion_rate >= 2:
            return 'Low'
        else:
            return 'Poor'
    
    def _assess_content_effectiveness(self, conversion_rate: float, volume: int) -> str:
        """콘텐츠 효과성 평가"""
        if conversion_rate >= 30 and volume >= 20:
            return 'Excellent'
        elif conversion_rate >= 15 and volume >= 50:
            return 'Good'
        elif conversion_rate >= 8 and volume >= 100:
            return 'Average'
        elif conversion_rate >= 3:
            return 'Below Average'
        else:
            return 'Poor'
    
    def _classify_content_type(self, page_path: str) -> str:
        """콘텐츠 타입 분류"""
        if '/posts/' in page_path:
            return 'Blog Post'
        elif '/skill-guide/' in page_path:
            if 'one-click' in page_path:
                return 'Product Guide'
            elif 'market-management' in page_path:
                return 'Management Guide'
            else:
                return 'General Guide'
        else:
            return 'Other Content'
    
    def generate_conversion_summary(self, classified_df: pd.DataFrame, 
                                  conversion_events: pd.DataFrame) -> Dict:
        """
        종합적인 전환율 요약 생성
        
        Returns:
            전환율 요약 딕셔너리
        """
        print("📋 전환율 종합 요약 생성...")
        
        summary = {
            'overview': {},
            'by_source': {},
            'by_content': {},
            'by_device': {},
            'by_time': {},
            'optimization_opportunities': []
        }
        
        # 1. 전체 개요
        stage_conversions = self.calculate_stage_conversions(classified_df, conversion_events)
        summary['overview'] = stage_conversions
        
        # 2. 소스별 분석
        source_conversions = self.calculate_source_conversions(classified_df)
        if not source_conversions.empty:
            summary['by_source'] = {
                'top_performers': source_conversions.head(5).to_dict('records'),
                'total_sources': len(source_conversions),
                'avg_conversion_rate': source_conversions['conversion_rate'].mean()
            }
        
        # 3. 콘텐츠별 분석
        content_conversions = self.calculate_content_conversions(classified_df)
        if not content_conversions.empty:
            summary['by_content'] = {
                'top_performers': content_conversions.head(5).to_dict('records'),
                'total_content': len(content_conversions),
                'avg_conversion_rate': content_conversions['conversion_rate'].mean()
            }
        
        # 4. 디바이스별 분석
        device_conversions = self.calculate_device_conversions(classified_df)
        if not device_conversions.empty:
            summary['by_device'] = device_conversions.to_dict('records')
        
        # 5. 시간대별 분석
        time_conversions = self.calculate_time_based_conversions(classified_df)
        summary['by_time'] = time_conversions
        
        # 6. 최적화 기회 식별
        summary['optimization_opportunities'] = self._identify_optimization_opportunities(
            stage_conversions, source_conversions, content_conversions
        )
        
        print("  ✅ 종합 요약 완료")
        return summary
    
    def _identify_optimization_opportunities(self, stage_conversions: Dict, 
                                          source_conversions: pd.DataFrame,
                                          content_conversions: pd.DataFrame) -> List[Dict]:
        """최적화 기회 식별"""
        opportunities = []
        
        # 퍼널 병목 구간 식별
        step_rates = stage_conversions.get('step_conversions', {})
        min_rate = min(step_rates.values()) if step_rates else 0
        bottleneck = min(step_rates.items(), key=lambda x: x[1]) if step_rates else None
        
        if bottleneck and bottleneck[1] < 50:
            opportunities.append({
                'type': 'Funnel Bottleneck',
                'description': f'주요 병목: {bottleneck[0]} ({bottleneck[1]:.1f}%)',
                'priority': 'High',
                'recommended_action': f'{bottleneck[0]} 단계 최적화 필요'
            })
        
        # 고성과 소스 확장 기회
        if not source_conversions.empty:
            high_performers = source_conversions[
                (source_conversions['conversion_rate'] > 15) & 
                (source_conversions['total_users'] < 100)
            ]
            
            for _, source in high_performers.iterrows():
                opportunities.append({
                    'type': 'Scale High Performer',
                    'description': f"{source['source_medium']} 확장 기회 ({source['conversion_rate']:.1f}% 전환율)",
                    'priority': 'Medium',
                    'recommended_action': f"{source['source_medium']} 예산/노력 증대"
                })
        
        # 저성과 콘텐츠 개선 기회
        if not content_conversions.empty:
            low_performers = content_conversions[
                (content_conversions['conversion_rate'] < 5) & 
                (content_conversions['total_users'] > 50)
            ]
            
            for _, content in low_performers.iterrows():
                opportunities.append({
                    'type': 'Content Optimization',
                    'description': f"{content['page_path']} 개선 필요 ({content['conversion_rate']:.1f}% 전환율)",
                    'priority': 'Medium',
                    'recommended_action': f"콘텐츠 품질 개선 또는 CTA 강화"
                })
        
        return opportunities[:10]  # 상위 10개만 반환

# 사용 예시
if __name__ == "__main__":
    # 설정 및 계산기 초기화
    config = Config()
    calculator = ConversionCalculator(config)
    
    # 샘플 데이터로 테스트
    sample_data = pd.DataFrame({
        'userPseudoId': ['user1', 'user1', 'user2', 'user2'],
        'sessionSource': ['google', 'google', 'naver', 'naver'],
        'sessionMedium': ['cpc', 'cpc', 'organic', 'organic'],
        'pagePath': ['/', '/posts/739', '/skill-guide/one-click', '/provider-join'],
        'funnel_stage': ['AWARENESS', 'INTEREST', 'AWARENESS', 'CONVERSION'],
        'eventTimestamp': ['2024-01-01 10:00:00', '2024-01-01 10:05:00', 
                          '2024-01-01 14:00:00', '2024-01-01 14:30:00']
    })
    
    # 전환율 계산
    conversions = calculator.calculate_stage_conversions(sample_data, pd.DataFrame())
    source_conversions = calculator.calculate_source_conversions(sample_data)
    
    print("전환율 계산 완료!")