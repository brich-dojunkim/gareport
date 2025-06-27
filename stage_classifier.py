"""
퍼널 단계 분류 모듈
사용자 이벤트를 AWARENESS → INTEREST → CONSIDERATION → CONVERSION 단계로 분류
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
from config import Config

class FunnelStageClassifier:
    """퍼널 단계 분류 클래스"""
    
    def __init__(self, config: Config = None):
        """
        Args:
            config: 설정 객체 (None이면 기본 설정 사용)
        """
        self.config = config or Config()
        self.stage_rules = self._initialize_stage_rules()
    
    def _initialize_stage_rules(self) -> Dict:
        """퍼널 단계 분류 규칙 초기화"""
        return {
            'AWARENESS': {
                'events': ['session_start', 'first_visit'],
                'pages': ['/', '/skill-guide/one-click'],
                'conditions': self._awareness_conditions,
                'priority': 1
            },
            'INTEREST': {
                'events': ['user_engagement', 'visit_blog', 'scroll'],
                'pages': ['/posts/*', '/skill-guide/*'],
                'conditions': self._interest_conditions,
                'priority': 2
            },
            'CONSIDERATION': {
                'events': ['page_view'],
                'pages': ['/fee-information', '/providers', '/service-plan/*'],
                'conditions': self._consideration_conditions,
                'priority': 3
            },
            'CONVERSION': {
                'events': ['회원가입2'],
                'pages': ['/provider-join'],
                'conditions': self._conversion_conditions,
                'priority': 4
            }
        }
    
    def classify_user_events(self, events_df: pd.DataFrame) -> pd.DataFrame:
        """
        사용자 이벤트를 퍼널 단계로 분류
        
        Args:
            events_df: 이벤트 데이터프레임
            
        Returns:
            퍼널 단계가 추가된 데이터프레임
        """
        print("🔄 퍼널 단계 분류 시작...")
        
        if events_df.empty:
            print("  ⚠️ 빈 데이터프레임")
            return events_df
        
        # 기본 컬럼 추가
        events_df = events_df.copy()
        events_df['funnel_stage'] = 'UNKNOWN'
        events_df['stage_confidence'] = 0.0
        events_df['page_category'] = events_df['pagePath'].apply(
            lambda x: self.config.get_page_category(x)
        )
        
        # 사용자별로 그룹핑하여 처리
        total_users = events_df['userPseudoId'].nunique()
        processed_users = 0
        
        user_groups = events_df.groupby('userPseudoId')
        classified_dfs = []
        
        for user_id, user_events in user_groups:
            # 사용자별 이벤트 분류
            user_classified = self._classify_user_journey(user_events)
            classified_dfs.append(user_classified)
            
            processed_users += 1
            if processed_users % 1000 == 0:
                print(f"  📊 진행률: {processed_users:,}/{total_users:,} 사용자 처리됨")
        
        result_df = pd.concat(classified_dfs, ignore_index=True)
        
        # 분류 결과 요약
        stage_counts = result_df['funnel_stage'].value_counts()
        print(f"  ✅ 분류 완료: {len(result_df):,}개 이벤트")
        for stage, count in stage_counts.items():
            print(f"    - {stage}: {count:,}개 ({count/len(result_df)*100:.1f}%)")
        
        return result_df
    
    def _classify_user_journey(self, user_events: pd.DataFrame) -> pd.DataFrame:
        """개별 사용자의 여정을 퍼널 단계로 분류"""
        user_events = user_events.copy().sort_values('eventTimestamp')
        
        # 각 이벤트에 대해 단계 분류
        for idx, row in user_events.iterrows():
            stage, confidence = self._classify_single_event(row, user_events)
            user_events.at[idx, 'funnel_stage'] = stage
            user_events.at[idx, 'stage_confidence'] = confidence
        
        return user_events
    
    def _classify_single_event(self, event_row: pd.Series, user_context: pd.DataFrame) -> Tuple[str, float]:
        """단일 이벤트의 퍼널 단계 분류"""
        max_confidence = 0.0
        best_stage = 'UNKNOWN'
        
        # 각 단계별로 확신도 계산
        for stage_name, stage_config in self.stage_rules.items():
            confidence = stage_config['conditions'](event_row, user_context, stage_config)
            
            if confidence > max_confidence:
                max_confidence = confidence
                best_stage = stage_name
        
        return best_stage, max_confidence
    
    def _awareness_conditions(self, event: pd.Series, context: pd.DataFrame, config: Dict) -> float:
        """AWARENESS 단계 판단 조건"""
        confidence = 0.0
        
        # 세션 시작 이벤트
        if event['eventName'] == 'session_start':
            confidence += 0.8
        
        # 첫 방문 이벤트
        if event['eventName'] == 'first_visit':
            confidence += 0.9
        
        # 외부 유입 (direct가 아닌 소스)
        if event['sessionSource'] != '(direct)':
            confidence += 0.3
        
        # 홈페이지 또는 랜딩 페이지
        if event['pagePath'] in ['/', '/skill-guide/one-click']:
            confidence += 0.4
        
        # 세션 내 첫 번째 이벤트
        if len(context) > 0:
            first_event = context.iloc[0]
            if event.name == first_event.name:
                confidence += 0.3
        
        return min(confidence, 1.0)
    
    def _interest_conditions(self, event: pd.Series, context: pd.DataFrame, config: Dict) -> float:
        """INTEREST 단계 판단 조건"""
        confidence = 0.0
        
        # 참여 이벤트
        if event['eventName'] in ['user_engagement', 'visit_blog']:
            confidence += 0.7
        
        # 콘텐츠 페이지 방문
        if '/posts/' in event['pagePath'] or '/skill-guide/' in event['pagePath']:
            confidence += 0.5
        
        # 참여 시간 (있는 경우)
        if 'engagementTimeMsec' in event and pd.notna(event['engagementTimeMsec']):
            engagement_sec = event['engagementTimeMsec'] / 1000
            if engagement_sec > self.config.min_engagement_time:
                confidence += 0.4
        
        # 블로그 방문 이벤트
        if event['eventName'] == 'visit_blog':
            confidence += 0.6
        
        # 이미 awareness 단계를 거친 경우
        prior_awareness = any(
            row['eventName'] in ['session_start', 'first_visit'] 
            for _, row in context.iterrows() 
            if row.name < event.name
        )
        if prior_awareness:
            confidence += 0.2
        
        return min(confidence, 1.0)
    
    def _consideration_conditions(self, event: pd.Series, context: pd.DataFrame, config: Dict) -> float:
        """CONSIDERATION 단계 판단 조건"""
        confidence = 0.0
        
        # 서비스 정보 페이지
        service_pages = ['/fee-information', '/providers', '/service-plan']
        if any(page in event['pagePath'] for page in service_pages):
            confidence += 0.8
        
        # 깊은 탐색 (여러 페이지 방문)
        unique_pages = context['pagePath'].nunique()
        if unique_pages >= 3:
            confidence += 0.3
        if unique_pages >= 5:
            confidence += 0.2
        
        # 세션 깊이 (3번째 이벤트 이후)
        event_position = len(context[context.index <= event.name])
        if event_position >= 3:
            confidence += 0.2
        
        # 이전에 interest 단계를 거친 경우
        prior_interest = any(
            row['eventName'] in ['user_engagement', 'visit_blog']
            for _, row in context.iterrows()
            if row.name < event.name
        )
        if prior_interest:
            confidence += 0.3
        
        # 재방문 세션 (추정)
        if event['sessionSource'] == '(direct)':
            confidence += 0.2
        
        return min(confidence, 1.0)
    
    def _conversion_conditions(self, event: pd.Series, context: pd.DataFrame, config: Dict) -> float:
        """CONVERSION 단계 판단 조건"""
        confidence = 0.0
        
        # 전환 이벤트
        if event['eventName'] == '회원가입2':
            confidence = 1.0
        
        # 가입 페이지에서의 특정 행동
        elif event['pagePath'] == '/provider-join':
            if event['eventName'] == 'page_view':
                confidence += 0.3
            elif event['eventName'] == 'user_engagement':
                confidence += 0.5
        
        return min(confidence, 1.0)
    
    def get_user_funnel_progression(self, events_df: pd.DataFrame) -> pd.DataFrame:
        """
        사용자별 퍼널 진행 상황 분석
        
        Returns:
            사용자별 퍼널 진행 요약 데이터프레임
        """
        print("📊 사용자별 퍼널 진행 분석...")
        
        user_progressions = []
        
        for user_id, user_events in events_df.groupby('userPseudoId'):
            stages_reached = user_events['funnel_stage'].unique()
            stages_reached = [s for s in stages_reached if s != 'UNKNOWN']
            
            progression = {
                'user_id': user_id,
                'total_events': len(user_events),
                'unique_pages': user_events['pagePath'].nunique(),
                'sessions': user_events['sessionId'].nunique(),
                'stages_reached': len(stages_reached),
                'reached_awareness': 'AWARENESS' in stages_reached,
                'reached_interest': 'INTEREST' in stages_reached,
                'reached_consideration': 'CONSIDERATION' in stages_reached,
                'reached_conversion': 'CONVERSION' in stages_reached,
                'highest_stage': self._get_highest_stage(stages_reached),
                'first_source': user_events.iloc[0]['sessionSource'],
                'first_medium': user_events.iloc[0]['sessionMedium']
            }
            
            user_progressions.append(progression)
        
        progression_df = pd.DataFrame(user_progressions)
        
        # 진행 통계 출력
        total_users = len(progression_df)
        conversion_rate = progression_df['reached_conversion'].sum() / total_users * 100
        
        print(f"  📈 전체 사용자: {total_users:,}명")
        print(f"  🎯 전환율: {conversion_rate:.1f}%")
        print(f"  📊 단계별 도달률:")
        for stage in ['AWARENESS', 'INTEREST', 'CONSIDERATION', 'CONVERSION']:
            reached = progression_df[f'reached_{stage.lower()}'].sum()
            rate = reached / total_users * 100
            print(f"    - {stage}: {reached:,}명 ({rate:.1f}%)")
        
        return progression_df
    
    def _get_highest_stage(self, stages: List[str]) -> str:
        """사용자가 도달한 최고 단계 반환"""
        stage_order = ['AWARENESS', 'INTEREST', 'CONSIDERATION', 'CONVERSION']
        
        for stage in reversed(stage_order):
            if stage in stages:
                return stage
        
        return 'NONE'
    
    def analyze_stage_transitions(self, events_df: pd.DataFrame) -> Dict:
        """퍼널 단계 간 전환 분석"""
        print("🔄 퍼널 단계 전환 분석...")
        
        transitions = {
            'AWARENESS_to_INTEREST': 0,
            'INTEREST_to_CONSIDERATION': 0,
            'CONSIDERATION_to_CONVERSION': 0,
            'total_users': events_df['userPseudoId'].nunique()
        }
        
        for user_id, user_events in events_df.groupby('userPseudoId'):
            stages = user_events['funnel_stage'].tolist()
            
            # 단계 간 전환 체크
            if 'AWARENESS' in stages and 'INTEREST' in stages:
                transitions['AWARENESS_to_INTEREST'] += 1
            if 'INTEREST' in stages and 'CONSIDERATION' in stages:
                transitions['INTEREST_to_CONSIDERATION'] += 1
            if 'CONSIDERATION' in stages and 'CONVERSION' in stages:
                transitions['CONSIDERATION_to_CONVERSION'] += 1
        
        # 전환율 계산
        total = transitions['total_users']
        transitions['conversion_rates'] = {
            'awareness_to_interest': transitions['AWARENESS_to_INTEREST'] / total * 100,
            'interest_to_consideration': transitions['INTEREST_to_CONSIDERATION'] / total * 100,
            'consideration_to_conversion': transitions['CONSIDERATION_to_CONVERSION'] / total * 100
        }
        
        print("  📊 단계별 전환율:")
        for key, rate in transitions['conversion_rates'].items():
            print(f"    - {key}: {rate:.1f}%")
        
        return transitions

# 사용 예시
if __name__ == "__main__":
    # 설정 및 분류기 초기화
    config = Config()
    classifier = FunnelStageClassifier(config)
    
    # 샘플 데이터로 테스트
    sample_data = pd.DataFrame({
        'userPseudoId': ['user1', 'user1', 'user2'],
        'eventName': ['session_start', 'user_engagement', '회원가입2'],
        'pagePath': ['/', '/posts/739', '/provider-join'],
        'sessionSource': ['google', 'google', 'direct'],
        'sessionMedium': ['cpc', 'cpc', 'none'],
        'eventTimestamp': ['2024-01-01 10:00:00', '2024-01-01 10:05:00', '2024-01-01 11:00:00']
    })
    
    # 분류 실행
    classified = classifier.classify_user_events(sample_data)
    print(classified[['eventName', 'pagePath', 'funnel_stage', 'stage_confidence']])