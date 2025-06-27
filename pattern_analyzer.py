"""
패턴 분석 모듈
변수 조합별 전환 패턴 및 최적화 인사이트 분석
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from itertools import combinations
from config import Config

class PatternAnalyzer:
    """패턴 분석 클래스"""
    
    def __init__(self, config: Config = None):
        """
        Args:
            config: 설정 객체
        """
        self.config = config or Config()
    
    def analyze_conversion_patterns(self, classified_df: pd.DataFrame, 
                                  page_sequences: pd.DataFrame,
                                  conversion_events: pd.DataFrame) -> Dict:
        """
        종합적인 전환 패턴 분석
        
        Args:
            classified_df: 퍼널 단계가 분류된 데이터
            page_sequences: 페이지 시퀀스 데이터
            conversion_events: 전환 이벤트 데이터
            
        Returns:
            패턴 분석 결과 딕셔너리
        """
        print("🔍 전환 패턴 종합 분석 시작...")
        
        patterns = {
            'source_page_combinations': self._analyze_source_page_patterns(classified_df),
            'journey_patterns': self._analyze_user_journey_patterns(page_sequences, conversion_events),
            'engagement_patterns': self._analyze_engagement_patterns(classified_df),
            'temporal_patterns': self._analyze_temporal_patterns(classified_df),
            'high_value_segments': self._identify_high_value_segments(classified_df),
            'conversion_paths': self._analyze_conversion_paths(page_sequences, conversion_events),
            'drop_off_points': self._identify_drop_off_points(classified_df),
            'optimization_insights': {}
        }
        
        # 최적화 인사이트 생성
        patterns['optimization_insights'] = self._generate_optimization_insights(patterns)
        
        print("✅ 패턴 분석 완료")
        return patterns
    
    def _analyze_source_page_patterns(self, classified_df: pd.DataFrame) -> List[Dict]:
        """소스-페이지 조합별 전환 패턴 분석"""
        print("  🚀 소스-페이지 조합 패턴 분석...")
        
        patterns = []
        
        # 사용자별 첫 소스와 주요 페이지 카테고리 매핑
        user_patterns = []
        for user_id, user_events in classified_df.groupby('userPseudoId'):
            first_event = user_events.iloc[0]
            converted = 'CONVERSION' in user_events['funnel_stage'].values
            
            # 방문한 페이지 카테고리들
            page_categories = set()
            for page in user_events['pagePath'].unique():
                category = self.config.get_page_category(page)
                page_categories.add(category)
            
            user_patterns.append({
                'user_id': user_id,
                'source': first_event['sessionSource'],
                'medium': first_event['sessionMedium'],
                'traffic_group': self.config.get_traffic_group(first_event['sessionSource'], first_event['sessionMedium']),
                'page_categories': page_categories,
                'converted': converted,
                'pages_visited': len(user_events['pagePath'].unique()),
                'events_count': len(user_events)
            })
        
        user_patterns_df = pd.DataFrame(user_patterns)
        
        # 소스-페이지카테고리 조합 분석
        for traffic_group in user_patterns_df['traffic_group'].unique():
            group_users = user_patterns_df[user_patterns_df['traffic_group'] == traffic_group]
            
            # 주요 페이지 카테고리 조합 찾기
            category_combinations = []
            for _, user in group_users.iterrows():
                sorted_categories = sorted(list(user['page_categories']))
                category_combinations.append(tuple(sorted_categories))
            
            # 조합별 전환율 계산
            combo_stats = {}
            for combo in set(category_combinations):
                combo_users = [user for user, user_combo in zip(group_users.itertuples(), category_combinations) if user_combo == combo]
                if len(combo_users) >= 5:  # 최소 5명 이상
                    conversions = sum(1 for user in combo_users if user.converted)
                    conversion_rate = conversions / len(combo_users) * 100
                    
                    combo_stats[combo] = {
                        'combination': ' + '.join(combo),
                        'users': len(combo_users),
                        'conversions': conversions,
                        'conversion_rate': conversion_rate
                    }
            
            if combo_stats:
                best_combo = max(combo_stats.values(), key=lambda x: x['conversion_rate'])
                patterns.append({
                    'traffic_group': traffic_group,
                    'best_combination': best_combo,
                    'total_combinations': len(combo_stats),
                    'group_users': len(group_users)
                })
        
        patterns.sort(key=lambda x: x['best_combination']['conversion_rate'], reverse=True)
        return patterns[:10]  # 상위 10개
    
    def _analyze_user_journey_patterns(self, page_sequences: pd.DataFrame, 
                                     conversion_events: pd.DataFrame) -> Dict:
        """사용자 여정 패턴 분석"""
        print("  🛤️ 사용자 여정 패턴 분석...")
        
        if page_sequences.empty:
            return {'error': '페이지 시퀀스 데이터가 없습니다'}
        
        # 전환한 사용자들의 여정 분석
        converted_users = set(conversion_events['userPseudoId'].unique()) if not conversion_events.empty else set()
        
        journey_patterns = {
            'common_paths': [],
            'conversion_paths': [],
            'path_lengths': {'converted': [], 'non_converted': []},
            'page_transitions': {}
        }
        
        # 사용자별 여정 추출
        user_journeys = []
        for user_id, user_pages in page_sequences.groupby('userPseudoId'):
            # 타임스탬프 순으로 정렬
            if 'eventTimestamp' in user_pages.columns:
                user_pages = user_pages.sort_values('eventTimestamp')
            
            journey = list(user_pages['pagePath'].values)
            converted = user_id in converted_users
            
            user_journeys.append({
                'user_id': user_id,
                'journey': journey,
                'length': len(journey),
                'converted': converted,
                'unique_pages': len(set(journey))
            })
        
        user_journeys_df = pd.DataFrame(user_journeys)
        
        # 여정 길이 분석
        converted_journeys = user_journeys_df[user_journeys_df['converted']]
        non_converted_journeys = user_journeys_df[~user_journeys_df['converted']]
        
        journey_patterns['path_lengths'] = {
            'converted': {
                'avg_length': converted_journeys['length'].mean() if not converted_journeys.empty else 0,
                'median_length': converted_journeys['length'].median() if not converted_journeys.empty else 0,
                'avg_unique_pages': converted_journeys['unique_pages'].mean() if not converted_journeys.empty else 0
            },
            'non_converted': {
                'avg_length': non_converted_journeys['length'].mean() if not non_converted_journeys.empty else 0,
                'median_length': non_converted_journeys['length'].median() if not non_converted_journeys.empty else 0,
                'avg_unique_pages': non_converted_journeys['unique_pages'].mean() if not non_converted_journeys.empty else 0
            }
        }
        
        # 공통 여정 패턴 찾기 (처음 3페이지)
        first_3_paths = []
        for journey_info in user_journeys:
            if len(journey_info['journey']) >= 3:
                first_3 = tuple(journey_info['journey'][:3])
                first_3_paths.append((first_3, journey_info['converted']))
        
        # 패턴별 전환율 계산
        path_stats = {}
        for path, converted in first_3_paths:
            if path not in path_stats:
                path_stats[path] = {'total': 0, 'converted': 0}
            path_stats[path]['total'] += 1
            if converted:
                path_stats[path]['converted'] += 1
        
        # 최소 10명 이상의 사용자가 있는 패턴만 분석
        common_paths = []
        for path, stats in path_stats.items():
            if stats['total'] >= 10:
                conversion_rate = stats['converted'] / stats['total'] * 100
                common_paths.append({
                    'path': ' → '.join(path),
                    'users': stats['total'],
                    'conversions': stats['converted'],
                    'conversion_rate': conversion_rate
                })
        
        journey_patterns['common_paths'] = sorted(common_paths, key=lambda x: x['conversion_rate'], reverse=True)[:10]
        
        return journey_patterns
    
    def _analyze_engagement_patterns(self, classified_df: pd.DataFrame) -> Dict:
        """참여도 패턴 분석"""
        print("  📊 참여도 패턴 분석...")
        
        engagement_patterns = {
            'event_sequences': [],
            'page_category_engagement': {},
            'engagement_conversion_correlation': {}
        }
        
        # 사용자별 이벤트 시퀀스 분석
        user_sequences = []
        for user_id, user_events in classified_df.groupby('userPseudoId'):
            if 'eventTimestamp' in user_events.columns:
                user_events = user_events.sort_values('eventTimestamp')
            
            event_sequence = list(user_events['eventName'].values)
            converted = 'CONVERSION' in user_events['funnel_stage'].values
            
            # 참여도 점수 계산
            engagement_score = self._calculate_engagement_score(user_events)
            
            user_sequences.append({
                'user_id': user_id,
                'event_sequence': event_sequence,
                'converted': converted,
                'engagement_score': engagement_score,
                'total_events': len(user_events)
            })
        
        sequences_df = pd.DataFrame(user_sequences)
        
        # 참여도와 전환 상관관계
        if not sequences_df.empty:
            converted_engagement = sequences_df[sequences_df['converted']]['engagement_score']
            non_converted_engagement = sequences_df[~sequences_df['converted']]['engagement_score']
            
            engagement_patterns['engagement_conversion_correlation'] = {
                'converted_avg_score': converted_engagement.mean() if not converted_engagement.empty else 0,
                'non_converted_avg_score': non_converted_engagement.mean() if not non_converted_engagement.empty else 0,
                'score_difference': (converted_engagement.mean() - non_converted_engagement.mean()) if not converted_engagement.empty and not non_converted_engagement.empty else 0
            }
        
        # 페이지 카테고리별 참여도
        page_engagement = {}
        for category in self.config.page_categories.keys():
            category_events = classified_df[classified_df['page_category'] == category]
            if not category_events.empty:
                users_in_category = category_events['userPseudoId'].unique()
                converted_users = category_events[category_events['funnel_stage'] == 'CONVERSION']['userPseudoId'].unique()
                
                page_engagement[category] = {
                    'total_users': len(users_in_category),
                    'converted_users': len(converted_users),
                    'conversion_rate': len(converted_users) / len(users_in_category) * 100 if len(users_in_category) > 0 else 0,
                    'avg_events_per_user': len(category_events) / len(users_in_category) if len(users_in_category) > 0 else 0
                }
        
        engagement_patterns['page_category_engagement'] = page_engagement
        
        return engagement_patterns
    
    def _analyze_temporal_patterns(self, classified_df: pd.DataFrame) -> Dict:
        """시간적 패턴 분석"""
        print("  ⏰ 시간적 패턴 분석...")
        
        if 'eventTimestamp' not in classified_df.columns:
            return {'error': '타임스탬프 데이터가 없습니다'}
        
        temporal_patterns = {
            'conversion_timing': {},
            'session_duration_patterns': {},
            'return_visit_patterns': {}
        }
        
        # 타임스탬프 변환
        classified_df = classified_df.copy()
        classified_df['datetime'] = pd.to_datetime(classified_df['eventTimestamp'])
        classified_df['hour'] = classified_df['datetime'].dt.hour
        classified_df['day_of_week'] = classified_df['datetime'].dt.day_name()
        
        # 전환까지 소요시간 분석
        conversion_timing = []
        for user_id, user_events in classified_df.groupby('userPseudoId'):
            user_events = user_events.sort_values('datetime')
            first_event = user_events.iloc[0]
            conversion_events = user_events[user_events['funnel_stage'] == 'CONVERSION']
            
            if not conversion_events.empty:
                first_conversion = conversion_events.iloc[0]
                time_to_conversion = (first_conversion['datetime'] - first_event['datetime']).total_seconds() / 3600  # 시간 단위
                
                conversion_timing.append({
                    'user_id': user_id,
                    'time_to_conversion_hours': time_to_conversion,
                    'first_source': first_event['sessionSource'],
                    'conversion_hour': first_conversion['hour'],
                    'conversion_day': first_conversion['day_of_week']
                })
        
        if conversion_timing:
            timing_df = pd.DataFrame(conversion_timing)
            temporal_patterns['conversion_timing'] = {
                'avg_time_to_conversion': timing_df['time_to_conversion_hours'].mean(),
                'median_time_to_conversion': timing_df['time_to_conversion_hours'].median(),
                'same_session_conversions': len(timing_df[timing_df['time_to_conversion_hours'] < 1]),
                'within_24h_conversions': len(timing_df[timing_df['time_to_conversion_hours'] < 24])
            }
        
        return temporal_patterns
    
    def _identify_high_value_segments(self, classified_df: pd.DataFrame) -> List[Dict]:
        """고가치 사용자 세그먼트 식별"""
        print("  💎 고가치 세그먼트 식별...")
        
        segments = []
        
        # 사용자별 특성 계산
        user_features = []
        for user_id, user_events in classified_df.groupby('userPseudoId'):
            converted = 'CONVERSION' in user_events['funnel_stage'].values
            
            features = {
                'user_id': user_id,
                'converted': converted,
                'total_events': len(user_events),
                'unique_pages': user_events['pagePath'].nunique(),
                'sessions': user_events['sessionId'].nunique() if 'sessionId' in user_events.columns else 1,
                'source': user_events.iloc[0]['sessionSource'],
                'medium': user_events.iloc[0]['sessionMedium'],
                'traffic_group': self.config.get_traffic_group(user_events.iloc[0]['sessionSource'], user_events.iloc[0]['sessionMedium']),
                'stages_reached': len(set(user_events['funnel_stage'].values) - {'UNKNOWN'}),
                'has_content_engagement': any(stage in user_events['funnel_stage'].values for stage in ['INTEREST']),
                'has_service_exploration': any(stage in user_events['funnel_stage'].values for stage in ['CONSIDERATION'])
            }
            
            user_features.append(features)
        
        users_df = pd.DataFrame(user_features)
        
        # 세그먼트별 전환율 분석
        segment_definitions = [
            {
                'name': 'High Engagement + Content Lovers',
                'condition': lambda df: (df['total_events'] >= 10) & (df['has_content_engagement'] == True),
                'description': '높은 참여도 + 콘텐츠 소비'
            },
            {
                'name': 'Multi-Session Explorers',
                'condition': lambda df: df['sessions'] >= 2,
                'description': '다중 세션 탐색자'
            },
            {
                'name': 'Paid Traffic High Intent',
                'condition': lambda df: (df['traffic_group'] == 'paid_search') & (df['stages_reached'] >= 3),
                'description': '유료 트래픽 고의도 사용자'
            },
            {
                'name': 'Content to Service Path',
                'condition': lambda df: (df['has_content_engagement'] == True) & (df['has_service_exploration'] == True),
                'description': '콘텐츠→서비스 탐색 경로'
            }
        ]
        
        for segment_def in segment_definitions:
            segment_users = users_df[segment_def['condition'](users_df)]
            
            if len(segment_users) >= 20:  # 최소 20명
                conversion_rate = segment_users['converted'].sum() / len(segment_users) * 100
                
                segments.append({
                    'segment_name': segment_def['name'],
                    'description': segment_def['description'],
                    'users_count': len(segment_users),
                    'conversions': segment_users['converted'].sum(),
                    'conversion_rate': conversion_rate,
                    'avg_events': segment_users['total_events'].mean(),
                    'avg_pages': segment_users['unique_pages'].mean(),
                    'segment_value': conversion_rate * len(segment_users)  # 가치 점수
                })
        
        segments.sort(key=lambda x: x['segment_value'], reverse=True)
        return segments[:5]  # 상위 5개 세그먼트
    
    def _analyze_conversion_paths(self, page_sequences: pd.DataFrame, 
                                conversion_events: pd.DataFrame) -> Dict:
        """전환 경로 상세 분석"""
        print("  🎯 전환 경로 상세 분석...")
        
        if page_sequences.empty or conversion_events.empty:
            return {'error': '페이지 시퀀스 또는 전환 이벤트 데이터가 없습니다'}
        
        conversion_paths = {
            'successful_paths': [],
            'path_analysis': {},
            'critical_pages': []
        }
        
        converted_users = set(conversion_events['userPseudoId'].unique())
        
        # 전환한 사용자들의 경로 분석
        successful_journeys = []
        for user_id in converted_users:
            user_pages = page_sequences[page_sequences['userPseudoId'] == user_id]
            if not user_pages.empty:
                if 'eventTimestamp' in user_pages.columns:
                    user_pages = user_pages.sort_values('eventTimestamp')
                
                journey = list(user_pages['pagePath'].values)
                successful_journeys.append(journey)
        
        # 경로 패턴 분석
        if successful_journeys:
            # 가장 일반적인 시작 페이지
            start_pages = [journey[0] for journey in successful_journeys if journey]
            start_page_counts = pd.Series(start_pages).value_counts()
            
            # 가장 일반적인 마지막 페이지 (전환 전)
            second_to_last_pages = [journey[-2] for journey in successful_journeys if len(journey) > 1]
            last_page_counts = pd.Series(second_to_last_pages).value_counts()
            
            conversion_paths['path_analysis'] = {
                'avg_path_length': np.mean([len(journey) for journey in successful_journeys]),
                'most_common_start': start_page_counts.index[0] if not start_page_counts.empty else None,
                'most_common_pre_conversion': last_page_counts.index[0] if not last_page_counts.empty else None,
                'total_conversion_paths': len(successful_journeys)
            }
            
            # 중요한 페이지 식별 (전환 경로에 자주 등장하는 페이지)
            all_pages_in_paths = []
            for journey in successful_journeys:
                all_pages_in_paths.extend(journey)
            
            page_frequency = pd.Series(all_pages_in_paths).value_counts()
            critical_pages = []
            
            for page, count in page_frequency.head(10).items():
                appearance_rate = count / len(successful_journeys) * 100
                critical_pages.append({
                    'page': page,
                    'appearances': count,
                    'appearance_rate': appearance_rate,
                    'page_category': self.config.get_page_category(page)
                })
            
            conversion_paths['critical_pages'] = critical_pages
        
        return conversion_paths
    
    def _identify_drop_off_points(self, classified_df: pd.DataFrame) -> List[Dict]:
        """이탈 지점 식별"""
        print("  🚪 이탈 지점 식별...")
        
        drop_off_points = []
        
        # 사용자별 마지막 단계 분석
        user_last_stages = []
        for user_id, user_events in classified_df.groupby('userPseudoId'):
            stages = user_events['funnel_stage'].values
            valid_stages = [s for s in stages if s != 'UNKNOWN']
            
            if valid_stages:
                # 가장 높은 단계를 마지막 단계로 설정
                stage_order = ['AWARENESS', 'INTEREST', 'CONSIDERATION', 'CONVERSION']
                last_stage = None
                for stage in reversed(stage_order):
                    if stage in valid_stages:
                        last_stage = stage
                        break
                
                if last_stage and last_stage != 'CONVERSION':
                    last_page = user_events.iloc[-1]['pagePath']
                    last_source = user_events.iloc[0]['sessionSource']
                    
                    user_last_stages.append({
                        'user_id': user_id,
                        'last_stage': last_stage,
                        'last_page': last_page,
                        'last_source': last_source,
                        'total_events': len(user_events)
                    })
        
        if user_last_stages:
            last_stages_df = pd.DataFrame(user_last_stages)
            
            # 단계별 이탈 분석
            for stage in ['AWARENESS', 'INTEREST', 'CONSIDERATION']:
                stage_dropoffs = last_stages_df[last_stages_df['last_stage'] == stage]
                
                if not stage_dropoffs.empty:
                    # 가장 많이 이탈하는 페이지
                    top_dropoff_pages = stage_dropoffs['last_page'].value_counts().head(5)
                    
                    drop_off_points.append({
                        'stage': stage,
                        'total_dropoffs': len(stage_dropoffs),
                        'dropoff_rate': len(stage_dropoffs) / len(last_stages_df) * 100,
                        'top_dropoff_pages': [
                            {'page': page, 'count': count, 'percentage': count/len(stage_dropoffs)*100}
                            for page, count in top_dropoff_pages.items()
                        ]
                    })
        
        return drop_off_points
    
    def _calculate_engagement_score(self, user_events: pd.DataFrame) -> float:
        """사용자 참여도 점수 계산"""
        score = 0.0
        
        # 이벤트 다양성
        unique_events = user_events['eventName'].nunique()
        score += unique_events * 2
        
        # 페이지 다양성
        unique_pages = user_events['pagePath'].nunique()
        score += unique_pages * 1.5
        
        # 깊이 있는 이벤트 (user_engagement, visit_blog 등)
        engagement_events = user_events[user_events['eventName'].isin(['user_engagement', 'visit_blog'])]
        score += len(engagement_events) * 3
        
        # 세션 수 (재방문)
        if 'sessionId' in user_events.columns:
            unique_sessions = user_events['sessionId'].nunique()
            score += unique_sessions * 5
        
        return score
    
    def _generate_optimization_insights(self, patterns: Dict) -> Dict:
        """패턴 분석 결과를 바탕으로 최적화 인사이트 생성"""
        print("  💡 최적화 인사이트 생성...")
        
        insights = {
            'top_opportunities': [],
            'source_optimization': [],
            'content_optimization': [],
            'journey_optimization': [],
            'quick_wins': []
        }
        
        # 1. 소스-페이지 조합 최적화
        source_patterns = patterns.get('source_page_combinations', [])
        if source_patterns:
            best_pattern = source_patterns[0]
            insights['source_optimization'].append({
                'type': 'Scale Best Combination',
                'traffic_group': best_pattern['traffic_group'],
                'combination': best_pattern['best_combination']['combination'],
                'current_conversion_rate': best_pattern['best_combination']['conversion_rate'],
                'recommendation': f"{best_pattern['traffic_group']} 트래픽의 {best_pattern['best_combination']['combination']} 경로 확대",
                'priority': 'High'
            })
        
        # 2. 여정 패턴 최적화
        journey_patterns = patterns.get('journey_patterns', {})
        if 'common_paths' in journey_patterns and journey_patterns['common_paths']:
            best_path = journey_patterns['common_paths'][0]
            insights['journey_optimization'].append({
                'type': 'Optimize High-Converting Path',
                'path': best_path['path'],
                'conversion_rate': best_path['conversion_rate'],
                'recommendation': f"'{best_path['path']}' 경로의 사용자 경험 개선 및 유도 강화",
                'priority': 'Medium'
            })
        
        # 3. 참여도 패턴 최적화
        engagement_patterns = patterns.get('engagement_patterns', {})
        if 'page_category_engagement' in engagement_patterns:
            page_engagement = engagement_patterns['page_category_engagement']
            
            # 높은 전환율을 보이는 페이지 카테고리 찾기
            high_converting_categories = [
                (category, data) for category, data in page_engagement.items()
                if data['conversion_rate'] > 20 and data['total_users'] > 50
            ]
            
            if high_converting_categories:
                best_category = max(high_converting_categories, key=lambda x: x[1]['conversion_rate'])
                insights['content_optimization'].append({
                    'type': 'Expand High-Converting Content',
                    'category': best_category[0],
                    'conversion_rate': best_category[1]['conversion_rate'],
                    'recommendation': f"{best_category[0]} 카테고리 콘텐츠 확대 (현재 {best_category[1]['conversion_rate']:.1f}% 전환율)",
                    'priority': 'Medium'
                })
        
        # 4. 고가치 세그먼트 활용
        high_value_segments = patterns.get('high_value_segments', [])
        if high_value_segments:
            top_segment = high_value_segments[0]
            insights['quick_wins'].append({
                'type': 'Target High-Value Segment',
                'segment': top_segment['segment_name'],
                'conversion_rate': top_segment['conversion_rate'],
                'users_count': top_segment['users_count'],
                'recommendation': f"{top_segment['segment_name']} 세그먼트 타겟팅 강화 ({top_segment['conversion_rate']:.1f}% 전환율)",
                'priority': 'High'
            })
        
        # 5. 이탈 지점 개선
        drop_off_points = patterns.get('drop_off_points', [])
        if drop_off_points:
            major_dropoff = max(drop_off_points, key=lambda x: x['total_dropoffs'])
            if major_dropoff['top_dropoff_pages']:
                top_dropoff_page = major_dropoff['top_dropoff_pages'][0]
                insights['quick_wins'].append({
                    'type': 'Fix Major Drop-off Point',
                    'stage': major_dropoff['stage'],
                    'page': top_dropoff_page['page'],
                    'dropoff_count': top_dropoff_page['count'],
                    'recommendation': f"{major_dropoff['stage']} 단계의 {top_dropoff_page['page']} 페이지 최적화 필요",
                    'priority': 'High'
                })
        
        # 상위 기회 요약
        all_opportunities = (
            insights['source_optimization'] + 
            insights['content_optimization'] + 
            insights['journey_optimization'] + 
            insights['quick_wins']
        )
        
        # 우선순위별 정렬
        priority_order = {'High': 3, 'Medium': 2, 'Low': 1}
        all_opportunities.sort(key=lambda x: priority_order.get(x['priority'], 0), reverse=True)
        
        insights['top_opportunities'] = all_opportunities[:5]
        
        return insights

# 사용 예시
if __name__ == "__main__":
    # 설정 및 분석기 초기화
    config = Config()
    analyzer = PatternAnalyzer(config)
    
    # 샘플 데이터로 테스트
    sample_classified = pd.DataFrame({
        'userPseudoId': ['user1', 'user1', 'user2', 'user2'],
        'sessionSource': ['google', 'google', 'naver', 'naver'],
        'sessionMedium': ['cpc', 'cpc', 'organic', 'organic'],
        'pagePath': ['/', '/posts/739', '/skill-guide/one-click', '/provider-join'],
        'funnel_stage': ['AWARENESS', 'INTEREST', 'AWARENESS', 'CONVERSION'],
        'eventName': ['session_start', 'visit_blog', 'session_start', '회원가입2'],
        'page_category': ['homepage', 'content', 'support', 'authentication'],
        'eventTimestamp': ['2024-01-01 10:00:00', '2024-01-01 10:05:00', 
                          '2024-01-01 14:00:00', '2024-01-01 14:30:00']
    })
    
    sample_sequences = pd.DataFrame({
        'userPseudoId': ['user1', 'user1', 'user2', 'user2'],
        'pagePath': ['/', '/posts/739', '/skill-guide/one-click', '/provider-join'],
        'eventTimestamp': ['2024-01-01 10:00:00', '2024-01-01 10:05:00', 
                          '2024-01-01 14:00:00', '2024-01-01 14:30:00']
    })
    
    sample_conversions = pd.DataFrame({
        'userPseudoId': ['user2'],
        'eventName': ['회원가입2']
    })
    
    # 패턴 분석 실행
    patterns = analyzer.analyze_conversion_patterns(
        sample_classified, sample_sequences, sample_conversions
    )
    
    print("패턴 분석 완료!")
    print(f"최적화 기회: {len(patterns['optimization_insights']['top_opportunities'])}개")