#!/usr/bin/env python3
"""
B-flow 퍼널 분석 메인 실행 파일
전환율 최적화를 위한 4단계 퍼널 분석 시스템
"""

import sys
import argparse
from datetime import datetime, timedelta
from data_loader import GA4DataLoader
from stage_classifier import FunnelStageClassifier
from conversion_calculator import ConversionCalculator
from pattern_analyzer import PatternAnalyzer
from report_generator import ReportGenerator
from config import Config

class FunnelAnalyzer:
    """B-flow 퍼널 분석 메인 클래스"""
    
    def __init__(self, config_path="config.json"):
        """
        Args:
            config_path: 설정 파일 경로
        """
        self.config = Config(config_path)
        self.data_loader = GA4DataLoader(
            self.config.credentials_path, 
            self.config.property_id
        )
        self.stage_classifier = FunnelStageClassifier()
        self.conversion_calculator = ConversionCalculator()
        self.pattern_analyzer = PatternAnalyzer()
        self.report_generator = ReportGenerator()
    
    def run_full_analysis(self, start_date, end_date, output_dir="output"):
        """
        전체 퍼널 분석 실행
        
        Args:
            start_date: 분석 시작일 (YYYY-MM-DD)
            end_date: 분석 종료일 (YYYY-MM-DD)
            output_dir: 결과 파일 저장 디렉토리
        """
        print(f"🚀 B-flow 퍼널 분석 시작: {start_date} ~ {end_date}")
        
        # 1. 데이터 수집
        print("📊 GA4 데이터 수집 중...")
        raw_data = self.data_loader.get_funnel_data(start_date, end_date)
        page_sequences = self.data_loader.get_page_sequences(start_date, end_date)
        conversion_events = self.data_loader.get_conversion_events(start_date, end_date)
        
        # 2. 퍼널 단계 분류
        print("🔄 퍼널 단계 분류 중...")
        classified_data = self.stage_classifier.classify_user_events(raw_data)
        
        # 3. 전환율 계산
        print("📈 전환율 계산 중...")
        conversion_rates = self.conversion_calculator.calculate_stage_conversions(
            classified_data, conversion_events
        )
        
        # 4. 패턴 분석
        print("🔍 변수 조합 패턴 분석 중...")
        patterns = self.pattern_analyzer.analyze_conversion_patterns(
            classified_data, page_sequences, conversion_events
        )
        
        # 5. 리포트 생성
        print("📋 분석 리포트 생성 중...")
        self.report_generator.generate_optimization_report(
            conversion_rates, patterns, output_dir
        )
        
        print(f"✅ 분석 완료! 결과는 {output_dir} 디렉토리에 저장되었습니다.")
        
        return {
            'conversion_rates': conversion_rates,
            'patterns': patterns,
            'output_dir': output_dir
        }
    
    def run_quick_analysis(self, days=30):
        """
        빠른 분석 (최근 N일)
        
        Args:
            days: 분석할 최근 일수
        """
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        
        return self.run_full_analysis(start_date, end_date, f"output_quick_{days}days")

def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description='B-flow 퍼널 분석 시스템')
    parser.add_argument('--start-date', type=str, help='분석 시작일 (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='분석 종료일 (YYYY-MM-DD)')
    parser.add_argument('--days', type=int, default=30, help='최근 N일 분석 (기본: 30일)')
    parser.add_argument('--output', type=str, default='output', help='출력 디렉토리')
    parser.add_argument('--config', type=str, default='config.json', help='설정 파일 경로')
    
    args = parser.parse_args()
    
    try:
        analyzer = FunnelAnalyzer(args.config)
        
        if args.start_date and args.end_date:
            # 특정 기간 분석
            results = analyzer.run_full_analysis(
                args.start_date, args.end_date, args.output
            )
        else:
            # 최근 N일 분석
            results = analyzer.run_quick_analysis(args.days)
        
        print("\n📊 분석 요약:")
        print(f"- 전체 퍼널 전환율: {results['conversion_rates'].get('overall', 'N/A')}")
        print(f"- 주요 병목 구간: {results['patterns'].get('bottleneck', 'N/A')}")
        print(f"- 최고 성과 조합: {results['patterns'].get('top_performer', 'N/A')}")
        
    except Exception as e:
        print(f"❌ 분석 중 오류 발생: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()