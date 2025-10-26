#!/usr/bin/env python3
"""
Simple and effective model performance metrics
Shows stakeholders that the CSM routing model is working well
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import snowflake.connector
from tabulate import tabulate
import os

class ModelMetrics:
    """Generate performance metrics for CSM routing model"""

    def __init__(self):
        self.conn = self.connect_to_snowflake()
        self.metrics = {}

    def connect_to_snowflake(self):
        """Establish Snowflake connection"""
        return snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse='COMPUTE_WH',
            database='DSV_WAREHOUSE',
            schema='DATA_SCIENCE'
        )

    def execute_query(self, query):
        """Execute query and return dataframe"""
        try:
            return pd.read_sql(query, self.conn)
        except Exception as e:
            print(f"Query error: {e}")
            return pd.DataFrame()

    def run_all_metrics(self):
        """Run all metric calculations"""
        print("="*80)
        print("CSM ROUTING MODEL - PERFORMANCE METRICS REPORT")
        print("="*80)
        print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()

        # 1. Workload Distribution
        self.measure_workload_distribution()

        # 2. Assignment Quality
        self.measure_assignment_quality()

        # 3. Exclusion Effectiveness
        self.measure_exclusion_effectiveness()

        # 4. Capacity Compliance
        self.measure_capacity_compliance()

        # 5. Recent Performance
        self.measure_recent_performance()

        # 6. Generate Summary Score
        self.generate_summary_score()

    def measure_workload_distribution(self):
        """Measure how evenly workload is distributed"""
        print("\n📊 1. WORKLOAD DISTRIBUTION METRICS")
        print("-" * 40)

        query = """
        SELECT
            COUNT(DISTINCT responsible_csm) as total_csms,
            AVG(account_count) as avg_accounts,
            STDDEV(account_count) as std_accounts,
            MIN(account_count) as min_accounts,
            MAX(account_count) as max_accounts,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY account_count) as q1_accounts,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY account_count) as median_accounts,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY account_count) as q3_accounts
        FROM (
            SELECT responsible_csm, COUNT(*) as account_count
            FROM data_csm_routing_account_health_revenue_tad_neediness
            WHERE responsible_csm IN (SELECT active_csm FROM resi_corp_active_csms)
            GROUP BY responsible_csm
        )
        """

        df = self.execute_query(query)
        if not df.empty:
            row = df.iloc[0]
            cv = (row['STD_ACCOUNTS'] / row['AVG_ACCOUNTS']) * 100

            print(f"Active CSMs: {int(row['TOTAL_CSMS'])}")
            print(f"Average accounts per CSM: {row['AVG_ACCOUNTS']:.1f}")
            print(f"Standard deviation: {row['STD_ACCOUNTS']:.1f}")
            print(f"Coefficient of Variation: {cv:.1f}%")
            print(f"Distribution: Min={int(row['MIN_ACCOUNTS'])}, Q1={int(row['Q1_ACCOUNTS'])}, Median={int(row['MEDIAN_ACCOUNTS'])}, Q3={int(row['Q3_ACCOUNTS'])}, Max={int(row['MAX_ACCOUNTS'])}")

            # Score
            if cv < 15:
                print("✅ EXCELLENT: Very balanced distribution (CV < 15%)")
                self.metrics['distribution_score'] = 100
            elif cv < 20:
                print("✅ GOOD: Well-balanced distribution (CV < 20%)")
                self.metrics['distribution_score'] = 85
            elif cv < 25:
                print("⚠️  FAIR: Some imbalance in distribution (CV < 25%)")
                self.metrics['distribution_score'] = 70
            else:
                print("❌ POOR: Significant imbalance (CV >= 25%)")
                self.metrics['distribution_score'] = 50

    def measure_assignment_quality(self):
        """Measure quality of assignments"""
        print("\n🎯 2. ASSIGNMENT QUALITY METRICS")
        print("-" * 40)

        query = """
        WITH recent_assignments AS (
            SELECT
                a.account_id,
                a.csm_name,
                h.neediness_score,
                h.core_health_score_color as health,
                CASE
                    WHEN csm.tenure_months < 3 THEN 'New'
                    WHEN csm.tenure_months < 6 THEN 'Junior'
                    WHEN csm.tenure_months < 12 THEN 'Mid'
                    ELSE 'Senior'
                END as tenure_category
            FROM ACCOUNT_CSM_ASSIGNMENTS_CANNE a
            JOIN data_csm_routing_account_health_revenue_tad_neediness h ON a.account_id = h.account_id_ob
            LEFT JOIN (
                SELECT preferred_csm_name,
                       DATEDIFF(month, MIN(calendar_dt), CURRENT_DATE()) as tenure_months
                FROM DSV_WAREHOUSE.PUBLIC.ACCOUNT_CSM_V2
                GROUP BY preferred_csm_name
            ) csm ON a.csm_name = csm.preferred_csm_name
            WHERE a.assignment_date >= DATEADD(day, -7, CURRENT_DATE())
        )
        SELECT
            COUNT(*) as total,
            -- High neediness to experienced
            SUM(CASE WHEN neediness_score >= 8 AND tenure_category IN ('Senior', 'Mid') THEN 1 ELSE 0 END) * 100.0 /
                NULLIF(SUM(CASE WHEN neediness_score >= 8 THEN 1 ELSE 0 END), 0) as high_need_match_pct,
            -- Red to senior
            SUM(CASE WHEN health = 'Red' AND tenure_category = 'Senior' THEN 1 ELSE 0 END) * 100.0 /
                NULLIF(SUM(CASE WHEN health = 'Red' THEN 1 ELSE 0 END), 0) as red_to_senior_pct,
            -- New CSMs get green
            SUM(CASE WHEN tenure_category = 'New' AND health = 'Green' THEN 1 ELSE 0 END) * 100.0 /
                NULLIF(SUM(CASE WHEN tenure_category = 'New' THEN 1 ELSE 0 END), 0) as new_to_green_pct
        FROM recent_assignments
        """

        df = self.execute_query(query)
        if not df.empty and df.iloc[0]['TOTAL'] > 0:
            row = df.iloc[0]

            print(f"Recent assignments analyzed: {int(row['TOTAL'])}")

            scores = []
            if pd.notna(row['HIGH_NEED_MATCH_PCT']):
                print(f"High neediness → Experienced CSMs: {row['HIGH_NEED_MATCH_PCT']:.1f}%")
                scores.append(row['HIGH_NEED_MATCH_PCT'])

            if pd.notna(row['RED_TO_SENIOR_PCT']):
                print(f"Red accounts → Senior CSMs: {row['RED_TO_SENIOR_PCT']:.1f}%")
                scores.append(row['RED_TO_SENIOR_PCT'])

            if pd.notna(row['NEW_TO_GREEN_PCT']):
                print(f"New CSMs → Green accounts: {row['NEW_TO_GREEN_PCT']:.1f}%")
                scores.append(row['NEW_TO_GREEN_PCT'])

            avg_score = np.mean(scores) if scores else 0
            self.metrics['quality_score'] = avg_score

            if avg_score >= 80:
                print(f"✅ EXCELLENT: Great assignment matching (avg: {avg_score:.1f}%)")
            elif avg_score >= 70:
                print(f"✅ GOOD: Good assignment matching (avg: {avg_score:.1f}%)")
            elif avg_score >= 60:
                print(f"⚠️  FAIR: Adequate matching (avg: {avg_score:.1f}%)")
            else:
                print(f"❌ POOR: Needs improvement (avg: {avg_score:.1f}%)")
        else:
            print("No recent assignments to analyze")
            self.metrics['quality_score'] = 0

    def measure_exclusion_effectiveness(self):
        """Measure if exclusion prevents repeated assignments"""
        print("\n🔄 3. EXCLUSION EFFECTIVENESS")
        print("-" * 40)

        query = """
        WITH csm_frequency AS (
            SELECT
                recommended_csm,
                COUNT(*) as total_recs,
                COUNT(DISTINCT DATE(recommendation_timestamp)) as days_with_recs
            FROM CSM_ROUTING_RECOMMENDATIONS_CANNE
            WHERE recommendation_timestamp >= DATEADD(day, -7, CURRENT_DATE())
            GROUP BY recommended_csm
        )
        SELECT
            COUNT(*) as csms_with_recs,
            MAX(total_recs) as max_recs_per_csm,
            AVG(total_recs) as avg_recs_per_csm,
            SUM(CASE WHEN total_recs > 10 THEN 1 ELSE 0 END) as csms_over_10,
            SUM(CASE WHEN total_recs > 5 THEN 1 ELSE 0 END) as csms_over_5
        FROM csm_frequency
        """

        df = self.execute_query(query)
        if not df.empty:
            row = df.iloc[0]

            print(f"CSMs receiving recommendations (7 days): {int(row['CSMS_WITH_RECS'])}")
            print(f"Max recommendations per CSM: {int(row['MAX_RECS_PER_CSM'])}")
            print(f"Average recommendations per CSM: {row['AVG_RECS_PER_CSM']:.1f}")
            print(f"CSMs with >10 recommendations: {int(row['CSMS_OVER_10'])}")
            print(f"CSMs with >5 recommendations: {int(row['CSMS_OVER_5'])}")

            if row['CSMS_OVER_10'] == 0:
                print("✅ EXCELLENT: No CSM overwhelmed with recommendations")
                self.metrics['exclusion_score'] = 100
            elif row['CSMS_OVER_10'] <= 2:
                print("✅ GOOD: Very few CSMs with excessive recommendations")
                self.metrics['exclusion_score'] = 85
            elif row['CSMS_OVER_10'] <= 4:
                print("⚠️  FAIR: Some CSMs receiving many recommendations")
                self.metrics['exclusion_score'] = 70
            else:
                print("❌ POOR: Multiple CSMs overwhelmed")
                self.metrics['exclusion_score'] = 50

    def measure_capacity_compliance(self):
        """Measure capacity limit compliance"""
        print("\n⚖️  4. CAPACITY COMPLIANCE")
        print("-" * 40)

        query = """
        WITH csm_capacity AS (
            SELECT
                responsible_csm,
                COUNT(*) as account_count,
                MAX(CASE
                    WHEN segment = 'Residential' AND account_level = 'Corporate' THEN 100
                    WHEN segment = 'Commercial' AND account_level = 'Corporate' THEN 70
                    WHEN segment = 'Commercial' AND account_level = 'Enterprise' THEN 35
                    WHEN segment = 'Residential' AND account_level = 'Enterprise' THEN 40
                    ELSE 85
                END) as max_capacity
            FROM data_csm_routing_account_health_revenue_tad_neediness
            WHERE responsible_csm IN (SELECT active_csm FROM resi_corp_active_csms)
            GROUP BY responsible_csm
        )
        SELECT
            COUNT(*) as total_csms,
            SUM(CASE WHEN account_count > max_capacity THEN 1 ELSE 0 END) as over_capacity,
            SUM(CASE WHEN account_count > max_capacity * 0.9 THEN 1 ELSE 0 END) as near_capacity,
            SUM(CASE WHEN account_count > 100 THEN 1 ELSE 0 END) as over_100
        FROM csm_capacity
        """

        df = self.execute_query(query)
        if not df.empty:
            row = df.iloc[0]

            print(f"Total CSMs analyzed: {int(row['TOTAL_CSMS'])}")
            print(f"CSMs over capacity: {int(row['OVER_CAPACITY'])}")
            print(f"CSMs at 90%+ capacity: {int(row['NEAR_CAPACITY'])}")
            print(f"CSMs with >100 accounts: {int(row['OVER_100'])}")

            if row['OVER_CAPACITY'] == 0:
                print("✅ EXCELLENT: No CSMs exceed capacity")
                self.metrics['capacity_score'] = 100
            elif row['OVER_CAPACITY'] <= 2:
                print("✅ GOOD: Very few capacity violations")
                self.metrics['capacity_score'] = 85
            elif row['OVER_CAPACITY'] <= 5:
                print("⚠️  FAIR: Some capacity issues")
                self.metrics['capacity_score'] = 70
            else:
                print("❌ POOR: Multiple capacity violations")
                self.metrics['capacity_score'] = 50

    def measure_recent_performance(self):
        """Measure recent model performance"""
        print("\n📈 5. RECENT MODEL PERFORMANCE")
        print("-" * 40)

        query = """
        SELECT
            COUNT(*) as total_recommendations,
            SUM(CASE WHEN was_assigned = TRUE THEN 1 ELSE 0 END) as assignments_made,
            SUM(CASE WHEN llm_feedback LIKE '%approved%' THEN 1 ELSE 0 END) as llm_approved,
            SUM(CASE WHEN llm_feedback LIKE '%rejected%' THEN 1 ELSE 0 END) as llm_rejected,
            COUNT(DISTINCT recommended_csm) as unique_csms_used
        FROM CSM_ROUTING_RECOMMENDATIONS_CANNE
        WHERE recommendation_timestamp >= DATEADD(day, -7, CURRENT_DATE())
        """

        df = self.execute_query(query)
        if not df.empty and df.iloc[0]['TOTAL_RECOMMENDATIONS'] > 0:
            row = df.iloc[0]

            assignment_rate = (row['ASSIGNMENTS_MADE'] / row['TOTAL_RECOMMENDATIONS']) * 100
            approval_rate = (row['LLM_APPROVED'] / max(row['LLM_APPROVED'] + row['LLM_REJECTED'], 1)) * 100

            print(f"Total recommendations (7 days): {int(row['TOTAL_RECOMMENDATIONS'])}")
            print(f"Assignments completed: {int(row['ASSIGNMENTS_MADE'])} ({assignment_rate:.1f}%)")
            print(f"LLM approval rate: {approval_rate:.1f}%")
            print(f"Unique CSMs utilized: {int(row['UNIQUE_CSMS_USED'])}")

            # Performance score based on approval rate (should be balanced)
            if 40 <= approval_rate <= 70:
                print("✅ EXCELLENT: Balanced LLM oversight")
                self.metrics['performance_score'] = 100
            elif 30 <= approval_rate <= 80:
                print("✅ GOOD: Reasonable LLM oversight")
                self.metrics['performance_score'] = 85
            else:
                print("⚠️  NEEDS TUNING: LLM approval rate outside optimal range")
                self.metrics['performance_score'] = 70
        else:
            print("No recent recommendations to analyze")
            self.metrics['performance_score'] = 0

    def generate_summary_score(self):
        """Generate overall model score"""
        print("\n" + "="*80)
        print("OVERALL MODEL ASSESSMENT")
        print("="*80)

        # Calculate weighted average
        weights = {
            'distribution_score': 0.25,
            'quality_score': 0.25,
            'exclusion_score': 0.20,
            'capacity_score': 0.20,
            'performance_score': 0.10
        }

        total_score = sum(
            self.metrics.get(key, 0) * weight
            for key, weight in weights.items()
        )

        # Print individual scores
        print("\nComponent Scores:")
        print("-" * 40)
        components = [
            ['Workload Distribution', f"{self.metrics.get('distribution_score', 0):.0f}%", f"{weights['distribution_score']*100:.0f}%"],
            ['Assignment Quality', f"{self.metrics.get('quality_score', 0):.0f}%", f"{weights['quality_score']*100:.0f}%"],
            ['Exclusion Effectiveness', f"{self.metrics.get('exclusion_score', 0):.0f}%", f"{weights['exclusion_score']*100:.0f}%"],
            ['Capacity Compliance', f"{self.metrics.get('capacity_score', 0):.0f}%", f"{weights['capacity_score']*100:.0f}%"],
            ['Model Performance', f"{self.metrics.get('performance_score', 0):.0f}%", f"{weights['performance_score']*100:.0f}%"]
        ]
        print(tabulate(components, headers=['Metric', 'Score', 'Weight'], tablefmt='simple'))

        # Overall grade
        print(f"\n🏆 OVERALL MODEL SCORE: {total_score:.0f}%")

        if total_score >= 90:
            print("\n✅ GRADE: A - EXCELLENT")
            print("The model is performing exceptionally well!")
            print("Ready for production deployment.")
        elif total_score >= 80:
            print("\n✅ GRADE: B - GOOD")
            print("The model is performing well.")
            print("Minor optimizations could improve performance.")
        elif total_score >= 70:
            print("\n⚠️  GRADE: C - SATISFACTORY")
            print("The model is functional but has room for improvement.")
            print("Consider addressing the lower-scoring components.")
        elif total_score >= 60:
            print("\n⚠️  GRADE: D - NEEDS IMPROVEMENT")
            print("The model requires attention to key metrics.")
            print("Focus on the lowest-scoring components first.")
        else:
            print("\n❌ GRADE: F - REQUIRES SIGNIFICANT WORK")
            print("The model needs substantial improvements.")
            print("Review all components and address critical issues.")

        # Key insights
        print("\n💡 KEY INSIGHTS:")
        print("-" * 40)

        lowest_score = min(self.metrics.values())
        highest_score = max(self.metrics.values())

        for key, score in self.metrics.items():
            if score == highest_score:
                metric_name = key.replace('_score', '').replace('_', ' ').title()
                print(f"✓ Strongest area: {metric_name} ({score:.0f}%)")
                break

        for key, score in self.metrics.items():
            if score == lowest_score:
                metric_name = key.replace('_score', '').replace('_', ' ').title()
                print(f"✗ Needs attention: {metric_name} ({score:.0f}%)")
                break

        # Save report
        report_file = f"model_metrics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w') as f:
            json.dump({
                'timestamp': datetime.now().isoformat(),
                'overall_score': total_score,
                'metrics': self.metrics,
                'weights': weights
            }, f, indent=2)

        print(f"\n📁 Report saved to: {report_file}")

        return total_score

def main():
    """Run the metrics report"""
    try:
        metrics = ModelMetrics()
        score = metrics.run_all_metrics()

        if metrics.conn:
            metrics.conn.close()

        return 0 if score >= 70 else 1
    except Exception as e:
        print(f"Error: {e}")
        return 1

if __name__ == "__main__":
    import sys
    sys.exit(main())