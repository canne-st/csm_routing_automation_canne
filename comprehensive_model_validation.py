#!/usr/bin/env python3
"""
Comprehensive Model Validation Suite for CSM Routing Automation
This script validates the model's performance across multiple dimensions
to prove its effectiveness to stakeholders.
"""

import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import json
from typing import Dict, List, Tuple
import snowflake.connector
import os
from csm_routing_automation import CSMRoutingAutomation

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.FileHandler(f'model_validation_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class CSMModelValidator:
    """Validates CSM routing model performance across multiple metrics"""

    def __init__(self):
        self.automation = CSMRoutingAutomation()
        self.metrics = {
            'distribution': {},
            'assignment_quality': {},
            'capacity': {},
            'exclusion': {},
            'performance': {},
            'llm_effectiveness': {}
        }
        self.test_results = []

    def run_validation_suite(self):
        """Run complete validation suite"""
        logger.info("="*80)
        logger.info("CSM ROUTING MODEL VALIDATION SUITE")
        logger.info("="*80)

        # 1. Test current state metrics
        self.validate_current_distribution()

        # 2. Test assignment quality
        self.validate_assignment_quality()

        # 3. Test exclusion logic
        self.validate_exclusion_logic()

        # 4. Test capacity management
        self.validate_capacity_management()

        # 5. Test LLM effectiveness
        self.validate_llm_effectiveness()

        # 6. Run simulated assignments
        self.run_simulation_tests()

        # 7. Generate comprehensive report
        self.generate_validation_report()

    def validate_current_distribution(self):
        """Validate current CSM book distribution"""
        logger.info("\n1. VALIDATING CURRENT DISTRIBUTION")
        logger.info("-" * 40)

        try:
            # Get current CSM books
            query = """
            SELECT
                responsible_csm as csm_name,
                COUNT(*) as account_count,
                AVG(neediness_score) as avg_neediness,
                SUM(CASE WHEN core_health_score_color = 'Red' THEN 1 ELSE 0 END) as red_accounts,
                SUM(CASE WHEN core_health_score_color = 'Yellow' THEN 1 ELSE 0 END) as yellow_accounts,
                SUM(CASE WHEN core_health_score_color = 'Green' THEN 1 ELSE 0 END) as green_accounts,
                SUM(revenue) as total_revenue
            FROM DSV_WAREHOUSE.DATA_SCIENCE.data_csm_routing_account_health_revenue_tad_neediness
            WHERE responsible_csm IS NOT NULL
                AND responsible_csm IN (
                    SELECT active_csm
                    FROM DSV_WAREHOUSE.DATA_SCIENCE.resi_corp_active_csms
                )
            GROUP BY responsible_csm
            """

            df = self.automation.execute_query(query)

            if not df.empty:
                # Calculate distribution metrics
                account_counts = df['ACCOUNT_COUNT'].values

                self.metrics['distribution'] = {
                    'mean_accounts': float(np.mean(account_counts)),
                    'std_accounts': float(np.std(account_counts)),
                    'cv_accounts': float(np.std(account_counts) / np.mean(account_counts) * 100),
                    'min_accounts': int(np.min(account_counts)),
                    'max_accounts': int(np.max(account_counts)),
                    'csms_over_85': int(sum(1 for x in account_counts if x > 85)),
                    'csms_over_100': int(sum(1 for x in account_counts if x > 100))
                }

                logger.info(f"✓ Mean accounts per CSM: {self.metrics['distribution']['mean_accounts']:.1f}")
                logger.info(f"✓ Standard deviation: {self.metrics['distribution']['std_accounts']:.1f}")
                logger.info(f"✓ Coefficient of variation: {self.metrics['distribution']['cv_accounts']:.1f}%")
                logger.info(f"✓ Range: {self.metrics['distribution']['min_accounts']} - {self.metrics['distribution']['max_accounts']}")

                # Check for issues
                if self.metrics['distribution']['cv_accounts'] > 20:
                    logger.warning(f"⚠ High variation in distribution (CV > 20%)")
                else:
                    logger.info(f"✅ Distribution is balanced (CV < 20%)")

                if self.metrics['distribution']['csms_over_85'] > 0:
                    logger.warning(f"⚠ {self.metrics['distribution']['csms_over_85']} CSMs over capacity (>85 accounts)")
                else:
                    logger.info(f"✅ No CSMs over standard capacity")

        except Exception as e:
            logger.error(f"Failed to validate distribution: {str(e)}")

    def validate_assignment_quality(self):
        """Validate quality of recent assignments"""
        logger.info("\n2. VALIDATING ASSIGNMENT QUALITY")
        logger.info("-" * 40)

        try:
            # Check recent assignments
            query = """
            WITH recent_assignments AS (
                SELECT
                    a.account_id,
                    a.csm_name,
                    a.assignment_date,
                    h.neediness_score,
                    h.core_health_score_color as health,
                    h.revenue,
                    c.tenure_months,
                    CASE
                        WHEN c.tenure_months < 3 THEN 'New'
                        WHEN c.tenure_months < 6 THEN 'Junior'
                        WHEN c.tenure_months < 12 THEN 'Mid'
                        WHEN c.tenure_months < 24 THEN 'Senior'
                        ELSE 'Expert'
                    END as tenure_category
                FROM DSV_WAREHOUSE.DATA_SCIENCE.ACCOUNT_CSM_ASSIGNMENTS_CANNE a
                JOIN DSV_WAREHOUSE.DATA_SCIENCE.data_csm_routing_account_health_revenue_tad_neediness h
                    ON a.account_id = h.account_id_ob
                LEFT JOIN (
                    SELECT
                        preferred_csm_name as csm_name,
                        DATEDIFF(month, MIN(calendar_dt), CURRENT_DATE()) as tenure_months
                    FROM DSV_WAREHOUSE.PUBLIC.ACCOUNT_CSM_V2
                    GROUP BY preferred_csm_name
                ) c ON a.csm_name = c.csm_name
                WHERE a.assignment_date >= DATEADD(day, -7, CURRENT_DATE())
            )
            SELECT
                COUNT(*) as total_assignments,
                -- Check if high neediness goes to experienced CSMs
                SUM(CASE
                    WHEN neediness_score >= 8 AND tenure_category IN ('Senior', 'Expert', 'Mid')
                    THEN 1 ELSE 0
                END) as high_need_to_experienced,
                SUM(CASE
                    WHEN neediness_score >= 8
                    THEN 1 ELSE 0
                END) as total_high_need,
                -- Check if red accounts go to senior CSMs
                SUM(CASE
                    WHEN health = 'Red' AND tenure_category IN ('Senior', 'Expert')
                    THEN 1 ELSE 0
                END) as red_to_senior,
                SUM(CASE
                    WHEN health = 'Red'
                    THEN 1 ELSE 0
                END) as total_red,
                -- Check new CSMs get easier accounts
                SUM(CASE
                    WHEN tenure_category = 'New' AND health = 'Green'
                    THEN 1 ELSE 0
                END) as new_csm_green,
                SUM(CASE
                    WHEN tenure_category = 'New'
                    THEN 1 ELSE 0
                END) as total_new_csm_assignments
            FROM recent_assignments
            """

            df = self.automation.execute_query(query)

            if not df.empty and df['TOTAL_ASSIGNMENTS'].iloc[0] > 0:
                row = df.iloc[0]

                # Calculate quality metrics
                high_need_match = (row['HIGH_NEED_TO_EXPERIENCED'] / max(row['TOTAL_HIGH_NEED'], 1)) * 100
                red_match = (row['RED_TO_SENIOR'] / max(row['TOTAL_RED'], 1)) * 100
                new_csm_match = (row['NEW_CSM_GREEN'] / max(row['TOTAL_NEW_CSM_ASSIGNMENTS'], 1)) * 100

                self.metrics['assignment_quality'] = {
                    'total_recent_assignments': int(row['TOTAL_ASSIGNMENTS']),
                    'high_neediness_match_rate': float(high_need_match),
                    'red_account_match_rate': float(red_match),
                    'new_csm_green_rate': float(new_csm_match)
                }

                logger.info(f"✓ Recent assignments analyzed: {row['TOTAL_ASSIGNMENTS']}")
                logger.info(f"✓ High neediness → Experienced CSMs: {high_need_match:.1f}%")
                logger.info(f"✓ Red accounts → Senior CSMs: {red_match:.1f}%")
                logger.info(f"✓ New CSMs → Green accounts: {new_csm_match:.1f}%")

                # Evaluate quality
                if high_need_match >= 80:
                    logger.info("✅ Excellent matching for high neediness accounts")
                elif high_need_match >= 60:
                    logger.info("✓ Good matching for high neediness accounts")
                else:
                    logger.warning("⚠ Poor matching for high neediness accounts")

        except Exception as e:
            logger.error(f"Failed to validate assignment quality: {str(e)}")

    def validate_exclusion_logic(self):
        """Validate that exclusion logic prevents repeated assignments"""
        logger.info("\n3. VALIDATING EXCLUSION LOGIC")
        logger.info("-" * 40)

        try:
            # Check for repeated assignments to same CSM
            query = """
            WITH csm_assignment_frequency AS (
                SELECT
                    recommended_csm as csm_name,
                    DATE(recommendation_timestamp) as assignment_date,
                    COUNT(*) as daily_assignments
                FROM DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE
                WHERE recommendation_timestamp >= DATEADD(day, -7, CURRENT_DATE())
                GROUP BY recommended_csm, DATE(recommendation_timestamp)
            )
            SELECT
                csm_name,
                COUNT(*) as days_with_assignments,
                SUM(daily_assignments) as total_assignments,
                MAX(daily_assignments) as max_daily_assignments,
                AVG(daily_assignments) as avg_daily_assignments
            FROM csm_assignment_frequency
            GROUP BY csm_name
            HAVING total_assignments > 3
            ORDER BY total_assignments DESC
            LIMIT 10
            """

            df = self.automation.execute_query(query)

            if not df.empty:
                # Check for concerning patterns
                max_assignments = df['TOTAL_ASSIGNMENTS'].max()
                csms_with_many = len(df[df['TOTAL_ASSIGNMENTS'] > 5])

                self.metrics['exclusion'] = {
                    'max_assignments_7d': int(max_assignments),
                    'csms_over_5_assignments': int(csms_with_many),
                    'top_assignee': df.iloc[0]['CSM_NAME'] if not df.empty else 'N/A',
                    'exclusion_effective': csms_with_many < 3  # Should be very few CSMs with >5 in 7 days
                }

                logger.info(f"✓ Max assignments to single CSM (7 days): {max_assignments}")
                logger.info(f"✓ CSMs with >5 assignments: {csms_with_many}")

                if csms_with_many <= 2:
                    logger.info("✅ Exclusion logic is working well")
                else:
                    logger.warning(f"⚠ {csms_with_many} CSMs receiving too many assignments")

            else:
                logger.info("✅ No CSMs with excessive assignments")
                self.metrics['exclusion']['exclusion_effective'] = True

        except Exception as e:
            logger.error(f"Failed to validate exclusion logic: {str(e)}")

    def validate_capacity_management(self):
        """Validate capacity limits are respected"""
        logger.info("\n4. VALIDATING CAPACITY MANAGEMENT")
        logger.info("-" * 40)

        try:
            # Check capacity by segment
            query = """
            SELECT
                h.segment,
                h.account_level,
                c.responsible_csm as csm_name,
                COUNT(*) as account_count,
                CASE
                    WHEN h.segment = 'Residential' AND h.account_level = 'Corporate' THEN 100
                    WHEN h.segment = 'Commercial' AND h.account_level = 'Corporate' THEN 70
                    WHEN h.segment = 'Commercial' AND h.account_level = 'Enterprise' THEN 35
                    WHEN h.segment = 'Construction' AND h.account_level = 'Enterprise' THEN 85
                    WHEN h.segment = 'Residential' AND h.account_level = 'Enterprise' THEN 40
                    ELSE 85
                END as max_capacity
            FROM DSV_WAREHOUSE.DATA_SCIENCE.data_csm_routing_account_health_revenue_tad_neediness h
            JOIN (
                SELECT DISTINCT account_id_ob, responsible_csm
                FROM DSV_WAREHOUSE.DATA_SCIENCE.data_csm_routing_account_health_revenue_tad_neediness
                WHERE responsible_csm IN (
                    SELECT active_csm FROM DSV_WAREHOUSE.DATA_SCIENCE.resi_corp_active_csms
                )
            ) c ON h.account_id_ob = c.account_id_ob
            GROUP BY h.segment, h.account_level, c.responsible_csm, max_capacity
            HAVING account_count > max_capacity * 0.8  -- Show CSMs at >80% capacity
            ORDER BY (account_count::float / max_capacity) DESC
            """

            df = self.automation.execute_query(query)

            over_capacity = 0
            near_capacity = 0

            if not df.empty:
                for _, row in df.iterrows():
                    utilization = (row['ACCOUNT_COUNT'] / row['MAX_CAPACITY']) * 100
                    if utilization > 100:
                        over_capacity += 1
                    elif utilization > 80:
                        near_capacity += 1

            self.metrics['capacity'] = {
                'csms_over_capacity': over_capacity,
                'csms_near_capacity': near_capacity,
                'capacity_management_effective': over_capacity == 0
            }

            logger.info(f"✓ CSMs over capacity: {over_capacity}")
            logger.info(f"✓ CSMs at 80-100% capacity: {near_capacity}")

            if over_capacity == 0:
                logger.info("✅ No CSMs exceed capacity limits")
            else:
                logger.warning(f"⚠ {over_capacity} CSMs exceed capacity limits")

        except Exception as e:
            logger.error(f"Failed to validate capacity: {str(e)}")

    def validate_llm_effectiveness(self):
        """Validate LLM review effectiveness"""
        logger.info("\n5. VALIDATING LLM EFFECTIVENESS")
        logger.info("-" * 40)

        try:
            # Check LLM review patterns
            query = """
            SELECT
                COUNT(*) as total_reviews,
                SUM(CASE WHEN llm_feedback LIKE '%approved%' THEN 1 ELSE 0 END) as approved,
                SUM(CASE WHEN llm_feedback LIKE '%rejected%' OR llm_feedback LIKE '%should be rejected%' THEN 1 ELSE 0 END) as rejected,
                SUM(CASE WHEN recommended_csm != assigned_csm AND assigned_csm IS NOT NULL THEN 1 ELSE 0 END) as csm_changed
            FROM DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE
            WHERE recommendation_timestamp >= DATEADD(day, -7, CURRENT_DATE())
                AND llm_feedback IS NOT NULL
            """

            df = self.automation.execute_query(query)

            if not df.empty and df['TOTAL_REVIEWS'].iloc[0] > 0:
                row = df.iloc[0]
                approval_rate = (row['APPROVED'] / row['TOTAL_REVIEWS']) * 100
                change_rate = (row['CSM_CHANGED'] / row['TOTAL_REVIEWS']) * 100

                self.metrics['llm_effectiveness'] = {
                    'total_reviews': int(row['TOTAL_REVIEWS']),
                    'approval_rate': float(approval_rate),
                    'change_rate': float(change_rate),
                    'llm_effective': 30 <= approval_rate <= 70  # Not too high, not too low
                }

                logger.info(f"✓ LLM reviews in last 7 days: {row['TOTAL_REVIEWS']}")
                logger.info(f"✓ Approval rate: {approval_rate:.1f}%")
                logger.info(f"✓ Assignment change rate: {change_rate:.1f}%")

                if 30 <= approval_rate <= 70:
                    logger.info("✅ LLM is providing balanced oversight")
                elif approval_rate > 90:
                    logger.warning("⚠ LLM approval rate very high - may need tuning")
                elif approval_rate < 30:
                    logger.warning("⚠ LLM rejection rate very high - optimization may need improvement")

        except Exception as e:
            logger.error(f"Failed to validate LLM effectiveness: {str(e)}")

    def run_simulation_tests(self):
        """Run simulated assignments to test model behavior"""
        logger.info("\n6. RUNNING SIMULATION TESTS")
        logger.info("-" * 40)

        test_scenarios = [
            {'name': 'Single Account Test', 'count': 1},
            {'name': 'Small Batch Test', 'count': 3},
            {'name': 'Medium Batch Test', 'count': 5}
        ]

        for scenario in test_scenarios:
            logger.info(f"\nTesting: {scenario['name']}")

            try:
                # Run test
                start_time = datetime.now()
                self.automation.run(test_limit=scenario['count'])
                end_time = datetime.now()

                duration = (end_time - start_time).total_seconds()

                self.test_results.append({
                    'scenario': scenario['name'],
                    'accounts': scenario['count'],
                    'duration': duration,
                    'success': True,
                    'avg_time_per_account': duration / scenario['count']
                })

                logger.info(f"✓ Completed in {duration:.1f} seconds")
                logger.info(f"✓ Average: {duration/scenario['count']:.1f} seconds per account")

            except Exception as e:
                logger.error(f"✗ Test failed: {str(e)}")
                self.test_results.append({
                    'scenario': scenario['name'],
                    'accounts': scenario['count'],
                    'success': False,
                    'error': str(e)
                })

    def generate_validation_report(self):
        """Generate comprehensive validation report"""
        logger.info("\n" + "="*80)
        logger.info("VALIDATION REPORT SUMMARY")
        logger.info("="*80)

        # Overall score calculation
        score = 0
        max_score = 0

        # Distribution score (25 points)
        max_score += 25
        if self.metrics.get('distribution', {}).get('cv_accounts', 100) < 20:
            score += 15
        if self.metrics.get('distribution', {}).get('csms_over_85', 1) == 0:
            score += 10

        # Assignment quality score (25 points)
        max_score += 25
        quality_rate = self.metrics.get('assignment_quality', {}).get('high_neediness_match_rate', 0)
        score += min(25, quality_rate * 0.25)

        # Exclusion effectiveness (20 points)
        max_score += 20
        if self.metrics.get('exclusion', {}).get('exclusion_effective', False):
            score += 20

        # Capacity management (20 points)
        max_score += 20
        if self.metrics.get('capacity', {}).get('capacity_management_effective', False):
            score += 20

        # LLM effectiveness (10 points)
        max_score += 10
        if self.metrics.get('llm_effectiveness', {}).get('llm_effective', False):
            score += 10

        overall_score = (score / max_score) * 100

        # Print summary
        logger.info(f"\n📊 OVERALL MODEL SCORE: {overall_score:.1f}%")
        logger.info(f"   Points: {score}/{max_score}")

        # Grade the model
        if overall_score >= 90:
            grade = "A - Excellent"
            logger.info(f"\n🏆 Grade: {grade}")
            logger.info("The model is performing exceptionally well!")
        elif overall_score >= 80:
            grade = "B - Good"
            logger.info(f"\n✅ Grade: {grade}")
            logger.info("The model is performing well with minor improvements needed.")
        elif overall_score >= 70:
            grade = "C - Acceptable"
            logger.info(f"\n✓ Grade: {grade}")
            logger.info("The model is functional but has room for improvement.")
        else:
            grade = "D - Needs Improvement"
            logger.info(f"\n⚠ Grade: {grade}")
            logger.info("The model requires significant improvements.")

        # Key metrics summary
        logger.info("\n📈 KEY PERFORMANCE INDICATORS:")
        logger.info("-" * 40)

        if 'distribution' in self.metrics:
            logger.info(f"• Workload balance (CV): {self.metrics['distribution'].get('cv_accounts', 'N/A'):.1f}%")
            logger.info(f"• CSMs over capacity: {self.metrics['distribution'].get('csms_over_85', 'N/A')}")

        if 'assignment_quality' in self.metrics:
            logger.info(f"• Assignment quality: {self.metrics['assignment_quality'].get('high_neediness_match_rate', 'N/A'):.1f}%")

        if 'exclusion' in self.metrics:
            logger.info(f"• Max assignments/CSM (7d): {self.metrics['exclusion'].get('max_assignments_7d', 'N/A')}")

        if 'llm_effectiveness' in self.metrics:
            logger.info(f"• LLM approval rate: {self.metrics['llm_effectiveness'].get('approval_rate', 'N/A'):.1f}%")

        # Test results
        if self.test_results:
            logger.info("\n🧪 SIMULATION TEST RESULTS:")
            logger.info("-" * 40)
            for test in self.test_results:
                if test['success']:
                    logger.info(f"✓ {test['scenario']}: {test['avg_time_per_account']:.1f}s per account")
                else:
                    logger.info(f"✗ {test['scenario']}: Failed")

        # Save detailed report
        report = {
            'timestamp': datetime.now().isoformat(),
            'overall_score': overall_score,
            'grade': grade,
            'metrics': self.metrics,
            'test_results': self.test_results
        }

        report_file = f'validation_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)

        logger.info(f"\n📁 Detailed report saved to: {report_file}")

        # Recommendations
        logger.info("\n💡 RECOMMENDATIONS:")
        logger.info("-" * 40)

        if self.metrics.get('distribution', {}).get('cv_accounts', 0) > 20:
            logger.info("• Consider rebalancing accounts to reduce variation")

        if self.metrics.get('distribution', {}).get('csms_over_85', 0) > 0:
            logger.info("• Address CSMs over capacity immediately")

        if self.metrics.get('assignment_quality', {}).get('high_neediness_match_rate', 100) < 70:
            logger.info("• Improve matching of high-neediness accounts to experienced CSMs")

        if not self.metrics.get('exclusion', {}).get('exclusion_effective', True):
            logger.info("• Review exclusion logic to prevent assignment clustering")

        return overall_score

def main():
    """Main entry point"""
    logger.info(f"Starting validation at: {datetime.now()}")

    validator = CSMModelValidator()
    score = validator.run_validation_suite()

    logger.info(f"\nValidation completed at: {datetime.now()}")

    # Return exit code based on score
    if score >= 70:
        logger.info("\n✅ Model validation PASSED")
        return 0
    else:
        logger.error("\n❌ Model validation FAILED")
        return 1

if __name__ == "__main__":
    sys.exit(main())