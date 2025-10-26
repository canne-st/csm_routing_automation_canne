#!/usr/bin/env python3
"""
Test 50 accounts with before/after comparison
Shows how the model maintains balanced distribution
"""

import pandas as pd
import numpy as np
from datetime import datetime
import json
import time
from tabulate import tabulate
from csm_routing_automation import CSMRoutingAutomation

class ModelImpactTest:
    """Test 50 accounts and measure impact on distribution"""

    def __init__(self):
        self.automation = CSMRoutingAutomation()
        self.before_state = {}
        self.after_state = {}
        self.test_results = []

    def capture_current_state(self):
        """Capture current CSM workload and health distribution"""
        print("\n" + "="*80)
        print("CAPTURING BASELINE METRICS (BEFORE STATE)")
        print("="*80)

        # Query current state
        query = """
        WITH csm_current_state AS (
            SELECT
                h.responsible_csm as csm_name,
                COUNT(*) as account_count,
                AVG(h.neediness_score) as avg_neediness,
                SUM(h.revenue) as total_revenue,
                SUM(h.tad_score) as total_tad,

                -- Health distribution
                SUM(CASE WHEN h.core_health_score_color = 'Red' THEN 1 ELSE 0 END) as red_accounts,
                SUM(CASE WHEN h.core_health_score_color = 'Yellow' THEN 1 ELSE 0 END) as yellow_accounts,
                SUM(CASE WHEN h.core_health_score_color = 'Green' THEN 1 ELSE 0 END) as green_accounts,

                -- Percentage distribution
                ROUND(100.0 * SUM(CASE WHEN h.core_health_score_color = 'Red' THEN 1 ELSE 0 END) / COUNT(*), 1) as red_pct,
                ROUND(100.0 * SUM(CASE WHEN h.core_health_score_color = 'Yellow' THEN 1 ELSE 0 END) / COUNT(*), 1) as yellow_pct,
                ROUND(100.0 * SUM(CASE WHEN h.core_health_score_color = 'Green' THEN 1 ELSE 0 END) / COUNT(*), 1) as green_pct

            FROM DSV_WAREHOUSE.DATA_SCIENCE.data_csm_routing_account_health_revenue_tad_neediness h
            WHERE h.responsible_csm IN (
                SELECT active_csm
                FROM DSV_WAREHOUSE.DATA_SCIENCE.resi_corp_active_csms
            )
            GROUP BY h.responsible_csm
        ),
        overall_stats AS (
            SELECT
                COUNT(*) as total_csms,
                AVG(account_count) as avg_accounts,
                STDDEV(account_count) as std_accounts,
                MIN(account_count) as min_accounts,
                MAX(account_count) as max_accounts,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY account_count) as median_accounts
            FROM csm_current_state
        )
        SELECT * FROM overall_stats
        CROSS JOIN (
            SELECT
                SUM(red_accounts) as total_red,
                SUM(yellow_accounts) as total_yellow,
                SUM(green_accounts) as total_green
            FROM csm_current_state
        )
        """

        df = self.automation.execute_query(query)

        if not df.empty:
            row = df.iloc[0]
            self.before_state = {
                'total_csms': int(row['TOTAL_CSMS']),
                'avg_accounts': float(row['AVG_ACCOUNTS']),
                'std_accounts': float(row['STD_ACCOUNTS']),
                'cv': float(row['STD_ACCOUNTS'] / row['AVG_ACCOUNTS'] * 100),
                'min_accounts': int(row['MIN_ACCOUNTS']),
                'max_accounts': int(row['MAX_ACCOUNTS']),
                'median_accounts': float(row['MEDIAN_ACCOUNTS']),
                'total_red': int(row['TOTAL_RED']),
                'total_yellow': int(row['TOTAL_YELLOW']),
                'total_green': int(row['TOTAL_GREEN']),
                'health_distribution': {
                    'red_pct': round(row['TOTAL_RED'] / (row['TOTAL_RED'] + row['TOTAL_YELLOW'] + row['TOTAL_GREEN']) * 100, 1),
                    'yellow_pct': round(row['TOTAL_YELLOW'] / (row['TOTAL_RED'] + row['TOTAL_YELLOW'] + row['TOTAL_GREEN']) * 100, 1),
                    'green_pct': round(row['TOTAL_GREEN'] / (row['TOTAL_RED'] + row['TOTAL_YELLOW'] + row['TOTAL_GREEN']) * 100, 1)
                }
            }

            # Get per-CSM details for top 10 most loaded
            detail_query = """
            SELECT
                responsible_csm as csm_name,
                COUNT(*) as account_count,
                AVG(neediness_score) as avg_neediness,
                SUM(CASE WHEN core_health_score_color = 'Red' THEN 1 ELSE 0 END) as red,
                SUM(CASE WHEN core_health_score_color = 'Yellow' THEN 1 ELSE 0 END) as yellow,
                SUM(CASE WHEN core_health_score_color = 'Green' THEN 1 ELSE 0 END) as green
            FROM DSV_WAREHOUSE.DATA_SCIENCE.data_csm_routing_account_health_revenue_tad_neediness
            WHERE responsible_csm IN (SELECT active_csm FROM DSV_WAREHOUSE.DATA_SCIENCE.resi_corp_active_csms)
            GROUP BY responsible_csm
            ORDER BY account_count DESC
            LIMIT 10
            """

            detail_df = self.automation.execute_query(detail_query)
            self.before_state['top_csms'] = detail_df.to_dict('records')

            # Print baseline metrics
            print("\n📊 BASELINE METRICS:")
            print("-" * 40)
            print(f"Total Active CSMs: {self.before_state['total_csms']}")
            print(f"Average accounts per CSM: {self.before_state['avg_accounts']:.1f}")
            print(f"Standard deviation: {self.before_state['std_accounts']:.1f}")
            print(f"Coefficient of Variation: {self.before_state['cv']:.1f}%")
            print(f"Range: {self.before_state['min_accounts']} - {self.before_state['max_accounts']}")
            print(f"Median: {self.before_state['median_accounts']:.1f}")

            print("\n🎨 HEALTH DISTRIBUTION:")
            print(f"Red accounts: {self.before_state['total_red']} ({self.before_state['health_distribution']['red_pct']:.1f}%)")
            print(f"Yellow accounts: {self.before_state['total_yellow']} ({self.before_state['health_distribution']['yellow_pct']:.1f}%)")
            print(f"Green accounts: {self.before_state['total_green']} ({self.before_state['health_distribution']['green_pct']:.1f}%)")

            print("\n👥 TOP 10 MOST LOADED CSMs (BEFORE):")
            headers = ['CSM', 'Accounts', 'Avg Need', 'Red', 'Yellow', 'Green']
            table_data = []
            for csm in self.before_state['top_csms']:
                table_data.append([
                    csm['CSM_NAME'][:20],
                    int(csm['ACCOUNT_COUNT']),
                    f"{csm['AVG_NEEDINESS']:.1f}",
                    int(csm['RED']),
                    int(csm['YELLOW']),
                    int(csm['GREEN'])
                ])
            print(tabulate(table_data, headers=headers, tablefmt='simple'))

            # Save to file
            with open('before_state.json', 'w') as f:
                json.dump(self.before_state, f, indent=2, default=str)
            print("\n💾 Saved baseline to before_state.json")

    def run_50_account_test(self):
        """Run test with 50 accounts: 3 batches of 10 + 20 individual"""
        print("\n" + "="*80)
        print("RUNNING 50 ACCOUNT TEST")
        print("="*80)

        total_start = datetime.now()

        # Part 1: Run 3 batches of 10 accounts each
        print("\n📦 PART 1: BATCH PROCESSING (3 × 10 accounts)")
        print("-" * 40)

        for batch_num in range(1, 4):
            print(f"\n🔄 Batch {batch_num}/3: Processing 10 accounts...")
            batch_start = datetime.now()

            try:
                # Run batch of 10
                self.automation.run(test_limit=10)

                batch_duration = (datetime.now() - batch_start).total_seconds()
                print(f"✅ Batch {batch_num} completed in {batch_duration:.1f} seconds")

                self.test_results.append({
                    'type': 'batch',
                    'batch_num': batch_num,
                    'accounts': 10,
                    'duration': batch_duration,
                    'success': True
                })

                # Small delay between batches
                if batch_num < 3:
                    print("⏳ Waiting 5 seconds before next batch...")
                    time.sleep(5)

            except Exception as e:
                print(f"❌ Batch {batch_num} failed: {str(e)}")
                self.test_results.append({
                    'type': 'batch',
                    'batch_num': batch_num,
                    'accounts': 10,
                    'success': False,
                    'error': str(e)
                })

        # Part 2: Run 20 individual accounts
        print("\n📝 PART 2: INDIVIDUAL PROCESSING (20 × 1 account)")
        print("-" * 40)

        individual_successes = 0
        individual_failures = 0

        for i in range(1, 21):
            print(f"\r⏳ Processing individual account {i}/20...", end='', flush=True)

            try:
                individual_start = datetime.now()
                self.automation.run(test_limit=1)
                individual_duration = (datetime.now() - individual_start).total_seconds()

                individual_successes += 1
                self.test_results.append({
                    'type': 'individual',
                    'account_num': i,
                    'duration': individual_duration,
                    'success': True
                })

                # Very short delay to avoid overwhelming
                time.sleep(0.5)

            except Exception as e:
                individual_failures += 1
                self.test_results.append({
                    'type': 'individual',
                    'account_num': i,
                    'success': False,
                    'error': str(e)
                })

        print(f"\n✅ Individual processing complete: {individual_successes} succeeded, {individual_failures} failed")

        total_duration = (datetime.now() - total_start).total_seconds()
        print(f"\n⏱️ Total test duration: {total_duration:.1f} seconds ({total_duration/60:.1f} minutes)")

    def capture_after_state(self):
        """Capture state after 50 assignments"""
        print("\n" + "="*80)
        print("CAPTURING POST-TEST METRICS (AFTER STATE)")
        print("="*80)

        # Use same query as before
        query = """
        WITH csm_current_state AS (
            SELECT
                h.responsible_csm as csm_name,
                COUNT(*) as account_count,
                AVG(h.neediness_score) as avg_neediness,
                SUM(h.revenue) as total_revenue,
                SUM(h.tad_score) as total_tad,

                -- Health distribution
                SUM(CASE WHEN h.core_health_score_color = 'Red' THEN 1 ELSE 0 END) as red_accounts,
                SUM(CASE WHEN h.core_health_score_color = 'Yellow' THEN 1 ELSE 0 END) as yellow_accounts,
                SUM(CASE WHEN h.core_health_score_color = 'Green' THEN 1 ELSE 0 END) as green_accounts,

                -- Percentage distribution
                ROUND(100.0 * SUM(CASE WHEN h.core_health_score_color = 'Red' THEN 1 ELSE 0 END) / COUNT(*), 1) as red_pct,
                ROUND(100.0 * SUM(CASE WHEN h.core_health_score_color = 'Yellow' THEN 1 ELSE 0 END) / COUNT(*), 1) as yellow_pct,
                ROUND(100.0 * SUM(CASE WHEN h.core_health_score_color = 'Green' THEN 1 ELSE 0 END) / COUNT(*), 1) as green_pct

            FROM DSV_WAREHOUSE.DATA_SCIENCE.data_csm_routing_account_health_revenue_tad_neediness h
            WHERE h.responsible_csm IN (
                SELECT active_csm
                FROM DSV_WAREHOUSE.DATA_SCIENCE.resi_corp_active_csms
            )
            GROUP BY h.responsible_csm
        ),
        overall_stats AS (
            SELECT
                COUNT(*) as total_csms,
                AVG(account_count) as avg_accounts,
                STDDEV(account_count) as std_accounts,
                MIN(account_count) as min_accounts,
                MAX(account_count) as max_accounts,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY account_count) as median_accounts
            FROM csm_current_state
        )
        SELECT * FROM overall_stats
        CROSS JOIN (
            SELECT
                SUM(red_accounts) as total_red,
                SUM(yellow_accounts) as total_yellow,
                SUM(green_accounts) as total_green
            FROM csm_current_state
        )
        """

        df = self.automation.execute_query(query)

        if not df.empty:
            row = df.iloc[0]
            self.after_state = {
                'total_csms': int(row['TOTAL_CSMS']),
                'avg_accounts': float(row['AVG_ACCOUNTS']),
                'std_accounts': float(row['STD_ACCOUNTS']),
                'cv': float(row['STD_ACCOUNTS'] / row['AVG_ACCOUNTS'] * 100),
                'min_accounts': int(row['MIN_ACCOUNTS']),
                'max_accounts': int(row['MAX_ACCOUNTS']),
                'median_accounts': float(row['MEDIAN_ACCOUNTS']),
                'total_red': int(row['TOTAL_RED']),
                'total_yellow': int(row['TOTAL_YELLOW']),
                'total_green': int(row['TOTAL_GREEN']),
                'health_distribution': {
                    'red_pct': round(row['TOTAL_RED'] / (row['TOTAL_RED'] + row['TOTAL_YELLOW'] + row['TOTAL_GREEN']) * 100, 1),
                    'yellow_pct': round(row['TOTAL_YELLOW'] / (row['TOTAL_RED'] + row['TOTAL_YELLOW'] + row['TOTAL_GREEN']) * 100, 1),
                    'green_pct': round(row['TOTAL_GREEN'] / (row['TOTAL_RED'] + row['TOTAL_YELLOW'] + row['TOTAL_GREEN']) * 100, 1)
                }
            }

            # Get per-CSM details for top 10 most loaded
            detail_query = """
            SELECT
                responsible_csm as csm_name,
                COUNT(*) as account_count,
                AVG(neediness_score) as avg_neediness,
                SUM(CASE WHEN core_health_score_color = 'Red' THEN 1 ELSE 0 END) as red,
                SUM(CASE WHEN core_health_score_color = 'Yellow' THEN 1 ELSE 0 END) as yellow,
                SUM(CASE WHEN core_health_score_color = 'Green' THEN 1 ELSE 0 END) as green
            FROM DSV_WAREHOUSE.DATA_SCIENCE.data_csm_routing_account_health_revenue_tad_neediness
            WHERE responsible_csm IN (SELECT active_csm FROM DSV_WAREHOUSE.DATA_SCIENCE.resi_corp_active_csms)
            GROUP BY responsible_csm
            ORDER BY account_count DESC
            LIMIT 10
            """

            detail_df = self.automation.execute_query(detail_query)
            self.after_state['top_csms'] = detail_df.to_dict('records')

            # Save to file
            with open('after_state.json', 'w') as f:
                json.dump(self.after_state, f, indent=2, default=str)
            print("💾 Saved after state to after_state.json")

    def compare_states(self):
        """Compare before and after states"""
        print("\n" + "="*80)
        print("IMPACT ANALYSIS: BEFORE vs AFTER 50 ASSIGNMENTS")
        print("="*80)

        if not self.before_state or not self.after_state:
            print("❌ Missing before/after state data")
            return

        # Calculate changes
        changes = {
            'avg_accounts_change': self.after_state['avg_accounts'] - self.before_state['avg_accounts'],
            'std_change': self.after_state['std_accounts'] - self.before_state['std_accounts'],
            'cv_change': self.after_state['cv'] - self.before_state['cv'],
            'max_change': self.after_state['max_accounts'] - self.before_state['max_accounts'],
            'min_change': self.after_state['min_accounts'] - self.before_state['min_accounts'],
        }

        print("\n📊 WORKLOAD DISTRIBUTION CHANGES:")
        print("-" * 40)

        comparison = [
            ['Metric', 'Before', 'After', 'Change'],
            ['Avg Accounts/CSM', f"{self.before_state['avg_accounts']:.1f}", f"{self.after_state['avg_accounts']:.1f}", f"{changes['avg_accounts_change']:+.1f}"],
            ['Std Deviation', f"{self.before_state['std_accounts']:.1f}", f"{self.after_state['std_accounts']:.1f}", f"{changes['std_change']:+.1f}"],
            ['CV %', f"{self.before_state['cv']:.1f}%", f"{self.after_state['cv']:.1f}%", f"{changes['cv_change']:+.1f}%"],
            ['Max Accounts', self.before_state['max_accounts'], self.after_state['max_accounts'], f"{changes['max_change']:+d}"],
            ['Min Accounts', self.before_state['min_accounts'], self.after_state['min_accounts'], f"{changes['min_change']:+d}"],
        ]

        print(tabulate(comparison, headers='firstrow', tablefmt='grid'))

        print("\n🎨 HEALTH DISTRIBUTION CHANGES:")
        print("-" * 40)

        health_comparison = [
            ['Health', 'Before Count', 'After Count', 'Change', 'Before %', 'After %'],
            ['Red', self.before_state['total_red'], self.after_state['total_red'],
             f"{self.after_state['total_red'] - self.before_state['total_red']:+d}",
             f"{self.before_state['health_distribution']['red_pct']:.1f}%",
             f"{self.after_state['health_distribution']['red_pct']:.1f}%"],
            ['Yellow', self.before_state['total_yellow'], self.after_state['total_yellow'],
             f"{self.after_state['total_yellow'] - self.before_state['total_yellow']:+d}",
             f"{self.before_state['health_distribution']['yellow_pct']:.1f}%",
             f"{self.after_state['health_distribution']['yellow_pct']:.1f}%"],
            ['Green', self.before_state['total_green'], self.after_state['total_green'],
             f"{self.after_state['total_green'] - self.before_state['total_green']:+d}",
             f"{self.before_state['health_distribution']['green_pct']:.1f}%",
             f"{self.after_state['health_distribution']['green_pct']:.1f}%"],
        ]

        print(tabulate(health_comparison, headers='firstrow', tablefmt='grid'))

        # Check which CSMs got assignments
        print("\n🎯 ASSIGNMENT DISTRIBUTION:")
        print("-" * 40)

        query = """
        SELECT
            recommended_csm,
            COUNT(*) as assignments_received
        FROM DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE
        WHERE recommendation_timestamp >= DATEADD(hour, -2, CURRENT_TIMESTAMP())
        GROUP BY recommended_csm
        ORDER BY assignments_received DESC
        """

        assignment_df = self.automation.execute_query(query)

        if not assignment_df.empty:
            print(f"Unique CSMs who received assignments: {len(assignment_df)}")
            print("\nTop recipients:")
            for _, row in assignment_df.head(10).iterrows():
                print(f"  • {row['RECOMMENDED_CSM']}: {int(row['ASSIGNMENTS_RECEIVED'])} assignments")

        # Performance summary
        print("\n📈 MODEL PERFORMANCE ASSESSMENT:")
        print("-" * 40)

        # CV is key metric for balance
        if abs(changes['cv_change']) < 2:
            print("✅ EXCELLENT: Workload balance maintained (CV change < 2%)")
        elif abs(changes['cv_change']) < 5:
            print("✅ GOOD: Workload balance slightly affected (CV change < 5%)")
        else:
            print("⚠️  WARNING: Significant change in workload balance")

        # Check if max increased significantly
        if changes['max_change'] <= 3:
            print("✅ EXCELLENT: No CSM overloaded (max increase ≤ 3)")
        elif changes['max_change'] <= 5:
            print("✅ GOOD: Controlled max increase")
        else:
            print("⚠️  WARNING: Some CSMs may be overloaded")

        # Test results summary
        batch_success = sum(1 for r in self.test_results if r['type'] == 'batch' and r['success'])
        individual_success = sum(1 for r in self.test_results if r['type'] == 'individual' and r['success'])

        print("\n🧪 TEST EXECUTION SUMMARY:")
        print(f"  • Batch tests: {batch_success}/3 successful")
        print(f"  • Individual tests: {individual_success}/20 successful")

        # Save full comparison
        comparison_report = {
            'timestamp': datetime.now().isoformat(),
            'before_state': self.before_state,
            'after_state': self.after_state,
            'changes': changes,
            'test_results': self.test_results
        }

        report_file = f'comparison_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
        with open(report_file, 'w') as f:
            json.dump(comparison_report, f, indent=2, default=str)

        print(f"\n📁 Full report saved to: {report_file}")

        return abs(changes['cv_change']) < 5  # Return True if balance maintained

def main():
    """Run the 50 account test with comparison"""
    print("🚀 CSM ROUTING MODEL - 50 ACCOUNT IMPACT TEST")
    print("="*80)

    tester = ModelImpactTest()

    try:
        # Step 1: Capture baseline
        tester.capture_current_state()

        # Step 2: Run 50 account test
        tester.run_50_account_test()

        # Step 3: Capture after state
        tester.capture_after_state()

        # Step 4: Compare
        success = tester.compare_states()

        print("\n" + "="*80)
        if success:
            print("✅ TEST PASSED: Model maintains good distribution!")
        else:
            print("⚠️  TEST COMPLETED: Review metrics for improvements")
        print("="*80)

        return 0 if success else 1

    except Exception as e:
        print(f"\n❌ Test failed: {str(e)}")
        return 1

if __name__ == "__main__":
    import sys
    sys.exit(main())