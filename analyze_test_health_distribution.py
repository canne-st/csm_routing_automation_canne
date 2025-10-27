#!/usr/bin/env python3
"""
Analyze health segment distribution from test logs
"""

import re
from collections import defaultdict
import sys

def analyze_health_distribution(log_file):
    """Parse log file and extract health segment distribution"""

    # Track assignments by CSM
    csm_assignments = defaultdict(lambda: {'total': 0, 'Red': 0, 'Yellow': 0, 'Green': 0})

    # Track overall distribution
    total_assignments = {'Red': 0, 'Yellow': 0, 'Green': 0}

    # Pattern to match assignment lines
    pattern = r"Assigned account (\w+) \(health: (Red|Yellow|Green)\) to ([\w\s]+) \(score:"

    try:
        with open(log_file, 'r') as f:
            for line in f:
                match = re.search(pattern, line)
                if match:
                    account_id = match.group(1)
                    health = match.group(2)
                    csm = match.group(3)

                    # Update CSM specific stats
                    csm_assignments[csm]['total'] += 1
                    csm_assignments[csm][health] += 1

                    # Update overall stats
                    total_assignments[health] += 1

    except FileNotFoundError:
        print(f"Error: File {log_file} not found")
        return None, None

    return csm_assignments, total_assignments

def print_analysis(csm_assignments, total_assignments, test_name):
    """Print formatted analysis"""

    print(f"\n{'='*80}")
    print(f"HEALTH SEGMENT DISTRIBUTION ANALYSIS - {test_name}")
    print(f"{'='*80}")

    # Overall distribution
    total = sum(total_assignments.values())
    if total > 0:
        print("\n📊 OVERALL DISTRIBUTION OF NEW ASSIGNMENTS:")
        print(f"  Total accounts assigned: {total}")
        print(f"  🔴 Red:    {total_assignments['Red']:3} accounts ({100*total_assignments['Red']/total:5.1f}%)")
        print(f"  🟡 Yellow: {total_assignments['Yellow']:3} accounts ({100*total_assignments['Yellow']/total:5.1f}%)")
        print(f"  🟢 Green:  {total_assignments['Green']:3} accounts ({100*total_assignments['Green']/total:5.1f}%)")

    # Per CSM distribution
    print(f"\n📈 PER CSM HEALTH SEGMENT BREAKDOWN:")
    print(f"{'CSM Name':<25} {'Total':<8} {'Red':<8} {'Yellow':<10} {'Green':<8} {'Red %':<8}")
    print("-" * 75)

    # Sort CSMs by total assignments
    sorted_csms = sorted(csm_assignments.items(), key=lambda x: x[1]['total'], reverse=True)

    for csm, stats in sorted_csms:
        if stats['total'] > 0:
            red_pct = 100 * stats['Red'] / stats['total']
            print(f"{csm:<25} {stats['total']:<8} {stats['Red']:<8} {stats['Yellow']:<10} {stats['Green']:<8} {red_pct:<8.1f}")

    # Highlight any concerning patterns
    print("\n⚠️  KEY OBSERVATIONS:")

    # Check for CSMs with high red account percentage
    high_red_csms = []
    for csm, stats in csm_assignments.items():
        if stats['total'] > 0 and stats['Red'] / stats['total'] > 0.2:
            high_red_csms.append((csm, stats['Red'], stats['total']))

    if high_red_csms:
        print("  - CSMs with >20% Red accounts:")
        for csm, red, total in high_red_csms:
            print(f"    • {csm}: {red}/{total} ({100*red/total:.0f}%) Red accounts")

    # Check for CSMs with multiple assignments
    multi_assignment_csms = [(csm, stats['total']) for csm, stats in csm_assignments.items() if stats['total'] >= 3]
    if multi_assignment_csms:
        print(f"\n  - CSMs with 3+ assignments in this test:")
        for csm, count in sorted(multi_assignment_csms, key=lambda x: x[1], reverse=True):
            print(f"    • {csm}: {count} accounts")

def main():
    print("\n" + "="*80)
    print("CSM ROUTING TEST - HEALTH SEGMENT DISTRIBUTION ANALYSIS")
    print("="*80)

    # Analyze first test
    log_file1 = "test_50_with_new_limits.log"
    csm1, total1 = analyze_health_distribution(log_file1)
    if csm1 and total1:
        print_analysis(csm1, total1, "Test 1 (Completed)")

    # Analyze second test
    log_file2 = "test_50_second_run.log"
    csm2, total2 = analyze_health_distribution(log_file2)
    if csm2 and total2:
        print_analysis(csm2, total2, "Test 2 (May be in progress)")

    # Compare if both available
    if total1 and total2:
        print("\n" + "="*80)
        print("📊 COMPARISON BETWEEN TESTS")
        print("="*80)

        total_1 = sum(total1.values())
        total_2 = sum(total2.values())

        if total_1 > 0 and total_2 > 0:
            print(f"\n{'Metric':<20} {'Test 1':<20} {'Test 2':<20}")
            print("-" * 60)
            print(f"{'Total Assigned':<20} {total_1:<20} {total_2:<20}")
            print(f"{'Red %':<20} {100*total1['Red']/total_1:<20.1f} {100*total2['Red']/total_2:<20.1f}")
            print(f"{'Yellow %':<20} {100*total1['Yellow']/total_1:<20.1f} {100*total2['Yellow']/total_2:<20.1f}")
            print(f"{'Green %':<20} {100*total1['Green']/total_1:<20.1f} {100*total2['Green']/total_2:<20.1f}")
            print(f"{'Unique CSMs':<20} {len([c for c in csm1 if csm1[c]['total']>0]):<20} {len([c for c in csm2 if csm2[c]['total']>0]):<20}")

if __name__ == "__main__":
    main()