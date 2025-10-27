#!/usr/bin/env python
# coding: utf-8

"""
CSM Routing Automation Script
Automatically assigns CSMs to accounts with 'Needs CSM' status
Uses PuLP optimization for batch assignments and smart best-fit for single accounts
"""

import pandas as pd
import numpy as np
import pulp
import json
from datetime import datetime, timedelta
import logging
import snowflake.connector
from sqlalchemy import create_engine
import io
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import load_pem_private_key
import time
from typing import Dict, List, Tuple, Optional
import copy
import anthropic

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('csm_routing_automation.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def convert_numpy_types(obj):
    """Convert numpy types and Decimal to Python types for JSON serialization"""
    from decimal import Decimal

    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, dict):
        return {k: convert_numpy_types(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_numpy_types(item) for item in obj]
    elif isinstance(obj, pd.Series):
        return convert_numpy_types(obj.to_dict())
    elif isinstance(obj, pd.DataFrame):
        return convert_numpy_types(obj.to_dict('records'))
    else:
        return obj

class CSMRoutingAutomation:
    """Main class for CSM routing automation"""

    def __init__(self, config_file='properties.json', limits_file='csm_category_limits.json'):
        """Initialize the automation with configuration"""
        self.config = self.load_config(config_file)
        self.limits = self.load_config(limits_file)
        self.snowflake_conn = None
        self.eligible_csm_list = []  # Will be populated from database
        self.assignment_history = []  # Track assignments in this session

        # Output table for storing recommendations - using _CANNE suffix
        self.recommendations_table = 'DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE'
        self.assignments_table = 'DSV_WAREHOUSE.DATA_SCIENCE.ACCOUNT_CSM_ASSIGNMENTS_CANNE'

        # Initialize Claude client if API key is available
        if 'ANTHROPIC_API_KEY' in self.config:
            self.claude_client = anthropic.Anthropic(api_key=self.config['ANTHROPIC_API_KEY'])
        else:
            self.claude_client = None
            logger.warning("No Anthropic API key found in config - LLM review will be skipped")

        # Session cache for neediness data - will be populated on first use
        self.neediness_cache = None
        self.cache_timestamp = None

    def populate_neediness_cache(self):
        """
        Run the neediness query ONCE for ALL accounts and cache results.
        This is called on-demand when enrichment is needed.
        """
        if self.neediness_cache is not None:
            logger.info("Neediness cache already populated, skipping query")
            return True

        logger.info("Populating neediness cache by running main neediness query...")

        try:
            # Get the main neediness query from file - MUST exist
            query = self.get_neediness_query_template()

            logger.info("Using main neediness query to populate cache")
            start_time = datetime.now()

            # Execute the query
            self.neediness_cache = self.execute_query(query)

            if self.neediness_cache.empty:
                logger.warning("Neediness query returned no data, using empty cache")
                self.neediness_cache = pd.DataFrame()
                return False

            # Standardize column names
            self.neediness_cache.columns = [
                col.lower().replace(' ', '_').replace('-', '_')
                for col in self.neediness_cache.columns
            ]

            # Ensure account_id is string
            if 'account_id' in self.neediness_cache.columns:
                self.neediness_cache['account_id'] = self.neediness_cache['account_id'].astype(str)

            elapsed = (datetime.now() - start_time).total_seconds()
            logger.info(f"Cache populated with {len(self.neediness_cache)} accounts in {elapsed:.2f} seconds")

            # Show statistics
            if 'neediness_score' in self.neediness_cache.columns:
                logger.info(f"Neediness distribution: {self.neediness_cache['neediness_score'].value_counts().to_dict()}")
            if 'health_segment' in self.neediness_cache.columns:
                logger.info(f"Health distribution: {self.neediness_cache['health_segment'].value_counts().to_dict()}")

            self.cache_timestamp = datetime.now()

            # Optionally save to CSV for debugging/backup
            try:
                cache_file = f"neediness_cache_session_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                self.neediness_cache.to_csv(cache_file, index=False)
                logger.info(f"Saved cache to {cache_file} for reference")
            except:
                pass  # Optional save, don't fail if it doesn't work

            return True

        except Exception as e:
            logger.error(f"Failed to populate neediness cache: {str(e)}")
            self.neediness_cache = pd.DataFrame()  # Use empty DataFrame as fallback
            return False

    def load_config(self, filepath):
        """Load configuration from JSON file"""
        with open(filepath) as file:
            return json.load(file)

    def private_key_deserializer(self, private_key_str):
        """Deserialize private key for Snowflake connection"""
        key_file = io.StringIO(private_key_str)
        private_key = serialization.load_pem_private_key(
            key_file.read().encode(),
            password=None,
        )
        return private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )

    def connect_snowflake(self):
        """Establish Snowflake connection"""
        try:
            private_key = self.private_key_deserializer(
                self.config["SNOWFLAKE_PRIVATE_KEY"].replace('\\n', '\n')
            )

            self.snowflake_conn = snowflake.connector.connect(
                user=self.config["SNOWFLAKE_USER"],
                private_key=private_key,
                account=self.config["snowflake_account_prod"],
                warehouse=self.config["snowflake_warehouse"],
                database=self.config["snowflake_database"],
                schema=self.config["snowflake_schema"],
                role=self.config["snowflake_role"]
            )
            logger.info("Connected to Snowflake successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {str(e)}")
            return False

    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute Snowflake query and return results as DataFrame"""
        try:
            cursor = self.snowflake_conn.cursor()
            cursor.execute(query)
            df = cursor.fetch_pandas_all()
            cursor.close()
            return df
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            return pd.DataFrame()

    def get_needs_csm_accounts(self, limit=None) -> pd.DataFrame:
        """Fetch accounts that need CSM assignment"""
        limit_clause = f"LIMIT {limit}" if limit else ""
        query = f"""
        SELECT
            account_id_ob as account_id,
            tenant_id,
            success_transition_status_ob
        FROM DSV_SHARE.PUBLIC.VW_ONBOARDING_DETAIL
        WHERE success_transition_status_ob = 'Needs CSM'
            AND account_id_ob IS NOT NULL
            and ob_account_level_ce = 'Corporate'
            and ob_team_segment = 'Residential'
            AND onboarding_status_ob in ('Success', 'Onboarding', 'Live')
            -- Exclude accounts already in recommendations table
            AND account_id_ob NOT IN (
                SELECT DISTINCT account_id
                FROM DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE
                WHERE account_id IS NOT NULL
            )
            -- Exclude accounts already in assignments table
            AND account_id_ob NOT IN (
                SELECT DISTINCT account_id
                FROM DSV_WAREHOUSE.DATA_SCIENCE.ACCOUNT_CSM_ASSIGNMENTS_CANNE
                WHERE account_id IS NOT NULL
            )
        {limit_clause}
        """

        df = self.execute_query(query)
        logger.info(f"Found {len(df)} accounts needing CSM assignment")

        # Rename columns to lowercase for consistency
        df.columns = [col.lower() for col in df.columns]
        return df

    def enrich_account_data(self, accounts_df: pd.DataFrame) -> pd.DataFrame:
        """Enrich account data from cached neediness scoring query results"""
        if accounts_df.empty:
            return accounts_df

        # Now columns should be lowercase after standardization
        account_ids_list = accounts_df['account_id'].astype(str).tolist()

        # Populate cache if not already done (runs main query ONCE for ALL accounts)
        if self.neediness_cache is None:
            logger.info("First enrichment request - populating neediness cache...")
            if not self.populate_neediness_cache():
                logger.error("Failed to populate neediness cache")
                # Return accounts with default values if cache population fails
                enriched = accounts_df.copy()
                self._fill_missing_enrichment_data(enriched)
                return enriched

        # Use cached data to filter for requested accounts
        logger.info(f"Using cached neediness data for {len(account_ids_list)} accounts")

        # Filter cache for the requested accounts
        enriched_data = self.neediness_cache[
            self.neediness_cache['account_id'].isin(account_ids_list)
        ].copy()

        if not enriched_data.empty:
            logger.info(f"Found {len(enriched_data)} accounts in cache")

            # Merge with original accounts_df to maintain all accounts
            enriched = accounts_df.merge(enriched_data, on='account_id', how='left')

            # Show sample of enriched data
            if len(enriched) > 0 and 'neediness_score' in enriched.columns:
                sample = enriched.iloc[0]
                logger.info(f"Sample enriched account - Neediness: {sample.get('neediness_score')}, "
                          f"Health: {sample.get('health_score')}, Revenue: {sample.get('revenue', 'N/A')}")

            # Fill any missing accounts with defaults
            self._fill_missing_enrichment_data(enriched)

            # Deduplicate
            original_count = len(enriched)
            enriched = enriched.drop_duplicates(subset=['account_id'], keep='first')
            if original_count > len(enriched):
                logger.info(f"Removed {original_count - len(enriched)} duplicate records")

            logger.info(f"Successfully enriched {len(enriched)} accounts from cache")
            return enriched

        else:
            logger.warning(f"No accounts found in cache for IDs: {account_ids_list[:3]}...")
            # Return accounts with default values
            enriched = accounts_df.copy()
            self._fill_missing_enrichment_data(enriched)
            return enriched

    def _fill_missing_enrichment_data(self, df: pd.DataFrame):
        """Fill missing values in enrichment data with defaults"""
        if 'neediness_score' in df.columns:
            df['neediness_score'] = df['neediness_score'].fillna(5)
        else:
            df['neediness_score'] = 5

        if 'tad_score' in df.columns:
            df['tad_score'] = df['tad_score'].fillna(0)
        else:
            df['tad_score'] = 0

        if 'health_score' in df.columns:
            df['health_score'] = df['health_score'].fillna(70)
        else:
            df['health_score'] = 70

        # Map total_mrr from neediness query to revenue field
        if 'total_mrr' in df.columns:
            df['revenue'] = df['total_mrr'].fillna(100000)
        elif 'revenue' in df.columns:
            df['revenue'] = df['revenue'].fillna(100000)
        else:
            df['revenue'] = 100000

        if 'tech_count' in df.columns:
            df['tech_count'] = df['tech_count'].fillna(5)
        else:
            df['tech_count'] = 5

        if 'segment' in df.columns:
            df['segment'] = df['segment'].fillna('Residential')
        else:
            df['segment'] = 'Residential'

        if 'account_level' in df.columns:
            df['account_level'] = df['account_level'].fillna('Corporate')
        else:
            df['account_level'] = 'Corporate'

        if 'neediness_category' in df.columns:
            df['neediness_category'] = df['neediness_category'].fillna('Low')
        else:
            df['neediness_category'] = 'Low'

        if 'health_segment' in df.columns:
            df['health_segment'] = df['health_segment'].fillna('Yellow')
        else:
            df['health_segment'] = 'Yellow'

    def get_neediness_query_template(self) -> str:
        """Returns the full neediness scoring query template"""
        # Load the comprehensive query from the SQL file - MUST exist
        query_file = 'neediness_scoring_main.sql'

        with open(query_file, 'r') as f:
            logger.info(f"Loaded neediness query from {query_file}")
            return f.read()

    def get_active_csms_and_managers_from_workday(self) -> Tuple[List[str], List[str]]:
        """Get list of active CSMs and their managers from Workday, filtered by resi_corp_active_csms table"""
        query = """
        WITH cte AS (
            SELECT PREFERRED_FULL_NAME,
                   CONCAT(legal_first_name, ' ', legal_last_name) AS full_name
            FROM DSV_WAREHOUSE.PUBLIC.FACT_WDAY_EMPLOYEE_WEEKLY_HISTORY
            WHERE 1=1
            GROUP BY PREFERRED_FULL_NAME, full_name
            HAVING COUNT(DISTINCT active_status) = 1
            ORDER BY full_name
        ),
        workday_csms AS (
            SELECT DISTINCT
                CONCAT(legal_first_name, ' ', legal_last_name) AS CSM,
                manager_name AS Manager
            FROM (
                SELECT *
                FROM DSV_WAREHOUSE.PUBLIC.FACT_WDAY_EMPLOYEE_WEEKLY_HISTORY
                WHERE 1=1
                    AND job_title ILIKE '%customer success manager%'
                    AND active_status = TRUE
                    AND week_end_date IN (
                        SELECT MAX(week_end_date)
                        FROM DSV_WAREHOUSE.PUBLIC.FACT_WDAY_EMPLOYEE_WEEKLY_HISTORY
                    )
                    AND manager_name IN (
                        SELECT DISTINCT PREFERRED_FULL_NAME
                        FROM (
                            SELECT DISTINCT
                                h.PREFERRED_FULL_NAME,
                                CONCAT(h.legal_first_name, ' ', h.legal_last_name) AS full_name,
                                h.legal_first_name,
                                h.legal_last_name,
                                h.company,
                                h.business_title,
                                h.active_status,
                                h.load_date,
                                h.week_end_date,
                                ROW_NUMBER() OVER(
                                    PARTITION BY CONCAT(h.legal_first_name, ' ', h.legal_last_name)
                                    ORDER BY h.load_date DESC, h.effective_date_for_current_position DESC
                                ) AS rn
                            FROM DSV_WAREHOUSE.PUBLIC.FACT_WDAY_EMPLOYEE_WEEKLY_HISTORY h
                            JOIN cte ON cte.full_name = CONCAT(h.legal_first_name, ' ', h.legal_last_name)
                            WHERE h.load_date IS NOT NULL
                            QUALIFY rn = 1
                        )
                        WHERE active_status = TRUE
                            and (lower(BUSINESS_TITLE) like '%manager%customer success%')
                            AND LOWER(company) NOT LIKE '%aspire%'
                            AND DATEDIFF(day, week_end_date::DATE, CURRENT_DATE) < 30
                        ORDER BY PREFERRED_FULL_NAME
                    )
                ORDER BY MANAGER_NAME, legal_first_name
            )
        )
        -- Filter by resi_corp_active_csms table to only include eligible CSMs
        SELECT w.CSM, w.Manager
        FROM workday_csms w
        INNER JOIN DSV_WAREHOUSE.DATA_SCIENCE.resi_corp_active_csms r
            ON w.CSM = r.active_csm
        """

        try:
            # First get the unfiltered count for logging
            unfiltered_query = """
            SELECT COUNT(DISTINCT CONCAT(legal_first_name, ' ', legal_last_name)) as total_csms
            FROM DSV_WAREHOUSE.PUBLIC.FACT_WDAY_EMPLOYEE_WEEKLY_HISTORY
            WHERE job_title ILIKE '%customer success manager%'
                AND active_status = TRUE
                AND week_end_date IN (
                    SELECT MAX(week_end_date)
                    FROM DSV_WAREHOUSE.PUBLIC.FACT_WDAY_EMPLOYEE_WEEKLY_HISTORY
                )
            """
            unfiltered_df = self.execute_query(unfiltered_query)
            unfiltered_df.columns = [col.lower() for col in unfiltered_df.columns]
            total_workday_csms = unfiltered_df['total_csms'].iloc[0] if not unfiltered_df.empty else 0

            # Now get the filtered results
            df = self.execute_query(query)
            if not df.empty:
                # Standardize column names to lowercase
                df.columns = [col.lower() for col in df.columns]

                # Get active CSMs if column exists
                if 'csm' in df.columns:
                    active_csms = df['csm'].unique().tolist()
                else:
                    active_csms = []
                    logger.warning("CSM column not found in Workday query")

                # Get managers if column exists
                if 'manager' in df.columns:
                    managers = df['manager'].dropna().unique().tolist()
                else:
                    managers = []
                    logger.warning("Manager column not found in Workday query")

                logger.info(f"Retrieved {len(active_csms)} active CSMs from Workday (filtered by resi_corp_active_csms table)")
                logger.info(f"Filtered out {total_workday_csms - len(active_csms)} CSMs who are not in resi_corp_active_csms")
                logger.info(f"Retrieved {len(managers)} managers from filtered CSMs")
                return active_csms, managers
            else:
                logger.warning("No CSM data retrieved after filtering by resi_corp_active_csms table")
                logger.info(f"Total CSMs in Workday before filtering: {total_workday_csms}")
                return [], []
        except Exception as e:
            logger.warning(f"Failed to get CSM data from Workday: {str(e)}. Using empty lists.")
            return [], []

    def get_csm_tenure_data(self) -> Dict:
        """Get CSM tenure information from Workday or calculated from first assignment"""
        tenure_query = """
        WITH csm_first_assignment AS (
            SELECT
                preferred_csm_name as csm_name,
                MIN(calendar_date) as first_assignment_date,
                MAX(calendar_date) as last_seen_date,
                COUNT(DISTINCT calendar_date) as active_days
            FROM DSV_WAREHOUSE.POST_SALES.VW_CUSTOMER_HISTORY_DAILY
            WHERE preferred_csm_name IS NOT NULL
                AND preferred_csm_role = 'Success Rep'
            GROUP BY preferred_csm_name
        )
        SELECT
            csm_name,
            first_assignment_date,
            last_seen_date,
            DATEDIFF(month, first_assignment_date, CURRENT_DATE()) as tenure_months,
            DATEDIFF(day, first_assignment_date, CURRENT_DATE()) as tenure_days,
            active_days,
            CASE
                WHEN DATEDIFF(month, first_assignment_date, CURRENT_DATE()) < 3 THEN 'New'
                WHEN DATEDIFF(month, first_assignment_date, CURRENT_DATE()) < 6 THEN 'Junior'
                WHEN DATEDIFF(month, first_assignment_date, CURRENT_DATE()) < 12 THEN 'Mid'
                WHEN DATEDIFF(month, first_assignment_date, CURRENT_DATE()) < 24 THEN 'Senior'
                ELSE 'Expert'
            END as tenure_category
        FROM csm_first_assignment
        WHERE last_seen_date >= DATEADD(month, -1, CURRENT_DATE())  -- Active in last month
        """

        try:
            df = self.execute_query(tenure_query)
            if not df.empty:
                # Standardize column names to lowercase
                df.columns = [col.lower() for col in df.columns]
                return df.set_index('csm_name').to_dict('index')
            else:
                return {}
        except Exception as e:
            logger.error(f"Failed to get CSM tenure data: {str(e)}")
            return {}

    def get_current_csm_books(self, min_account_threshold: int = 5) -> Dict:
        """Get current CSM book assignments and metrics from cached neediness data

        Args:
            min_account_threshold: Minimum number of accounts a CSM must have to be eligible.
                                 CSMs with fewer accounts may be from different segments or have data issues.
        """

        # Ensure neediness cache is populated
        if self.neediness_cache is None:
            logger.info("Populating neediness cache before getting CSM books...")
            if not self.populate_neediness_cache():
                logger.error("Failed to populate neediness cache")
                return {}

        # Get active CSMs and managers from Workday
        active_csms_workday, managers_to_exclude = self.get_active_csms_and_managers_from_workday()

        # Get CSM tenure data
        csm_tenure = self.get_csm_tenure_data()

        # Use the cached neediness data to build CSM books
        logger.info("Building CSM books from cached neediness data...")

        # Get ALL accounts with CSMs (not just Residential Corporate)
        # This ensures we count total workload for capacity checking
        all_accounts_df = self.neediness_cache[
            (self.neediness_cache.get('responsible_csm', '').notna()) &
            (self.neediness_cache.get('responsible_csm', '') != '')
        ].copy()

        # But also get Residential Corporate subset for segment-specific metrics
        df = self.neediness_cache[
            (self.neediness_cache.get('segment', 'Residential') == 'Residential') &
            (self.neediness_cache.get('account_level', 'Corporate') == 'Corporate') &
            (self.neediness_cache.get('responsible_csm', '').notna()) &
            (self.neediness_cache.get('responsible_csm', '') != '')
        ].copy()

        # Get the responsible_csm column name (might be 'responsible_csm' or 'responsible csm')
        csm_col = None
        for col in df.columns:
            if 'responsible' in col.lower() and 'csm' in col.lower():
                csm_col = col
                break

        if csm_col is None:
            logger.warning("No responsible CSM column found in cache")
            return {}

        logger.info(f"Using column '{csm_col}' for CSM names")

        # Load active CSMs filter
        active_csms_filter_query = """
        SELECT active_csm
        FROM DSV_WAREHOUSE.DATA_SCIENCE.resi_corp_active_csms
        """
        active_csms_filter_df = self.execute_query(active_csms_filter_query)
        active_csms_filter_df.columns = [col.lower() for col in active_csms_filter_df.columns]
        active_csms_filter = set(active_csms_filter_df['active_csm'].tolist())

        # Filter by active CSMs from resi_corp_active_csms
        df = df[df[csm_col].isin(active_csms_filter)]
        all_accounts_df = all_accounts_df[all_accounts_df[csm_col].isin(active_csms_filter)]

        # Rename CSM column to standard name for consistency
        df = df.rename(columns={csm_col: 'csm_name'})
        all_accounts_df = all_accounts_df.rename(columns={csm_col: 'responsible_csm'})

        # Check if dataframe is empty
        if df.empty:
            logger.warning("No CSM book data found after filtering by resi_corp_active_csms table")
            logger.info(f"Total accounts in cache: {len(self.neediness_cache)}")
            return {}

        # Get counts before filtering
        total_csms_before = df['csm_name'].nunique()
        logger.info(f"CSMs with Residential Corporate accounts: {total_csms_before}")

        # Group by CSM to create book structure
        # Only include CSMs who are both:
        # 1. Active in Workday
        # 2. Not managers
        # 3. Have current book assignments for Residential Corporate
        csm_books = {}
        for csm in df['csm_name'].unique():
            # Skip if CSM is a manager or not active in Workday
            if csm and csm not in managers_to_exclude:
                # IMPORTANT: Only include CSMs who are active in Workday
                # This ensures we don't assign to CSMs who have left the company
                if active_csms_workday and csm not in active_csms_workday:
                    logger.warning(f"CSM {csm} has assignments but not found in active Workday CSMs - skipping")
                    continue

                csm_df = df[df['csm_name'] == csm]

                # Get TOTAL account count across ALL segments for capacity checking
                total_accounts = all_accounts_df[all_accounts_df['responsible_csm'] == csm]['account_id'].nunique()

                # Get health segment distribution (for Residential Corporate only)
                health_dist = csm_df['health_segment'].value_counts().to_dict() if 'health_segment' in csm_df.columns else {}

                # Get tenure information
                tenure_info = csm_tenure.get(csm, {
                    'tenure_months': 6,  # Default to 6 months if not found
                    'tenure_category': 'Mid',
                    'tenure_days': 180
                })

                csm_books[csm] = {
                    'accounts': csm_df.to_dict('records'),
                    'count': total_accounts,  # Use TOTAL count for capacity checking
                    'resi_corp_count': len(csm_df),  # Keep segment-specific count for reference
                    'total_neediness': csm_df['neediness_score'].fillna(0).sum() if 'neediness_score' in csm_df.columns else len(csm_df) * 5,
                    'total_revenue': csm_df['total_mrr'].fillna(0).sum() if 'total_mrr' in csm_df.columns else len(csm_df) * 100000,
                    'total_tad': csm_df['tad_score'].fillna(0).sum() if 'tad_score' in csm_df.columns else 0,
                    'total_tech_count': csm_df['mts+mis'].fillna(0).sum() if 'mts+mis' in csm_df.columns else len(csm_df) * 5,
                    'industries': csm_df['industry'].value_counts().to_dict() if 'industry' in csm_df.columns else {},
                    'health_distribution': {
                        'Red': health_dist.get('Red', 0),
                        'Yellow': health_dist.get('Yellow', 0),
                        'Green': health_dist.get('Green', 0),
                        'total': len(csm_df)
                    },
                    'tenure_months': tenure_info.get('tenure_months', 6),
                    'tenure_category': tenure_info.get('tenure_category', 'Mid'),
                    'tenure_days': tenure_info.get('tenure_days', 180)
                }

        # Filter out CSMs with too few accounts (likely from different segments or data issues)
        excluded_csms = {}
        filtered_csm_books = {}

        for csm, data in csm_books.items():
            if data['count'] < min_account_threshold:
                excluded_csms[csm] = {
                    'count': data['count'],
                    'reason': f'Below minimum threshold ({data["count"]} < {min_account_threshold})',
                    'likely_issue': 'Different segment, new CSM, or data quality issue'
                }
            else:
                filtered_csm_books[csm] = data

        # Log excluded CSMs
        if excluded_csms:
            logger.warning(f"Excluding {len(excluded_csms)} CSMs with < {min_account_threshold} accounts:")
            for csm, info in excluded_csms.items():
                logger.warning(f"  - {csm}: {info['count']} accounts - {info['likely_issue']}")

        # Check if we have enough eligible CSMs left
        if len(filtered_csm_books) < 3:
            logger.warning(f"Only {len(filtered_csm_books)} eligible CSMs after filtering. Consider lowering threshold.")
            # In extreme cases, use all CSMs with at least 1 account
            if len(filtered_csm_books) == 0:
                logger.error("No eligible CSMs after filtering! Using all CSMs with assignments.")
                filtered_csm_books = csm_books

        # Update the eligible CSM list from filtered data
        # These are CSMs who have current Residential Corporate assignments, are not managers,
        # and have at least the minimum number of accounts
        self.eligible_csm_list = list(filtered_csm_books.keys())

        logger.info(f"Retrieved book data for {len(csm_books)} CSMs from neediness cache")
        logger.info(f"CSMs with Residential Corporate books: {total_csms_before}")
        logger.info(f"CSMs after resi_corp_active_csms filter: {len(df['csm_name'].unique())}")
        logger.info(f"After minimum account threshold (>= {min_account_threshold} accounts): {len(filtered_csm_books)} eligible CSMs")
        logger.info(f"Active CSMs from Workday (after resi_corp_active_csms filter): {len(active_csms_workday)}")
        logger.info(f"Managers to exclude: {', '.join(managers_to_exclude) if managers_to_exclude else 'None'}")
        logger.info(f"Final eligible CSMs for assignment: {', '.join(self.eligible_csm_list)}")

        return filtered_csm_books

    def create_recommendations_table(self):
        """Create the recommendations tracking table if it doesn't exist"""
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {self.recommendations_table} (
            recommendation_id NUMBER AUTOINCREMENT PRIMARY KEY,
            account_id VARCHAR(50),
            recommended_csm VARCHAR(100),
            recommendation_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            assignment_method VARCHAR(50),
            neediness_score NUMBER,
            health_score NUMBER,
            revenue NUMBER,
            account_segment VARCHAR(50),
            account_level VARCHAR(50),
            optimization_score FLOAT,
            llm_feedback VARCHAR(500),
            was_assigned BOOLEAN DEFAULT FALSE,
            actual_assigned_csm VARCHAR(100),
            assignment_date TIMESTAMP_NTZ,
            run_id VARCHAR(50),
            batch_size NUMBER
        )
        """
        try:
            cursor = self.snowflake_conn.cursor()
            cursor.execute(create_table_query)
            cursor.close()
            logger.info(f"Recommendations table {self.recommendations_table} verified/created (using _CANNE suffix)")
        except Exception as e:
            logger.error(f"Failed to create recommendations table: {str(e)}")

    def get_recent_csm_recommendations(self, csm_name: str, hours: int = 4) -> Dict:
        """Get recent recommendations AND actual assignments for a CSM from the database"""
        # CRITICAL FIX: Check BOTH recommendations AND actual assignments
        query = f"""
        WITH all_assignments AS (
            -- Get recommendations from recommendations table
            SELECT
                recommendation_timestamp as timestamp,
                neediness_score,
                'recommendation' as source_type
            FROM {self.recommendations_table}
            WHERE recommended_csm = '{csm_name}'
                AND recommendation_timestamp >= DATEADD(hour, -{hours}, CURRENT_TIMESTAMP())

            UNION ALL

            -- ALSO get actual assignments from assignments table
            -- Note: assignments table doesn't have neediness_score
            SELECT
                assignment_date as timestamp,
                NULL as neediness_score,  -- assignments table doesn't have this column
                'assignment' as source_type
            FROM {self.assignments_table}
            WHERE csm_name = '{csm_name}'
                AND assignment_date >= DATEADD(hour, -{hours}, CURRENT_TIMESTAMP())
        )
        SELECT
            COUNT(*) as total_recommendations,
            COUNT(CASE WHEN timestamp >= DATEADD(hour, -1, CURRENT_TIMESTAMP()) THEN 1 END) as last_1_hour,
            COUNT(CASE WHEN timestamp >= DATEADD(hour, -4, CURRENT_TIMESTAMP()) THEN 1 END) as last_4_hours,
            COUNT(CASE WHEN timestamp >= DATEADD(hour, -24, CURRENT_TIMESTAMP()) THEN 1 END) as last_24_hours,
            MAX(timestamp) as most_recent_recommendation,
            AVG(neediness_score) as avg_neediness_assigned,  -- will be NULL-aware average
            SUM(CASE WHEN source_type = 'assignment' THEN 1 ELSE 0 END) as actual_assignments
        FROM all_assignments
        """

        try:
            df = self.execute_query(query)
            if not df.empty:
                # Standardize column names to lowercase
                df.columns = [col.lower() for col in df.columns]
                return df.iloc[0].to_dict()
            else:
                return {
                    'total_recommendations': 0,
                    'last_1_hour': 0,
                    'last_4_hours': 0,
                    'last_24_hours': 0,
                    'most_recent_recommendation': None,
                    'avg_neediness_assigned': 0,
                    'actual_assignments': 0
                }
        except Exception as e:
            logger.error(f"Failed to get recent recommendations for {csm_name}: {str(e)}")
            return {
                'total_recommendations': 0,
                'last_1_hour': 0,
                'last_4_hours': 0,
                'last_24_hours': 0
            }

    def get_csm_health_distribution(self, csm_name: str) -> Dict:
        """Get the distribution of health scores for a CSM's current book"""
        query = f"""
        SELECT
            CORE_HEALTH_SCORE_COLOR as health_segment,
            COUNT(*) as count
        FROM DSV_WAREHOUSE.POST_SALES.VW_CUSTOMER_HISTORY_DAILY
        WHERE responsible_csm_name = '{csm_name}'
            AND is_current = TRUE
            AND is_customer = TRUE
        GROUP BY CORE_HEALTH_SCORE_COLOR
        """

        try:
            df = self.execute_query(query)
            if not df.empty:
                # Standardize column names to lowercase
                df.columns = [col.lower() for col in df.columns]
                distribution = df.set_index('health_segment')['count'].to_dict()
                distribution['total'] = sum(distribution.values())
                return distribution
            else:
                return {'Red': 0, 'Yellow': 0, 'Green': 0, 'total': 0}
        except Exception as e:
            logger.error(f"Failed to get health distribution for {csm_name}: {str(e)}")
            return {'Red': 0, 'Yellow': 0, 'Green': 0, 'total': 0}

    def update_recommendation_after_llm(self, account_id: str, new_csm: str, original_csm: str,
                                       llm_feedback: str, run_id: str):
        """Update recommendation after LLM review with new CSM assignment"""
        try:
            cursor = self.snowflake_conn.cursor()

            # Insert a new record showing the LLM-revised assignment
            insert_query = f"""
            INSERT INTO {self.recommendations_table} (
                account_id,
                recommended_csm,
                assignment_method,
                llm_feedback,
                run_id,
                was_assigned
            ) VALUES (
                '{account_id}',
                '{new_csm}',
                'llm_revised',
                '{llm_feedback}',
                '{run_id}_revised',
                TRUE
            )
            """
            cursor.execute(insert_query)

            # Mark the original recommendation as not assigned if CSM changed
            if new_csm != original_csm:
                update_query = f"""
                UPDATE {self.recommendations_table}
                SET was_assigned = FALSE,
                    llm_feedback = 'Revised by LLM - reassigned to {new_csm}'
                WHERE account_id = '{account_id}'
                    AND recommended_csm = '{original_csm}'
                    AND run_id = '{run_id}'
                """
                cursor.execute(update_query)

            self.snowflake_conn.commit()
            cursor.close()
            logger.info(f"Updated recommendation for {account_id}: {original_csm} -> {new_csm}")

        except Exception as e:
            logger.error(f"Failed to update recommendation after LLM review: {str(e)}")

    def store_recommendation(self, account_id: str, csm_name: str, account_data: pd.Series,
                           optimization_score: float, method: str, run_id: str, batch_size: int,
                           llm_feedback: str = None):
        """Store a CSM recommendation in the database"""
        try:
            cursor = self.snowflake_conn.cursor()

            # FIXED: Check if a recommendation already exists for this account in this run
            check_query = f"""
            SELECT COUNT(*) as count
            FROM {self.recommendations_table}
            WHERE account_id = '{account_id}'
              AND run_id = '{run_id}'
              AND assignment_method = '{method}'
            """

            cursor.execute(check_query)
            result = cursor.fetchone()

            if result and result[0] > 0:
                logger.debug(f"Recommendation already exists for account {account_id} in run {run_id}, skipping duplicate")
                cursor.close()
                return

            insert_query = f"""
            INSERT INTO {self.recommendations_table} (
                account_id,
                recommended_csm,
                assignment_method,
                neediness_score,
                health_score,
                revenue,
                account_segment,
                account_level,
                optimization_score,
                llm_feedback,
                run_id,
                batch_size
            ) VALUES (
                '{account_id}',
                '{csm_name}',
                '{method}',
                {account_data.get('neediness_score', 0)},
                {account_data.get('health_score', 0)},
                {account_data.get('revenue', 0)},
                '{account_data.get('segment', 'Unknown')}',
                '{account_data.get('account_level', 'Unknown')}',
                {optimization_score},
                {f"'{llm_feedback}'" if llm_feedback else 'NULL'},
                '{run_id}',
                {batch_size}
            )
            """

            cursor.execute(insert_query)
            self.snowflake_conn.commit()
            cursor.close()

        except Exception as e:
            logger.error(f"Failed to store recommendation for account {account_id}: {str(e)}")

    def get_recently_assigned_csms(self, current_batch_assignments: dict = None, num_accounts_processing: int = 1) -> list:
        """
        Smart exclusion of CSMs based on context:
        - For current batch: exclude CSMs already assigned in THIS run
        - For small batches (1-5): exclude CSMs with assignments in last 15 mins
        - For medium batches (6-20): exclude CSMs with >2 assignments in last hour
        - For large batches (20+): exclude CSMs with >5 assignments in last 4 hours
        """
        logger.info(f"🔍 Checking for recently assigned CSMs (batch size: {num_accounts_processing})")
        recently_assigned = set()

        # First, exclude CSMs already assigned in current batch/session
        if current_batch_assignments:
            batch_csms = set(current_batch_assignments.values())
            if batch_csms:
                recently_assigned.update(batch_csms)
                logger.info(f"Excluding {len(batch_csms)} CSMs already assigned in current batch: {list(batch_csms)}")

        # Dynamic time window based on batch size
        if num_accounts_processing <= 5:
            # Small batch: check last 2 hours and exclude any CSM with recent assignments
            time_window = 2  # 2 hours (was 15 minutes - too short!)
            max_allowed = 0  # Any assignment in last 2 hours = excluded
            window_desc = "2 hours"
        elif num_accounts_processing <= 20:
            # Medium batch: moderate exclusion (1 hour, allow 1-2)
            time_window = 1
            max_allowed = 2
            window_desc = "1 hour"
        else:
            # Large batch: lenient exclusion (4 hours, allow up to 5)
            time_window = 4
            max_allowed = 5
            window_desc = "4 hours"

        # Check recent assignment velocity
        query = f"""
        WITH recent_assignments AS (
            -- Include ALL recent recommendations, not just assigned ones
            -- This prevents the same CSM from being repeatedly recommended
            SELECT recommended_csm as csm_name,
                   COUNT(*) as assignment_count
            FROM {self.recommendations_table}
            WHERE recommendation_timestamp >= DATEADD(hour, -{time_window}, CURRENT_TIMESTAMP())
            GROUP BY recommended_csm

            UNION ALL

            SELECT csm_name,
                   COUNT(*) as assignment_count
            FROM {self.assignments_table}
            WHERE assignment_date >= DATEADD(hour, -{time_window}, CURRENT_TIMESTAMP())
            GROUP BY csm_name
        )
        SELECT csm_name,
               SUM(assignment_count) as total_assignments
        FROM recent_assignments
        GROUP BY csm_name
        ORDER BY total_assignments DESC
        """

        try:
            df = self.execute_query(query)
            if not df.empty:
                logger.info(f"Recent CSM activity (last {window_desc}, max allowed: {max_allowed}):")
                for _, row in df.iterrows():
                    csm_name = row['CSM_NAME']
                    total_assignments = int(row['TOTAL_ASSIGNMENTS'])

                    if total_assignments > max_allowed:
                        recently_assigned.add(csm_name)
                        logger.info(f"  ❌ {csm_name}: {total_assignments} assignments -> EXCLUDING")
                    else:
                        logger.info(f"  ✓ {csm_name}: {total_assignments} assignments -> available")

                if recently_assigned:
                    logger.info(f"📊 Total excluded: {len(recently_assigned)} CSMs")
                else:
                    logger.info(f"📊 No CSMs excluded - all available for assignment")
            else:
                logger.info(f"📊 No recent CSM activity found in last {window_desc}")

        except Exception as e:
            logger.warning(f"Could not get recently assigned CSMs: {str(e)}")

        return list(recently_assigned)

    def cache_all_csm_recency_data(self, csm_list: list) -> Dict:
        """
        Bulk fetch recency data for all CSMs at once to avoid repeated queries
        Returns a dictionary with recency data for each CSM
        """
        if not csm_list:
            return {}

        logger.info(f"Bulk fetching recency data for {len(csm_list)} CSMs...")

        # Build a single query to get all CSM recency data at once
        # CRITICAL: Check BOTH recommendations AND actual assignments tables
        csm_names_str = "', '".join(csm_list)
        query = f"""
        WITH all_assignments AS (
            -- Get recommendations from recommendations table
            SELECT
                recommended_csm as csm_name,
                recommendation_timestamp as timestamp,
                neediness_score
            FROM {self.recommendations_table}
            WHERE recommended_csm IN ('{csm_names_str}')
              AND recommendation_timestamp >= DATEADD(day, -7, CURRENT_TIMESTAMP())

            UNION ALL

            -- ALSO get actual assignments from assignments table (CRITICAL FIX!)
            -- Note: assignments table doesn't have neediness_score, use NULL
            SELECT
                csm_name,
                assignment_date as timestamp,
                NULL as neediness_score  -- assignments table doesn't have this column
            FROM {self.assignments_table}
            WHERE csm_name IN ('{csm_names_str}')
              AND assignment_date >= DATEADD(day, -7, CURRENT_TIMESTAMP())
        )
        SELECT
            csm_name,
            COUNT(*) as total_recommendations,
            COUNT(CASE WHEN timestamp >= DATEADD(hour, -1, CURRENT_TIMESTAMP()) THEN 1 END) as last_1_hour,
            COUNT(CASE WHEN timestamp >= DATEADD(hour, -4, CURRENT_TIMESTAMP()) THEN 1 END) as last_4_hours,
            COUNT(CASE WHEN timestamp >= DATEADD(hour, -24, CURRENT_TIMESTAMP()) THEN 1 END) as last_24_hours,
            COUNT(CASE WHEN timestamp >= DATEADD(day, -7, CURRENT_TIMESTAMP()) THEN 1 END) as last_7_days,
            AVG(neediness_score) as avg_neediness  -- will be NULL for assignments, that's ok
        FROM all_assignments
        GROUP BY csm_name
        """

        try:
            cursor = self.snowflake_conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()

            # Build the cache dictionary
            recency_cache = {}

            # Initialize all CSMs with zero values
            for csm in csm_list:
                recency_cache[csm] = {
                    'total_recommendations': 0,
                    'last_1_hour': 0,
                    'last_4_hours': 0,
                    'last_24_hours': 0,
                    'recent_assignments_7d': 0,
                    'avg_neediness_assigned': 0
                }

            # Update with actual data
            for row in results:
                csm_name = row[0]
                if csm_name in recency_cache:
                    recency_cache[csm_name] = {
                        'total_recommendations': row[1],
                        'last_1_hour': row[2],
                        'last_4_hours': row[3],
                        'last_24_hours': row[4],
                        'recent_assignments_7d': row[5],
                        'avg_neediness_assigned': row[6] if row[6] else 0
                    }

            logger.info(f"Cached recency data for {len(recency_cache)} CSMs")
            return recency_cache

        except Exception as e:
            logger.error(f"Failed to cache recency data: {str(e)}")
            return {}

    def calculate_assignment_recency_penalty(self, csm_name: str, recency_data: Dict = None) -> float:
        """
        Calculate penalty based on how recently CSM received recommendations from the database
        Returns higher penalty for more recent/frequent recommendations
        Uses exponential scaling to strongly discourage repeated assignments
        Can use cached data if provided to avoid repeated queries
        """
        # Use cached data if available, otherwise query database
        if recency_data and csm_name in recency_data:
            recent_data = recency_data[csm_name]
        else:
            recent_data = self.get_recent_csm_recommendations(csm_name, 24)

        # Calculate weighted penalty with exponential scaling
        penalty = 0

        # EXTREME exponential penalty for very recent recommendations (last hour)
        if recent_data['last_1_hour'] > 0:
            # Massive penalty: 1M for 1, 4M for 2, 9M for 3, etc.
            penalty += 1000000 * (recent_data['last_1_hour'] ** 2)

        # Very heavy penalty for recommendations in last 4 hours
        last_4_hours_excluding_1 = recent_data['last_4_hours'] - recent_data['last_1_hour']
        if last_4_hours_excluding_1 > 0:
            # Heavy penalty: 100K for 1, 400K for 2, 900K for 3, etc.
            penalty += 100000 * (last_4_hours_excluding_1 ** 2)

        # Heavy penalty for recommendations in last 24 hours
        last_24_hours_excluding_4 = recent_data['last_24_hours'] - recent_data['last_4_hours']
        if last_24_hours_excluding_4 > 0:
            # Strong penalty: 10K for 1, 40K for 2, 90K for 3, 160K for 4, etc.
            penalty += 10000 * (last_24_hours_excluding_4 ** 2)

        # Additional penalty based on total recent assignments (7 days)
        recent_7d = recent_data.get('recent_assignments_7d', 0)
        if recent_7d > 5:
            # Strong penalty for CSMs with many assignments in past week
            penalty += 50 * (recent_7d - 5)  # 50 penalty per assignment over 5

        # Additional penalty if CSM has high average neediness score assignments
        avg_neediness = recent_data.get('avg_neediness_assigned')
        if avg_neediness and avg_neediness > 7:
            penalty += 50  # Increased penalty if CSM is getting high neediness accounts

        # Log the penalty calculation for debugging
        logger.debug(f"Recency penalty for {csm_name}: {penalty} (1hr:{recent_data['last_1_hour']}, 4hr:{last_4_hours_excluding_1}, 24hr:{last_24_hours_excluding_4}, 7d:{recent_7d})")

        return penalty

    def calculate_book_imbalance(self, csm_books: Dict) -> Dict:
        """Calculate imbalance metrics across all CSM books"""
        metrics = {
            'counts': [],
            'neediness': [],
            'revenue': [],
            'tad': []
        }

        for csm, book in csm_books.items():
            metrics['counts'].append(book['count'])
            metrics['neediness'].append(book['total_neediness'])
            metrics['revenue'].append(book['total_revenue'])
            metrics['tad'].append(book['total_tad'])

        return {
            'count_variance': np.var(metrics['counts']),
            'neediness_variance': np.var(metrics['neediness']),
            'revenue_variance': np.var(metrics['revenue']),
            'tad_variance': np.var(metrics['tad']),
            'count_std': np.std(metrics['counts']),
            'neediness_std': np.std(metrics['neediness']),
            'revenue_std': np.std(metrics['revenue']),
            'tad_std': np.std(metrics['tad'])
        }

    def assign_single_account_optimized(self, account: pd.Series, csm_books: Dict, excluded_csms: list = None) -> Tuple[str, float, list]:
        """
        Assign single account using optimization logic
        Considers book balance, health score distribution, and recent recommendations
        Returns: (best_csm, optimization_score, top_alternatives_with_scores)
        """
        best_csm = None
        best_score = float('inf')
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Track all CSM scores for ranking
        all_csm_scores = []

        # Get eligible CSMs based on account segment and level
        segment_level = f"{account.get('segment', 'Residential').lower().replace(' & construction', '').replace(' ', '_')}_{account.get('account_level', 'Corporate').lower()}"
        max_accounts = self.limits.get(segment_level, {}).get('max_accounts_per_csm', 85)

        # Get all eligible CSMs (no MT filtering)
        eligible_csms = [csm for csm in self.eligible_csm_list if csm in csm_books]

        # Remove excluded CSMs
        if excluded_csms:
            eligible_csms = [csm for csm in eligible_csms if csm not in excluded_csms]
            logger.info(f"Excluding CSMs: {excluded_csms}")

        logger.info(f"Evaluating {len(eligible_csms)} eligible CSMs for account {account.get('account_id')} with health: {account.get('health_segment', 'Unknown')}")
        logger.info(f"Account segment: {account.get('segment')}, level: {account.get('account_level')}, max_accounts: {max_accounts}")

        # Cache all CSM recency data at once to avoid repeated queries
        recency_cache = self.cache_all_csm_recency_data(eligible_csms)

        # Track health score distribution for each CSM
        skipped_due_to_capacity = 0
        for csm in eligible_csms:
            # Check capacity constraint - TEMPORARILY DISABLED FOR TESTING
            # Uncomment for production
            # if csm_books[csm]['count'] >= max_accounts:
            #     skipped_due_to_capacity += 1
            #     continue

            # Calculate current health distribution
            csm_health_dist = self.get_csm_health_distribution(csm)

            # Calculate what the book would look like with this account
            simulated_books = copy.deepcopy(csm_books)
            simulated_books[csm]['count'] += 1
            simulated_books[csm]['total_neediness'] += account.get('neediness_score', 0)
            simulated_books[csm]['total_revenue'] += account.get('revenue', 0)
            simulated_books[csm]['total_tad'] += account.get('tad_score', 0)

            # Calculate imbalance metrics after assignment
            imbalance = self.calculate_book_imbalance(simulated_books)

            # Base score calculation
            score = (
                imbalance['count_variance'] * 0.20 +
                imbalance['neediness_variance'] * 0.25 +
                imbalance['revenue_variance'] * 0.15 +
                imbalance['tad_variance'] * 0.20
            )

            # Add STRONG penalty for high account counts (scaled to match variance magnitudes)
            current_count = csm_books[csm]['count']
            # Get max limit from config (default to 105 for residential_corporate)
            max_accounts = self.limits.get('residential_corporate', {}).get('max_accounts_per_csm', 105)
            warning_threshold = max_accounts - 5  # Start warning 5 accounts before max

            if current_count >= max_accounts:
                score += (current_count - (max_accounts - 1)) * 10000000  # 10M penalty per account over limit
            elif current_count >= warning_threshold:
                score += (current_count - (warning_threshold - 1)) * 1000000   # 1M penalty per account near limit
            elif current_count >= (warning_threshold - 10):
                score += (current_count - (warning_threshold - 11)) * 100000    # 100K penalty per account approaching limit

            # Health score color matching and penalties
            account_health = account.get('health_segment', 'Yellow')
            csm_book_info = csm_books.get(csm, {})
            health_dist = csm_book_info.get('health_distribution', {})

            if account_health == 'Red':
                # Red accounts need experienced CSMs
                red_pct = (health_dist.get('Red', 0) / max(health_dist.get('total', 1), 1))

                # Heavy penalty if CSM already has > 30% Red accounts
                if red_pct > 0.3:
                    score += 50

                # Prefer senior CSMs for Red accounts
                tenure_category = csm_book_info.get('tenure_category', 'Mid')
                if tenure_category == 'New':
                    score += 80  # Strong penalty for new CSMs with Red accounts
                elif tenure_category == 'Junior':
                    score += 40  # Moderate penalty for junior CSMs
                elif tenure_category in ['Senior', 'Expert']:
                    score -= 10  # Bonus for experienced CSMs handling Red accounts

            elif account_health == 'Green':
                # Green accounts can go to any CSM but prefer balancing
                green_pct = (health_dist.get('Green', 0) / max(health_dist.get('total', 1), 1))
                if green_pct > 0.5:
                    score += 20  # Avoid concentration of Green accounts

                # New CSMs can handle Green accounts well
                if csm_book_info.get('tenure_category') == 'New' and green_pct < 0.6:
                    score -= 5  # Small bonus for giving Green accounts to new CSMs

            else:  # Yellow accounts
                # Yellow accounts - balanced approach
                yellow_pct = (health_dist.get('Yellow', 0) / max(health_dist.get('total', 1), 1))
                if yellow_pct > 0.4:
                    score += 15

            # Tenure-based penalties and bonuses
            tenure_months = csm_book_info.get('tenure_months', 6)
            tenure_category = csm_book_info.get('tenure_category', 'Mid')

            # High neediness accounts should go to experienced CSMs
            if account.get('neediness_score', 0) >= 8:
                if tenure_months < 3:
                    score += 60  # Heavy penalty for new CSMs with high neediness
                elif tenure_months < 6:
                    score += 30  # Moderate penalty for junior CSMs
                elif tenure_months >= 24:
                    score -= 15  # Bonus for very experienced CSMs

            # New CSMs shouldn't get too many accounts quickly
            if tenure_months < 3:
                current_count = csm_book_info.get('count', 0)
                if current_count > 40:
                    score += 50  # Penalty if new CSM already has many accounts
                # Check recent assignments for new CSMs using cached data
                recent_data = recency_cache.get(csm, {})
                if recent_data.get('last_24_hours', 0) > 2:
                    score += 100  # Heavy penalty for overloading new CSMs

            # Add STRONG penalty for recent recommendations using cached data
            recency_penalty = self.calculate_assignment_recency_penalty(csm, recency_cache)

            # Multiply base recency penalty to make it more impactful
            recency_penalty *= 2.0  # Double the impact of recency

            # Adjust based on tenure (experienced CSMs can handle slightly more)
            if tenure_months >= 24:
                recency_penalty *= 0.8  # 20% reduction for expert CSMs
            elif tenure_months < 6:
                recency_penalty *= 1.5  # 50% increase for new/junior CSMs

            score += recency_penalty

            # Additional penalty based on neediness concentration
            if account.get('neediness_score', 0) >= 8:
                recent_data = recency_cache.get(csm, {})
                avg_neediness = recent_data.get('avg_neediness_assigned')
                if avg_neediness is not None and avg_neediness > 7:
                    # Higher penalty for junior CSMs getting multiple high neediness
                    if tenure_category in ['New', 'Junior']:
                        score += 50
                    else:
                        score += 30

            logger.debug(f"CSM {csm}: score={score:.2f}, recency_penalty={recency_penalty:.2f}, health_dist={csm_health_dist}")

            # Track all CSM scores for alternatives
            all_csm_scores.append({
                'csm': csm,
                'score': score,
                'recency_penalty': recency_penalty,
                'current_accounts': csm_books[csm]['count'],
                'health_dist': csm_health_dist,
                'recent_assignments_24h': recency_cache.get(csm, {}).get('last_24_hours', 0)
            })

            if score < best_score:
                best_score = score
                best_csm = csm

        # Sort CSMs by score to get top alternatives
        all_csm_scores.sort(key=lambda x: x['score'])

        # Get top 5 alternatives (excluding the best one, but including it in the list for LLM to see)
        top_alternatives = all_csm_scores[:6]  # Get top 6 (best + 5 alternatives)

        # Store the recommendation in the database
        if best_csm:
            self.store_recommendation(
                account_id=account.get('account_id'),
                csm_name=best_csm,
                account_data=account,
                optimization_score=best_score,
                method='single_optimized',
                run_id=run_id,
                batch_size=1
            )
            logger.info(f"Assigned account {account.get('account_id')} (health: {account.get('health_segment')}) to {best_csm} (score: {best_score:.2f})")
            logger.info(f"Top alternatives: {[alt['csm'] for alt in top_alternatives[:5]]}")
        else:
            logger.warning(f"No eligible CSM found for account {account.get('account_id')}")
            if skipped_due_to_capacity > 0:
                logger.warning(f"Skipped {skipped_due_to_capacity} CSMs due to capacity constraints (>= {max_accounts} accounts)")

        return best_csm, best_score, top_alternatives

    def optimize_batch_with_pulp(self, accounts_df: pd.DataFrame, csm_books: Dict, excluded_csms: list = None) -> Dict:
        """
        Use PuLP to optimize batch assignment of multiple accounts
        Includes recency penalty and health score distribution in the objective function
        """
        logger.info(f"Starting PuLP optimization for {len(accounts_df)} accounts")
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Initialize the optimization problem
        prob = pulp.LpProblem("CSM_Batch_Assignment", pulp.LpMinimize)

        # Get eligible CSMs (all CSMs, no MT filtering)
        eligible_csms = [csm for csm in self.eligible_csm_list if csm in csm_books]

        # Remove excluded CSMs
        if excluded_csms:
            eligible_csms = [csm for csm in eligible_csms if csm not in excluded_csms]
            logger.info(f"Excluding CSMs from batch optimization: {excluded_csms}")

        # Cache all CSM recency data at once to avoid repeated queries
        recency_cache = self.cache_all_csm_recency_data(eligible_csms)

        # Get health distributions for all CSMs
        csm_health_dists = {}
        for csm in eligible_csms:
            csm_health_dists[csm] = self.get_csm_health_distribution(csm)

        # Create binary decision variables
        x = {}
        for i, account in accounts_df.iterrows():
            for csm in eligible_csms:
                x[i, csm] = pulp.LpVariable(f"assign_{i}_{csm}", cat='Binary')

        # Constraint: Each account must be assigned to exactly one CSM
        for i in accounts_df.index:
            valid_vars = [x[i, csm] for csm in eligible_csms if (i, csm) in x]
            if valid_vars:
                prob += pulp.lpSum(valid_vars) == 1

        # Constraint: Respect CSM capacity limits
        segment_level = "residential_corporate"  # Default for now
        max_accounts = self.limits.get(segment_level, {}).get('max_accounts_per_csm', 85)

        for csm in eligible_csms:
            current_count = csm_books[csm]['count']
            new_assignments = pulp.lpSum([x[i, csm] for i in accounts_df.index if (i, csm) in x])
            prob += current_count + new_assignments <= max_accounts

        # Calculate projected metrics for objective function
        projected_counts = {}
        projected_neediness = {}
        projected_revenue = {}
        projected_tad = {}

        for csm in eligible_csms:
            projected_counts[csm] = csm_books[csm]['count'] + pulp.lpSum(
                [x[i, csm] for i in accounts_df.index if (i, csm) in x]
            )

            projected_neediness[csm] = csm_books[csm]['total_neediness'] + pulp.lpSum(
                [x[i, csm] * accounts_df.loc[i, 'neediness_score']
                 for i in accounts_df.index if (i, csm) in x]
            )

            projected_revenue[csm] = csm_books[csm]['total_revenue'] + pulp.lpSum(
                [x[i, csm] * accounts_df.loc[i, 'revenue']
                 for i in accounts_df.index if (i, csm) in x]
            )

            projected_tad[csm] = csm_books[csm]['total_tad'] + pulp.lpSum(
                [x[i, csm] * accounts_df.loc[i, 'tad_score']
                 for i in accounts_df.index if (i, csm) in x]
            )

        # Objective: Minimize variance + recency penalties
        mean_count = pulp.lpSum(projected_counts.values()) / len(eligible_csms)
        mean_neediness = pulp.lpSum(projected_neediness.values()) / len(eligible_csms)
        mean_revenue = pulp.lpSum(projected_revenue.values()) / len(eligible_csms)
        mean_tad = pulp.lpSum(projected_tad.values()) / len(eligible_csms)

        # Create auxiliary variables for absolute deviations (PuLP linearization)
        # We'll minimize the sum of positive and negative deviations
        count_dev_pos = {}
        count_dev_neg = {}
        neediness_dev_pos = {}
        neediness_dev_neg = {}

        for csm in eligible_csms:
            count_dev_pos[csm] = pulp.LpVariable(f"count_dev_pos_{csm}", lowBound=0, cat='Continuous')
            count_dev_neg[csm] = pulp.LpVariable(f"count_dev_neg_{csm}", lowBound=0, cat='Continuous')
            neediness_dev_pos[csm] = pulp.LpVariable(f"need_dev_pos_{csm}", lowBound=0, cat='Continuous')
            neediness_dev_neg[csm] = pulp.LpVariable(f"need_dev_neg_{csm}", lowBound=0, cat='Continuous')

            # Add constraints to define deviations
            prob += projected_counts[csm] - mean_count == count_dev_pos[csm] - count_dev_neg[csm]
            prob += projected_neediness[csm] - mean_neediness == neediness_dev_pos[csm] - neediness_dev_neg[csm]

        # Calculate variance components as sum of absolute deviations
        count_variance = pulp.lpSum([count_dev_pos[csm] + count_dev_neg[csm] for csm in eligible_csms])
        neediness_variance = pulp.lpSum([neediness_dev_pos[csm] + neediness_dev_neg[csm] for csm in eligible_csms])

        # For simplicity, skip revenue and TAD variance in optimization
        revenue_variance = 0
        tad_variance = 0

        # Add recency penalties to objective using cached data
        recency_penalties = pulp.lpSum([
            x[i, csm] * self.calculate_assignment_recency_penalty(csm, recency_cache)
            for i in accounts_df.index
            for csm in eligible_csms
            if (i, csm) in x
        ])

        # Combined objective with weights - INCREASED recency penalty weight
        prob += (
            0.20 * count_variance +
            0.20 * neediness_variance +
            0.15 * revenue_variance +
            0.15 * tad_variance +
            0.30 * recency_penalties  # INCREASED recency penalty weight to 30%
        )

        # Solve the optimization
        prob.solve(pulp.PULP_CBC_CMD(msg=0))

        # Extract assignments and store recommendations
        assignments = {}
        if pulp.LpStatus[prob.status] == 'Optimal':
            for i in accounts_df.index:
                for csm in eligible_csms:
                    if (i, csm) in x and x[i, csm].varValue == 1:
                        account_id = accounts_df.loc[i, 'account_id']
                        assignments[account_id] = csm

                        # Store recommendation in database
                        self.store_recommendation(
                            account_id=account_id,
                            csm_name=csm,
                            account_data=accounts_df.loc[i],
                            optimization_score=pulp.value(prob.objective),
                            method='batch_optimized',
                            run_id=run_id,
                            batch_size=len(accounts_df)
                        )

            logger.info(f"PuLP optimization completed successfully. Assigned {len(assignments)} accounts")
            logger.info(f"Stored {len(assignments)} recommendations in database with run_id: {run_id}")
        else:
            logger.error(f"PuLP optimization failed with status: {pulp.LpStatus[prob.status]}")

        return assignments

    def _prepare_assignment_analysis(self, assignments: Dict, accounts_df: pd.DataFrame, csm_books: Dict) -> Dict:
        """Prepare detailed assignment analysis for LLM review"""
        analysis = {
            'assignments': [],
            'book_stats': [],
            'health_distribution': {}
        }

        # Detailed assignment information
        for account_id, csm_name in assignments.items():
            account_info = accounts_df[accounts_df['account_id'] == account_id].iloc[0] if not accounts_df[accounts_df['account_id'] == account_id].empty else {}
            csm_info = csm_books.get(csm_name, {})
            recent_recs = self.get_recent_csm_recommendations(csm_name, 168)  # Last 7 days

            analysis['assignments'].append({
                'account_id': account_id,
                'assigned_csm': csm_name,
                'account_details': {
                    'neediness_score': account_info.get('neediness_score', 0),
                    'neediness_category': account_info.get('neediness_category', 'Unknown'),
                    'health_segment': account_info.get('health_segment', 'Unknown'),
                    'revenue': account_info.get('revenue', 0),
                    'tech_count': account_info.get('tech_count', 0),
                    'segment': account_info.get('segment', 'Unknown'),
                    'churn_risk': account_info.get('churn_stage', 'Not at risk'),
                    'tad_score': account_info.get('tad_score', 0),
                    'is_parent_account': account_info.get('is_parent_account', False)
                },
                'csm_current_state': {
                    'current_accounts': csm_info.get('count', 0),
                    'total_neediness': csm_info.get('total_neediness', 0),
                    'total_revenue': csm_info.get('total_revenue', 0),
                    'avg_neediness': csm_info.get('total_neediness', 0) / max(csm_info.get('count', 1), 1),
                    'recent_assignments_7d': recent_recs.get('actual_assignments', 0),  # Use actual 7-day count
                    'recent_high_neediness': recent_recs.get('avg_neediness_assigned', 0),
                    'tenure_months': csm_info.get('tenure_months', 6),
                    'tenure_category': csm_info.get('tenure_category', 'Mid'),
                    'health_distribution': csm_info.get('health_distribution', {})
                }
            })

        # Current book statistics
        for csm, info in csm_books.items():
            health_dist = self.get_csm_health_distribution(csm)
            csm_stat = {
                'csm': csm,
                'accounts': info['count'],
                'total_neediness': info['total_neediness'],
                'avg_neediness': info['total_neediness'] / max(info['count'], 1),
                'total_revenue': info['total_revenue'],
                'health_distribution': {
                    'red_pct': (health_dist.get('Red', 0) / max(health_dist.get('total', 1), 1)) * 100,
                    'yellow_pct': (health_dist.get('Yellow', 0) / max(health_dist.get('total', 1), 1)) * 100,
                    'green_pct': (health_dist.get('Green', 0) / max(health_dist.get('total', 1), 1)) * 100
                }
            }
            analysis['book_stats'].append(csm_stat)
            analysis['health_distribution'][csm] = health_dist

        # Print book stats for visibility
        logger.info("=" * 60)
        logger.info("CSM BOOK STATS (Data sent to LLM):")
        logger.info("=" * 60)
        for stat in analysis['book_stats']:
            logger.info(f"CSM: {stat['csm']}")
            logger.info(f"  Accounts: {stat['accounts']}")
            logger.info(f"  Avg Neediness: {stat['avg_neediness']:.2f}")
            logger.info(f"  Total Revenue: ${stat['total_revenue']:,.2f}")
            logger.info(f"  Health: Red={stat['health_distribution']['red_pct']:.1f}%, "
                       f"Yellow={stat['health_distribution']['yellow_pct']:.1f}%, "
                       f"Green={stat['health_distribution']['green_pct']:.1f}%")
        logger.info("=" * 60)

        return analysis

    def _get_historical_performance_data(self, csm_names: list) -> Dict:
        """Get historical performance metrics for CSMs"""
        performance_data = {}

        for csm in set(csm_names):
            # Get 30-day historical data
            query = f"""
            SELECT
                COUNT(DISTINCT account_id) as accounts_assigned_30d,
                AVG(neediness_score) as avg_neediness_assigned,
                SUM(CASE WHEN neediness_score >= 8 THEN 1 ELSE 0 END) as high_neediness_count,
                COUNT(DISTINCT DATE(recommendation_timestamp)) as active_days,
                MAX(neediness_score) as max_neediness_assigned,
                MIN(neediness_score) as min_neediness_assigned
            FROM {self.recommendations_table}
            WHERE recommended_csm = '{csm}'
                AND recommendation_timestamp >= DATEADD(day, -30, CURRENT_TIMESTAMP())
                AND was_assigned = TRUE
            """

            try:
                df = self.execute_query(query)
                if not df.empty:
                    performance_data[csm] = convert_numpy_types(df.iloc[0].to_dict())
                else:
                    performance_data[csm] = {
                        'accounts_assigned_30d': 0,
                        'avg_neediness_assigned': 0,
                        'high_neediness_count': 0,
                        'active_days': 0
                    }
            except Exception as e:
                logger.error(f"Failed to get historical data for {csm}: {str(e)}")
                performance_data[csm] = {}

        return performance_data

    def _calculate_detailed_metrics(self, assignments: Dict, accounts_df: pd.DataFrame, csm_books: Dict) -> Dict:
        """Calculate detailed metrics for before and after assignment"""
        metrics = {
            'current': {},
            'projected': {},
            'projected_health': {}
        }

        # Current metrics
        all_counts = [book['count'] for book in csm_books.values()]
        all_neediness = [book['total_neediness'] for book in csm_books.values()]
        all_revenue = [book['total_revenue'] for book in csm_books.values()]

        metrics['current'] = {
            'account_count_std': np.std(all_counts),
            'account_count_mean': np.mean(all_counts),
            'account_count_cv': (np.std(all_counts) / np.mean(all_counts)) * 100 if np.mean(all_counts) > 0 else 0,
            'neediness_std': np.std(all_neediness),
            'neediness_mean': np.mean(all_neediness),
            'revenue_std': np.std(all_revenue),
            'revenue_mean': np.mean(all_revenue)
        }

        # Projected metrics after assignments
        projected_books = copy.deepcopy(csm_books)
        for account_id, csm_name in assignments.items():
            if csm_name in projected_books:
                account_info = accounts_df[accounts_df['account_id'] == account_id].iloc[0]
                projected_books[csm_name]['count'] += 1
                projected_books[csm_name]['total_neediness'] += account_info.get('neediness_score', 0)
                projected_books[csm_name]['total_revenue'] += account_info.get('revenue', 0)

        proj_counts = [book['count'] for book in projected_books.values()]
        proj_neediness = [book['total_neediness'] for book in projected_books.values()]
        proj_revenue = [book['total_revenue'] for book in projected_books.values()]

        metrics['projected'] = {
            'account_count_std': np.std(proj_counts),
            'account_count_mean': np.mean(proj_counts),
            'account_count_cv': (np.std(proj_counts) / np.mean(proj_counts)) * 100 if np.mean(proj_counts) > 0 else 0,
            'account_count_max': max(proj_counts),
            'account_count_min': min(proj_counts),
            'neediness_std': np.std(proj_neediness),
            'neediness_mean': np.mean(proj_neediness),
            'neediness_variance_change': ((np.var(proj_neediness) - np.var(all_neediness)) / np.var(all_neediness)) * 100 if np.var(all_neediness) > 0 else 0,
            'revenue_std': np.std(proj_revenue),
            'revenue_mean': np.mean(proj_revenue),
            'csms_over_80_accounts': sum(1 for c in proj_counts if c > 80),
            # Use max_accounts from config instead of hardcoded 85
            'csms_at_max_capacity': sum(1 for c in proj_counts if c >= self.limits.get('residential_corporate', {}).get('max_accounts_per_csm', 105))
        }

        # Projected health distribution
        for csm in assignments.values():
            if csm not in metrics['projected_health']:
                metrics['projected_health'][csm] = {'Red': 0, 'Yellow': 0, 'Green': 0}

        for account_id, csm_name in assignments.items():
            account_info = accounts_df[accounts_df['account_id'] == account_id].iloc[0]
            health = account_info.get('health_segment', 'Yellow')
            if health in metrics['projected_health'][csm_name]:
                metrics['projected_health'][csm_name][health] += 1

        return metrics

    def _identify_potential_issues(self, analysis: Dict, metrics: Dict) -> list:
        """Identify potential issues with the assignments"""
        issues = []

        # Check workload balance
        if metrics['projected']['account_count_cv'] > 20:
            issues.append({
                'type': 'WORKLOAD_IMBALANCE',
                'severity': 'HIGH',
                'detail': f"Account count coefficient of variation is {metrics['projected']['account_count_cv']:.1f}% (threshold: 20%)"
            })

        # Check for overloaded CSMs
        if metrics['projected']['csms_at_max_capacity'] > 0:
            max_accounts = self.limits.get('residential_corporate', {}).get('max_accounts_per_csm', 105)
            issues.append({
                'type': 'CAPACITY_EXCEEDED',
                'severity': 'CRITICAL',
                'detail': f"{metrics['projected']['csms_at_max_capacity']} CSMs will exceed maximum capacity of {max_accounts} accounts"
            })

        # Check neediness variance increase
        if metrics['projected'].get('neediness_variance_change', 0) > 30:
            issues.append({
                'type': 'NEEDINESS_CONCENTRATION',
                'severity': 'MEDIUM',
                'detail': f"Neediness variance increasing by {metrics['projected']['neediness_variance_change']:.1f}%"
            })

        # Check for CSMs getting too many accounts in this batch
        csm_assignment_counts = {}
        for assignment in analysis['assignments']:
            csm = assignment['assigned_csm']
            csm_assignment_counts[csm] = csm_assignment_counts.get(csm, 0) + 1

        for csm, count in csm_assignment_counts.items():
            if count > 3:
                issues.append({
                    'type': 'BATCH_CONCENTRATION',
                    'severity': 'MEDIUM',
                    'detail': f"{csm} is getting {count} accounts in this batch (recommended max: 3)"
                })

        # Check health distribution issues
        for assignment in analysis['assignments']:
            csm = assignment['assigned_csm']
            if assignment['account_details']['health_segment'] == 'Red':
                csm_health = analysis['health_distribution'].get(csm, {})
                red_pct = (csm_health.get('Red', 0) / max(csm_health.get('total', 1), 1)) * 100
                if red_pct > 35:
                    issues.append({
                        'type': 'RED_ACCOUNT_CONCENTRATION',
                        'severity': 'MEDIUM',
                        'detail': f"{csm} already has {red_pct:.1f}% Red accounts and is getting another Red account"
                    })

        return issues

    def review_assignments_with_llm(self, assignments: Dict, accounts_df: pd.DataFrame, csm_books: Dict, excluded_csms: list = None) -> Tuple[bool, str, Dict]:
        """
        Comprehensive LLM review with detailed context and specific evaluation criteria
        Returns: (should_rerun, feedback_message, revised_assignments)
        """
        if not self.claude_client:
            logger.info("LLM client not available, skipping review")
            return False, "LLM review skipped - no API key", assignments

        try:
            # Gather comprehensive assignment details
            assignment_analysis = self._prepare_assignment_analysis(assignments, accounts_df, csm_books)

            # Get historical performance data
            historical_data = self._get_historical_performance_data(assignments.values())

            # Calculate detailed metrics
            metrics_analysis = self._calculate_detailed_metrics(assignments, accounts_df, csm_books)

            # Identify potential issues
            issues = self._identify_potential_issues(assignment_analysis, metrics_analysis)

            # Prepare alternatives for each account
            alternatives_info = {}
            if hasattr(self, 'assignment_alternatives'):
                for account_id, alternatives in self.assignment_alternatives.items():
                    if alternatives:
                        # Get top 5 alternatives (or fewer if not available)
                        top_5 = alternatives[:5]
                        alternatives_info[account_id] = [
                            {
                                'csm': alt['csm'],
                                'score': round(alt['score'], 2),
                                'current_accounts': alt['current_accounts'],
                                'recent_24h': alt['recent_assignments_24h'],
                                'health_mix': alt['health_dist']
                            }
                            for alt in top_5
                        ]

            # Create comprehensive prompt for Claude
            prompt = f"""You are an expert CSM routing analyst. Conduct a thorough review of these account assignments.

## IMPORTANT: EXCLUDED CSMS
The following CSMs have recent assignments and should NOT be suggested as alternatives:
{json.dumps(excluded_csms if excluded_csms else [], indent=2)}

## TOP ALTERNATIVE CSM OPTIONS FOR EACH ACCOUNT:
{json.dumps(convert_numpy_types(alternatives_info), indent=2)}

**Note**: The CSMs are ranked by optimization score (lower is better). The first CSM in each list is the current assignment.
You should ONLY suggest changes if there's a significantly better alternative from this list.
Choose from these top 5 alternatives to ensure diversity and avoid repeatedly assigning the same CSMs.

## NEW ASSIGNMENTS DETAIL:
{json.dumps(convert_numpy_types(assignment_analysis['assignments']), indent=2)}

## PRE-ASSIGNMENT CSM BOOK ANALYSIS:
{json.dumps(convert_numpy_types(assignment_analysis['book_stats']), indent=2)}

## POST-ASSIGNMENT PROJECTED METRICS:
{json.dumps(convert_numpy_types(metrics_analysis['projected']), indent=2)}

## HEALTH SCORE DISTRIBUTION:
Before Assignment:
{json.dumps(convert_numpy_types(assignment_analysis['health_distribution']), indent=2)}

After Assignment:
{json.dumps(convert_numpy_types(metrics_analysis['projected_health']), indent=2)}

## HISTORICAL CSM PERFORMANCE (Last 30 Days):
{json.dumps(convert_numpy_types(historical_data), indent=2)}

## IDENTIFIED CONCERNS:
{json.dumps(convert_numpy_types(issues), indent=2)}

## SPECIFIC EVALUATION CRITERIA:

1. **Workload Balance** (Critical):
   - Is the standard deviation of account counts > 20% of mean?
   - Are any CSMs getting > 3 accounts in this batch?
   - Will any CSM exceed their maximum capacity (from config)?

2. **CSM Tenure & Experience Matching** (Critical):
   - Are Red accounts going to experienced CSMs (Senior/Expert)?
   - Are new CSMs (<3 months) receiving appropriate accounts (preferably Green)?
   - Are high neediness accounts (score >= 8) assigned to CSMs with 6+ months tenure?
   - Is any new CSM getting more than 2 accounts in this batch?

3. **Health Score Color Matching** (High Priority):
   - Red accounts should go to CSMs with tenure >= 12 months
   - Green accounts can go to newer CSMs for development
   - Yellow accounts need balanced distribution
   - No CSM should have > 35% Red accounts after assignment

4. **Neediness Distribution** (High Priority):
   - High neediness accounts (score >= 8) should go to Senior/Expert CSMs
   - Is the neediness variance increasing by > 30%?
   - Are junior CSMs protected from getting multiple high neediness accounts?

5. **Batch Assignment Logic** (High Priority for Multi-Account):
   - In batch of {len(assignments)} accounts, evaluate collective impact
   - Check if any single CSM is receiving too many accounts (max 3 per batch)
   - Ensure batch doesn't overload new/junior CSMs
   - Verify health score mix in batch is appropriate for each CSM's experience

6. **Revenue Distribution** (Medium Priority):
   - High-value accounts ($100k+) should go to experienced CSMs
   - Are enterprise accounts being assigned to CSMs with 12+ months tenure?

7. **Recent Assignment Pattern** (Medium Priority):
   - Has any CSM received > 5 assignments in last 7 days?
   - Are new CSMs (<3 months) getting > 2 assignments in 24 hours?
   - Cooling period more important for junior CSMs

8. **Special Considerations**:
   - Parent/child account relationships maintained?
   - Industry expertise matched where applicable?
   - Timezone alignment considered?

## YOUR TASK:
1. Analyze each criterion systematically
2. Identify SPECIFIC problems (not general observations)
3. Only disapprove if there are SIGNIFICANT imbalances that would harm customer experience
4. Consider cumulative effect of assignments, not just individual ones

Respond with a JSON object:
{{
    "approve": true/false,
    "confidence_score": 0-100 (how confident you are in this decision),
    "feedback": "Specific 1-2 sentence explanation of your decision",
    "critical_issues": ["List of critical problems requiring immediate rebalancing"],
    "warnings": ["List of non-critical concerns to monitor"],
    "specific_reassignments": {{"account_id": "suggested_csm"}} or null,  // IMPORTANT: You MUST select from the TOP ALTERNATIVE CSM OPTIONS provided above! Pick the 2nd, 3rd, 4th, or 5th best alternative to ensure diversity. Do NOT suggest any CSM from the EXCLUDED CSMS list!
    "metrics_summary": {{
        "workload_balance": "good/fair/poor",
        "neediness_distribution": "good/fair/poor",
        "health_balance": "good/fair/poor",
        "overall_quality": "good/fair/poor"
    }}
}}

CRITICAL RULES FOR REASSIGNMENTS:
1. You MUST ONLY suggest CSMs from the "TOP ALTERNATIVE CSM OPTIONS" section above
2. Prefer the 2nd, 3rd, or 4th best alternatives over the 1st to ensure diversity
3. If the same CSM appears as #1 for multiple accounts, distribute to alternatives
4. NEVER suggest CSMs from the EXCLUDED CSMS list
5. If no good alternative exists in the top 5, approve the original assignment

Be specific and actionable. Default to approval unless there are clear, significant problems."""

            # Call Claude Sonnet with higher token limit for detailed analysis
            response = self.claude_client.messages.create(
                model="claude-3-5-sonnet-20241022",
                max_tokens=1500,
                temperature=0.1,  # Slightly higher for more nuanced analysis
                messages=[
                    {"role": "user", "content": prompt}
                ]
            )

            # Parse response
            response_text = response.content[0].text
            logger.debug(f"LLM Response: {response_text[:500]}...")

            # Extract JSON with better error handling
            import re
            json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
            if json_match:
                review_result = json.loads(json_match.group())
            else:
                logger.error("Failed to parse LLM JSON response")
                review_result = {
                    "approve": True,
                    "confidence_score": 0,
                    "feedback": "Could not parse LLM response, proceeding with assignments",
                    "critical_issues": [],
                    "warnings": ["LLM response parsing failed"],
                    "specific_reassignments": None,
                    "metrics_summary": {}
                }

            # Log detailed feedback
            logger.info(f"LLM Decision: {'APPROVED' if review_result.get('approve') else 'REJECTED'}")
            logger.info(f"Confidence Score: {review_result.get('confidence_score', 0)}%")
            logger.info(f"Feedback: {review_result.get('feedback', 'No feedback')}")

            if review_result.get('critical_issues'):
                logger.warning(f"Critical Issues: {', '.join(review_result['critical_issues'])}")

            if review_result.get('warnings'):
                logger.info(f"Warnings: {', '.join(review_result['warnings'])}")

            if review_result.get('metrics_summary'):
                logger.info(f"Quality Metrics: {review_result['metrics_summary']}")

            # Handle specific reassignments if suggested
            revised_assignments = assignments.copy()
            if review_result.get('specific_reassignments'):
                for account_id, new_csm in review_result['specific_reassignments'].items():
                    if account_id in revised_assignments:
                        logger.info(f"LLM suggests reassigning {account_id} from {revised_assignments[account_id]} to {new_csm}")
                        revised_assignments[account_id] = new_csm

            # Determine if we should rerun based on confidence and critical issues
            should_rerun = (
                not review_result.get('approve', True) or
                review_result.get('confidence_score', 100) < 60 or
                len(review_result.get('critical_issues', [])) > 0
            )

            feedback = review_result.get('feedback', 'No specific feedback provided')

            return should_rerun, feedback, revised_assignments

        except Exception as e:
            logger.error(f"Error during LLM review: {str(e)}", exc_info=True)
            return False, f"LLM review failed: {str(e)}", assignments

    def update_assignments_in_snowflake(self, assignments: Dict, llm_feedback: str = None) -> bool:
        """Update CSM assignments back to Snowflake with LLM feedback"""
        logger.info(f"DEBUG: update_assignments_in_snowflake called with {len(assignments) if assignments else 0} assignments")
        if not assignments:
            logger.info("No assignments to update")
            return True

        try:
            logger.info(f"DEBUG: Connecting to Snowflake to write {len(assignments)} assignments to ACCOUNT_CSM_ASSIGNMENTS_CANNE")
            cursor = self.snowflake_conn.cursor()

            # First ensure the assignments table exists in DATA_SCIENCE schema with _CANNE suffix
            create_table_query = """
            CREATE TABLE IF NOT EXISTS DSV_WAREHOUSE.DATA_SCIENCE.ACCOUNT_CSM_ASSIGNMENTS_CANNE (
                account_id VARCHAR(50) PRIMARY KEY,
                csm_name VARCHAR(100),
                assignment_date TIMESTAMP_NTZ,
                assignment_method VARCHAR(50),
                llm_review_feedback TEXT,
                last_updated TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
            """
            cursor.execute(create_table_query)
            logger.info("Ensured ACCOUNT_CSM_ASSIGNMENTS_CANNE table exists in DATA_SCIENCE schema")

            for account_id, csm_name in assignments.items():
                # Escape single quotes in LLM feedback for SQL
                llm_feedback_escaped = llm_feedback.replace("'", "''") if llm_feedback else ''

                # Update the assignment in our _CANNE table in DATA_SCIENCE schema
                # All tables must be in DATA_SCIENCE schema with _CANNE suffix
                update_query = f"""
                UPDATE DSV_WAREHOUSE.DATA_SCIENCE.ACCOUNT_CSM_ASSIGNMENTS_CANNE
                SET
                    csm_name = '{csm_name}',
                    assignment_date = CURRENT_TIMESTAMP(),
                    assignment_method = 'automated_routing',
                    llm_review_feedback = '{llm_feedback_escaped}',
                    last_updated = CURRENT_TIMESTAMP()
                WHERE account_id = '{account_id}'
                """

                # If record doesn't exist, insert it
                insert_query = f"""
                INSERT INTO DSV_WAREHOUSE.DATA_SCIENCE.ACCOUNT_CSM_ASSIGNMENTS_CANNE
                    (account_id, csm_name, assignment_date, assignment_method, llm_review_feedback, last_updated)
                SELECT
                    '{account_id}',
                    '{csm_name}',
                    CURRENT_TIMESTAMP(),
                    'automated_routing',
                    '{llm_feedback_escaped}',
                    CURRENT_TIMESTAMP()
                WHERE NOT EXISTS (
                    SELECT 1 FROM DSV_WAREHOUSE.DATA_SCIENCE.ACCOUNT_CSM_ASSIGNMENTS_CANNE
                    WHERE account_id = '{account_id}'
                )
                """

                logger.info(f"DEBUG: Executing UPDATE for account {account_id}")
                cursor.execute(update_query)
                logger.info(f"DEBUG: Executing INSERT for account {account_id}")
                cursor.execute(insert_query)

                # Note: We don't update VW_ONBOARDING_DETAIL directly as it's a view
                # The source system should handle status updates based on our assignment table
                logger.info(f"Saved assignment for account {account_id} to CSM {csm_name} in ACCOUNT_CSM_ASSIGNMENTS_CANNE")

                # Mark recommendation as assigned
                recommendation_update = f"""
                UPDATE {self.recommendations_table}
                SET was_assigned = TRUE,
                    actual_assigned_csm = '{csm_name}',
                    assignment_date = CURRENT_TIMESTAMP(),
                    llm_feedback = '{llm_feedback_escaped}'
                WHERE account_id = '{account_id}'
                    AND recommended_csm = '{csm_name}'
                    AND was_assigned = FALSE
                """
                cursor.execute(recommendation_update)

            self.snowflake_conn.commit()
            cursor.close()
            logger.info(f"Successfully updated {len(assignments)} assignments in Snowflake")

            # Display updated portfolio metrics after assignments
            self.display_updated_portfolio_metrics(assignments)

            return True

        except Exception as e:
            logger.error(f"Failed to update assignments in Snowflake: {str(e)}")
            self.snowflake_conn.rollback()
            return False

    def display_updated_portfolio_metrics(self, assignments):
        """Display updated CSM portfolio metrics after assignments

        Args:
            assignments: Dictionary of account_id -> csm_name assignments
        """
        try:
            logger.info("\n" + "="*80)
            logger.info("📊 UPDATED CSM PORTFOLIO METRICS (After New Assignments)")
            logger.info("="*80)

            # Get the unique CSMs who received assignments
            assigned_csms = list(set(assignments.values()))

            if not assigned_csms:
                logger.info("No assignments to update metrics for")
                return

            # Format CSM names for SQL
            csm_names_str = "','".join(assigned_csms)

            # Query to get BEFORE state (existing portfolios)
            before_query = f"""
            WITH csm_before AS (
                SELECT
                    responsible_csm as csm_name,
                    COUNT(*) as total_accounts_before,
                    SUM(CASE WHEN CORE_HEALTH_SCORE_COLOR = 'Red' THEN 1 ELSE 0 END) as red_before,
                    SUM(CASE WHEN CORE_HEALTH_SCORE_COLOR = 'Yellow' THEN 1 ELSE 0 END) as yellow_before,
                    SUM(CASE WHEN CORE_HEALTH_SCORE_COLOR = 'Green' THEN 1 ELSE 0 END) as green_before
                FROM DSV_WAREHOUSE.PUBLIC_DATA_SETS.SALESFORCE_ACCOUNT_ALL_W_CHURN_SCORE_V
                WHERE responsible_csm IN ('{csm_names_str}')
                    AND REAL_ESTATE_MARKET = 'Residential'
                    AND MANAGEMENT_LEVEL = 'Corporate Accounts'
                GROUP BY responsible_csm
            )
            SELECT * FROM csm_before
            ORDER BY csm_name
            """

            # Query to get NEW assignments with health segments
            new_assignments_query = f"""
            WITH new_assignments AS (
                SELECT
                    a.csm_name,
                    a.account_id,
                    COALESCE(s.CORE_HEALTH_SCORE_COLOR, 'Unknown') as health_segment
                FROM {self.assignments_table} a
                LEFT JOIN DSV_WAREHOUSE.PUBLIC_DATA_SETS.SALESFORCE_ACCOUNT_ALL_W_CHURN_SCORE_V s
                    ON a.account_id = s.account_id_ob
                WHERE a.csm_name IN ('{csm_names_str}')
                    AND a.assignment_date >= DATEADD(minute, -10, CURRENT_TIMESTAMP())
            ),
            new_counts AS (
                SELECT
                    csm_name,
                    COUNT(*) as new_accounts,
                    SUM(CASE WHEN health_segment = 'Red' THEN 1 ELSE 0 END) as new_red,
                    SUM(CASE WHEN health_segment = 'Yellow' THEN 1 ELSE 0 END) as new_yellow,
                    SUM(CASE WHEN health_segment = 'Green' THEN 1 ELSE 0 END) as new_green
                FROM new_assignments
                GROUP BY csm_name
            )
            SELECT * FROM new_counts
            ORDER BY csm_name
            """

            # Execute queries
            before_df = self.execute_query(before_query)
            new_df = self.execute_query(new_assignments_query)

            # Merge and calculate AFTER metrics
            logger.info("\nCSM Portfolio Changes:")
            logger.info("-" * 80)
            logger.info(f"{'CSM Name':<25} {'Metric':<15} {'Before':<20} {'New Assigned':<20} {'After':<20}")
            logger.info("-" * 80)

            for csm in assigned_csms:
                # Get before data - handle case sensitivity
                before_row = None
                if not before_df.empty:
                    # Convert column names to uppercase for consistent access
                    before_df.columns = [col.upper() for col in before_df.columns]
                    before_row = before_df[before_df['CSM_NAME'] == csm]

                if before_row is None or before_row.empty:
                    total_before = 0
                    red_before = 0
                    yellow_before = 0
                    green_before = 0
                else:
                    before_row = before_row.iloc[0]
                    total_before = int(before_row.get('TOTAL_ACCOUNTS_BEFORE', 0))
                    red_before = int(before_row.get('RED_BEFORE', 0))
                    yellow_before = int(before_row.get('YELLOW_BEFORE', 0))
                    green_before = int(before_row.get('GREEN_BEFORE', 0))

                # Get new assignments data - handle case sensitivity
                new_row = None
                if not new_df.empty:
                    # Convert column names to uppercase for consistent access
                    new_df.columns = [col.upper() for col in new_df.columns]
                    new_row = new_df[new_df['CSM_NAME'] == csm]

                if new_row is None or new_row.empty:
                    new_accounts = 0
                    new_red = 0
                    new_yellow = 0
                    new_green = 0
                else:
                    new_row = new_row.iloc[0]
                    new_accounts = int(new_row.get('NEW_ACCOUNTS', 0))
                    new_red = int(new_row.get('NEW_RED', 0))
                    new_yellow = int(new_row.get('NEW_YELLOW', 0))
                    new_green = int(new_row.get('NEW_GREEN', 0))

                # Calculate after
                total_after = total_before + new_accounts
                red_after = red_before + new_red
                yellow_after = yellow_before + new_yellow
                green_after = green_before + new_green

                # Display results
                logger.info(f"{csm:<25} {'Total Accounts':<15} {total_before:<20} +{new_accounts:<19} {total_after:<20}")
                logger.info(f"{'':<25} {'Red Accounts':<15} {red_before} ({100*red_before/max(total_before,1):.1f}%){'':^11} +{new_red:<19} {red_after} ({100*red_after/max(total_after,1):.1f}%)")
                logger.info(f"{'':<25} {'Yellow Accounts':<15} {yellow_before} ({100*yellow_before/max(total_before,1):.1f}%){'':^11} +{new_yellow:<19} {yellow_after} ({100*yellow_after/max(total_after,1):.1f}%)")
                logger.info(f"{'':<25} {'Green Accounts':<15} {green_before} ({100*green_before/max(total_before,1):.1f}%){'':^11} +{new_green:<19} {green_after} ({100*green_after/max(total_after,1):.1f}%)")

                # Check if approaching limit
                max_limit = self.limits.get('residential_corporate', {}).get('max_accounts_per_csm', 105)
                if total_after >= max_limit - 5:
                    logger.warning(f"  ⚠️ {csm} now has {total_after} accounts (approaching limit of {max_limit})")

                logger.info("-" * 80)

            logger.info("\n✅ Portfolio metrics updated successfully")
            logger.info("="*80)

        except Exception as e:
            logger.error(f"Failed to display updated portfolio metrics: {str(e)}")
            # Don't fail the main process if metrics display fails
            pass

    def run(self, test_limit=None):
        """Main execution method

        Args:
            test_limit: Optional limit on number of accounts to process (for testing)
        """
        logger.info("Starting CSM Routing Automation")
        if test_limit:
            logger.info(f"TEST MODE: Limited to {test_limit} account(s)")

        # Connect to Snowflake
        if not self.connect_snowflake():
            logger.error("Failed to connect to Snowflake. Exiting.")
            return

        try:
            # Get accounts needing CSM
            needs_csm_df = self.get_needs_csm_accounts(limit=test_limit)

            if needs_csm_df.empty:
                logger.info("No accounts need CSM assignment at this time")
                return

            # Filter for Residential Corporate accounts only (as per requirements)
            # Enrich the data first to get segment information
            enriched_df = self.enrich_account_data(needs_csm_df)

            # Filter for Residential Corporate
            # Temporarily removed Residential Corporate filter for testing - processing ALL accounts
            # resi_corp_df = enriched_df[
            #     (enriched_df['segment'] == 'Residential') &
            #     (enriched_df['account_level'] == 'Corporate')
            # ]
            resi_corp_df = enriched_df  # Process ALL accounts needing CSM

            # FIXED: Ensure no duplicate account_ids before final processing
            if not resi_corp_df.empty:
                original_count = len(resi_corp_df)
                resi_corp_df = resi_corp_df.drop_duplicates(subset=['account_id'], keep='first')
                if original_count > len(resi_corp_df):
                    logger.info(f"Removed {original_count - len(resi_corp_df)} duplicate accounts")

            if resi_corp_df.empty:
                logger.info("No accounts need assignment")
                return

            logger.info(f"Processing {len(resi_corp_df)} unique accounts (ALL SEGMENTS - filter disabled for testing)")

            # Get current CSM books with minimum account threshold from configuration
            min_accounts = self.limits.get('residential_corporate', {}).get('min_accounts_for_eligibility', 5)
            csm_books = self.get_current_csm_books(min_account_threshold=min_accounts)

            # Create recommendations table if it doesn't exist
            self.create_recommendations_table()

            assignments = {}
            max_retries = 2  # Maximum number of retries based on LLM feedback
            retry_count = 0
            llm_feedback = None
            run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

            while retry_count <= max_retries:
                # Clear previous assignments if this is a retry
                if retry_count > 0:
                    logger.info(f"Retry {retry_count}: Re-running assignment optimization based on LLM feedback")
                    assignments = {}

                # Get recently assigned CSMs to exclude (smart exclusion based on batch size)
                recently_assigned = self.get_recently_assigned_csms(
                    current_batch_assignments=assignments,
                    num_accounts_processing=len(resi_corp_df)
                )

                # Check if all eligible CSMs would be excluded
                eligible_count = len([csm for csm in self.eligible_csm_list if csm in csm_books])
                excluded_count = len(recently_assigned)

                if excluded_count >= eligible_count and eligible_count > 0:
                    logger.warning(f"⚠️  All {eligible_count} eligible CSMs would be excluded. Bypassing exclusion to allow assignment.")
                    recently_assigned = []  # Reset exclusion list

                # Process based on batch size
                if len(resi_corp_df) == 1:
                    # Single account - use optimized best fit
                    logger.info("Processing single account with optimized best fit")
                    account = resi_corp_df.iloc[0]
                    csm, score, top_alternatives = self.assign_single_account_optimized(account, csm_books, excluded_csms=recently_assigned)
                    if csm:
                        assignments[account['account_id']] = csm
                        # Store alternatives for LLM review
                        if not hasattr(self, 'assignment_alternatives'):
                            self.assignment_alternatives = {}
                        self.assignment_alternatives[account['account_id']] = top_alternatives
                        # Update the csm_books for next iteration
                        csm_books[csm]['count'] += 1
                        csm_books[csm]['total_neediness'] += account.get('neediness_score', 0)
                        csm_books[csm]['total_revenue'] += account.get('revenue', 0)
                        csm_books[csm]['total_tad'] += account.get('tad_score', 0)
                else:
                    # Multiple accounts - use PuLP optimization
                    logger.info(f"Processing {len(resi_corp_df)} accounts with PuLP optimization")
                    assignments = self.optimize_batch_with_pulp(resi_corp_df, csm_books, excluded_csms=recently_assigned)

                    # Fallback: If PuLP optimization fails, process accounts one by one
                    if not assignments:
                        logger.warning("PuLP optimization failed or returned no assignments. Falling back to individual assignment...")
                        assignments = {}
                        for _, account in resi_corp_df.iterrows():
                            # Update exclusion list to include CSMs already assigned in this batch
                            current_exclusions = recently_assigned.copy()
                            if assignments:
                                current_exclusions.extend(list(set(assignments.values())))

                            csm, score, top_alternatives = self.assign_single_account_optimized(account, csm_books, excluded_csms=current_exclusions)
                            if csm:
                                assignments[account['account_id']] = csm
                                # Store alternatives for LLM review
                                if not hasattr(self, 'assignment_alternatives'):
                                    self.assignment_alternatives = {}
                                self.assignment_alternatives[account['account_id']] = top_alternatives
                                # Update the csm_books for next account
                                csm_books[csm]['count'] += 1
                                csm_books[csm]['total_neediness'] += account.get('neediness_score', 0)
                                csm_books[csm]['total_revenue'] += account.get('revenue', 0)
                                csm_books[csm]['total_tad'] += account.get('tad_score', 0)
                                logger.info(f"Assigned {account['account_id']} to {csm} (fallback mode)")
                            else:
                                logger.warning(f"Could not assign account {account['account_id']} - all CSMs at capacity")

                # Review assignments with LLM
                logger.info(f"DEBUG: Assignments ready for review: {len(assignments) if assignments else 0}")
                logger.info(f"DEBUG: Claude client available: {self.claude_client is not None}")
                if assignments and self.claude_client:
                    logger.info("Reviewing assignments with Claude Sonnet...")
                    should_rerun, llm_feedback, revised_assignments = self.review_assignments_with_llm(
                        assignments, resi_corp_df, csm_books, excluded_csms=recently_assigned
                    )

                    if should_rerun and retry_count < max_retries:
                        logger.warning(f"LLM recommends rerunning optimization. Feedback: {llm_feedback}")

                        # If LLM provided revised assignments, use them instead of rerunning
                        if revised_assignments and revised_assignments != assignments:
                            logger.info(f"LLM provided revised assignments: {revised_assignments}")
                            # Revert old assignments from csm_books
                            if assignments:
                                for account_id, old_csm in assignments.items():
                                    account = resi_corp_df[resi_corp_df['account_id'] == account_id].iloc[0]
                                    csm_books[old_csm]['count'] -= 1
                                    csm_books[old_csm]['total_neediness'] -= account.get('neediness_score', 0)
                                    csm_books[old_csm]['total_revenue'] -= account.get('revenue', 0)
                                    csm_books[old_csm]['total_tad'] -= account.get('tad_score', 0)

                            # Apply new assignments to csm_books
                            for account_id, new_csm in revised_assignments.items():
                                account = resi_corp_df[resi_corp_df['account_id'] == account_id].iloc[0]
                                csm_books[new_csm]['count'] += 1
                                csm_books[new_csm]['total_neediness'] += account.get('neediness_score', 0)
                                csm_books[new_csm]['total_revenue'] += account.get('revenue', 0)
                                csm_books[new_csm]['total_tad'] += account.get('tad_score', 0)

                            assignments = revised_assignments
                            break  # Exit retry loop with LLM's suggested assignments
                        else:
                            # No revised assignments, retry optimization
                            retry_count += 1
                            # Revert csm_books changes if single account
                            if len(resi_corp_df) == 1 and assignments:
                                for account_id, csm in assignments.items():
                                    account = resi_corp_df[resi_corp_df['account_id'] == account_id].iloc[0]
                                    csm_books[csm]['count'] -= 1
                                    csm_books[csm]['total_neediness'] -= account.get('neediness_score', 0)
                                    csm_books[csm]['total_revenue'] -= account.get('revenue', 0)
                                    csm_books[csm]['total_tad'] -= account.get('tad_score', 0)
                            continue  # Retry the optimization
                    else:
                        if should_rerun:
                            logger.warning(f"LLM recommends rerunning but max retries reached. Proceeding with current assignments.")
                        else:
                            logger.info(f"LLM approved assignments. Feedback: {llm_feedback}")
                        break  # Exit the retry loop
                else:
                    # No LLM review or no assignments
                    break

            # Update assignments in Snowflake
            logger.info(f"DEBUG: About to update assignments in Snowflake. Assignments: {len(assignments) if assignments else 0}")
            if assignments:
                logger.info(f"DEBUG: Calling update_assignments_in_snowflake with {len(assignments)} assignments")
                success = self.update_assignments_in_snowflake(assignments, llm_feedback)
                logger.info(f"DEBUG: update_assignments_in_snowflake returned: {success}")
                if success:
                    logger.info(f"Successfully assigned {len(assignments)} accounts to CSMs")

                    # Log summary
                    for account_id, csm in assignments.items():
                        logger.info(f"  - Account {account_id} -> CSM {csm}")

                    # Log LLM feedback if available
                    if llm_feedback:
                        logger.info(f"LLM Feedback stored: {llm_feedback}")
            else:
                logger.warning("No valid CSM assignments could be made")

            # Generate balance report
            self.generate_balance_report(csm_books)

        except Exception as e:
            logger.error(f"Error during execution: {str(e)}")
            raise
        finally:
            if self.snowflake_conn:
                self.snowflake_conn.close()
                logger.info("Closed Snowflake connection")

    def generate_balance_report(self, csm_books: Dict):
        """Generate a report on current book balance"""
        imbalance = self.calculate_book_imbalance(csm_books)

        logger.info("=== CSM Book Balance Report ===")
        logger.info(f"Account Count Std Dev: {imbalance['count_std']:.2f}")
        logger.info(f"Neediness Score Std Dev: {imbalance['neediness_std']:.2f}")
        logger.info(f"Revenue Std Dev: ${imbalance['revenue_std']:,.2f}")
        logger.info(f"TAD Score Std Dev: {imbalance['tad_std']:.2f}")

        # Check if rebalancing might be needed
        mean_count = np.mean([b['count'] for b in csm_books.values()])
        if imbalance['count_std'] > mean_count * 0.2:
            logger.warning("Account count variance exceeds 20% of mean - consider manual rebalancing")

        # Log recent assignment history
        recent_1h = sum(1 for a in self.assignment_history
                       if a['timestamp'] >= datetime.now() - timedelta(hours=1))
        recent_24h = len(self.assignment_history)
        logger.info(f"Recent assignments: {recent_1h} in last hour, {recent_24h} in last 24 hours")


def main():
    """Main entry point for the automation"""
    automation = CSMRoutingAutomation()

    # Run once or set up as scheduled job
    while True:
        try:
            automation.run()

            # Wait before next run (adjust as needed)
            # For production, consider using a proper scheduler like cron or Airflow
            wait_minutes = 15  # Run every 15 minutes
            logger.info(f"Waiting {wait_minutes} minutes before next run...")
            time.sleep(wait_minutes * 60)

        except KeyboardInterrupt:
            logger.info("Automation stopped by user")
            break
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            # Wait before retrying
            time.sleep(300)  # Wait 5 minutes before retry


if __name__ == "__main__":
    main()