-- Comprehensive Neediness Scoring Query for CSM Routing Automation
-- This is the main source of truth for all account data
-- Uses RESPONSIBLE_CSM_NAME for CSM assignments

WITH customer_data AS (
    SELECT
        main.* EXCLUDE (accountlevel),
        COALESCE(
            CASE
                WHEN parent_acc.parent_account_level = 'OneToMany' THEN 'Corporate'
                WHEN parent_acc.parent_account_level = 'MidMarket' THEN 'Corporate'
                WHEN parent_acc.parent_account_level IN ('SMB', 'Emerging') THEN 'Corporate'
                ELSE parent_acc.parent_account_level
            END,
            CASE
                WHEN acc_level.account_level = 'OneToMany' THEN 'Corporate'
                WHEN acc_level.account_level = 'MidMarket' THEN 'Corporate'
                WHEN acc_level.account_level IN ('SMB', 'Emerging') THEN 'Corporate'
                ELSE acc_level.account_level
            END
        ) AS "Account Level",

        -- Add this column in your SELECT statement
        CASE
            WHEN main.account_id IN (SELECT DISTINCT ultimate_parent_account_c FROM DSV_WAREHOUSE.PUBLIC.VW_SALESFORCE_ACCOUNT) THEN 1
            ELSE 0
        END AS Is_Parent_Account,

        COALESCE(
            parent_acc.parent_market_category_original,
            acc_level.market_category_original
        ) AS Segment,

        acc_level.market_category_original as Individual_Segment,
        acc.owner_id,
        acc.owner_name,
        acc.ultimate_parent_account_c,
        acc.ultimate_parent_account_name,

        -- Pro product calculations
        (main.direct_mail + main.dispatch_pro + main.marketing_pro + main.phones_pro + main.pb_pro + main.reputation) AS sum_pro_products,
        (main.direct_mail + main.dispatch_pro + main.marketing_pro + main.phones_pro + main.pb_pro + main.reputation) / 6 AS pro_product_penetration,

        -- Industry calculations
        CASE WHEN main.industry_new IN ('HVAC', 'Electrical', 'Plumbing', 'Garage Door', 'Chimney') THEN 'Standard' ELSE 'Non-Standard' END AS IndustryClass,
        CASE WHEN main.industry_new IN ('HVAC', 'Electrical', 'Plumbing', 'Garage Door', 'Chimney') THEN 0 ELSE 1 END AS "Industry Rating",

        -- Scores and ratings
        CASE
            WHEN main.tadscore <= 75 THEN 3
            WHEN main.tadscore > 75 AND tadscore <= 120 THEN 2
            WHEN main.tadscore > 120 AND tadscore <= 160 THEN 1
            WHEN main.tadscore > 160 THEN 2
        END AS tadscore_rating,

        CASE
            WHEN main.HEALTHSCORE = 'Red' THEN 3
            WHEN main.HEALTHSCORE = 'Yellow' THEN 2
            WHEN main.HEALTHSCORE = 'Green' THEN 0
        END AS HEALTHSCORE_Rating,

        CASE
            WHEN (main.direct_mail + main.dispatch_pro + main.marketing_pro + main.phones_pro + main.pb_pro + main.reputation) = 0 THEN 0
            WHEN (main.direct_mail + main.dispatch_pro + main.marketing_pro + main.phones_pro + main.pb_pro + main.reputation) = 1 THEN 1
            WHEN (main.direct_mail + main.dispatch_pro + main.marketing_pro + main.phones_pro + main.pb_pro + main.reputation) = 2 THEN 1
            WHEN (main.direct_mail + main.dispatch_pro + main.marketing_pro + main.phones_pro + main.pb_pro + main.reputation) BETWEEN 3 AND 5 THEN 2
            WHEN (main.direct_mail + main.dispatch_pro + main.marketing_pro + main.phones_pro + main.pb_pro + main.reputation) > 5 THEN 3
        END AS ProProduct_Rating,

        -- Triage data
        triage_cases.total_triage_cases_last_120_days,
        CASE WHEN triage_cases.total_triage_cases_last_120_days > 0 THEN 1 ELSE 0 END AS Triage_Rating,

        -- Email metrics
        email_freq.email_count,
        email_freq.email_count / ((DATEDIFF(day, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) -
            ((DATEDIFF(week, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) * 2))) / 5) AS Emails_per_week,

        CASE
            WHEN email_freq.email_count / ((DATEDIFF(day, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) -
                ((DATEDIFF(week, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) * 2))) / 5) >= 3 THEN '1) Daily'
            WHEN email_freq.email_count / ((DATEDIFF(day, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) -
                ((DATEDIFF(week, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) * 2))) / 5) BETWEEN 0.9 AND 3 THEN '2) Weekly'
            WHEN email_freq.email_count / ((DATEDIFF(day, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) -
                ((DATEDIFF(week, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) * 2))) / 5) BETWEEN 0.5 AND 0.9 THEN '3) Bi-Weekly'
            WHEN email_freq.email_count / ((DATEDIFF(day, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) -
                ((DATEDIFF(week, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) * 2))) / 5) BETWEEN 0.2 AND 0.5 THEN '4) Monthly'
            WHEN email_freq.email_count / ((DATEDIFF(day, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) -
                ((DATEDIFF(week, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) * 2))) / 5) BETWEEN 0.01 AND 0.2 THEN '5) Quarterly'
            WHEN (email_freq.email_count / ((DATEDIFF(day, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) -
                ((DATEDIFF(week, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) * 2))) / 5) = 0)
                OR (email_freq.email_count / ((DATEDIFF(day, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) -
                ((DATEDIFF(week, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) * 2))) / 5) IS NULL) THEN '6) No Email last 120 days'
        END AS Email_Freq,

        -- Call metrics
        call_freq.call_count,
        call_freq.call_count / ((DATEDIFF(day, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) -
            ((DATEDIFF(week, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) * 2))) / 5) AS Calls_per_week,

        CASE
            WHEN call_freq.call_count / ((DATEDIFF(day, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) -
                ((DATEDIFF(week, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) * 2))) / 5) >= 3 THEN '1) Daily'
            WHEN call_freq.call_count / ((DATEDIFF(day, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) -
                ((DATEDIFF(week, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) * 2))) / 5) BETWEEN 0.9 AND 3 THEN '2) Weekly'
            WHEN call_freq.call_count / ((DATEDIFF(day, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) -
                ((DATEDIFF(week, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) * 2))) / 5) BETWEEN 0.5 AND 0.9 THEN '3) Bi-Weekly'
            WHEN call_freq.call_count / ((DATEDIFF(day, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) -
                ((DATEDIFF(week, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) * 2))) / 5) BETWEEN 0.2 AND 0.5 THEN '4) Monthly'
            WHEN call_freq.call_count / ((DATEDIFF(day, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) -
                ((DATEDIFF(week, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) * 2))) / 5) BETWEEN 0.01 AND 0.2 THEN '5) Quarterly'
            WHEN (call_freq.call_count / ((DATEDIFF(day, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) -
                ((DATEDIFF(week, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) * 2))) / 5) = 0)
                OR (call_freq.call_count / ((DATEDIFF(day, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) -
                ((DATEDIFF(week, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) * 2))) / 5) IS NULL) THEN '6) No Calls last 120 days'
        END AS Calls_Freq,

        -- Support metrics and ratings
        supp_cases.support_case_count,
        CASE WHEN supp_cases.support_case_count > 12 THEN 1 ELSE 0 END AS Support_rating,

        -- Additional data
        time_zone.time_zone,
        CASE WHEN churn_risk.churn_stage IS NULL THEN 'Not at risk' ELSE churn_risk.churn_stage END AS churn_stage,

        -- CSM info
        cust_hist.responsible_csm_name,
        cust_hist.responsible_csm_manager_name,
        cust_hist.PRO_SPECIALIST_NAME,
        cust_hist.CORE_HEALTH_SCORE,
        cust_hist.CORE_HEALTH_SCORE_color,
        cust_hist.ACTIVE_MANAGED_TECH_COUNT AS "MTs+MIs",
        cust_hist.TENURE_IN_SUCCESS_MONTHS AS "Months in Success",

        -- Threshold calculation
        CASE
            WHEN (CASE
                WHEN cust_hist.RESPONSIBLE_CSM_TEAM LIKE '%C&C%' THEN 'Commercial & Construction'
                WHEN cust_hist.RESPONSIBLE_CSM_TEAM IS NULL AND cust_hist.MARKET_CATEGORY IN ('Commercial', 'Construction') THEN 'Commercial & Construction'
                ELSE 'Residential'
            END) = 'Commercial & Construction' AND cust_hist.TENURE_IN_SUCCESS_MONTHS < 12 THEN 100
            WHEN (CASE
                WHEN cust_hist.RESPONSIBLE_CSM_TEAM LIKE '%C&C%' THEN 'Commercial & Construction'
                WHEN cust_hist.RESPONSIBLE_CSM_TEAM IS NULL AND cust_hist.MARKET_CATEGORY IN ('Commercial', 'Construction') THEN 'Commercial & Construction'
                ELSE 'Residential'
            END) = 'Commercial & Construction' AND cust_hist.TENURE_IN_SUCCESS_MONTHS >= 12 THEN 125
            WHEN (CASE
                WHEN cust_hist.RESPONSIBLE_CSM_TEAM LIKE '%C&C%' THEN 'Commercial & Construction'
                WHEN cust_hist.RESPONSIBLE_CSM_TEAM IS NULL AND cust_hist.MARKET_CATEGORY IN ('Commercial', 'Construction') THEN 'Commercial & Construction'
                ELSE 'Residential'
            END) = 'Residential' AND cust_hist.TENURE_IN_SUCCESS_MONTHS < 3 THEN 100
            WHEN (CASE
                WHEN cust_hist.RESPONSIBLE_CSM_TEAM LIKE '%C&C%' THEN 'Commercial & Construction'
                WHEN cust_hist.RESPONSIBLE_CSM_TEAM IS NULL AND cust_hist.MARKET_CATEGORY IN ('Commercial', 'Construction') THEN 'Commercial & Construction'
                ELSE 'Residential'
            END) = 'Residential' AND cust_hist.TENURE_IN_SUCCESS_MONTHS >= 3 THEN 125
        END AS "TAD Threshold",

        cust_hist.MARKET_CATEGORY,
        related_tenants_main.related_tenants,
        pro_bundle.customer_trade_classification,
        pro_bundle.pro_product_category,
        pro_bundle.pro_product_category_original,
        fgate.pro_product_category_fgate,

        -- Financial data
        mrr.core_mrr,
        mrr.Total_Pro_Mrr,
        mrr.TOTAL_MRR,
        mrr.marketing_pro_mrr,
        mrr.contact_center_pro_mrr,
        mrr.phones_pro_mrr,
        mrr.scheduling_pro_mrr,
        mrr.dispatch_pro_mrr,
        mrr.fleet_pro_mrr,
        mrr.pricebook_pro_mrr,
        mrr.sales_pro_mrr,
        mrr.billed_month,

        -- Rating calculations
        round(1 / LEFT(
            CASE
                WHEN email_freq.email_count / ((DATEDIFF(day, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) -
                    ((DATEDIFF(week, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) * 2))) / 5) >= 3 THEN '1) Daily'
                WHEN email_freq.email_count / ((DATEDIFF(day, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) -
                    ((DATEDIFF(week, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) * 2))) / 5) BETWEEN 0.9 AND 3 THEN '2) Weekly'
                WHEN email_freq.email_count / ((DATEDIFF(day, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) -
                    ((DATEDIFF(week, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) * 2))) / 5) BETWEEN 0.5 AND 0.9 THEN '3) Bi-Weekly'
                WHEN email_freq.email_count / ((DATEDIFF(day, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) -
                    ((DATEDIFF(week, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) * 2))) / 5) BETWEEN 0.2 AND 0.5 THEN '4) Monthly'
                WHEN email_freq.email_count / ((DATEDIFF(day, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) -
                    ((DATEDIFF(week, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) * 2))) / 5) BETWEEN 0.01 AND 0.2 THEN '5) Quarterly'
                WHEN (email_freq.email_count / ((DATEDIFF(day, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) -
                    ((DATEDIFF(week, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) * 2))) / 5) = 0)
                    OR (email_freq.email_count / ((DATEDIFF(day, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) -
                    ((DATEDIFF(week, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) * 2))) / 5) IS NULL) THEN '6) No Email last 120 days'
            END::string, 1)::float, 2) AS Email_rating,

        round(1 / LEFT(
            CASE
                WHEN call_freq.call_count / ((DATEDIFF(day, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) -
                    ((DATEDIFF(week, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) * 2))) / 5) >= 3 THEN '1) Daily'
                WHEN call_freq.call_count / ((DATEDIFF(day, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) -
                    ((DATEDIFF(week, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) * 2))) / 5) BETWEEN 0.9 AND 3 THEN '2) Weekly'
                WHEN call_freq.call_count / ((DATEDIFF(day, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) -
                    ((DATEDIFF(week, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) * 2))) / 5) BETWEEN 0.5 AND 0.9 THEN '3) Bi-Weekly'
                WHEN call_freq.call_count / ((DATEDIFF(day, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) -
                    ((DATEDIFF(week, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) * 2))) / 5) BETWEEN 0.2 AND 0.5 THEN '4) Monthly'
                WHEN call_freq.call_count / ((DATEDIFF(day, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) -
                    ((DATEDIFF(week, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) * 2))) / 5) BETWEEN 0.01 AND 0.2 THEN '5) Quarterly'
                WHEN (call_freq.call_count / ((DATEDIFF(day, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) -
                    ((DATEDIFF(week, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) * 2))) / 5) = 0)
                    OR (call_freq.call_count / ((DATEDIFF(day, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) -
                    ((DATEDIFF(week, DATEADD(day, -120, CURRENT_DATE), CURRENT_DATE) * 2))) / 5) IS NULL) THEN '6) No Calls last 120 days'
            END::string, 1)::float, 2) AS Calls_rating
    FROM
        dsv_warehouse.public.agg_bireport_squad_tenant main

    -- Triage cases
    LEFT JOIN (
        SELECT
            account_id,
            COUNT(1) AS total_triage_cases_last_120_days
        FROM DSV_WAREHOUSE.PUBlIC.VW_SALESFORCE_CASE
        WHERE 1=1
            AND initial_case_record_type_c = 'ST Internal - Triage Team'
            AND is_deleted = FALSE
            AND created_date > current_date - INTERVAL '120 day'
        GROUP BY account_id
    ) triage_cases ON main.account_id = triage_cases.account_id

    -- Email frequency
    LEFT JOIN (
        SELECT
            account_id,
            COUNT(1) AS email_count
        FROM (
            SELECT account_id, 1 AS email
            FROM DSV_WAREHOUSE.PUBLIC.vw_salesforce_case
            WHERE 1=1
                AND created_date > current_date - INTERVAL '120 day'
                AND origin = 'CSM Team - Email'

            UNION ALL

            SELECT account_id, 1 AS email
            FROM DSV_WAREHOUSE.PUBLIC.VW_GAINSIGHT_CSM_ACTIVITY
            WHERE activity_type_new = 'Email'
                AND activity_date > current_date - INTERVAL '120 day'
        )
        GROUP BY account_id
    ) email_freq ON main.account_id = email_freq.account_id

    -- Call frequency
    LEFT JOIN (
        SELECT
            account_id,
            COUNT(1) AS call_count
        FROM DSV_WAREHOUSE.PUBLIC.VW_GAINSIGHT_CSM_ACTIVITY
        WHERE activity_type_new = 'Call'
            AND activity_date > current_date - INTERVAL '120 day'
        GROUP BY account_id
    ) call_freq ON main.account_id = call_freq.account_id

    -- Support cases
    LEFT JOIN (
        SELECT
            account_id,
            COUNT(1) AS support_case_count
        FROM DSV_WAREHOUSE.PUBLIC.vw_salesforce_case
        WHERE 1=1
            AND LOWER(initial_case_record_type_c) LIKE '%support%'
            AND created_date > current_date - INTERVAL '120 day'
            AND is_deleted = FALSE
        GROUP BY account_id
    ) supp_cases ON main.account_id = supp_cases.account_id

    -- Timezone
    LEFT JOIN (
        SELECT
            account_id,
            MAX(time_zone) AS time_zone
        FROM DSV_WAREHOUSE.POST_SALES.VW_CUSTOMER_HISTORY_DAILY
        GROUP BY account_id, current_date
    ) time_zone ON main.account_id = time_zone.account_id

    -- Churn risk status
    LEFT JOIN (
        SELECT
            account_id,
            MIN(churn_stage) AS churn_stage
        FROM (
            SELECT DISTINCT
                c.account_id,
                CASE
                    WHEN c.status IN ('Working to Save','Executive Escalation','New') THEN 'Churn - Working to Save'
                    ELSE '0 - Churn - Confirmed'
                END AS churn_stage
            FROM DSV_WAREHOUSE.PUBLIC.VW_SALESFORCE_CASE c
            WHERE c.account_id IS NOT NULL
        )
        GROUP BY account_id
    ) churn_risk ON churn_risk.account_id = main.account_id

    -- Customer history
    LEFT JOIN (
        SELECT *
        FROM DSV_WAREHOUSE.POST_SALES.VW_CUSTOMER_HISTORY_DAILY
        WHERE is_current = TRUE
            AND is_customer = TRUE
    ) cust_hist ON cust_hist.account_id = main.account_id

    -- Related tenants
    LEFT JOIN (
        SELECT
            account_id,
            related_tenants_sub.related_tenants
        FROM DSV_WAREHOUSE.POST_SALES.VW_CUSTOMER_HISTORY_DAILY cust_hist_tenants
        LEFT JOIN (
            SELECT
                SFDC_ULTIMATE_PARENT_ID,
                COUNT(1)-1 AS related_tenants
            FROM DSV_WAREHOUSE.POST_SALES.VW_CUSTOMER_HISTORY_DAILY
            WHERE calendar_date = (
                SELECT MAX(calendar_date)
                FROM DSV_WAREHOUSE.POST_SALES.VW_CUSTOMER_HISTORY_DAILY
            )
            GROUP BY sfdc_ultimate_parent_id
        ) related_tenants_sub ON cust_hist_tenants.SFDC_ULTIMATE_PARENT_ID = related_tenants_sub.SFDC_ULTIMATE_PARENT_ID
        WHERE calendar_date = (
            SELECT MAX(calendar_date)
            FROM DSV_WAREHOUSE.POST_SALES.VW_CUSTOMER_HISTORY_DAILY
        )
    ) related_tenants_main ON related_tenants_main.account_id = main.account_id

    -- Pro bundle
    LEFT JOIN (
        SELECT
            account_id,
            account_name,
            tenant_name,
            customer_status,
            customer_trade_classification,
            has_dispatch_pro_fg,
            has_marketing_pro_fg,
            has_scheduling_pro_fg,
            has_fleet_pro_fg,
            has_phones_pro_fg,
            has_pricebook_pro_fg,
            has_sales_pro_fg,
            has_contact_center_pro_fg,
            CONCAT(
                CASE WHEN has_dispatch_pro_fg = 1 THEN 'DI' ELSE '' END,
                CASE WHEN has_fleet_pro_fg = 1 THEN 'FL' ELSE '' END,
                CASE WHEN has_marketing_pro_fg = 1 THEN 'MA' ELSE '' END,
                CASE WHEN has_phones_pro_fg = 1 THEN 'PH' ELSE '' END,
                CASE WHEN has_pricebook_pro_fg = 1 THEN 'PR' ELSE '' END,
                CASE WHEN has_sales_pro_fg = 1 THEN 'SA' ELSE '' END,
                CASE WHEN has_scheduling_pro_fg = 1 THEN 'SC' ELSE '' END,
                CASE WHEN has_contact_center_pro_fg = 1 THEN 'CC' ELSE '' END
            ) AS pro_product_category,
            CONCAT(
                CASE WHEN has_dispatch_pro_fg = 1 THEN 'D' ELSE '' END,
                CASE WHEN has_fleet_pro_fg = 1 THEN 'F' ELSE '' END,
                CASE WHEN has_marketing_pro_fg = 1 THEN 'M' ELSE '' END,
                CASE WHEN has_phones_pro_fg = 1 THEN 'H' ELSE '' END,
                CASE WHEN has_pricebook_pro_fg = 1 THEN 'P' ELSE '' END,
                CASE WHEN has_sales_pro_fg = 1 THEN 'A' ELSE '' END,
                CASE WHEN has_scheduling_pro_fg = 1 THEN 'S' ELSE '' END,
                CASE WHEN has_contact_center_pro_fg = 1 THEN 'C' ELSE '' END
            ) AS pro_product_category_original
        FROM DSV_WAREHOUSE.POST_SALES.VW_CUSTOMER_HISTORY_DAILY
        WHERE calendar_date = (
            SELECT MAX(calendar_date)
            FROM DSV_WAREHOUSE.POST_SALES.VW_CUSTOMER_HISTORY_DAILY
        )
    ) pro_bundle ON pro_bundle.account_id = main.account_id

    -- Feature gate
    LEFT JOIN (
        SELECT
            _tenant_id AS tenantid,
            CONCAT(
                CASE WHEN has_dispatch_pro_fg = 1 THEN 'D' ELSE '' END,
                CASE WHEN has_fleet_pro_fg = 1 THEN 'F' ELSE '' END,
                CASE WHEN has_marketing_pro_fg = 1 THEN 'M' ELSE '' END,
                CASE WHEN has_phones_pro_fg = 1 THEN 'H' ELSE '' END,
                CASE WHEN has_pricebook_pro_fg = 1 THEN 'P' ELSE '' END,
                CASE WHEN has_sales_pro_fg = 1 THEN 'A' ELSE '' END,
                CASE WHEN has_scheduling_pro_fg = 1 THEN 'S' ELSE '' END,
                CASE WHEN has_contact_center_pro_fg = 1 THEN 'C' ELSE '' END

            ) AS pro_product_category_fgate
        FROM (
            SELECT
                _tenant_id,
                MAX(CASE WHEN FG_NAME = 'Marketing' THEN 1 ELSE 0 END) AS has_marketing_pro_fg,
                MAX(CASE WHEN FG_NAME = 'SmartDispatchQueueV2ServiceEnabled' THEN 1 ELSE 0 END) AS has_dispatch_pro_fg,
                MAX(CASE WHEN FG_NAME = 'FleetProIntegration' THEN 1 ELSE 0 END) AS has_fleet_pro_fg,
                MAX(CASE WHEN FG_NAME = 'EnablePricebookPro' THEN 1 ELSE 0 END) AS has_pricebook_pro_fg,
                MAX(CASE WHEN FG_NAME = 'EnableSalesPro' THEN 1 ELSE 0 END) AS has_sales_pro_fg,
                MAX(CASE WHEN FG_NAME = 'EnableServiceTitanPhonesUser' THEN 1 ELSE 0 END) AS has_phones_pro_fg,
                MAX(CASE WHEN FG_NAME = 'SchedulingPro' THEN 1 ELSE 0 END) AS has_scheduling_pro_fg,
                MAX(CASE WHEN FG_NAME = 'EnableContactCenterPro' THEN 1 ELSE 0 END) AS has_contact_center_pro_fg
            FROM TENANT_DATA.FEATURE_GATE.FEATURE_GATE_SCD
            WHERE 1=1
                AND (FG_NAME IN (
                    'Marketing',
                    'SmartDispatchQueueV2ServiceEnabled',
                    'FleetProIntegration',
                    'EnablePricebookPro',
                    'EnableSalesPro',
                    'EnableServiceTitanPhonesUser',
                    'SchedulingPro',
                    'EnableContactCenterPro'
                ))
                AND FG_VALUE = 'True'
                AND (FROM_DATE <> TO_DATE OR TO_DATE IS NULL)
            GROUP BY _tenant_id
        )
    ) fgate ON main.tenantid = fgate.tenantid

    -- MRR data
    LEFT JOIN (
        SELECT
            report_date,
            ACCOUNT_ID,
            billed_month,
            core_mrr,
            total_pro_product_mrr AS Total_Pro_Mrr,
            TOTAL_MRR,
            core_sale_qty,
            core_billed_min_qty,
            total_pro_product_mrr,
            marketing_pro_mrr,
            contact_center_pro_mrr,
            phones_pro_mrr,
            scheduling_pro_mrr,
            dispatch_pro_mrr,
            fleet_pro_mrr,
            pricebook_pro_mrr,
            sales_pro_mrr
        FROM dsv_warehouse.post_sales.al_final_bookofbusiness_temp
        WHERE report_date = (
            SELECT MAX(report_date)
            FROM dsv_warehouse.post_sales.al_final_bookofbusiness_temp
        )
    ) mrr ON mrr.account_id = main.account_id

    -- Account level
    LEFT JOIN (
        SELECT
            account_id,
            account_level,
            market_category_original
        FROM dsv_warehouse.public.fact_customer_segmentation_latest
        WHERE calendar_date = (
            SELECT MAX(calendar_date)
            FROM dsv_warehouse.public.fact_customer_segmentation_latest
        )
        AND account_level IS NOT NULL
    ) acc_level ON acc_level.account_id = main.account_id

    -- get parent account level
    LEFT JOIN (
        SELECT
            a.id AS account_id,
            p.account_level AS parent_account_level,
            p.market_category_original AS parent_market_category_original
        FROM DSV_WAREHOUSE.PUBLIC.VW_SALESFORCE_ACCOUNT a
        JOIN dsv_warehouse.public.fact_customer_segmentation_latest p
            ON a.ultimate_parent_account_c = p.account_id
        WHERE p.calendar_date = (
            SELECT MAX(calendar_date)
            FROM dsv_warehouse.public.fact_customer_segmentation_latest
        )
    ) parent_acc ON main.account_id = parent_acc.account_id

    left join (
        select a.id, a.ultimate_parent_account_c, a.ultimate_parent_account_name, a.owner_id, workday.preferred_full_name owner_name
        from DSV_WAREHOUSE.PUBLIC.VW_SALESFORCE_ACCOUNT a

        left join DSV_WAREHOUSE.PUBLIC.VW_SALESFORCE_USER user
        on user.id = a.owner_id

        left join (
            select preferred_full_name, job_title
            from DSV_WAREHOUSE.PUBLIC.FACT_WDAY_EMPLOYEE_WEEKLY_HISTORY
            where 1=1
                and job_title ilike '%enterprise customer sales%'
                and lower(job_title) not like '%manager%'
                and active_status = TRUE
                and week_end_date in (select max(week_end_date) from DSV_WAREHOUSE.PUBLIC.FACT_WDAY_EMPLOYEE_WEEKLY_HISTORY)
        ) workday
        on user.name = workday.preferred_full_name
    ) acc ON main.account_id = acc.id
),

final_customer_data AS (
    SELECT
        account_id,
        tenantname,
        tenantid,
        ultimate_parent_account_c,
        ultimate_parent_account_name,
        owner_id,
        owner_name,
        RESPONSIBLE_CSM_NAME AS "Responsible CSM",
        RESPONSIBLE_CSM_MANAGER_NAME AS "Manager",
        PRO_SPECIALIST_NAME,
        Segment,
        Individual_Segment,
        "Account Level",
        "Months in Success",
        tenantstatus AS "Customer Status",
        MARKET_CATEGORY AS "Market Category",
        customer_trade_classification,
        industry_new,
        IndustryClass AS Industry,
        "Industry Rating",
        "MTs+MIs",
        tadscore AS "TAD Score",
        "TAD Threshold",
        CASE
            WHEN tadscore >= "TAD Threshold" THEN 0
            ELSE 1
        END AS "TAD Rating",
        tadscore_rating,
        CORE_HEALTH_SCORE AS "Health Score",
        CORE_HEALTH_SCORE_color AS "Health Segment",
        HEALTHSCORE_Rating,
        PRO_PRODUCT_PENETRATION AS "Product Penetration",
        SUM_PRO_PRODUCTS AS "Total Products LOE",
        ProProduct_Rating,
        TOTAL_TRIAGE_CASES_LAST_120_DAYS,
        Triage_Rating,
        support_case_count,
        Support_rating,
        EMAILS_PER_WEEK AS "Emails per week",
        EMAIL_FREQ AS "Email Freq",
        Email_rating,
        CALL_COUNT AS "Total Calls last 120 days",
        CALLS_PER_WEEK AS "Calls per week",
        CALLS_FREQ AS "Call Freq",
        Calls_rating,
        related_tenants AS "Total Related Tenants",
        CASE
            WHEN related_tenants > 2 THEN 1
            ELSE 0
        END AS "Tenant Count Rating",
        CASE
            WHEN COALESCE(related_tenants, 0) = 0 THEN 1
            WHEN Is_Parent_Account = 1 THEN 1
            ELSE 0
        END AS Is_Parent_Account,
        pro_product_category,
        pro_product_category_original,
        pro_product_category_fgate,
        core_mrr,
        Total_Pro_Mrr,
        TOTAL_MRR,
        marketing_pro_mrr,
        contact_center_pro_mrr,
        phones_pro_mrr,
        scheduling_pro_mrr,
        dispatch_pro_mrr,
        fleet_pro_mrr,
        pricebook_pro_mrr,
        sales_pro_mrr,
        billed_month,
        churn_stage,
        time_zone,
        ROUND(
            "Industry Rating" 
            + HEALTHSCORE_Rating 
            + "TAD Rating" 
            + ProProduct_Rating 
            + Calls_rating 
            + Email_rating
            + Triage_Rating 
            + Support_rating 
            + (CASE WHEN related_tenants > 2 THEN 1 ELSE 0 END)
        ) AS "Neediness Score",
        CASE
            WHEN ROUND(
                "Industry Rating"  + HEALTHSCORE_Rating + "TAD Rating" +
                ProProduct_Rating + Calls_rating + Email_rating + Triage_Rating + 
                Support_rating + (CASE WHEN related_tenants > 2 THEN 1 ELSE 0 END)
            ) <= 4 THEN 'Low'
            WHEN ROUND(
                "Industry Rating"  + HEALTHSCORE_Rating + "TAD Rating" +
                ProProduct_Rating + Calls_rating + Email_rating + Triage_Rating + 
                Support_rating + (CASE WHEN related_tenants > 2 THEN 1 ELSE 0 END)
            ) BETWEEN 5 AND 7 THEN 'Medium'
            WHEN ROUND(
                "Industry Rating"  + HEALTHSCORE_Rating + "TAD Rating" +
                ProProduct_Rating + Calls_rating + Email_rating + Triage_Rating + 
                Support_rating + (CASE WHEN related_tenants > 2 THEN 1 ELSE 0 END)
            ) >= 8 THEN 'High'
            ELSE NULL
        END AS "Neediness Category"
    FROM customer_data
)

SELECT *
FROM final_customer_data
WHERE 1=1
     AND "Customer Status" IN ('Success', 'Onboarding', 'Live')