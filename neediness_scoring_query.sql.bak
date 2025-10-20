-- Comprehensive Neediness Scoring Query for CSM Routing Automation
-- This query calculates neediness scores and retrieves all relevant account details

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

        -- TAD Score rating
        CASE
            WHEN main.tadscore <= 75 THEN 3
            WHEN main.tadscore > 75 AND tadscore <= 120 THEN 2
            WHEN main.tadscore > 120 AND tadscore <= 160 THEN 1
            WHEN main.tadscore > 160 THEN 2
        END AS tadscore_rating,

        -- Health Score rating
        CASE
            WHEN main.HEALTHSCORE = 'Red' THEN 3
            WHEN main.HEALTHSCORE = 'Yellow' THEN 2
            WHEN main.HEALTHSCORE = 'Green' THEN 0
        END AS HEALTHSCORE_Rating,

        -- Pro Product rating
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

        -- TAD Threshold calculation
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

        -- Email and Call ratings
        round(1 / LEFT(Email_Freq::string, 1)::float, 2) AS Email_rating,
        round(1 / LEFT(Calls_Freq::string, 1)::float, 2) AS Calls_rating

    FROM
        dsv_warehouse.public.agg_bireport_squad_tenant main

    -- Include all LEFT JOIN clauses here
    -- [All the LEFT JOINs from the original query]

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

        -- Calculate Neediness Score
        ROUND(
            "Industry Rating" + tadscore_rating + HEALTHSCORE_Rating + "TAD Rating" +
            ProProduct_Rating + Calls_rating + Email_rating + Triage_Rating +
            Support_rating + (CASE WHEN related_tenants > 2 THEN 1 ELSE 0 END)
        ) AS "Neediness Score",

        -- Calculate Neediness Category
        CASE
            WHEN ROUND(
                "Industry Rating" + tadscore_rating + HEALTHSCORE_Rating + "TAD Rating" +
                ProProduct_Rating + Calls_rating + Email_rating + Triage_Rating +
                Support_rating + (CASE WHEN related_tenants > 2 THEN 1 ELSE 0 END)
            ) <= 4 THEN 'Low'
            WHEN ROUND(
                "Industry Rating" + tadscore_rating + HEALTHSCORE_Rating + "TAD Rating" +
                ProProduct_Rating + Calls_rating + Email_rating + Triage_Rating +
                Support_rating + (CASE WHEN related_tenants > 2 THEN 1 ELSE 0 END)
            ) BETWEEN 5 AND 7 THEN 'Medium'
            WHEN ROUND(
                "Industry Rating" + tadscore_rating + HEALTHSCORE_Rating + "TAD Rating" +
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