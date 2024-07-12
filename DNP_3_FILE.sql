<br> </br>
TRUNCATE TABLE LANDED_MARGIN_CALC;
<br> </br>
TRUNCATE TABLE LANDED_MARGIN_W_OMNI_FLOW_PATH_PREPARED;
<br> </br>
TRUNCATE TABLE LANDED_MARGIN_W_FREIGHT;
<br> </br>
TRUNCATE TABLE LANDED_MARGIN_W_FREIGHT_PREPARED;
<br> </br>
TRUNCATE TABLE ALLOCATION_W_BUYER;
<br> </br>
TRUNCATE TABLE ALLOCATION_V3_SF_GROUPED;
<br> </br>
TRUNCATE TABLE ALLOCATION_DC_VARIABLE_FIXED_STACKED_GRP;
<br> </br>
TRUNCATE TABLE DC_3PL_ALLOCATION_JOINED_STACKED;
<br> </br>
TRUNCATE TABLE DC_3PL_ALLOCATION_COST_UNION;
<br> </br>
TRUNCATE TABLE DC_3PL_ALLOCATION_JOINED_STACKED_JOINED_GRP;
<br> </br>
TRUNCATE TABLE FLOW_PATH_PREPAID_LATEST_FLOWPATH;
<br> </br>
TRUNCATE TABLE FLOW_PATH_PREPAID_LATEST_FLOWPATH_PREPARED;
<br> </br>
TRUNCATE TABLE Sales_for_freight_table;
<br> </br>
TRUNCATE TABLE FLOW_PATH_FINAL_STACKED_AGG;
<br> </br>
TRUNCATE TABLE SALES_FOR_FREIGHT_TABLE_JOINED_AGG_LAST;
<br> </br>
TRUNCATE TABLE FLOW_PATH_FINAL_STACKED_AGG_JOINED;
<br> </br>
TRUNCATE TABLE FLOW_PATH_FINAL_STACKED_W_ARTICLE;
<br> </br>
TRUNCATE TABLE FLOW_PATH_FINAL_STACKED_W_ARTICLE_PREPARED;
<br> </br>
TRUNCATE TABLE FLOW_PATH_FINAL_STACKED_W_ARTICLE_DC3PL;
<br> 
---
</br>
TRUNCATE TABLE allocation_v2_sf_grouped_v;
<br> </br>
TRUNCATE TABLE ALLOCATION_V2_SF_GROUPED_3PL;
<br> </br>
TRUNCATE TABLE ALLOCATION_V2_SF_GROUPED_3PL_BY_STORE_NO;
<br> </br>
TRUNCATE TABLE DC_3PL_Location_Mapping_channel;
<br> </br>
TRUNCATE TABLE ALLOCATION_FIXED_COST_SF_GROUPED_V_DC_PREPARED;
<br> </br>
TRUNCATE TABLE ALLOCATION_DC_3PL_OMNI_FULFILLMENT_GRP;
<br> </br>
TRUNCATE TABLE ALLOCATION_V3_SF_GROUPED_GRP_PREPARED;
<br> </br>
TRUNCATE TABLE ALLOCATION_VARIABLE_STACKED;
<br> </br>
TRUNCATE TABLE ALLOCATION_VARIABLE_STACKED_GRP;
<br> </br>
TRUNCATE TABLE ALLOCATION_FIXED_COST_SF_GROUPED_V_DC_PREPARED_STACKED;
<br> </br>
TRUNCATE TABLE ALLOCATION_FIXED_COST_SF_GROUPED_V_DC_PREPARED_STACKED_GRP;
<br> </br>
TRUNCATE TABLE ALLOCATION_FIXED_COST_SF_GROUPED_V_3PL_PREPARED;
<br> </br>
TRUNCATE TABLE VISUAL_TABLES_GRP_JOINED;
<br> </br>
TRUNCATE TABLE VISUAL_TABLES_GRP_JOINED_AGG;
<br> </br>
TRUNCATE TABLE VISUAL_TABLES_GRP_AGGREGATED;
<br> </br>
TRUNCATE TABLE VISUAL_TABLES_GRP_AGGREGATED_JOINED;

<br>
----spark function
-- input_file= 'ALLOCATION_CONTROL_PT2_SP'
-- master_file= 'ALLOCATION_W_INVENTORY_SHRINK_PREPARED_RENAMED'
-- output_file = 'ALLOCATION_SP_P2_SP'
	
--INSERT SPARK FUNCTION

/*VISUAL_TABLES_W_CHANNEL*/
---run spar function create ALLOCATION_SP_P2_SP
 --ALLOCATION_P2_SF

</br>
CREATE OR REPLACE TEMP TABLE ALLOCATION_P2_SF
AS
SELECT DISTINCT
  ARTICLE_NO as ARTICLE_NO , 
  MERCH_YEAR as MERCH_YEAR , 
  VENDOR_ID as VENDOR_ID , 
  PARENT_VENDOR_ID as PARENT_VENDOR_ID , 
  SCANDOWN_FLAG as scandown_flag , 
  MARKDOWN_FLAG as markdown_flag , 
  YEAR_PERIOD as YEAR_PERIOD , 
  FREIGHT_FLAG as freight_flag , 
  VENDOR_SUPPORT_FAST_FLAG as vendor_support_fast_flag , 
  SUPPORT_RATE_FLAG as support_rate_flag , 
  CO_CODE as CO_CODE , 
  SUM_TYPE_CODE as SUM_TYPE_CODE , 
  PRICE_OVRD_FLAG as PRICE_OVRD_FLAG , 
  SCAN_ITEM_FLAG as SCAN_ITEM_FLAG , 
  PRICE_TYPE_CODE as PRICE_TYPE_CODE , 
  BUY_ONLINE_CODE as BUY_ONLINE_CODE , 
  YEAR_WEEK as YEAR_WEEK , 
  YEAR_QUARTER as YEAR_QUARTER , 
  ARTICLE_DESC as ARTICLE_DESC , 
  ARTICLE_FULL as ARTICLE_FULL , 
  ARTICLE_TYPE_NAME as ARTICLE_TYPE_NAME , 
  ARTICLE_TYPE_CODE as ARTICLE_TYPE_CODE , 
  ARTICLE_TYPE_FULL as ARTICLE_TYPE_FULL , 
  LABEL_TYPE_CODE as LABEL_TYPE_CODE , 
  UOM_CODE as UOM_CODE , 
  UOM_DESC as UOM_DESC , 
  UOM_FULL as UOM_FULL , 
  ARTICLE_LENGTH as ARTICLE_LENGTH , 
  ARTICLE_WIDTH as ARTICLE_WIDTH , 
  ARTICLE_HEIGHT as ARTICLE_HEIGHT , 
  ARTICLE_WEIGHT as ARTICLE_WEIGHT , 
  ARTICLE_SIZE as ARTICLE_SIZE , 
  ARTICLE_COUNT as ARTICLE_COUNT , 
  LOC_TYPE_CODE as LOC_TYPE_CODE , 
  PRIMARY_VENDOR_ID as PRIMARY_VENDOR_ID , 
  ARTICLE_CAT_CODE as ARTICLE_CAT_CODE , 
  ARTICLE_CAT_NAME as ARTICLE_CAT_NAME , 
  ARTICLE_CAT_FULL as ARTICLE_CAT_FULL , 
  DEPT_CODE as DEPT_CODE , 
  DEPT_NAME as DEPT_NAME , 
  DEPT_FULL as DEPT_FULL , 
  LOB_NO as LOB_NO , 
  LOB_DESC as LOB_DESC , 
  LOB_FULL as LOB_FULL , 
  MAJ_DEPT_NO as MAJ_DEPT_NO , 
  MAJ_DEPT_DESC as MAJ_DEPT_DESC , 
  MAJ_DEPT_FULL as MAJ_DEPT_FULL , 
  BRAND_NAME as BRAND_NAME , 
  PVT_LABEL_DESC as PVT_LABEL_DESC , 
  VENDOR_NAME as VENDOR_NAME , 
  VENDOR_FULL as VENDOR_FULL , 
  VENDOR_CO_NAME as VENDOR_CO_NAME , 
  REGION_CODE as REGION_CODE , 
  ORDER_LINE_TYPE as ORDER_LINE_TYPE , 
  SCAC as SCAC , 
  OMS_ORDER_LINE_SCHEDULE_SHIP_NODE as OMS_ORDER_LINE_SCHEDULE_SHIP_NODE , 
  SALES_QTY as SALES_QTY , 
  QTY_ALLOCATE_FREIGHT as qty_allocate_freight , 
  SALES_AMT as SALES_AMT , 
  COST_AMT as COST_AMT , 
  MARGIN as margin , 
  COST_ALLOCATE_FREIGHT as cost_allocate_freight , 
  DISC_AMT as DISC_AMT , 
  OVRD_AMT as OVRD_AMT , 
  TRANS_TAX_AMT as TRANS_TAX_AMT , 
  GROSS_SALES as GROSS_SALES , 
  MARKDOWN_PERC as markdown_perc , 
  SHIPPING_REVENUE as SHIPPING_REVENUE , 
  SHIPPING_DISC as SHIPPING_DISC , 
  ACTUAL_SHIPPING_REVENUE as ACTUAL_SHIPPING_REVENUE , 
  OMNI_TSC_DELIVERY as omni_tsc_delivery , 
  OMNI_SMALL_PACKAGE_LTL as omni_small_package_ltl , 
  OMNI_ROADIE as omni_roadie , 
  OMNI_MULTI_SALVAGE as omni_multi_salvage , 
  SALES_ALLOCATE as SALES_ALLOCATE , 
  COST_ALLOCATE as COST_ALLOCATE , 
  UNSELLABLES as unsellables , 
  DEFECTIVES as defectives , 
  DC_SHRINK as dc_shrink , 
  THEFT as theft , 
  DEFECTIVE_ALLOWANCE as defective_allowance , 
  BATTERY_CORE as battery_core , 
  MIXING_CENTER as mixing_center , 
  SHRINK_INVENTORY as shrink_inventory , 
  NEW_STORE_DISCOUNTS as new_store_discounts , 
  CASH_DISCOUNTS as cash_discounts , 
  POP as pop , 
  PDQ as pdq , 
  MARKETING as marketing , 
  VENDOR_COMPLIANCE as vendor_compliance , 
  BOOTH_RENTAL as booth_rental , 
  SHRINK_RATE as shrink_rate , 
  INVENTORY_SHRINK as inventory_shrink , 
  TOTAL_SHRINK as total_shrink , 
  WEIGHTED_DOMESTIC_INBOUND as weighted_domestic_inbound , 
  WEIGHTED_OCEAN_FREIGHT as weighted_ocean_freight , 
  OCEAN_FREIGHT as ocean_freight , 
  WEIGHTED_DOMESTIC_OUTBOUND as weighted_domestic_outbound , 
  WEIGHTED_FREIGHT_COST as weighted_freight_cost , 
  FREIGHT_COST as freight_cost , 
  WEIGHTED_AGENCY_FEE as weighted_agency_fee , 
  AGENCY_FEE as agency_fee , 
  WEIGHTED_DUTY_RATE as weighted_duty_rate , 
  DUTY_TARRIFS as duty_tarrifs , 
  FREIGHT_JOIN_FLAG as freight_join_flag , 
  PREPAID_SHARE as prepaid_share , 
  WEIGHTED_PREPAID_FREIGHT as weighted_prepaid_freight , 
  PREPAID_FLAG as prepaid_flag , 
  PREPAID_SHARE_MODIFIED as prepaid_share_modified , 
  PREPAID_FREIGHT as prepaid_freight , 
  NON_PREPAID_SHARE as non_prepaid_share , 
  DOMESTIC_INBOUND as domestic_inbound , 
  DOMESTIC_OUTBOUND as domestic_outbound , 
  TOTAL_FREIGHT as total_freight , 
  DOMESTIC_OUTBOUND_W_PREPAID as domestic_outbound_w_prepaid , 
  CHANNEL as channel , 
  VENDOR_SUPPORT_PERC as vendor_support_perc , 
  VENDOR_SUPPORT_DOL as vendor_support_dol , 
  VENDOR_SUPPORT_FAST_PERC as vendor_support_fast_perc , 
  VENDOR_SUPPORT_FAST_DOL as vendor_support_fast_dol , 
  TENANT149_LM_MARKDOWNS_NEW_FILTERED as lm_markdowns_final , 
  LM_SCANDOWN_PREPARED_FILTERED as lm_scandown_final , 
  TENANT149_DATA_VOLUME_REBATE_2022_FOLD_BY_PARENT_VENDOR_STACKED_PERIOD as volume_rebate_2022_final , 
  MARKDOWNS_FINAL_SF as markdowns_final , 
  VENDOR_SUPPORT_FAST_EXCEPTION as vendor_support_fast_exception_final , 
  VOLUME_REBATE_DEVIATION as volume_rebate_deviation_final , 
  SCANDOWNS_FINAL_SF as scandowns_final , 
  TENANT149_DATA_FREIGHT_OTHER_CHARGES_PREPARED_BY_YEAR as freight_other_charges_final , 
  VOLUME_REBATE_2021_2020_ALLOCATE as volume_rebate_2021_2020_final , 
  PRICE_CUTS_ALLOCATE as price_cuts_final , 
  TENANT149_LM_MARKDOWNS_NEW_FILTERED_L2 as lm_markdowns_final_l2 , 
  LM_SCANDOWN_PREPARED_FILTERED_L2 as lm_scandown_final_l2 , 
  TENANT149_DATA_VOLUME_REBATE_2022_FOLD_BY_PARENT_VENDOR_STACKED_PERIOD_L2 as volume_rebate_2022_final_l2 , 
  MARKDOWNS_FINAL_SF_L2 as markdowns_final_l2 , 
  VENDOR_SUPPORT_FAST_EXCEPTION_L2 as vendor_support_fast_exception_final_l2 , 
  SCANDOWNS_FINAL_SF_L2 as scandowns_final_l2 , 
  VOLUME_REBATE_2021_2020_ALLOCATE_L2 as volume_rebate_2021_2020_final_l2 , 
  PRICE_CUTS_ALLOCATE_L2 as price_cuts_final_l2 
FROM 
    ALLOCATION_SP_P2_SP;

<br>
	--ALLOCATION_P2_SF_ROLLUP
</br> 
CREATE OR REPLACE TEMPORARY TABLE "ALLOCATION_P2_SF_ROLLUP"
AS
WITH CTE AS (
    SELECT DISTINCT
        ARTICLE_NO,
        MERCH_YEAR,
        VENDOR_ID,
        PARENT_VENDOR_ID,
        scandown_flag,
        markdown_flag,
        YEAR_PERIOD,
        freight_flag,
        vendor_support_fast_flag,
        support_rate_flag,
        CO_CODE,
        SUM_TYPE_CODE,
        PRICE_OVRD_FLAG,
        SCAN_ITEM_FLAG,
        PRICE_TYPE_CODE,
        BUY_ONLINE_CODE,
        YEAR_WEEK,
        YEAR_QUARTER,
        ARTICLE_DESC,
        ARTICLE_FULL,
        ARTICLE_TYPE_NAME,
        ARTICLE_TYPE_CODE,
        ARTICLE_TYPE_FULL,
        LABEL_TYPE_CODE,
        UOM_CODE,
        UOM_DESC,
        UOM_FULL,
        ARTICLE_LENGTH,
        ARTICLE_WIDTH,
        ARTICLE_HEIGHT,
        ARTICLE_WEIGHT,
        ARTICLE_SIZE,
        ARTICLE_COUNT,
        LOC_TYPE_CODE,
        PRIMARY_VENDOR_ID,
        ARTICLE_CAT_CODE,
        ARTICLE_CAT_NAME,
        ARTICLE_CAT_FULL,
        DEPT_CODE,
        DEPT_NAME,
        DEPT_FULL,
        LOB_NO,
        LOB_DESC,
        LOB_FULL,
        MAJ_DEPT_NO,
        MAJ_DEPT_DESC,
        MAJ_DEPT_FULL,
        BRAND_NAME,
        PVT_LABEL_DESC,
        VENDOR_NAME,
        VENDOR_FULL,
        VENDOR_CO_NAME,
        REGION_CODE,
        ORDER_LINE_TYPE,
        SCAC,
        OMS_ORDER_LINE_SCHEDULE_SHIP_NODE,
        SALES_QTY,
        qty_allocate_freight,
        SALES_AMT,
        COST_AMT,
        margin,
        cost_allocate_freight,
        DISC_AMT,
        OVRD_AMT,
        TRANS_TAX_AMT,
        GROSS_SALES,
        markdown_perc,
        SHIPPING_REVENUE,
        SHIPPING_DISC,
        ACTUAL_SHIPPING_REVENUE,
        omni_tsc_delivery,
        omni_small_package_ltl,
        omni_roadie,
        Omni_multi_salvage,
        SALES_ALLOCATE,
        COST_ALLOCATE,
        unsellables,
        defectives,
        dc_shrink,
        theft,
        defective_allowance,
        battery_core,
        mixing_center,
        shrink_inventory,
        new_store_discounts,
        cash_discounts,
        pop,
        pdq,
        marketing,
        vendor_compliance,
        booth_rental,
        shrink_rate,
        inventory_shrink,
        total_shrink,
        weighted_domestic_inbound,
        weighted_ocean_freight,
        ocean_freight,
        weighted_domestic_outbound,
        weighted_freight_cost,
        freight_cost,
        weighted_agency_fee,
        agency_fee,
        weighted_duty_rate,
        duty_tarrifs,
        freight_join_flag,
        prepaid_share,
        weighted_prepaid_freight,
        prepaid_flag,
        prepaid_share_modified,
        prepaid_freight,
        non_prepaid_share,
        domestic_inbound,
        domestic_outbound,
        total_freight,
        domestic_outbound_w_prepaid,
        channel,
        vendor_support_perc,
        vendor_support_dol,
        vendor_support_fast_perc,
        vendor_support_fast_dol,
        lm_markdowns_final,
        lm_scandown_final,
        volume_rebate_2022_final,
        markdowns_final,
        vendor_support_fast_exception_final,
        volume_rebate_deviation_final,
        scandowns_final,
        freight_other_charges_final,
        volume_rebate_2021_2020_final,
        price_cuts_final,
        lm_markdowns_final_l2,
        lm_scandown_final_l2,
        volume_rebate_2022_final_l2,
        markdowns_final_l2,
        vendor_support_fast_exception_final_l2,
        scandowns_final_l2,
        volume_rebate_2021_2020_final_l2,
        price_cuts_final_l2,
        COALESCE(CASE WHEN new_channel = 'Omni' THEN 'Omni-Shipped' ELSE new_channel END, new_channel) AS new_channel
      FROM (
        SELECT 
          ARTICLE_NO,
          MERCH_YEAR,
          VENDOR_ID,
          PARENT_VENDOR_ID,
          scandown_flag,
          markdown_flag,
          YEAR_PERIOD,
          freight_flag,
          vendor_support_fast_flag,
          support_rate_flag,
          CO_CODE,
          SUM_TYPE_CODE,
          PRICE_OVRD_FLAG,
          SCAN_ITEM_FLAG,
          PRICE_TYPE_CODE,
          BUY_ONLINE_CODE,
          YEAR_WEEK,
          YEAR_QUARTER,
          ARTICLE_DESC,
          ARTICLE_FULL,
          ARTICLE_TYPE_NAME,
          ARTICLE_TYPE_CODE,
          ARTICLE_TYPE_FULL,
          LABEL_TYPE_CODE,
          UOM_CODE,
          UOM_DESC,
          UOM_FULL,
          ARTICLE_LENGTH,
          ARTICLE_WIDTH,
          ARTICLE_HEIGHT,
          ARTICLE_WEIGHT,
          ARTICLE_SIZE,
          ARTICLE_COUNT,
          LOC_TYPE_CODE,
          PRIMARY_VENDOR_ID,
          ARTICLE_CAT_CODE,
          ARTICLE_CAT_NAME,
          ARTICLE_CAT_FULL,
          DEPT_CODE,
          DEPT_NAME,
          DEPT_FULL,
          LOB_NO,
          LOB_DESC,
          LOB_FULL,
          MAJ_DEPT_NO,
          MAJ_DEPT_DESC,
          MAJ_DEPT_FULL,
          BRAND_NAME,
          PVT_LABEL_DESC,
          VENDOR_NAME,
          VENDOR_FULL,
          VENDOR_CO_NAME,
          REGION_CODE,
          ORDER_LINE_TYPE,
          SCAC,
          OMS_ORDER_LINE_SCHEDULE_SHIP_NODE,
          SALES_QTY,
          qty_allocate_freight,
          SALES_AMT,
          COST_AMT,
          margin,
          cost_allocate_freight,
          DISC_AMT,
          OVRD_AMT,
          TRANS_TAX_AMT,
          GROSS_SALES,
          markdown_perc,
          SHIPPING_REVENUE,
          SHIPPING_DISC,
          ACTUAL_SHIPPING_REVENUE,
          omni_tsc_delivery,
          omni_small_package_ltl,
          Omni_roadie,
          omni_multi_salvage AS Omni_multi_salvage,
          SALES_ALLOCATE,
          COST_ALLOCATE,
          unsellables,
          defectives,
          dc_shrink,
          theft,
          defective_allowance,
          battery_core,
          mixing_center,
          shrink_inventory,
          new_store_discounts,
          cash_discounts,
          pop,
          pdq,
          marketing,
          vendor_compliance,
          booth_rental,
          shrink_rate,
          inventory_shrink,
          total_shrink,
          weighted_domestic_inbound,
          weighted_ocean_freight,
          ocean_freight,
          weighted_domestic_outbound,
          weighted_freight_cost,
          freight_cost,
          weighted_agency_fee,
          agency_fee,
          weighted_duty_rate,
          duty_tarrifs,
          freight_join_flag,
          prepaid_share,
          weighted_prepaid_freight,
          prepaid_flag,
          prepaid_share_modified,
          prepaid_freight,
          non_prepaid_share,
          domestic_inbound,
          domestic_outbound,
          total_freight,
          domestic_outbound_w_prepaid,
          channel,
          vendor_support_perc,
          vendor_support_dol,
          vendor_support_fast_perc,
          vendor_support_fast_dol,
          lm_markdowns_final,
          lm_scandown_final,
          volume_rebate_2022_final,
          markdowns_final,
          vendor_support_fast_exception_final,
          volume_rebate_deviation_final,
          scandowns_final,
          freight_other_charges_final,
          volume_rebate_2021_2020_final,
          price_cuts_final,
          lm_markdowns_final_l2,
          lm_scandown_final_l2,
          volume_rebate_2022_final_l2,
          markdowns_final_l2,
          vendor_support_fast_exception_final_l2,
          scandowns_final_l2,
          volume_rebate_2021_2020_final_l2,
          price_cuts_final_l2,
          CASE WHEN (ORDER_LINE_TYPE = 'BOPIS') AND (channel = 'Omni') THEN 'Omni-BOPIS' ELSE CASE WHEN (ORDER_LINE_TYPE = 'BODFS') AND (channel = 'Omni') THEN 'Omni-BODFS' ELSE CASE WHEN (ORDER_LINE_TYPE = 'Return') OR (SCAC = 'Return') THEN 'Omni-Return' ELSE channel END END END AS new_channel
        FROM ALLOCATION_P2_SF
        ) bopis_bodfs_flag
        )
  SELECT DISTINCT
    MERCH_YEAR AS "MERCH_YEAR",
    PARENT_VENDOR_ID AS "PARENT_VENDOR_ID",
    VENDOR_ID AS "VENDOR_ID",
    scandown_flag AS "scandown_flag",
    markdown_flag AS "markdown_flag",
    ARTICLE_NO AS "ARTICLE_NO",
    YEAR_PERIOD AS "YEAR_PERIOD",
    freight_flag AS "freight_flag",
    CO_CODE AS "CO_CODE",
    SUM_TYPE_CODE AS "SUM_TYPE_CODE",
    PRICE_OVRD_FLAG AS "PRICE_OVRD_FLAG",
    SCAN_ITEM_FLAG AS "SCAN_ITEM_FLAG",
    PRICE_TYPE_CODE AS "PRICE_TYPE_CODE",
    BUY_ONLINE_CODE AS "BUY_ONLINE_CODE",
    YEAR_WEEK AS "YEAR_WEEK",
    YEAR_QUARTER AS "YEAR_QUARTER",
    ARTICLE_DESC AS "ARTICLE_DESC",
    ARTICLE_FULL AS "ARTICLE_FULL",
    ARTICLE_TYPE_NAME AS "ARTICLE_TYPE_NAME",
    ARTICLE_TYPE_FULL AS "ARTICLE_TYPE_FULL",
    LABEL_TYPE_CODE AS "LABEL_TYPE_CODE",
    UOM_CODE AS "UOM_CODE",
    UOM_DESC AS "UOM_DESC",
    UOM_FULL AS "UOM_FULL",
    ARTICLE_LENGTH AS "ARTICLE_LENGTH",
    ARTICLE_WIDTH AS "ARTICLE_WIDTH",
    ARTICLE_HEIGHT AS "ARTICLE_HEIGHT",
    ARTICLE_WEIGHT AS "ARTICLE_WEIGHT",
    ARTICLE_SIZE AS "ARTICLE_SIZE",
    ARTICLE_COUNT AS "ARTICLE_COUNT",
    LOC_TYPE_CODE AS "LOC_TYPE_CODE",
    ARTICLE_TYPE_CODE AS "ARTICLE_TYPE_CODE",
    PRIMARY_VENDOR_ID AS "PRIMARY_VENDOR_ID",
    ARTICLE_CAT_CODE AS "ARTICLE_CAT_CODE",
    ARTICLE_CAT_NAME AS "ARTICLE_CAT_NAME",
    ARTICLE_CAT_FULL AS "ARTICLE_CAT_FULL",
    DEPT_CODE AS "DEPT_CODE",
    DEPT_NAME AS "DEPT_NAME",
    DEPT_FULL AS "DEPT_FULL",
    LOB_NO AS "LOB_NO",
    LOB_DESC AS "LOB_DESC",
    LOB_FULL AS "LOB_FULL",
    MAJ_DEPT_NO AS "MAJ_DEPT_NO",
    MAJ_DEPT_DESC AS "MAJ_DEPT_DESC",
    MAJ_DEPT_FULL AS "MAJ_DEPT_FULL",
    PVT_LABEL_DESC AS "PVT_LABEL_DESC",
    BRAND_NAME AS "BRAND_NAME",
    VENDOR_NAME AS "VENDOR_NAME",
    VENDOR_FULL AS "VENDOR_FULL",
    VENDOR_CO_NAME AS "VENDOR_CO_NAME",
    REGION_CODE AS "REGION_CODE",
    channel AS "channel",
    ORDER_LINE_TYPE AS "ORDER_LINE_TYPE",
    SCAC AS "SCAC",
    OMS_ORDER_LINE_SCHEDULE_SHIP_NODE AS "OMS_ORDER_LINE_SCHEDULE_SHIP_NODE",
    SALES_QTY_sum AS "SALES_QTY",
    SALES_AMT_sum AS "SALES_AMT",
    COST_AMT_sum AS "COST_AMT",
    margin_sum AS "margin",
    DISC_AMT_sum AS "DISC_AMT",
    OVRD_AMT_sum AS "OVRD_AMT",
    TRANS_TAX_AMT_sum AS "TRANS_TAX_AMT",
    GROSS_SALES_sum AS "GROSS_SALES",
    SHIPPING_REVENUE_sum AS "SHIPPING_REVENUE",
    SHIPPING_DISC_sum AS "SHIPPING_DISC",
    ACTUAL_SHIPPING_REVENUE_sum AS "ACTUAL_SHIPPING_REVENUE",
    omni_tsc_delivery_sum AS "omni_tsc_delivery",
    omni_small_package_ltl_sum AS "omni_small_package_ltl",
    omni_roadie_sum AS "omni_roadie",
    Omni_multi_salvage_sum AS "Omni_multi_salvage",
    SALES_ALLOCATE_sum AS "SALES_ALLOCATE",
    COST_ALLOCATE_sum AS "COST_ALLOCATE",
    unsellables_sum AS "unsellables",
    defectives_sum AS "defectives",
    dc_shrink_sum AS "dc_shrink",
    theft_sum AS "theft",
    defective_allowance_sum AS "defective_allowance",
    battery_core_sum AS "battery_core",
    mixing_center_sum AS "mixing_center",
    new_store_discounts_sum AS "new_store_discounts",
    cash_discounts_sum AS "cash_discounts",
    pop_sum AS "pop",
    pdq_sum AS "pdq",
    marketing_sum AS "marketing",
    vendor_compliance_sum AS "vendor_compliance",
    booth_rental_sum AS "booth_rental",
    inventory_shrink_sum AS "inventory_shrink",
    total_shrink_sum AS "total_shrink",
    weighted_domestic_inbound_sum AS "weighted_domestic_inbound",
    weighted_ocean_freight_sum AS "weighted_ocean_freight",
    ocean_freight_sum AS "ocean_freight",
    weighted_domestic_outbound_sum AS "weighted_domestic_outbound",
    weighted_freight_cost_sum AS "weighted_freight_cost",
    freight_cost_sum AS "freight_cost",
    weighted_agency_fee_sum AS "weighted_agency_fee",
    agency_fee_sum AS "agency_fee",
    weighted_duty_rate_sum AS "weighted_duty_rate",
    duty_tarrifs_sum AS "duty_tarrifs",
    domestic_inbound_sum AS "domestic_inbound",
    total_freight_sum AS "total_freight",
    domestic_outbound_w_prepaid_sum AS "domestic_outbound",
    vendor_support_dol_sum AS "vendor_support_dol",
    vendor_support_fast_dol_sum AS "vendor_support_fast_dol",
    freight_other_charges_final_sum AS "freight_other_charges_final",
    volume_rebate_combined_sum AS "volume_rebate",
    price_cuts_combined_sum AS "price_cuts",
    scanbacks_combined_sum AS "scanbacks",
    markdowns_combined_sum AS "markdowns",
    vsf_exception_sum AS "vsf_exception"
  FROM(    SELECT
        MERCH_YEAR AS MERCH_YEAR,
        PARENT_VENDOR_ID AS PARENT_VENDOR_ID,
        VENDOR_ID AS VENDOR_ID,
        scandown_flag AS scandown_flag,
        markdown_flag AS markdown_flag,
        ARTICLE_NO AS ARTICLE_NO,
        YEAR_PERIOD AS YEAR_PERIOD,
        freight_flag AS freight_flag,
        CO_CODE AS CO_CODE,
        SUM_TYPE_CODE AS SUM_TYPE_CODE,
        PRICE_OVRD_FLAG AS PRICE_OVRD_FLAG,
        SCAN_ITEM_FLAG AS SCAN_ITEM_FLAG,
        PRICE_TYPE_CODE AS PRICE_TYPE_CODE,
        BUY_ONLINE_CODE AS BUY_ONLINE_CODE,
        YEAR_WEEK AS YEAR_WEEK,
        YEAR_QUARTER AS YEAR_QUARTER,
        ARTICLE_DESC AS ARTICLE_DESC,
        ARTICLE_FULL AS ARTICLE_FULL,
        ARTICLE_TYPE_NAME AS ARTICLE_TYPE_NAME,
        ARTICLE_TYPE_FULL AS ARTICLE_TYPE_FULL,
        LABEL_TYPE_CODE AS LABEL_TYPE_CODE,
        UOM_CODE AS UOM_CODE,
        UOM_DESC AS UOM_DESC,
        UOM_FULL AS UOM_FULL,
        ARTICLE_LENGTH AS ARTICLE_LENGTH,
        ARTICLE_WIDTH AS ARTICLE_WIDTH,
        ARTICLE_HEIGHT AS ARTICLE_HEIGHT,
        ARTICLE_WEIGHT AS ARTICLE_WEIGHT,
        ARTICLE_SIZE AS ARTICLE_SIZE,
        ARTICLE_COUNT AS ARTICLE_COUNT,
        LOC_TYPE_CODE AS LOC_TYPE_CODE,
        ARTICLE_TYPE_CODE AS ARTICLE_TYPE_CODE,
        PRIMARY_VENDOR_ID AS PRIMARY_VENDOR_ID,
        ARTICLE_CAT_CODE AS ARTICLE_CAT_CODE,
        ARTICLE_CAT_NAME AS ARTICLE_CAT_NAME,
        ARTICLE_CAT_FULL AS ARTICLE_CAT_FULL,
        DEPT_CODE AS DEPT_CODE,
        DEPT_NAME AS DEPT_NAME,
        DEPT_FULL AS DEPT_FULL,
        LOB_NO AS LOB_NO,
        LOB_DESC AS LOB_DESC,
        LOB_FULL AS LOB_FULL,
        MAJ_DEPT_NO AS MAJ_DEPT_NO,
        MAJ_DEPT_DESC AS MAJ_DEPT_DESC,
        MAJ_DEPT_FULL AS MAJ_DEPT_FULL,
        PVT_LABEL_DESC AS PVT_LABEL_DESC,
        BRAND_NAME AS BRAND_NAME,
        VENDOR_NAME AS VENDOR_NAME,
        VENDOR_FULL AS VENDOR_FULL,
        VENDOR_CO_NAME AS VENDOR_CO_NAME,
        REGION_CODE AS REGION_CODE,
        channel AS channel,
        ORDER_LINE_TYPE AS ORDER_LINE_TYPE,
        SCAC AS SCAC,
        OMS_ORDER_LINE_SCHEDULE_SHIP_NODE AS OMS_ORDER_LINE_SCHEDULE_SHIP_NODE,
        SUM(SALES_QTY) AS SALES_QTY_sum,
        SUM(SALES_AMT) AS SALES_AMT_sum,
        SUM(COST_AMT) AS COST_AMT_sum,
        SUM(margin) AS margin_sum,
        SUM(DISC_AMT) AS DISC_AMT_sum,
        SUM(OVRD_AMT) AS OVRD_AMT_sum,
        SUM(TRANS_TAX_AMT) AS TRANS_TAX_AMT_sum,
        SUM(GROSS_SALES) AS GROSS_SALES_sum,
        SUM(SHIPPING_REVENUE) AS SHIPPING_REVENUE_sum,
        SUM(SHIPPING_DISC) AS SHIPPING_DISC_sum,
        SUM(ACTUAL_SHIPPING_REVENUE) AS ACTUAL_SHIPPING_REVENUE_sum,
        SUM(omni_tsc_delivery) AS omni_tsc_delivery_sum,
        SUM(omni_small_package_ltl) AS omni_small_package_ltl_sum,
        SUM(omni_roadie) AS omni_roadie_sum,
        SUM(Omni_multi_salvage) AS Omni_multi_salvage_sum,
        SUM(SALES_ALLOCATE) AS SALES_ALLOCATE_sum,
        SUM(COST_ALLOCATE) AS COST_ALLOCATE_sum,
        SUM(unsellables) AS unsellables_sum,
        SUM(defectives) AS defectives_sum,
        SUM(dc_shrink) AS dc_shrink_sum,
        SUM(theft) AS theft_sum,
        SUM(defective_allowance) AS defective_allowance_sum,
        SUM(battery_core) AS battery_core_sum,
        SUM(mixing_center) AS mixing_center_sum,
        SUM(new_store_discounts) AS new_store_discounts_sum,
        SUM(cash_discounts) AS cash_discounts_sum,
        SUM(pop) AS pop_sum,
        SUM(pdq) AS pdq_sum,
        SUM(marketing) AS marketing_sum,
        SUM(vendor_compliance) AS vendor_compliance_sum,
        SUM(booth_rental) AS booth_rental_sum,
        SUM(inventory_shrink) AS inventory_shrink_sum,
        SUM(total_shrink) AS total_shrink_sum,
        SUM(weighted_domestic_inbound) AS weighted_domestic_inbound_sum,
        SUM(weighted_ocean_freight) AS weighted_ocean_freight_sum,
        SUM(ocean_freight) AS ocean_freight_sum,
        SUM(weighted_domestic_outbound) AS weighted_domestic_outbound_sum,
        SUM(weighted_freight_cost) AS weighted_freight_cost_sum,
        SUM(freight_cost) AS freight_cost_sum,
        SUM(weighted_agency_fee) AS weighted_agency_fee_sum,
        SUM(agency_fee) AS agency_fee_sum,
        SUM(weighted_duty_rate) AS weighted_duty_rate_sum,
        SUM(duty_tarrifs) AS duty_tarrifs_sum,
        SUM(domestic_inbound) AS domestic_inbound_sum,
        SUM(total_freight) AS total_freight_sum,
        SUM(domestic_outbound_w_prepaid) AS domestic_outbound_w_prepaid_sum,
        SUM(vendor_support_dol) AS vendor_support_dol_sum,
        SUM(vendor_support_fast_dol) AS vendor_support_fast_dol_sum,
        SUM(freight_other_charges_final) AS freight_other_charges_final_sum,
        SUM(volume_rebate_combined) AS volume_rebate_combined_sum,
        SUM(price_cuts_combined) AS price_cuts_combined_sum,
        SUM(scanbacks_combined) AS scanbacks_combined_sum,
        SUM(markdowns_combined) AS markdowns_combined_sum,
        SUM(vsf_exception) AS vsf_exception_sum
        FROM (
        SELECT 
            ARTICLE_NO AS ARTICLE_NO,
            MERCH_YEAR AS MERCH_YEAR,
            VENDOR_ID AS VENDOR_ID,
            PARENT_VENDOR_ID AS PARENT_VENDOR_ID,
            scandown_flag AS scandown_flag,
            markdown_flag AS markdown_flag,
            YEAR_PERIOD AS YEAR_PERIOD,
            freight_flag AS freight_flag,
            vendor_support_fast_flag AS vendor_support_fast_flag,
            support_rate_flag AS support_rate_flag,
            CO_CODE AS CO_CODE,
            SUM_TYPE_CODE AS SUM_TYPE_CODE,
            PRICE_OVRD_FLAG AS PRICE_OVRD_FLAG,
            SCAN_ITEM_FLAG AS SCAN_ITEM_FLAG,
            PRICE_TYPE_CODE AS PRICE_TYPE_CODE,
            BUY_ONLINE_CODE AS BUY_ONLINE_CODE,
            YEAR_WEEK AS YEAR_WEEK,
            YEAR_QUARTER AS YEAR_QUARTER,
            ARTICLE_DESC AS ARTICLE_DESC,
            ARTICLE_FULL AS ARTICLE_FULL,
            ARTICLE_TYPE_NAME AS ARTICLE_TYPE_NAME,
            ARTICLE_TYPE_CODE AS ARTICLE_TYPE_CODE,
            ARTICLE_TYPE_FULL AS ARTICLE_TYPE_FULL,
            LABEL_TYPE_CODE AS LABEL_TYPE_CODE,
            UOM_CODE AS UOM_CODE,
            UOM_DESC AS UOM_DESC,
            UOM_FULL AS UOM_FULL,
            ARTICLE_LENGTH AS ARTICLE_LENGTH,
            ARTICLE_WIDTH AS ARTICLE_WIDTH,
            ARTICLE_HEIGHT AS ARTICLE_HEIGHT,
            ARTICLE_WEIGHT AS ARTICLE_WEIGHT,
            ARTICLE_SIZE AS ARTICLE_SIZE,
            ARTICLE_COUNT AS ARTICLE_COUNT,
            LOC_TYPE_CODE AS LOC_TYPE_CODE,
            PRIMARY_VENDOR_ID AS PRIMARY_VENDOR_ID,
            ARTICLE_CAT_CODE AS ARTICLE_CAT_CODE,
            ARTICLE_CAT_NAME AS ARTICLE_CAT_NAME,
            ARTICLE_CAT_FULL AS ARTICLE_CAT_FULL,
            DEPT_CODE AS DEPT_CODE,
            DEPT_NAME AS DEPT_NAME,
            DEPT_FULL AS DEPT_FULL,
            LOB_NO AS LOB_NO,
            LOB_DESC AS LOB_DESC,
            LOB_FULL AS LOB_FULL,
            MAJ_DEPT_NO AS MAJ_DEPT_NO,
            MAJ_DEPT_DESC AS MAJ_DEPT_DESC,
            MAJ_DEPT_FULL AS MAJ_DEPT_FULL,
            BRAND_NAME AS BRAND_NAME,
            PVT_LABEL_DESC AS PVT_LABEL_DESC,
            VENDOR_NAME AS VENDOR_NAME,
            VENDOR_FULL AS VENDOR_FULL,
            VENDOR_CO_NAME AS VENDOR_CO_NAME,
            REGION_CODE AS REGION_CODE,
            ORDER_LINE_TYPE AS ORDER_LINE_TYPE,
            SCAC AS SCAC,
            OMS_ORDER_LINE_SCHEDULE_SHIP_NODE AS OMS_ORDER_LINE_SCHEDULE_SHIP_NODE,
            SALES_QTY AS SALES_QTY,
            qty_allocate_freight AS qty_allocate_freight,
            SALES_AMT AS SALES_AMT,
            COST_AMT AS COST_AMT,
            margin AS margin,
            cost_allocate_freight AS cost_allocate_freight,
            DISC_AMT AS DISC_AMT,
            OVRD_AMT AS OVRD_AMT,
            TRANS_TAX_AMT AS TRANS_TAX_AMT,
            GROSS_SALES AS GROSS_SALES,
            markdown_perc AS markdown_perc,
            SHIPPING_REVENUE AS SHIPPING_REVENUE,
            SHIPPING_DISC AS SHIPPING_DISC,
            ACTUAL_SHIPPING_REVENUE AS ACTUAL_SHIPPING_REVENUE,
            omni_tsc_delivery AS omni_tsc_delivery,
            omni_small_package_ltl AS omni_small_package_ltl,
            omni_roadie AS omni_roadie,
            Omni_multi_salvage AS Omni_multi_salvage,
            SALES_ALLOCATE AS SALES_ALLOCATE,
            COST_ALLOCATE AS COST_ALLOCATE,
            unsellables AS unsellables,
            defectives AS defectives,
            dc_shrink AS dc_shrink,
            theft AS theft,
            defective_allowance AS defective_allowance,
            battery_core AS battery_core,
            mixing_center AS mixing_center,
            shrink_inventory AS shrink_inventory,
            new_store_discounts AS new_store_discounts,
            cash_discounts AS cash_discounts,
            pop AS pop,
            pdq AS pdq,
            marketing AS marketing,
            vendor_compliance AS vendor_compliance,
            booth_rental AS booth_rental,
            shrink_rate AS shrink_rate,
            inventory_shrink AS inventory_shrink,
            total_shrink AS total_shrink,
            weighted_domestic_inbound AS weighted_domestic_inbound,
            weighted_ocean_freight AS weighted_ocean_freight,
            ocean_freight AS ocean_freight,
            weighted_domestic_outbound AS weighted_domestic_outbound,
            weighted_freight_cost AS weighted_freight_cost,
            freight_cost AS freight_cost,
            weighted_agency_fee AS weighted_agency_fee,
            agency_fee AS agency_fee,
            weighted_duty_rate AS weighted_duty_rate,
            duty_tarrifs AS duty_tarrifs,
            freight_join_flag AS freight_join_flag,
            prepaid_share AS prepaid_share,
            weighted_prepaid_freight AS weighted_prepaid_freight,
            prepaid_flag AS prepaid_flag,
            prepaid_share_modified AS prepaid_share_modified,
            prepaid_freight AS prepaid_freight,
            non_prepaid_share AS non_prepaid_share,
            domestic_inbound AS domestic_inbound,
            domestic_outbound AS domestic_outbound,
            total_freight AS total_freight,
            domestic_outbound_w_prepaid AS domestic_outbound_w_prepaid,
            new_channel AS channel,
            vendor_support_perc AS vendor_support_perc,
            vendor_support_dol AS vendor_support_dol,
            vendor_support_fast_perc AS vendor_support_fast_perc,
            vendor_support_fast_dol AS vendor_support_fast_dol,
            lm_markdowns_final AS lm_markdowns_final,
            lm_scandown_final AS lm_scandown_final,
            volume_rebate_2022_final AS volume_rebate_2022_final,
            markdowns_final AS markdowns_final,
            vendor_support_fast_exception_final AS vendor_support_fast_exception_final,
            volume_rebate_deviation_final AS volume_rebate_deviation_final,
            scandowns_final AS scandowns_final,
            freight_other_charges_final AS freight_other_charges_final,
            volume_rebate_2021_2020_final AS volume_rebate_2021_2020_final,
            price_cuts_final AS price_cuts_final,
            lm_markdowns_final_l2 AS lm_markdowns_final_l2,
            lm_scandown_final_l2 AS lm_scandown_final_l2,
            volume_rebate_2022_final_l2 AS volume_rebate_2022_final_l2,
            markdowns_final_l2 AS markdowns_final_l2,
            vendor_support_fast_exception_final_l2 AS vendor_support_fast_exception_final_l2,
            scandowns_final_l2 AS scandowns_final_l2,
            volume_rebate_2021_2020_final_l2 AS volume_rebate_2021_2020_final_l2,
            price_cuts_final_l2 AS price_cuts_final_l2,
            COALESCE(volume_rebate_2021_2020_final, 0) + COALESCE(volume_rebate_2021_2020_final_l2, 0) + COALESCE(volume_rebate_deviation_final, 0) + COALESCE(volume_rebate_2022_final, 0) + COALESCE(volume_rebate_2022_final_l2, 0) AS volume_rebate_combined,
            COALESCE(price_cuts_final, 0) + COALESCE(price_cuts_final_l2, 0) AS price_cuts_combined,
            COALESCE(scandowns_final, 0) + COALESCE(scandowns_final_l2, 0) + COALESCE(lm_scandown_final, 0) + COALESCE(lm_scandown_final_l2, 0) AS scanbacks_combined,
            COALESCE(markdowns_final, 0) + COALESCE(markdowns_final_l2, 0) + COALESCE(lm_markdowns_final, 0) + COALESCE(lm_markdowns_final_l2, 0) AS markdowns_combined,
            COALESCE(vendor_support_fast_exception_final, 0) + COALESCE(vendor_support_fast_exception_final_l2, 0) AS vsf_exception
          FROM CTE
        ) Before_Grouping  
      GROUP BY MERCH_YEAR, PARENT_VENDOR_ID, VENDOR_ID, scandown_flag, markdown_flag, ARTICLE_NO, YEAR_PERIOD, freight_flag, CO_CODE, SUM_TYPE_CODE, PRICE_OVRD_FLAG, SCAN_ITEM_FLAG, PRICE_TYPE_CODE, BUY_ONLINE_CODE, YEAR_WEEK, YEAR_QUARTER, ARTICLE_DESC, ARTICLE_FULL, ARTICLE_TYPE_NAME, ARTICLE_TYPE_FULL, LABEL_TYPE_CODE, UOM_CODE, UOM_DESC, UOM_FULL, ARTICLE_LENGTH, ARTICLE_WIDTH, ARTICLE_HEIGHT, ARTICLE_WEIGHT, ARTICLE_SIZE, ARTICLE_COUNT, LOC_TYPE_CODE, ARTICLE_TYPE_CODE, PRIMARY_VENDOR_ID, ARTICLE_CAT_CODE, ARTICLE_CAT_NAME, ARTICLE_CAT_FULL, DEPT_CODE, DEPT_NAME, DEPT_FULL, LOB_NO, LOB_DESC, LOB_FULL, MAJ_DEPT_NO, MAJ_DEPT_DESC, MAJ_DEPT_FULL, PVT_LABEL_DESC, BRAND_NAME, VENDOR_NAME, VENDOR_FULL, VENDOR_CO_NAME, REGION_CODE, channel, ORDER_LINE_TYPE, SCAC, OMS_ORDER_LINE_SCHEDULE_SHIP_NODE)A


<br>
	--LANDED_MARGIN_CALC
</br>
insert into LANDED_MARGIN_CALC 
WITH CTE AS (
    SELECT DISTINCT
        "MERCH_YEAR",
        "PARENT_VENDOR_ID",
        "VENDOR_ID",
        "scandown_flag",
        "markdown_flag",
        "ARTICLE_NO",
        "YEAR_PERIOD",
        "freight_flag",
        "CO_CODE",
        "SUM_TYPE_CODE",
        "PRICE_OVRD_FLAG",
        "SCAN_ITEM_FLAG",
        "PRICE_TYPE_CODE",
        "BUY_ONLINE_CODE",
        "YEAR_WEEK",
        "YEAR_QUARTER",
        "ARTICLE_DESC",
        "ARTICLE_FULL",
        "ARTICLE_TYPE_NAME",
        "ARTICLE_TYPE_FULL",
        "LABEL_TYPE_CODE",
        "UOM_CODE",
        "UOM_DESC",
        "UOM_FULL",
        "ARTICLE_LENGTH",
        "ARTICLE_WIDTH",
        "ARTICLE_HEIGHT",
        "ARTICLE_WEIGHT",
        "ARTICLE_SIZE",
        "ARTICLE_COUNT",
        "LOC_TYPE_CODE",
        "ARTICLE_TYPE_CODE",
        "PRIMARY_VENDOR_ID",
        "ARTICLE_CAT_CODE",
        "ARTICLE_CAT_NAME",
        "ARTICLE_CAT_FULL",
        "DEPT_CODE",
        "DEPT_NAME",
        "DEPT_FULL",
        "LOB_NO",
        "LOB_DESC",
        "LOB_FULL",
        "MAJ_DEPT_NO",
        "MAJ_DEPT_DESC",
        "MAJ_DEPT_FULL",
        "PVT_LABEL_DESC",
        "BRAND_NAME",
        "VENDOR_NAME",
        "VENDOR_FULL",
        "VENDOR_CO_NAME",
        "REGION_CODE",
        "channel",
        "ORDER_LINE_TYPE",
        "SCAC",
        "OMS_ORDER_LINE_SCHEDULE_SHIP_NODE",
        "SALES_QTY",
        "SALES_AMT",
        "COST_AMT",
        "margin",
        "DISC_AMT",
        "OVRD_AMT",
        "TRANS_TAX_AMT",
        "GROSS_SALES",
        "SHIPPING_REVENUE",
        "SHIPPING_DISC",
        "ACTUAL_SHIPPING_REVENUE",
        "omni_tsc_delivery",
        "omni_small_package_ltl",
        "omni_roadie",
        "Omni_multi_salvage" AS "Omni_multi_salvage",
        "SALES_ALLOCATE",
        "COST_ALLOCATE",
        "unsellables",
        "defectives",
        "dc_shrink",
        "theft",
        "defective_allowance",
        "battery_core",
        "mixing_center",
        "new_store_discounts",
        "cash_discounts",
        "pop",
        "pdq",
        "marketing",
        "vendor_compliance",
        "booth_rental",
        "inventory_shrink",
        "total_shrink",
        "weighted_domestic_inbound",
        "weighted_ocean_freight",
        "ocean_freight",
        "weighted_domestic_outbound",
        "weighted_freight_cost",
        "freight_cost",
        "weighted_agency_fee",
        "agency_fee",
        "weighted_duty_rate",
        "duty_tarrifs",
        "domestic_inbound",
        "total_freight",
        "domestic_outbound",
        "vendor_support_dol",
        "vendor_support_fast_dol",
        "freight_other_charges_final",
        "volume_rebate",
        "price_cuts",
        "scanbacks",
        "markdowns",
        "vsf_exception",
        COALESCE("freight_other_charges_final", 0) AS "freight_other_charges"
      FROM "ALLOCATION_P2_SF_ROLLUP" 
)
SELECT DISTINCT
    "MERCH_YEAR",
    "PARENT_VENDOR_ID",
    "VENDOR_ID",
    "scandown_flag",
    "markdown_flag",
    "ARTICLE_NO",
    "YEAR_PERIOD",
    "freight_flag",
    "CO_CODE",
    "SUM_TYPE_CODE",
    "PRICE_OVRD_FLAG",
    "SCAN_ITEM_FLAG",
    "PRICE_TYPE_CODE",
    "BUY_ONLINE_CODE",
    "YEAR_WEEK",
    "YEAR_QUARTER",
    "ARTICLE_DESC",
    "ARTICLE_FULL",
    "ARTICLE_TYPE_NAME",
    "ARTICLE_TYPE_FULL",
    "LABEL_TYPE_CODE",
    "UOM_CODE",
    "UOM_DESC",
    "UOM_FULL",
    "ARTICLE_LENGTH",
    "ARTICLE_WIDTH",
    "ARTICLE_HEIGHT",
    "ARTICLE_WEIGHT",
    "ARTICLE_SIZE",
    "ARTICLE_COUNT",
    "LOC_TYPE_CODE",
    "ARTICLE_TYPE_CODE",
    "PRIMARY_VENDOR_ID",
    "ARTICLE_CAT_CODE",
    "ARTICLE_CAT_NAME",
    "ARTICLE_CAT_FULL",
    "DEPT_CODE",
    "DEPT_NAME",
    "DEPT_FULL",
    "LOB_NO",
    "LOB_DESC",
    "LOB_FULL",
    "MAJ_DEPT_NO",
    "MAJ_DEPT_DESC",
    "MAJ_DEPT_FULL",
    "PVT_LABEL_DESC",
    "BRAND_NAME",
    "VENDOR_NAME",
    "VENDOR_FULL",
    "VENDOR_CO_NAME",
    "REGION_CODE",
    "channel",
    "ORDER_LINE_TYPE",
    "SCAC",
    "OMS_ORDER_LINE_SCHEDULE_SHIP_NODE",
    "SALES_QTY",
    "SALES_AMT",
    "COST_AMT",
    "margin",
    "DISC_AMT",
    "OVRD_AMT",
    "TRANS_TAX_AMT",
    "GROSS_SALES",
    "SHIPPING_REVENUE",
    "SHIPPING_DISC",
    "ACTUAL_SHIPPING_REVENUE",
    "omni_tsc_delivery",
    "omni_small_package_ltl",
    "omni_roadie",
    "Omni_multi_salvage",
    "SALES_ALLOCATE",
    "COST_ALLOCATE",
    "unsellables",
    "defectives",
    "dc_shrink",
    "theft",
    "defective_allowance",
    "battery_core",
    "mixing_center",
    "new_store_discounts",
    "cash_discounts",
    "pop",
    "pdq",
    "marketing",
    "vendor_compliance",
    "booth_rental",
    "inventory_shrink",
    COALESCE(COALESCE("unsellables", 0) + COALESCE("defectives", 0) + COALESCE("dc_shrink", 0) + COALESCE("theft", 0) + COALESCE("inventory_shrink", 0) + COALESCE("Omni_multi_salvage", 0), "total_shrink") AS "total_shrink",
    "weighted_domestic_inbound",
    "weighted_ocean_freight",
    "ocean_freight",
    "weighted_domestic_outbound",
    "weighted_freight_cost",
    "freight_cost",
    "weighted_agency_fee",
    "agency_fee",
    "weighted_duty_rate",
    "duty_tarrifs",
    "domestic_inbound",
    COALESCE(COALESCE("domestic_inbound", 0) + COALESCE("domestic_outbound", 0) + COALESCE("freight_other_charges", 0) + COALESCE("ocean_freight", 0) + COALESCE("agency_fee", 0) + COALESCE("duty_tarrifs", 0), "total_freight") AS "total_freight",
    "domestic_outbound",
    "vendor_support_dol",
    "vendor_support_fast_dol",
    "freight_other_charges_final",
    "volume_rebate",
    "price_cuts",
    "scanbacks",
    "markdowns",
    "vsf_exception",
    "freight_other_charges",
    (((((((COALESCE("volume_rebate", 0) + COALESCE("vendor_support_dol", 0) + COALESCE("vendor_support_fast_dol", 0) + COALESCE("vsf_exception", 0) + COALESCE("price_cuts", 0) + COALESCE("defective_allowance", 0)) - COALESCE("battery_core", 0)) - COALESCE("mixing_center", 0)) + COALESCE("scanbacks", 0) + COALESCE("markdowns", 0) + COALESCE("new_store_discounts", 0)) - COALESCE("cash_discounts", 0)) + COALESCE("pop", 0) + COALESCE("pdq", 0)) - COALESCE("marketing", 0)) + COALESCE("vendor_compliance", 0) + COALESCE("booth_rental", 0) AS TOTAL_FUNDING,
    (((COALESCE("new_store_discounts", 0) - COALESCE("cash_discounts", 0)) + COALESCE("pop", 0) + COALESCE("pdq", 0)) - COALESCE("marketing", 0)) + COALESCE("vendor_compliance", 0) + COALESCE("booth_rental", 0) AS funding_outside_lm,
    (((COALESCE("volume_rebate", 0) + COALESCE("vendor_support_dol", 0) + COALESCE("vendor_support_fast_dol", 0) + COALESCE("vsf_exception", 0) + COALESCE("price_cuts", 0) + COALESCE("defective_allowance", 0)) - COALESCE("battery_core", 0)) - COALESCE("mixing_center", 0)) + COALESCE("scanbacks", 0) + COALESCE("markdowns", 0) AS funding_in_lm,
    ((((((((((COALESCE("SALES_AMT", 0) - COALESCE("COST_AMT", 0)) + COALESCE("volume_rebate", 0) + COALESCE("vendor_support_dol", 0) + COALESCE("vendor_support_fast_dol", 0) + COALESCE("vsf_exception", 0) + COALESCE("price_cuts", 0) + COALESCE("defective_allowance", 0)) - COALESCE("battery_core", 0)) - COALESCE("mixing_center", 0)) + COALESCE("scanbacks", 0) + COALESCE("markdowns", 0) + COALESCE("unsellables", 0) + COALESCE("defectives", 0) + COALESCE("dc_shrink", 0) + COALESCE("theft", 0) + COALESCE("inventory_shrink", 0) + COALESCE("Omni_multi_salvage", 0)) - COALESCE("domestic_inbound", 0)) - COALESCE("domestic_outbound", 0)) - COALESCE("freight_other_charges", 0)) - COALESCE("ocean_freight", 0)) - COALESCE("agency_fee", 0)) - COALESCE("duty_tarrifs", 0) AS landed_margin
FROM CTE;

<br>
--CREATING TEMP TABLE FOR LANDED_MARGIN_W_OMNI_FLOW_PATH_PREPARED
</br>
CREATE  OR REPLACE TEMPORARY TABLE LANDED_MARGIN_W_OMNI_FLOW_PATH_PREPARED_TEMP (
	MERCH_YEAR NUMBER(38,0),
	PARENT_VENDOR_ID NUMBER(38,0),
	VENDOR_ID NUMBER(38,0),
	"scandown_flag" NUMBER(38,0),
	"markdown_flag" NUMBER(38,0),
	ARTICLE_NO NUMBER(38,0),
	YEAR_PERIOD NUMBER(38,0),
	"freight_flag" NUMBER(38,0),
	CO_CODE VARCHAR(4194304),
	SUM_TYPE_CODE VARCHAR(4194304),
	PRICE_OVRD_FLAG BOOLEAN,
	SCAN_ITEM_FLAG BOOLEAN,
	PRICE_TYPE_CODE VARCHAR(4194304),
	BUY_ONLINE_CODE VARCHAR(4194304),
	YEAR_WEEK NUMBER(38,0),
	YEAR_QUARTER NUMBER(38,0),
	ARTICLE_DESC VARCHAR(4194304),
	ARTICLE_FULL VARCHAR(4194304),
	ARTICLE_TYPE_NAME VARCHAR(4194304),
	ARTICLE_TYPE_FULL VARCHAR(4194304),
	LABEL_TYPE_CODE NUMBER(38,0),
	UOM_CODE VARCHAR(4194304),
	UOM_DESC VARCHAR(4194304),
	UOM_FULL VARCHAR(4194304),
	ARTICLE_LENGTH FLOAT,
	ARTICLE_WIDTH FLOAT,
	ARTICLE_HEIGHT FLOAT,
	ARTICLE_WEIGHT FLOAT,
	ARTICLE_SIZE VARCHAR(4194304),
	ARTICLE_COUNT VARCHAR(4194304),
	LOC_TYPE_CODE VARCHAR(4194304),
	ARTICLE_TYPE_CODE VARCHAR(4194304),
	PRIMARY_VENDOR_ID NUMBER(38,0),
	ARTICLE_CAT_CODE NUMBER(38,0),
	ARTICLE_CAT_NAME VARCHAR(4194304),
	ARTICLE_CAT_FULL VARCHAR(4194304),
	DEPT_CODE NUMBER(38,0),
	DEPT_NAME VARCHAR(4194304),
	DEPT_FULL VARCHAR(4194304),
	LOB_NO NUMBER(38,0),
	LOB_DESC VARCHAR(4194304),
	LOB_FULL VARCHAR(4194304),
	MAJ_DEPT_NO NUMBER(38,0),
	MAJ_DEPT_DESC VARCHAR(4194304),
	MAJ_DEPT_FULL VARCHAR(4194304),
	PVT_LABEL_DESC VARCHAR(4194304),
	BRAND_NAME VARCHAR(4194304),
	VENDOR_NAME VARCHAR(4194304),
	VENDOR_FULL VARCHAR(4194304),
	VENDOR_CO_NAME VARCHAR(4194304),
	REGION_CODE VARCHAR(4194304),
	"channel" VARCHAR(4194304),
	ORDER_LINE_TYPE VARCHAR(4194304),
	SCAC VARCHAR(4194304),
	OMS_ORDER_LINE_SCHEDULE_SHIP_NODE NUMBER(38,0),
	SALES_QTY FLOAT,
	SALES_AMT FLOAT,
	COST_AMT FLOAT,
	"margin" FLOAT,
	DISC_AMT FLOAT,
	OVRD_AMT FLOAT,
	TRANS_TAX_AMT FLOAT,
	GROSS_SALES FLOAT,
	SALES_ALLOCATE FLOAT,
	COST_ALLOCATE FLOAT,
	"unsellables" FLOAT,
	"defectives" FLOAT,
	"dc_shrink" FLOAT,
	"theft" FLOAT,
	"defective_allowance" FLOAT,
	"battery_core" FLOAT,
	"mixing_center" FLOAT,
	"new_store_discounts" FLOAT,
	"cash_discounts" FLOAT,
	"pop" FLOAT,
	"pdq" FLOAT,
	"marketing" FLOAT,
	"vendor_compliance" FLOAT,
	"booth_rental" FLOAT,
	"funding_outside_lm" FLOAT,
	"inventory_shrink" FLOAT,
	"total_shrink" FLOAT,
	"weighted_domestic_inbound" FLOAT,
	"weighted_ocean_freight" FLOAT,
	"ocean_freight" FLOAT,
	"weighted_domestic_outbound" FLOAT,
	"weighted_freight_cost" FLOAT,
	"freight_cost" FLOAT,
	"weighted_agency_fee" FLOAT,
	"agency_fee" FLOAT,
	"weighted_duty_rate" FLOAT,
	"duty_tarrifs" FLOAT,
	"domestic_inbound" FLOAT,
	"total_freight" FLOAT,
	"domestic_outbound" FLOAT,
	SHIPPING_REVENUE FLOAT,
	SHIPPING_DISC FLOAT,
	ACTUAL_SHIPPING_REVENUE FLOAT,
	"omni_tsc_delivery" FLOAT,
	"omni_small_package_ltl" FLOAT,
	"omni_roadie" FLOAT,
	"Omni_multi_salvage" FLOAT,
	"vendor_support_dol" FLOAT,
	"vendor_support_fast_dol" FLOAT,
	"freight_other_charges_final" FLOAT,
	"freight_other_charges" FLOAT,
	"volume_rebate" FLOAT,
	"price_cuts" FLOAT,
	"scanbacks" FLOAT,
	"markdowns" FLOAT,
	"vsf_exception" FLOAT,
	"landed_margin" FLOAT,
	"funding_in_lm" FLOAT,
	"total_funding" FLOAT,
	"omni_flow_path" VARCHAR(16777216),
	"Facility_Type" VARCHAR(255),
	"omni_flow_path_modified" VARCHAR(22),
	"small_parcel_ltl_cost" FLOAT,
	"last_mile_cost" FLOAT,
	"last_mile_delivery_combined" FLOAT,
	"omni_freight_flag" NUMBER(1,0)
);

<br>
-- ALLOCATION_W_BUYER_TEMP
</br>
TRUNCATE TABLE LANDED_MARGIN_W_OMNI_FLOW_PATH_PREPARED_TEMP;

<br>
--LANDED_MARGIN_W_OMNI_FLOW_PATH_PREPARED
</br>
insert into  LANDED_MARGIN_W_OMNI_FLOW_PATH_PREPARED_TEMP
WITH LANDED_MARGIN_W_OMNI_FLOW_PATH AS
(
SELECT DISTINCT
    "landed_margin_calc"."MERCH_YEAR" AS "MERCH_YEAR",
    "landed_margin_calc"."PARENT_VENDOR_ID" AS "PARENT_VENDOR_ID",
    "landed_margin_calc"."VENDOR_ID" AS "VENDOR_ID",
    "landed_margin_calc"."scandown_flag" AS "scandown_flag",
    "landed_margin_calc"."markdown_flag" AS "markdown_flag",
    "landed_margin_calc"."ARTICLE_NO" AS "ARTICLE_NO",
    "landed_margin_calc"."YEAR_PERIOD" AS "YEAR_PERIOD",
    "landed_margin_calc"."freight_flag" AS "freight_flag",
    "landed_margin_calc"."CO_CODE" AS "CO_CODE",
    "landed_margin_calc"."SUM_TYPE_CODE" AS "SUM_TYPE_CODE",
    "landed_margin_calc"."PRICE_OVRD_FLAG" AS "PRICE_OVRD_FLAG",
    "landed_margin_calc"."SCAN_ITEM_FLAG" AS "SCAN_ITEM_FLAG",
    "landed_margin_calc"."PRICE_TYPE_CODE" AS "PRICE_TYPE_CODE",
    "landed_margin_calc"."BUY_ONLINE_CODE" AS "BUY_ONLINE_CODE",
    "landed_margin_calc"."YEAR_WEEK" AS "YEAR_WEEK",
    "landed_margin_calc"."YEAR_QUARTER" AS "YEAR_QUARTER",
    "landed_margin_calc"."ARTICLE_DESC" AS "ARTICLE_DESC",
    "landed_margin_calc"."ARTICLE_FULL" AS "ARTICLE_FULL",
    "landed_margin_calc"."ARTICLE_TYPE_NAME" AS "ARTICLE_TYPE_NAME",
    "landed_margin_calc"."ARTICLE_TYPE_FULL" AS "ARTICLE_TYPE_FULL",
    "landed_margin_calc"."LABEL_TYPE_CODE" AS "LABEL_TYPE_CODE",
    "landed_margin_calc"."UOM_CODE" AS "UOM_CODE",
    "landed_margin_calc"."UOM_DESC" AS "UOM_DESC",
    "landed_margin_calc"."UOM_FULL" AS "UOM_FULL",
    "landed_margin_calc"."ARTICLE_LENGTH" AS "ARTICLE_LENGTH",
    "landed_margin_calc"."ARTICLE_WIDTH" AS "ARTICLE_WIDTH",
    "landed_margin_calc"."ARTICLE_HEIGHT" AS "ARTICLE_HEIGHT",
    "landed_margin_calc"."ARTICLE_WEIGHT" AS "ARTICLE_WEIGHT",
    "landed_margin_calc"."ARTICLE_SIZE" AS "ARTICLE_SIZE",
    "landed_margin_calc"."ARTICLE_COUNT" AS "ARTICLE_COUNT",
    "landed_margin_calc"."LOC_TYPE_CODE" AS "LOC_TYPE_CODE",
    "landed_margin_calc"."ARTICLE_TYPE_CODE" AS "ARTICLE_TYPE_CODE",
    "landed_margin_calc"."PRIMARY_VENDOR_ID" AS "PRIMARY_VENDOR_ID",
    "landed_margin_calc"."ARTICLE_CAT_CODE" AS "ARTICLE_CAT_CODE",
    "landed_margin_calc"."ARTICLE_CAT_NAME" AS "ARTICLE_CAT_NAME",
    "landed_margin_calc"."ARTICLE_CAT_FULL" AS "ARTICLE_CAT_FULL",
    "landed_margin_calc"."DEPT_CODE" AS "DEPT_CODE",
    "landed_margin_calc"."DEPT_NAME" AS "DEPT_NAME",
    "landed_margin_calc"."DEPT_FULL" AS "DEPT_FULL",
    "landed_margin_calc"."LOB_NO" AS "LOB_NO",
    "landed_margin_calc"."LOB_DESC" AS "LOB_DESC",
    "landed_margin_calc"."LOB_FULL" AS "LOB_FULL",
    "landed_margin_calc"."MAJ_DEPT_NO" AS "MAJ_DEPT_NO",
    "landed_margin_calc"."MAJ_DEPT_DESC" AS "MAJ_DEPT_DESC",
    "landed_margin_calc"."MAJ_DEPT_FULL" AS "MAJ_DEPT_FULL",
    "landed_margin_calc"."PVT_LABEL_DESC" AS "PVT_LABEL_DESC",
    "landed_margin_calc"."BRAND_NAME" AS "BRAND_NAME",
    "landed_margin_calc"."VENDOR_NAME" AS "VENDOR_NAME",
    "landed_margin_calc"."VENDOR_FULL" AS "VENDOR_FULL",
    "landed_margin_calc"."VENDOR_CO_NAME" AS "VENDOR_CO_NAME",
    "landed_margin_calc"."REGION_CODE" AS "REGION_CODE",
    "landed_margin_calc"."channel" AS "channel",
    "landed_margin_calc"."ORDER_LINE_TYPE" AS "ORDER_LINE_TYPE",
    "landed_margin_calc"."SCAC" AS "SCAC",
    "landed_margin_calc"."OMS_ORDER_LINE_SCHEDULE_SHIP_NODE" AS "OMS_ORDER_LINE_SCHEDULE_SHIP_NODE",
    "landed_margin_calc"."SALES_QTY" AS "SALES_QTY",
    "landed_margin_calc"."SALES_AMT" AS "SALES_AMT",
    "landed_margin_calc"."COST_AMT" AS "COST_AMT",
    "landed_margin_calc"."margin" AS "margin",
    "landed_margin_calc"."DISC_AMT" AS "DISC_AMT",
    "landed_margin_calc"."OVRD_AMT" AS "OVRD_AMT",
    "landed_margin_calc"."TRANS_TAX_AMT" AS "TRANS_TAX_AMT",
    "landed_margin_calc"."GROSS_SALES" AS "GROSS_SALES",
    "landed_margin_calc"."SHIPPING_REVENUE" AS "SHIPPING_REVENUE",
    "landed_margin_calc"."SHIPPING_DISC" AS "SHIPPING_DISC",
    "landed_margin_calc"."ACTUAL_SHIPPING_REVENUE" AS "ACTUAL_SHIPPING_REVENUE",
    "landed_margin_calc"."omni_tsc_delivery" AS "omni_tsc_delivery",
    "landed_margin_calc"."omni_small_package_ltl" AS "omni_small_package_ltl",
    "landed_margin_calc"."omni_roadie" AS "omni_roadie",
    "landed_margin_calc"."omni_multi_salvage" AS "Omni_multi_salvage",
    "landed_margin_calc"."SALES_ALLOCATE" AS "SALES_ALLOCATE",
    "landed_margin_calc"."COST_ALLOCATE" AS "COST_ALLOCATE",
    "landed_margin_calc"."unsellables" AS "unsellables",
    "landed_margin_calc"."defectives" AS "defectives",
    "landed_margin_calc"."dc_shrink" AS "dc_shrink",
    "landed_margin_calc"."theft" AS "theft",
    "landed_margin_calc"."defective_allowance" AS "defective_allowance",
    "landed_margin_calc"."battery_core" AS "battery_core",
    "landed_margin_calc"."mixing_center" AS "mixing_center",
    "landed_margin_calc"."new_store_discounts" AS "new_store_discounts",
    "landed_margin_calc"."cash_discounts" AS "cash_discounts",
    "landed_margin_calc"."pop" AS "pop",
    "landed_margin_calc"."pdq" AS "pdq",
    "landed_margin_calc"."marketing" AS "marketing",
    "landed_margin_calc"."vendor_compliance" AS "vendor_compliance",
    "landed_margin_calc"."booth_rental" AS "booth_rental",
    "landed_margin_calc"."funding_outside_lm" AS "funding_outside_lm",
    "landed_margin_calc"."inventory_shrink" AS "inventory_shrink",
    "landed_margin_calc"."total_shrink" AS "total_shrink",
    "landed_margin_calc"."weighted_domestic_inbound" AS "weighted_domestic_inbound",
    "landed_margin_calc"."weighted_ocean_freight" AS "weighted_ocean_freight",
    "landed_margin_calc"."ocean_freight" AS "ocean_freight",
    "landed_margin_calc"."weighted_domestic_outbound" AS "weighted_domestic_outbound",
    "landed_margin_calc"."weighted_freight_cost" AS "weighted_freight_cost",
    "landed_margin_calc"."freight_cost" AS "freight_cost",
    "landed_margin_calc"."weighted_agency_fee" AS "weighted_agency_fee",
    "landed_margin_calc"."agency_fee" AS "agency_fee",
    "landed_margin_calc"."weighted_duty_rate" AS "weighted_duty_rate",
    "landed_margin_calc"."duty_tarrifs" AS "duty_tarrifs",
    "landed_margin_calc"."domestic_inbound" AS "domestic_inbound",
    "landed_margin_calc"."total_freight" AS "total_freight",
    "landed_margin_calc"."domestic_outbound" AS "domestic_outbound",
    "landed_margin_calc"."vendor_support_dol" AS "vendor_support_dol",
    "landed_margin_calc"."vendor_support_fast_dol" AS "vendor_support_fast_dol",
    "landed_margin_calc"."freight_other_charges_final" AS "freight_other_charges_final",
    "landed_margin_calc"."freight_other_charges" AS "freight_other_charges",
    "landed_margin_calc"."volume_rebate" AS "volume_rebate",
    "landed_margin_calc"."price_cuts" AS "price_cuts",
    "landed_margin_calc"."scanbacks" AS "scanbacks",
    "landed_margin_calc"."markdowns" AS "markdowns",
    "landed_margin_calc"."vsf_exception" AS "vsf_exception",
    "landed_margin_calc"."landed_margin" AS "landed_margin",
    "landed_margin_calc"."funding_in_lm" AS "funding_in_lm",
    "landed_margin_calc"."total_funding" AS "total_funding",
    "omni_small_package_flag_prepared".omni_flow_path AS "omni_flow_path",
    "TSC_facilities_final".Facility_Type AS "Facility_Type"
  FROM "LANDED_MARGIN_CALC" "landed_margin_calc"
    LEFT JOIN "OMNI_SMALL_PACKAGE_FLAG_PREPARED" "omni_small_package_flag_prepared"
      ON  ( UPPER("landed_margin_calc"."ORDER_LINE_TYPE") = UPPER("omni_small_package_flag_prepared"."ORDER_LINE_TYPE"))
        AND ( UPPER("landed_margin_calc"."SCAC") = UPPER("omni_small_package_flag_prepared"."SCAC"))
    LEFT JOIN "TSC_FACILITIES_FINAL" "TSC_facilities_final"
      ON CAST("landed_margin_calc"."OMS_ORDER_LINE_SCHEDULE_SHIP_NODE" AS NUMBER) = CAST("TSC_facilities_final".Facility_Code AS NUMBER)
    )
    SELECT 
    "MERCH_YEAR",
    "PARENT_VENDOR_ID",
    "VENDOR_ID",
    "scandown_flag",
    "markdown_flag",
    "ARTICLE_NO",
    "YEAR_PERIOD",
    "freight_flag",
    "CO_CODE",
    "SUM_TYPE_CODE",
    "PRICE_OVRD_FLAG",
    "SCAN_ITEM_FLAG",
    "PRICE_TYPE_CODE",
    "BUY_ONLINE_CODE",
    "YEAR_WEEK",
    "YEAR_QUARTER",
    "ARTICLE_DESC",
    "ARTICLE_FULL",
    "ARTICLE_TYPE_NAME",
    "ARTICLE_TYPE_FULL",
    "LABEL_TYPE_CODE",
    "UOM_CODE",
    "UOM_DESC",
    "UOM_FULL",
    "ARTICLE_LENGTH",
    "ARTICLE_WIDTH",
    "ARTICLE_HEIGHT",
    "ARTICLE_WEIGHT",
    "ARTICLE_SIZE",
    "ARTICLE_COUNT",
    "LOC_TYPE_CODE",
    "ARTICLE_TYPE_CODE",
    "PRIMARY_VENDOR_ID",
    "ARTICLE_CAT_CODE",
    "ARTICLE_CAT_NAME",
    "ARTICLE_CAT_FULL",
    "DEPT_CODE",
    "DEPT_NAME",
    "DEPT_FULL",
    "LOB_NO",
    "LOB_DESC",
    "LOB_FULL",
    "MAJ_DEPT_NO",
    "MAJ_DEPT_DESC",
    "MAJ_DEPT_FULL",
    "PVT_LABEL_DESC",
    "BRAND_NAME",
    "VENDOR_NAME",
    "VENDOR_FULL",
    "VENDOR_CO_NAME",
    "REGION_CODE",
    "channel",
    "ORDER_LINE_TYPE",
    "SCAC",
    "OMS_ORDER_LINE_SCHEDULE_SHIP_NODE",
    "SALES_QTY",
    "SALES_AMT",
    "COST_AMT",
    "margin",
    "DISC_AMT",
    "OVRD_AMT",
    "TRANS_TAX_AMT",
    "GROSS_SALES",
    "SALES_ALLOCATE",
    "COST_ALLOCATE",
    "unsellables",
    "defectives",
    "dc_shrink",
    "theft",
    "defective_allowance",
    "battery_core",
    "mixing_center",
    "new_store_discounts",
    "cash_discounts",
    "pop",
    "pdq",
    "marketing",
    "vendor_compliance",
    "booth_rental",
    "funding_outside_lm",
    "inventory_shrink",
    "total_shrink",
    "weighted_domestic_inbound",
    "weighted_ocean_freight",
    "ocean_freight",
    "weighted_domestic_outbound",
    "weighted_freight_cost",
    "freight_cost",
    "weighted_agency_fee",
    "agency_fee",
    "weighted_duty_rate",
    "duty_tarrifs",
    "domestic_inbound",
    "total_freight",
    "domestic_outbound",
    "SHIPPING_REVENUE",
    "SHIPPING_DISC",
    "ACTUAL_SHIPPING_REVENUE",
    "omni_tsc_delivery",
    "omni_small_package_ltl",
    "omni_roadie",
    "Omni_multi_salvage",
    "vendor_support_dol",
    "vendor_support_fast_dol",
    "freight_other_charges_final",
    "freight_other_charges",
    "volume_rebate",
    "price_cuts",
    "scanbacks",
    "markdowns",
    "vsf_exception",
    "landed_margin",
    "funding_in_lm",
    "total_funding",
    "omni_flow_path",
    "Facility_Type",
    "omni_flow_path_modified",
    "small_parcel_ltl_cost",
    "last_mile_cost",
    COALESCE("last_mile_cost", 0) + COALESCE("omni_tsc_delivery", 0) + COALESCE("omni_roadie", 0) AS "last_mile_delivery_combined",
    CASE WHEN ("omni_flow_path_modified" = 'BOPIS') OR ("omni_flow_path_modified" = 'BODFS - TSC Delivery') OR ("omni_flow_path_modified" = 'BODFS - Roadie') OR ("omni_flow_path_modified" = 'Store - Ship to Home') OR ("omni_flow_path_modified" = 'Store - Ship to Store') THEN 1 ELSE 0 END AS "omni_freight_flag"
  FROM (
    SELECT 
        "MERCH_YEAR",
        "PARENT_VENDOR_ID",
        "VENDOR_ID",
        "scandown_flag",
        "markdown_flag",
        "ARTICLE_NO",
        "YEAR_PERIOD",
        "freight_flag",
        "CO_CODE",
        "SUM_TYPE_CODE",
        "PRICE_OVRD_FLAG",
        "SCAN_ITEM_FLAG",
        "PRICE_TYPE_CODE",
        "BUY_ONLINE_CODE",
        "YEAR_WEEK",
        "YEAR_QUARTER",
        "ARTICLE_DESC",
        "ARTICLE_FULL",
        "ARTICLE_TYPE_NAME",
        "ARTICLE_TYPE_FULL",
        "LABEL_TYPE_CODE",
        "UOM_CODE",
        "UOM_DESC",
        "UOM_FULL",
        "ARTICLE_LENGTH",
        "ARTICLE_WIDTH",
        "ARTICLE_HEIGHT",
        "ARTICLE_WEIGHT",
        "ARTICLE_SIZE",
        "ARTICLE_COUNT",
        "LOC_TYPE_CODE",
        "ARTICLE_TYPE_CODE",
        "PRIMARY_VENDOR_ID",
        "ARTICLE_CAT_CODE",
        "ARTICLE_CAT_NAME",
        "ARTICLE_CAT_FULL",
        "DEPT_CODE",
        "DEPT_NAME",
        "DEPT_FULL",
        "LOB_NO",
        "LOB_DESC",
        "LOB_FULL",
        "MAJ_DEPT_NO",
        "MAJ_DEPT_DESC",
        "MAJ_DEPT_FULL",
        "PVT_LABEL_DESC",
        "BRAND_NAME",
        "VENDOR_NAME",
        "VENDOR_FULL",
        "VENDOR_CO_NAME",
        "REGION_CODE",
        "channel",
        "ORDER_LINE_TYPE",
        "SCAC",
        "OMS_ORDER_LINE_SCHEDULE_SHIP_NODE",
        "SALES_QTY",
        "SALES_AMT",
        "COST_AMT",
        "margin",
        "DISC_AMT",
        "OVRD_AMT",
        "TRANS_TAX_AMT",
        "GROSS_SALES",
        "SALES_ALLOCATE",
        "COST_ALLOCATE",
        "unsellables",
        "defectives",
        "dc_shrink",
        "theft",
        "defective_allowance",
        "battery_core",
        "mixing_center",
        "new_store_discounts",
        "cash_discounts",
        "pop",
        "pdq",
        "marketing",
        "vendor_compliance",
        "booth_rental",
        "funding_outside_lm",
        "inventory_shrink",
        "total_shrink",
        "weighted_domestic_inbound",
        "weighted_ocean_freight",
        "ocean_freight",
        "weighted_domestic_outbound",
        "weighted_freight_cost",
        "freight_cost",
        "weighted_agency_fee",
        "agency_fee",
        "weighted_duty_rate",
        "duty_tarrifs",
        "domestic_inbound",
        "total_freight",
        "domestic_outbound",
        "SHIPPING_REVENUE",
        "SHIPPING_DISC",
        "ACTUAL_SHIPPING_REVENUE",
        "omni_tsc_delivery",
        "omni_small_package_ltl",
        "omni_roadie",
        "Omni_multi_salvage",
        "vendor_support_dol",
        "vendor_support_fast_dol",
        "freight_other_charges_final",
        "freight_other_charges",
        "volume_rebate",
        "price_cuts",
        "scanbacks",
        "markdowns",
        "vsf_exception",
        "landed_margin",
        "funding_in_lm",
        "total_funding",
        "omni_flow_path",
        "Facility_Type",
        CASE WHEN "ORDER_LINE_TYPE" = 'BOPIS' THEN 'BOPIS' 
        ELSE CASE WHEN ("ORDER_LINE_TYPE" = 'BODFS') AND ("SCAC" = 'TMSDD') THEN 'BODFS - TSC Delivery' 
        ELSE CASE WHEN ("ORDER_LINE_TYPE" = 'BODFS') AND ("SCAC" = 'ROADIE') THEN 'BODFS - Roadie' 
        ELSE CASE WHEN ("ORDER_LINE_TYPE" = 'S2H') AND (UPPER("Facility_Type") = 'DC') THEN 'DC - Ship to Home' 
        ELSE CASE WHEN ("ORDER_LINE_TYPE" = 'S2S') AND (UPPER("Facility_Type") = 'DC') THEN 'DC - Ship to Store' 
        ELSE CASE WHEN ("ORDER_LINE_TYPE" = 'S2H') AND (UPPER("Facility_Type") = 'VENDOR') THEN 'Vendor - Ship to Home' 
        ELSE CASE WHEN ("ORDER_LINE_TYPE" = 'S2S') AND (UPPER("Facility_Type") = 'VENDOR') THEN 'Vendor - Ship to Store' 
        ELSE CASE WHEN ("ORDER_LINE_TYPE" = 'S2H') AND (UPPER("Facility_Type")= 'STORE') THEN 'Store - Ship to Home' 
        ELSE CASE WHEN ("ORDER_LINE_TYPE" = 'S2S') AND (UPPER("Facility_Type") = 'STORE') THEN 'Store - Ship to Store' 
        ELSE CASE WHEN "ORDER_LINE_TYPE" = 'SFS' THEN 'Store - Ship to Home'
        ELSE CASE WHEN ("ORDER_LINE_TYPE" = 'Return') OR ("SCAC" = 'Return') THEN 'Omni Return'
         ELSE 'NA'
          END END END END END END END END END END END AS "omni_flow_path_modified",
        CASE WHEN "omni_flow_path" = 'Small Parcel/LTL' THEN "omni_small_package_ltl" ELSE 0 END AS "small_parcel_ltl_cost",
        CASE WHEN "omni_flow_path" = 'Last Mile' THEN "omni_small_package_ltl" ELSE 0 END AS "last_mile_cost"
      FROM "LANDED_MARGIN_W_OMNI_FLOW_PATH"
    ) A
<br>
--CREATING TEMP TABLE FOR LANDED_MARGIN_W_FREIGHT
</br>
create or replace TEMP TABLE LANDED_MARGIN_W_FREIGHT_TEMP (
	MERCH_YEAR NUMBER(38,0),
	PARENT_VENDOR_ID NUMBER(38,0),
	VENDOR_ID NUMBER(38,0),
	"scandown_flag" NUMBER(38,0),
	"markdown_flag" NUMBER(38,0),
	ARTICLE_NO NUMBER(38,0),
	YEAR_PERIOD NUMBER(38,0),
	FREIGHT_FLAG NUMBER(38,0),
	CO_CODE VARCHAR(16777216),
	SUM_TYPE_CODE VARCHAR(16777216),
	PRICE_OVRD_FLAG BOOLEAN,
	SCAN_ITEM_FLAG BOOLEAN,
	PRICE_TYPE_CODE VARCHAR(16777216),
	BUY_ONLINE_CODE VARCHAR(16777216),
	YEAR_WEEK NUMBER(38,0),
	YEAR_QUARTER NUMBER(38,0),
	ARTICLE_DESC VARCHAR(16777216),
	ARTICLE_FULL VARCHAR(16777216),
	ARTICLE_TYPE_NAME VARCHAR(16777216),
	ARTICLE_TYPE_FULL VARCHAR(16777216),
	LABEL_TYPE_CODE NUMBER(38,0),
	UOM_CODE VARCHAR(16777216),
	UOM_DESC VARCHAR(16777216),
	UOM_FULL VARCHAR(16777216),
	ARTICLE_LENGTH FLOAT,
	ARTICLE_WIDTH FLOAT,
	ARTICLE_HEIGHT FLOAT,
	ARTICLE_WEIGHT FLOAT,
	ARTICLE_SIZE VARCHAR(16777216),
	ARTICLE_COUNT VARCHAR(16777216),
	LOC_TYPE_CODE VARCHAR(16777216),
	ARTICLE_TYPE_CODE VARCHAR(16777216),
	PRIMARY_VENDOR_ID NUMBER(38,0),
	ARTICLE_CAT_CODE NUMBER(38,0),
	ARTICLE_CAT_NAME VARCHAR(16777216),
	ARTICLE_CAT_FULL VARCHAR(16777216),
	DEPT_CODE NUMBER(38,0),
	DEPT_NAME VARCHAR(16777216),
	DEPT_FULL VARCHAR(16777216),
	LOB_NO NUMBER(38,0),
	LOB_DESC VARCHAR(16777216),
	LOB_FULL VARCHAR(16777216),
	MAJ_DEPT_NO NUMBER(38,0),
	MAJ_DEPT_DESC VARCHAR(16777216),
	MAJ_DEPT_FULL VARCHAR(16777216),
	PVT_LABEL_DESC VARCHAR(16777216),
	BRAND_NAME VARCHAR(16777216),
	VENDOR_NAME VARCHAR(16777216),
	VENDOR_FULL VARCHAR(16777216),
	VENDOR_CO_NAME VARCHAR(16777216),
	REGION_CODE VARCHAR(16777216),
	"channel" VARCHAR(16777216),
	ORDER_LINE_TYPE VARCHAR(16777216),
	SCAC VARCHAR(16777216),
	OMS_ORDER_LINE_SCHEDULE_SHIP_NODE NUMBER(38,0),
	SALES_QTY FLOAT,
	SALES_AMT FLOAT,
	COST_AMT FLOAT,
	"margin" FLOAT,
	DISC_AMT FLOAT,
	OVRD_AMT FLOAT,
	TRANS_TAX_AMT FLOAT,
	GROSS_SALES FLOAT,
	SALES_ALLOCATE FLOAT,
	COST_ALLOCATE FLOAT,
	"unsellables" FLOAT,
	"defectives" FLOAT,
	"dc_shrink" FLOAT,
	"theft" FLOAT,
	"defective_allowance" FLOAT,
	"battery_core" FLOAT,
	"mixing_center" FLOAT,
	"new_store_discounts" FLOAT,
	"cash_discounts" FLOAT,
	"pop" FLOAT,
	"pdq" FLOAT,
	"marketing" FLOAT,
	"vendor_compliance" FLOAT,
	"booth_rental" FLOAT,
	"funding_outside_lm" FLOAT,
	"inventory_shrink" FLOAT,
	"total_shrink" FLOAT,
	"weighted_domestic_inbound" FLOAT,
	"weighted_ocean_freight" FLOAT,
	"ocean_freight" FLOAT,
	"weighted_domestic_outbound" FLOAT,
	"weighted_freight_cost" FLOAT,
	"freight_cost" FLOAT,
	"weighted_agency_fee" FLOAT,
	"agency_fee" FLOAT,
	"weighted_duty_rate" FLOAT,
	"duty_tarrifs" FLOAT,
	"domestic_inbound" FLOAT,
	"total_freight" FLOAT,
	"domestic_outbound" FLOAT,
	SHIPPING_REVENUE FLOAT,
	SHIPPING_DISC FLOAT,
	ACTUAL_SHIPPING_REVENUE FLOAT,
	OMNI_TSC_DELIVERY FLOAT,
	OMNI_SMALL_PACKAGE_LTL FLOAT,
	OMNI_ROADIE FLOAT,
	"omni_multi_salvage" FLOAT,
	"vendor_support_dol" FLOAT,
	"vendor_support_fast_dol" FLOAT,
	"freight_other_charges_final" FLOAT,
	"freight_other_charges" FLOAT,
	"volume_rebate" NUMBER(38,0),
	"price_cuts" NUMBER(38,0),
	"scanbacks" NUMBER(38,0),
	"markdowns" NUMBER(38,0),
	"vsf_exception" NUMBER(38,0),
	"landed_margin" FLOAT,
	"funding_in_lm" FLOAT,
	"total_funding" FLOAT,
	OMNI_FLOW_PATH VARCHAR(16777216),
	LAST_MILE_COST FLOAT,
	LAST_MILE_DELIVERY_COMBINED VARCHAR(16777216),
	SMALL_PARCEL_LTL_COST FLOAT,
	FACILITY_TYPE VARCHAR(16777216),
	OMNI_FLOW_PATH_MODIFIED VARCHAR(16777216),
	OMNI_FREIGHT_FLAG NUMBER(38,0),
	LASTEST_FREIGHT_PER_UNIT FLOAT
);

<br>
-- LANDED_MARGIN_W_FREIGHT_TEMP
</br>
TRUNCATE TABLE LANDED_MARGIN_W_FREIGHT_TEMP;

<br>
--LANDED_MARGIN_W_FREIGHT
</br>
insert into LANDED_MARGIN_W_FREIGHT_TEMP
SELECT DISTINCT
    "landed"."MERCH_YEAR" AS "MERCH_YEAR",
    "landed"."PARENT_VENDOR_ID" AS "PARENT_VENDOR_ID",
    "landed"."VENDOR_ID" AS "VENDOR_ID",
    "landed"."scandown_flag" AS "scandown_flag",
    "landed"."markdown_flag" AS "markdown_flag",
    "landed"."ARTICLE_NO" AS "ARTICLE_NO",
    "landed"."YEAR_PERIOD" AS "YEAR_PERIOD",
    "landed"."freight_flag" AS "FREIGHT_FLAG",
    "landed"."CO_CODE" AS "CO_CODE",
    "landed"."SUM_TYPE_CODE" AS "SUM_TYPE_CODE",
    "landed"."PRICE_OVRD_FLAG" AS "PRICE_OVRD_FLAG",
    "landed"."SCAN_ITEM_FLAG" AS "SCAN_ITEM_FLAG",
    "landed"."PRICE_TYPE_CODE" AS "PRICE_TYPE_CODE",
    "landed"."BUY_ONLINE_CODE" AS "BUY_ONLINE_CODE",
    "landed"."YEAR_WEEK" AS "YEAR_WEEK",
    "landed"."YEAR_QUARTER" AS "YEAR_QUARTER",
    "landed"."ARTICLE_DESC" AS "ARTICLE_DESC",
    "landed"."ARTICLE_FULL" AS "ARTICLE_FULL",
    "landed"."ARTICLE_TYPE_NAME" AS "ARTICLE_TYPE_NAME",
    "landed"."ARTICLE_TYPE_FULL" AS "ARTICLE_TYPE_FULL",
    "landed"."LABEL_TYPE_CODE" AS "LABEL_TYPE_CODE",
    "landed"."UOM_CODE" AS "UOM_CODE",
    "landed"."UOM_DESC" AS "UOM_DESC",
    "landed"."UOM_FULL" AS "UOM_FULL",
    "landed"."ARTICLE_LENGTH" AS "ARTICLE_LENGTH",
    "landed"."ARTICLE_WIDTH" AS "ARTICLE_WIDTH",
    "landed"."ARTICLE_HEIGHT" AS "ARTICLE_HEIGHT",
    "landed"."ARTICLE_WEIGHT" AS "ARTICLE_WEIGHT",
    "landed"."ARTICLE_SIZE" AS "ARTICLE_SIZE",
    "landed"."ARTICLE_COUNT" AS "ARTICLE_COUNT",
    "landed"."LOC_TYPE_CODE" AS "LOC_TYPE_CODE",
    "landed"."ARTICLE_TYPE_CODE" AS "ARTICLE_TYPE_CODE",
    "landed"."PRIMARY_VENDOR_ID" AS "PRIMARY_VENDOR_ID",
    "landed"."ARTICLE_CAT_CODE" AS "ARTICLE_CAT_CODE",
    "landed"."ARTICLE_CAT_NAME" AS "ARTICLE_CAT_NAME",
    "landed"."ARTICLE_CAT_FULL" AS "ARTICLE_CAT_FULL",
    "landed"."DEPT_CODE" AS "DEPT_CODE",
    "landed"."DEPT_NAME" AS "DEPT_NAME",
    "landed"."DEPT_FULL" AS "DEPT_FULL",
    "landed"."LOB_NO" AS "LOB_NO",
    "landed"."LOB_DESC" AS "LOB_DESC",
    "landed"."LOB_FULL" AS "LOB_FULL",
    "landed"."MAJ_DEPT_NO" AS "MAJ_DEPT_NO",
    "landed"."MAJ_DEPT_DESC" AS "MAJ_DEPT_DESC",
    "landed"."MAJ_DEPT_FULL" AS "MAJ_DEPT_FULL",
    "landed"."PVT_LABEL_DESC" AS "PVT_LABEL_DESC",
    "landed"."BRAND_NAME" AS "BRAND_NAME",
    "landed"."VENDOR_NAME" AS "VENDOR_NAME",
    "landed"."VENDOR_FULL" AS "VENDOR_FULL",
    "landed"."VENDOR_CO_NAME" AS "VENDOR_CO_NAME",
    "landed"."REGION_CODE" AS "REGION_CODE",
    "landed"."channel" AS "channel",
    "landed"."ORDER_LINE_TYPE" AS "ORDER_LINE_TYPE",
    "landed"."SCAC" AS "SCAC",
    "landed"."OMS_ORDER_LINE_SCHEDULE_SHIP_NODE" AS "OMS_ORDER_LINE_SCHEDULE_SHIP_NODE",
    "landed"."SALES_QTY" AS "SALES_QTY",
    "landed"."SALES_AMT" AS "SALES_AMT",
    "landed"."COST_AMT" AS "COST_AMT",
    "landed"."margin" AS "margin",
    "landed"."DISC_AMT" AS "DISC_AMT",
    "landed"."OVRD_AMT" AS "OVRD_AMT",
    "landed"."TRANS_TAX_AMT" AS "TRANS_TAX_AMT",
    "landed"."GROSS_SALES" AS "GROSS_SALES",
    "landed"."SALES_ALLOCATE" AS "SALES_ALLOCATE",
    "landed"."COST_ALLOCATE" AS "COST_ALLOCATE",
    "landed"."unsellables" AS "unsellables",
    "landed"."defectives" AS "defectives",
    "landed"."dc_shrink" AS "dc_shrink",
    "landed"."theft" AS "theft",
    "landed"."defective_allowance" AS "defective_allowance",
    "landed"."battery_core" AS "battery_core",
    "landed"."mixing_center" AS "mixing_center",
    "landed"."new_store_discounts" AS "new_store_discounts",
    "landed"."cash_discounts" AS "cash_discounts",
    "landed"."pop" AS "pop",
    "landed"."pdq" AS "pdq",
    "landed"."marketing" AS "marketing",
    "landed"."vendor_compliance" AS "vendor_compliance",
    "landed"."booth_rental" AS "booth_rental",
    "landed"."funding_outside_lm" AS "funding_outside_lm",
    "landed"."inventory_shrink" AS "inventory_shrink",
    "landed"."total_shrink" AS "total_shrink",
    "landed"."weighted_domestic_inbound" AS "weighted_domestic_inbound",
    "landed"."weighted_ocean_freight" AS "weighted_ocean_freight",
    "landed"."ocean_freight" AS "ocean_freight",
    "landed"."weighted_domestic_outbound" AS "weighted_domestic_outbound",
    "landed"."weighted_freight_cost" AS "weighted_freight_cost",
    "landed"."freight_cost" AS "freight_cost",
    "landed"."weighted_agency_fee" AS "weighted_agency_fee",
    "landed"."agency_fee" AS "agency_fee",
    "landed"."weighted_duty_rate" AS "weighted_duty_rate",
    "landed"."duty_tarrifs" AS "duty_tarrifs",
    "landed"."domestic_inbound" AS "domestic_inbound",
    "landed"."total_freight" AS "total_freight",
    "landed"."domestic_outbound" AS "domestic_outbound",
    "landed"."SHIPPING_REVENUE" AS "SHIPPING_REVENUE",
    "landed"."SHIPPING_DISC" AS "SHIPPING_DISC",
    "landed"."ACTUAL_SHIPPING_REVENUE" AS "ACTUAL_SHIPPING_REVENUE",
     "landed"."omni_tsc_delivery" AS "OMNI_TSC_DELIVERY",
    "landed"."omni_small_package_ltl"  AS "OMNI_SMALL_PACKAGE_LTL",
    "landed"."omni_roadie" AS "OMNI_ROADIE",
    "landed"."Omni_multi_salvage" AS "omni_multi_salvage",
    "landed"."vendor_support_dol" AS "vendor_support_dol",
    "landed"."vendor_support_fast_dol" AS "vendor_support_fast_dol",
    "landed"."freight_other_charges_final" AS "freight_other_charges_final",
    "landed"."freight_other_charges" AS "freight_other_charges",
    "landed"."volume_rebate" AS "volume_rebate",
    "landed"."price_cuts" AS "price_cuts",
    "landed"."scanbacks" AS "scanbacks",
    "landed"."markdowns" AS "markdowns",
    "landed"."vsf_exception" AS "vsf_exception",
    "landed"."landed_margin" AS "landed_margin",
    "landed"."funding_in_lm" AS "funding_in_lm",
    "landed"."total_funding" AS "total_funding",
    "landed"."omni_flow_path" AS "OMNI_FLOW_PATH",
    "landed"."last_mile_cost" AS "LAST_MILE_COST",
    "landed"."last_mile_delivery_combined"  AS "LAST_MILE_DELIVERY_COMBINED",
    "landed"."small_parcel_ltl_cost" AS "SMALL_PARCEL_LTL_COST",
    "landed"."Facility_Type"  AS "FACILITY_TYPE",
    "landed"."omni_flow_path_modified" AS "OMNI_FLOW_PATH_MODIFIED",
    "landed"."omni_freight_flag" AS "OMNI_FREIGHT_FLAG",
    "store"."LASTEST_FREIGHT_PER_UNIT" AS "LASTEST_FREIGHT_PER_UNIT"
FROM LANDED_MARGIN_W_OMNI_FLOW_PATH_PREPARED_TEMP"landed"
LEFT JOIN (
    SELECT
        R.YEAR_PERIOD,
        R.ARTICLE_NO,
        R.VENDOR_ID	,          
        R.SALES_QTY	,
        R."TOTAL_FREIGHT",
        R."FREIGHT_PER_UNIT",                      
        R.VALUE_PARTITION,
        first_Value(R."FREIGHT_PER_UNIT")over (partition by R.ARTICLE_NO, R.VENDOR_ID, R.VALUE_PARTITION order by R.YEAR_PERIOD ASC)  as "LASTEST_FREIGHT_PER_UNIT",
        1 AS FREIGHT_FLAG
    FROM
        (
        SELECT   
            I.YEAR_PERIOD,
            I.ARTICLE_NO,
            I.VENDOR_ID,         
            I.SALES_QTY,
            I."TOTAL_FREIGHT",
            I."FREIGHT_PER_UNIT",             
            sum(case when ifnull( I."FREIGHT_PER_UNIT" ,0) = 0 then 0 else 1 end) over (Partition By I.ARTICLE_NO, I.VENDOR_ID order by I.YEAR_PERIOD ) as VALUE_PARTITION
        FROM 
            (
            select *,
            CASE
                WHEN SALES_QTY=0 THEN 0
                ELSE TOTAL_FREIGHT/SALES_QTY
            END AS FREIGHT_PER_UNIT
            from(
                SELECT 
                    "YEAR_PERIOD" AS "YEAR_PERIOD",
                    "ARTICLE_NO" AS "ARTICLE_NO",
                    "VENDOR_ID" AS "VENDOR_ID",
                    "SALES_QTY" AS "SALES_QTY",
                    "TOTAL_FREIGHT" AS "TOTAL_FREIGHT"
                FROM (
                    SELECT 
                        "YEAR_PERIOD" AS "YEAR_PERIOD",
                        "ARTICLE_NO" AS "ARTICLE_NO",
                        "VENDOR_ID" AS "VENDOR_ID",
                        SUM("SALES_QTY") AS "SALES_QTY",
                        SUM("TOTAL_FREIGHT") AS "TOTAL_FREIGHT"
                    FROM (
                        SELECT 
                            YEAR_PERIOD,
                            ARTICLE_NO,
                            VENDOR_ID,
                            SALES_QTY,
                            "total_freight" as "TOTAL_FREIGHT"
                        FROM LANDED_MARGIN_W_OMNI_FLOW_PATH_PREPARED_TEMP
                        WHERE "channel" = 'Store'
                        ) "dku__beforegrouping"
                    GROUP BY "YEAR_PERIOD", "ARTICLE_NO", "VENDOR_ID"
                    )"dku5"
                UNION ALL
                SELECT 
                    "YEAR_PERIOD" AS "YEAR_PERIOD",
                    "ARTICLE_NO" AS "ARTICLE_NO",
                    "VENDOR_ID" AS "VENDOR_ID",
                    NULL AS "SALES_QTY",
                    NULL AS "TOTAL_FREIGHT"
                FROM (
                    SELECT
                        "ARTICLE_NO",
                        "VENDOR_ID",
                        "YEAR_PERIOD"
                    FROM (
                        SELECT *
                        FROM (
                            SELECT 
                                "store"."ARTICLE_NO" AS "ARTICLE_NO",
                                "store"."VENDOR_ID" AS "VENDOR_ID",
                                "store"."YEAR_PERIOD" AS "YEAR_PERIOD",
                                "store_fieght"."YEAR_PERIOD" AS "ST_YEAR_PERIOD"
                            FROM (
                                SELECT 
                                    "store"."ARTICLE_NO" AS "ARTICLE_NO",
                                    "store"."VENDOR_ID" AS "VENDOR_ID",
                                    "time"."YEAR_PERIOD" AS "YEAR_PERIOD"
                                FROM (  
                                    SELECT 
                                        "ARTICLE_NO" AS "ARTICLE_NO",
                                        "VENDOR_ID" AS "VENDOR_ID"
                                    FROM (
                                        SELECT 
                                            "YEAR_PERIOD" AS "YEAR_PERIOD",
                                            "ARTICLE_NO" AS "ARTICLE_NO",
                                            "VENDOR_ID" AS "VENDOR_ID",
                                            "SALES_QTY" AS "SALES_QTY",
                                            "TOTAL_FREIGHT" AS "TOTAL_FREIGHT"
                                        FROM (
                                            SELECT 
                                                "YEAR_PERIOD" AS "YEAR_PERIOD",
                                                "ARTICLE_NO" AS "ARTICLE_NO",
                                                "VENDOR_ID" AS "VENDOR_ID",
                                                SUM("SALES_QTY") AS "SALES_QTY",
                                                SUM("TOTAL_FREIGHT") AS "TOTAL_FREIGHT"
                                            FROM (
                                                SELECT DISTINCT
                                                    YEAR_PERIOD,
                                                    ARTICLE_NO,
                                                    VENDOR_ID,
                                                    SALES_QTY,
                                                    "total_freight" as "TOTAL_FREIGHT"
                                                FROM LANDED_MARGIN_W_OMNI_FLOW_PATH_PREPARED_TEMP
                                                WHERE "channel" = 'Store'
                                                ) "dku__beforegrouping"
                                            GROUP BY "YEAR_PERIOD", "ARTICLE_NO", "VENDOR_ID"
                                            )"dku5"
                                        ) "dku__beforegrouping"
                                    GROUP BY "ARTICLE_NO", "VENDOR_ID"
                                    )"store"
                                CROSS JOIN TIME_DIMENSION_BY_YEAR_PERIOD_FILTERED "time"
                                ) "store"
                            LEFT JOIN ( SELECT 
                                            "YEAR_PERIOD" AS "YEAR_PERIOD",
                                            "ARTICLE_NO" AS "ARTICLE_NO",
                                            "VENDOR_ID" AS "VENDOR_ID",
                                            SUM("SALES_QTY") AS "SALES_QTY",
                                            SUM("TOTAL_FREIGHT") AS "TOTAL_FREIGHT"
                                        FROM (
                                            SELECT DISTINCT
                                                YEAR_PERIOD,
                                                ARTICLE_NO,
                                                VENDOR_ID,
                                                SALES_QTY,
                                                "total_freight" as "TOTAL_FREIGHT"
                                            FROM LANDED_MARGIN_W_OMNI_FLOW_PATH_PREPARED_TEMP
                                            WHERE UPPER("channel") = 'STORE'
                                            ) "dku__beforegrouping"
                                        GROUP BY "YEAR_PERIOD", "ARTICLE_NO", "VENDOR_ID"
                                        ) "store_fieght"
                                ON ("store"."ARTICLE_NO" = "store_fieght"."ARTICLE_NO")
                                AND ("store"."VENDOR_ID" = "store_fieght"."VENDOR_ID")
                                AND ("store"."YEAR_PERIOD" = "store_fieght"."YEAR_PERIOD")
                            ) "unfiltered_query"
                        WHERE "ST_YEAR_PERIOD" IS NULL
                        )"dku3"
                    GROUP BY "ARTICLE_NO", "VENDOR_ID", "YEAR_PERIOD"
                    )"dku4"
            )"dku5"
            ) I
        ORDER BY 
            I.YEAR_PERIOD,	
            I.ARTICLE_NO,	
            I.VENDOR_ID,	          
            I.SALES_QTY,	
            I."TOTAL_FREIGHT",	
            I."FREIGHT_PER_UNIT"              
    ) as R    
    ORDER BY 
        R.YEAR_PERIOD,
        R.ARTICLE_NO,
        R.VENDOR_ID	      
    ) "store"
ON ("landed"."VENDOR_ID" = "store"."VENDOR_ID")
    AND ("landed"."ARTICLE_NO" = "store"."ARTICLE_NO")
    AND ("landed"."YEAR_PERIOD" = "store"."YEAR_PERIOD")
    AND ("landed"."omni_freight_flag" = "store"."FREIGHT_FLAG");


<br>
	--STORE_FREIGHT_BY_ARTICLE_VENDOR
</br>
CREATE OR REPLACE TEMPORARY TABLE "STORE_FREIGHT_BY_ARTICLE_VENDOR"
AS
SELECT DISTINCT
  "YEAR_PERIOD" AS "YEAR_PERIOD",
  "ARTICLE_NO" AS "ARTICLE_NO",
  "VENDOR_ID" AS "VENDOR_ID",
  SUM("SALES_QTY") AS "SALES_QTY",
  SUM("total_freight") AS "total_freight"
FROM "LANDED_MARGIN_W_OMNI_FLOW_PATH_PREPARED_TEMP"
WHERE UPPER("channel") = 'STORE'
GROUP BY "YEAR_PERIOD", "ARTICLE_NO", "VENDOR_ID";

<br> 
--Creating dataset TIME_DIMENSION_BY_YEAR_PERIOD_FILTERED
-- CREATE or replace TEMPORARY TABLE TIME_DIMENSION_BY_YEAR_PERIOD_FILTERED AS 
-- SELECT 
--    YEAR_PERIOD
-- FROM TIME_DIMENSION_BY_YEAR_PERIOD
-- WHERE YEAR_PERIOD >= 202001 AND YEAR_PERIOD >= 202307;

--STORE_FREIGHT_NEW_ARTICLE_VENDORS
</br>
CREATE OR REPLACE TEMPORARY TABLE "STORE_FREIGHT_NEW_ARTICLE_VENDORS"
AS
WITH store_freight_by_article_vendor_by_ARTICLE_NO_joined_joined AS (
  
 SELECT DISTINCT
    "store_freight_by_article_vendor_by_ARTICLE_NO"."ARTICLE_NO" AS "ARTICLE_NO",
    "store_freight_by_article_vendor_by_ARTICLE_NO"."VENDOR_ID" AS "VENDOR_ID",
    "time_dimension_by_YEAR_PERIOD_filtered"."YEAR_PERIOD" AS "YEAR_PERIOD",
     "store_freight_by_article_vendor"."YEAR_PERIOD" AS "st_YEAR_PERIOD"
  FROM (SELECT DISTINCT
        "ARTICLE_NO" AS "ARTICLE_NO",
        "VENDOR_ID" AS "VENDOR_ID"
        FROM "STORE_FREIGHT_BY_ARTICLE_VENDOR"
        GROUP BY "ARTICLE_NO", "VENDOR_ID")"store_freight_by_article_vendor_by_ARTICLE_NO"
  CROSS JOIN "TIME_DIMENSION_BY_YEAR_PERIOD_FILTERED" "time_dimension_by_YEAR_PERIOD_filtered"
   LEFT JOIN "STORE_FREIGHT_BY_ARTICLE_VENDOR" "store_freight_by_article_vendor"
        ON ("store_freight_by_article_vendor_by_ARTICLE_NO"."ARTICLE_NO" = "store_freight_by_article_vendor"."ARTICLE_NO")
          AND ("store_freight_by_article_vendor_by_ARTICLE_NO"."VENDOR_ID" = "store_freight_by_article_vendor"."VENDOR_ID")
          AND ("time_dimension_by_YEAR_PERIOD_filtered"."YEAR_PERIOD" = "store_freight_by_article_vendor"."YEAR_PERIOD")
      WHERE "st_YEAR_PERIOD" IS NULL
      )
      SELECT DISTINCT
    "ARTICLE_NO" AS "ARTICLE_NO",
    "VENDOR_ID" AS "VENDOR_ID",
    "YEAR_PERIOD" AS "YEAR_PERIOD"
     FROM store_freight_by_article_vendor_by_ARTICLE_NO_joined_joined
     GROUP BY "ARTICLE_NO", "VENDOR_ID", "YEAR_PERIOD";

<br>
--STORE_FREIGHT_COST_PER_ARTICLE_LATEST_PREPARED
</br>
CREATE OR REPLACE TEMPORARY TABLE "STORE_FREIGHT_COST_PER_ARTICLE_LATEST_PREPARED"
  AS
  WITH store_freight_cost_per_article AS (
  SELECT *,
    CASE WHEN "SALES_QTY" = 0 THEN 0 ELSE "total_freight" / NULLIF("SALES_QTY", 0) END AS "freight_per_unit"
  FROM (
  SELECT 
      "YEAR_PERIOD" AS "YEAR_PERIOD",
      "ARTICLE_NO" AS "ARTICLE_NO",
      "VENDOR_ID" AS "VENDOR_ID",
      "SALES_QTY" AS "SALES_QTY",
      "total_freight" AS "total_freight"
    FROM "STORE_FREIGHT_BY_ARTICLE_VENDOR"
  UNION ALL
  SELECT 
      "YEAR_PERIOD" AS "YEAR_PERIOD",
      "ARTICLE_NO" AS "ARTICLE_NO",
      "VENDOR_ID" AS "VENDOR_ID",
      NULL AS "SALES_QTY",
      NULL AS "total_freight"
    FROM "STORE_FREIGHT_NEW_ARTICLE_VENDORS")A
    )
    SELECT
            R.YEAR_PERIOD	
          , R.ARTICLE_NO	
          , R.VENDOR_ID	          
          , R.SALES_QTY	
          , R."total_freight"	
          , R."freight_per_unit"                      
          , R.value_partition
          , first_Value(R."freight_per_unit")over (partition by R.ARTICLE_NO, R.VENDOR_ID, R.value_partition order by R.YEAR_PERIOD ASC)  as "lastest_freight_per_unit",
          1 AS "freight_flag"
  FROM
      (
        SELECT   DISTINCT
              I.YEAR_PERIOD	
            , I.ARTICLE_NO	
            , I.VENDOR_ID	          
            , I.SALES_QTY	
            , I."total_freight"	
            , I."freight_per_unit"              
            , sum(case when ifnull( I."freight_per_unit" ,0) = 0 then 0 else 1 end) over (Partition By I.ARTICLE_NO, I.VENDOR_ID order by I.YEAR_PERIOD ) as value_partition
        FROM 
            store_freight_cost_per_article I
        ORDER BY 
              I.YEAR_PERIOD	
            , I.ARTICLE_NO	
            , I.VENDOR_ID	          
            , I.SALES_QTY	
            , I."total_freight"	
            , I."freight_per_unit"              
  ) as R    
  ORDER BY 
      R.YEAR_PERIOD	
    , R.ARTICLE_NO	
    , R.VENDOR_ID	 ;     

<br>
--CREATING TEMP TABLE FOR LANDED_MARGIN_W_FREIGHT_PREPARED
</br>
create or replace TEMP TABLE LANDED_MARGIN_W_FREIGHT_PREPARED_TEMP (
	MERCH_YEAR NUMBER(38,0),
	PARENT_VENDOR_ID NUMBER(38,0),
	VENDOR_ID NUMBER(38,0),
	"scandown_flag" NUMBER(38,0),
	"markdown_flag" NUMBER(38,0),
	ARTICLE_NO NUMBER(38,0),
	YEAR_PERIOD NUMBER(38,0),
	FREIGHT_FLAG NUMBER(38,0),
	CO_CODE VARCHAR(16777216),
	SUM_TYPE_CODE VARCHAR(16777216),
	PRICE_OVRD_FLAG BOOLEAN,
	SCAN_ITEM_FLAG BOOLEAN,
	PRICE_TYPE_CODE VARCHAR(16777216),
	BUY_ONLINE_CODE VARCHAR(16777216),
	YEAR_WEEK NUMBER(38,0),
	YEAR_QUARTER NUMBER(38,0),
	ARTICLE_DESC VARCHAR(16777216),
	ARTICLE_FULL VARCHAR(16777216),
	ARTICLE_TYPE_NAME VARCHAR(16777216),
	ARTICLE_TYPE_FULL VARCHAR(16777216),
	LABEL_TYPE_CODE NUMBER(38,0),
	UOM_CODE VARCHAR(16777216),
	UOM_DESC VARCHAR(16777216),
	UOM_FULL VARCHAR(16777216),
	ARTICLE_LENGTH FLOAT,
	ARTICLE_WIDTH FLOAT,
	ARTICLE_HEIGHT FLOAT,
	ARTICLE_WEIGHT FLOAT,
	ARTICLE_SIZE VARCHAR(16777216),
	ARTICLE_COUNT VARCHAR(16777216),
	LOC_TYPE_CODE VARCHAR(16777216),
	ARTICLE_TYPE_CODE VARCHAR(16777216),
	PRIMARY_VENDOR_ID NUMBER(38,0),
	ARTICLE_CAT_CODE NUMBER(38,0),
	ARTICLE_CAT_NAME VARCHAR(16777216),
	ARTICLE_CAT_FULL VARCHAR(16777216),
	DEPT_CODE NUMBER(38,0),
	DEPT_NAME VARCHAR(16777216),
	DEPT_FULL VARCHAR(16777216),
	LOB_NO NUMBER(38,0),
	LOB_DESC VARCHAR(16777216),
	LOB_FULL VARCHAR(16777216),
	MAJ_DEPT_NO NUMBER(38,0),
	MAJ_DEPT_DESC VARCHAR(16777216),
	MAJ_DEPT_FULL VARCHAR(16777216),
	PVT_LABEL_DESC VARCHAR(16777216),
	BRAND_NAME VARCHAR(16777216),
	VENDOR_NAME VARCHAR(16777216),
	VENDOR_FULL VARCHAR(16777216),
	VENDOR_CO_NAME VARCHAR(16777216),
	REGION_CODE VARCHAR(16777216),
	"channel" VARCHAR(16777216),
	ORDER_LINE_TYPE VARCHAR(16777216),
	SCAC VARCHAR(16777216),
	OMS_ORDER_LINE_SCHEDULE_SHIP_NODE NUMBER(38,0),
	SALES_QTY FLOAT,
	SALES_AMT FLOAT,
	COST_AMT FLOAT,
	"margin" FLOAT,
	DISC_AMT FLOAT,
	OVRD_AMT FLOAT,
	TRANS_TAX_AMT FLOAT,
	GROSS_SALES FLOAT,
	SALES_ALLOCATE FLOAT,
	COST_ALLOCATE FLOAT,
	"unsellables" FLOAT,
	"defectives" FLOAT,
	"dc_shrink" FLOAT,
	"theft" FLOAT,
	"defective_allowance" FLOAT,
	"battery_core" FLOAT,
	"mixing_center" FLOAT,
	"new_store_discounts" FLOAT,
	"cash_discounts" FLOAT,
	"pop" FLOAT,
	"pdq" FLOAT,
	"marketing" FLOAT,
	"vendor_compliance" FLOAT,
	"booth_rental" FLOAT,
	"funding_outside_lm" FLOAT,
	"inventory_shrink" FLOAT,
	"total_shrink" FLOAT,
	"weighted_domestic_inbound" FLOAT,
	"weighted_ocean_freight" FLOAT,
	"weighted_domestic_outbound" FLOAT,
	"weighted_freight_cost" FLOAT,
	"freight_cost" FLOAT,
	"weighted_agency_fee" FLOAT,
	"agency_fee" FLOAT,
	"weighted_duty_rate" FLOAT,
	"duty_tarrifs" FLOAT,
	"domestic_inbound" FLOAT,
	"total_freight" FLOAT,
	"domestic_outbound" FLOAT,
	SHIPPING_REVENUE FLOAT,
	SHIPPING_DISC FLOAT,
	ACTUAL_SHIPPING_REVENUE FLOAT,
	OMNI_TSC_DELIVERY FLOAT,
	OMNI_SMALL_PACKAGE_LTL FLOAT,
	OMNI_ROADIE FLOAT,
	"omni_multi_salvage" FLOAT,
	"vendor_support_dol" FLOAT,
	"vendor_support_fast_dol" FLOAT,
	"freight_other_charges_final" FLOAT,
	"freight_other_charges" FLOAT,
	"volume_rebate" NUMBER(38,0),
	"price_cuts" NUMBER(38,0),
	"scanbacks" NUMBER(38,0),
	"markdowns" NUMBER(38,0),
	"vsf_exception" NUMBER(38,0),
	"landed_margin" FLOAT,
	"funding_in_lm" FLOAT,
	"total_funding" FLOAT,
	OMNI_FLOW_PATH VARCHAR(16777216),
	LAST_MILE_COST FLOAT,
	LAST_MILE_DELIVERY_COMBINED VARCHAR(16777216),
	SMALL_PARCEL_LTL_COST FLOAT,
	FACILITY_TYPE VARCHAR(16777216),
	OMNI_FLOW_PATH_MODIFIED VARCHAR(16777216),
	OMNI_FREIGHT_FLAG NUMBER(38,0),
	LASTEST_FREIGHT_PER_UNIT FLOAT,
	OCEAN_FREIGHT FLOAT,
	OMNI_FREIGHT FLOAT
);


<br>
-- LANDED_MARGIN_W_FREIGHT_PREPARED_TEMP
</br>
TRUNCATE TABLE LANDED_MARGIN_W_FREIGHT_PREPARED_TEMP;

<br>
--LANDED_MARGIN_W_FREIGHT_PREPARED
</br>
insert into "LANDED_MARGIN_W_FREIGHT_PREPARED_TEMP"
WITH CTE AS (
SELECT DISTINCT
    "landed_margin_w_omni_flow_path_prepared"."MERCH_YEAR" AS "MERCH_YEAR",
    "landed_margin_w_omni_flow_path_prepared"."PARENT_VENDOR_ID" AS "PARENT_VENDOR_ID",
    "landed_margin_w_omni_flow_path_prepared"."VENDOR_ID" AS "VENDOR_ID",
    "landed_margin_w_omni_flow_path_prepared"."scandown_flag" AS "scandown_flag",
    "landed_margin_w_omni_flow_path_prepared"."markdown_flag" AS "markdown_flag",
    "landed_margin_w_omni_flow_path_prepared"."ARTICLE_NO" AS "ARTICLE_NO",
    "landed_margin_w_omni_flow_path_prepared"."YEAR_PERIOD" AS "YEAR_PERIOD",
    "landed_margin_w_omni_flow_path_prepared"."freight_flag" AS "freight_flag",
    "landed_margin_w_omni_flow_path_prepared"."CO_CODE" AS "CO_CODE",
    "landed_margin_w_omni_flow_path_prepared"."SUM_TYPE_CODE" AS "SUM_TYPE_CODE",
    "landed_margin_w_omni_flow_path_prepared"."PRICE_OVRD_FLAG" AS "PRICE_OVRD_FLAG",
    "landed_margin_w_omni_flow_path_prepared"."SCAN_ITEM_FLAG" AS "SCAN_ITEM_FLAG",
    "landed_margin_w_omni_flow_path_prepared"."PRICE_TYPE_CODE" AS "PRICE_TYPE_CODE",
    "landed_margin_w_omni_flow_path_prepared"."BUY_ONLINE_CODE" AS "BUY_ONLINE_CODE",
    "landed_margin_w_omni_flow_path_prepared"."YEAR_WEEK" AS "YEAR_WEEK",
    "landed_margin_w_omni_flow_path_prepared"."YEAR_QUARTER" AS "YEAR_QUARTER",
    "landed_margin_w_omni_flow_path_prepared"."ARTICLE_DESC" AS "ARTICLE_DESC",
    "landed_margin_w_omni_flow_path_prepared"."ARTICLE_FULL" AS "ARTICLE_FULL",
    "landed_margin_w_omni_flow_path_prepared"."ARTICLE_TYPE_NAME" AS "ARTICLE_TYPE_NAME",
    "landed_margin_w_omni_flow_path_prepared"."ARTICLE_TYPE_FULL" AS "ARTICLE_TYPE_FULL",
    "landed_margin_w_omni_flow_path_prepared"."LABEL_TYPE_CODE" AS "LABEL_TYPE_CODE",
    "landed_margin_w_omni_flow_path_prepared"."UOM_CODE" AS "UOM_CODE",
    "landed_margin_w_omni_flow_path_prepared"."UOM_DESC" AS "UOM_DESC",
    "landed_margin_w_omni_flow_path_prepared"."UOM_FULL" AS "UOM_FULL",
    "landed_margin_w_omni_flow_path_prepared"."ARTICLE_LENGTH" AS "ARTICLE_LENGTH",
    "landed_margin_w_omni_flow_path_prepared"."ARTICLE_WIDTH" AS "ARTICLE_WIDTH",
    "landed_margin_w_omni_flow_path_prepared"."ARTICLE_HEIGHT" AS "ARTICLE_HEIGHT",
    "landed_margin_w_omni_flow_path_prepared"."ARTICLE_WEIGHT" AS "ARTICLE_WEIGHT",
    "landed_margin_w_omni_flow_path_prepared"."ARTICLE_SIZE" AS "ARTICLE_SIZE",
    "landed_margin_w_omni_flow_path_prepared"."ARTICLE_COUNT" AS "ARTICLE_COUNT",
    "landed_margin_w_omni_flow_path_prepared"."LOC_TYPE_CODE" AS "LOC_TYPE_CODE",
    "landed_margin_w_omni_flow_path_prepared"."ARTICLE_TYPE_CODE" AS "ARTICLE_TYPE_CODE",
    "landed_margin_w_omni_flow_path_prepared"."PRIMARY_VENDOR_ID" AS "PRIMARY_VENDOR_ID",
    "landed_margin_w_omni_flow_path_prepared"."ARTICLE_CAT_CODE" AS "ARTICLE_CAT_CODE",
    "landed_margin_w_omni_flow_path_prepared"."ARTICLE_CAT_NAME" AS "ARTICLE_CAT_NAME",
    "landed_margin_w_omni_flow_path_prepared"."ARTICLE_CAT_FULL" AS "ARTICLE_CAT_FULL",
    "landed_margin_w_omni_flow_path_prepared"."DEPT_CODE" AS "DEPT_CODE",
    "landed_margin_w_omni_flow_path_prepared"."DEPT_NAME" AS "DEPT_NAME",
    "landed_margin_w_omni_flow_path_prepared"."DEPT_FULL" AS "DEPT_FULL",
    "landed_margin_w_omni_flow_path_prepared"."LOB_NO" AS "LOB_NO",
    "landed_margin_w_omni_flow_path_prepared"."LOB_DESC" AS "LOB_DESC",
    "landed_margin_w_omni_flow_path_prepared"."LOB_FULL" AS "LOB_FULL",
    "landed_margin_w_omni_flow_path_prepared"."MAJ_DEPT_NO" AS "MAJ_DEPT_NO",
    "landed_margin_w_omni_flow_path_prepared"."MAJ_DEPT_DESC" AS "MAJ_DEPT_DESC",
    "landed_margin_w_omni_flow_path_prepared"."MAJ_DEPT_FULL" AS "MAJ_DEPT_FULL",
    "landed_margin_w_omni_flow_path_prepared"."PVT_LABEL_DESC" AS "PVT_LABEL_DESC",
    "landed_margin_w_omni_flow_path_prepared"."BRAND_NAME" AS "BRAND_NAME",
    "landed_margin_w_omni_flow_path_prepared"."VENDOR_NAME" AS "VENDOR_NAME",
    "landed_margin_w_omni_flow_path_prepared"."VENDOR_FULL" AS "VENDOR_FULL",
    "landed_margin_w_omni_flow_path_prepared"."VENDOR_CO_NAME" AS "VENDOR_CO_NAME",
    "landed_margin_w_omni_flow_path_prepared"."REGION_CODE" AS "REGION_CODE",
    "landed_margin_w_omni_flow_path_prepared"."channel" AS "channel",
    "landed_margin_w_omni_flow_path_prepared"."ORDER_LINE_TYPE" AS "ORDER_LINE_TYPE",
    "landed_margin_w_omni_flow_path_prepared"."SCAC" AS "SCAC",
    "landed_margin_w_omni_flow_path_prepared"."OMS_ORDER_LINE_SCHEDULE_SHIP_NODE" AS "OMS_ORDER_LINE_SCHEDULE_SHIP_NODE",
    "landed_margin_w_omni_flow_path_prepared"."SALES_QTY" AS "SALES_QTY",
    "landed_margin_w_omni_flow_path_prepared"."SALES_AMT" AS "SALES_AMT",
    "landed_margin_w_omni_flow_path_prepared"."COST_AMT" AS "COST_AMT",
    "landed_margin_w_omni_flow_path_prepared"."margin" AS "margin",
    "landed_margin_w_omni_flow_path_prepared"."DISC_AMT" AS "DISC_AMT",
    "landed_margin_w_omni_flow_path_prepared"."OVRD_AMT" AS "OVRD_AMT",
    "landed_margin_w_omni_flow_path_prepared"."TRANS_TAX_AMT" AS "TRANS_TAX_AMT",
    "landed_margin_w_omni_flow_path_prepared"."GROSS_SALES" AS "GROSS_SALES",
    "landed_margin_w_omni_flow_path_prepared"."SALES_ALLOCATE" AS "SALES_ALLOCATE",
    "landed_margin_w_omni_flow_path_prepared"."COST_ALLOCATE" AS "COST_ALLOCATE",
    "landed_margin_w_omni_flow_path_prepared"."unsellables" AS "unsellables",
    "landed_margin_w_omni_flow_path_prepared"."defectives" AS "defectives",
    "landed_margin_w_omni_flow_path_prepared"."dc_shrink" AS "dc_shrink",
    "landed_margin_w_omni_flow_path_prepared"."theft" AS "theft",
    "landed_margin_w_omni_flow_path_prepared"."defective_allowance" AS "defective_allowance",
    "landed_margin_w_omni_flow_path_prepared"."battery_core" AS "battery_core",
    "landed_margin_w_omni_flow_path_prepared"."mixing_center" AS "mixing_center",
    "landed_margin_w_omni_flow_path_prepared"."new_store_discounts" AS "new_store_discounts",
    "landed_margin_w_omni_flow_path_prepared"."cash_discounts" AS "cash_discounts",
    "landed_margin_w_omni_flow_path_prepared"."pop" AS "pop",
    "landed_margin_w_omni_flow_path_prepared"."pdq" AS "pdq",
    "landed_margin_w_omni_flow_path_prepared"."marketing" AS "marketing",
    "landed_margin_w_omni_flow_path_prepared"."vendor_compliance" AS "vendor_compliance",
    "landed_margin_w_omni_flow_path_prepared"."booth_rental" AS "booth_rental",
    "landed_margin_w_omni_flow_path_prepared"."funding_outside_lm" AS "funding_outside_lm",
    "landed_margin_w_omni_flow_path_prepared"."inventory_shrink" AS "inventory_shrink",
    "landed_margin_w_omni_flow_path_prepared"."total_shrink" AS "total_shrink",
    "landed_margin_w_omni_flow_path_prepared"."weighted_domestic_inbound" AS "weighted_domestic_inbound",
    "landed_margin_w_omni_flow_path_prepared"."weighted_ocean_freight" AS "weighted_ocean_freight",
    "landed_margin_w_omni_flow_path_prepared"."ocean_freight" AS "ocean_freight",
    "landed_margin_w_omni_flow_path_prepared"."weighted_domestic_outbound" AS "weighted_domestic_outbound",
    "landed_margin_w_omni_flow_path_prepared"."weighted_freight_cost" AS "weighted_freight_cost",
    "landed_margin_w_omni_flow_path_prepared"."freight_cost" AS "freight_cost",
    "landed_margin_w_omni_flow_path_prepared"."weighted_agency_fee" AS "weighted_agency_fee",
    "landed_margin_w_omni_flow_path_prepared"."agency_fee" AS "agency_fee",
    "landed_margin_w_omni_flow_path_prepared"."weighted_duty_rate" AS "weighted_duty_rate",
    "landed_margin_w_omni_flow_path_prepared"."duty_tarrifs" AS "duty_tarrifs",
    "landed_margin_w_omni_flow_path_prepared"."domestic_inbound" AS "domestic_inbound",
    "landed_margin_w_omni_flow_path_prepared"."total_freight" AS "total_freight",
    "landed_margin_w_omni_flow_path_prepared"."domestic_outbound" AS "domestic_outbound",
    "landed_margin_w_omni_flow_path_prepared"."SHIPPING_REVENUE" AS "SHIPPING_REVENUE",
    "landed_margin_w_omni_flow_path_prepared"."SHIPPING_DISC" AS "SHIPPING_DISC",
    "landed_margin_w_omni_flow_path_prepared"."ACTUAL_SHIPPING_REVENUE" AS "ACTUAL_SHIPPING_REVENUE",
    "landed_margin_w_omni_flow_path_prepared"."omni_tsc_delivery" AS "omni_tsc_delivery",
    "landed_margin_w_omni_flow_path_prepared"."omni_small_package_ltl" AS "omni_small_package_ltl",
    "landed_margin_w_omni_flow_path_prepared"."omni_roadie" AS "omni_roadie",
    "landed_margin_w_omni_flow_path_prepared"."Omni_multi_salvage" AS "0mni_multi_salvage",
    "landed_margin_w_omni_flow_path_prepared"."vendor_support_dol" AS "vendor_support_dol",
    "landed_margin_w_omni_flow_path_prepared"."vendor_support_fast_dol" AS "vendor_support_fast_dol",
    "landed_margin_w_omni_flow_path_prepared"."freight_other_charges_final" AS "freight_other_charges_final",
    "landed_margin_w_omni_flow_path_prepared"."freight_other_charges" AS "freight_other_charges",
    "landed_margin_w_omni_flow_path_prepared"."volume_rebate" AS "volume_rebate",
    "landed_margin_w_omni_flow_path_prepared"."price_cuts" AS "price_cuts",
    "landed_margin_w_omni_flow_path_prepared"."scanbacks" AS "scanbacks",
    "landed_margin_w_omni_flow_path_prepared"."markdowns" AS "markdowns",
    "landed_margin_w_omni_flow_path_prepared"."vsf_exception" AS "vsf_exception",
    "landed_margin_w_omni_flow_path_prepared"."landed_margin" AS "landed_margin",
    "landed_margin_w_omni_flow_path_prepared"."funding_in_lm" AS "funding_in_lm",
    "landed_margin_w_omni_flow_path_prepared"."total_funding" AS "total_funding",
    "landed_margin_w_omni_flow_path_prepared"."omni_flow_path" AS "OMNI_FLOW_PATH",
    "landed_margin_w_omni_flow_path_prepared"."last_mile_cost" AS "last_mile_cost",
    "landed_margin_w_omni_flow_path_prepared"."last_mile_delivery_combined" AS "last_mile_delivery_combined",
    "landed_margin_w_omni_flow_path_prepared"."small_parcel_ltl_cost" AS "small_parcel_ltl_cost",
    "landed_margin_w_omni_flow_path_prepared"."Facility_Type" AS "Facility_Type",
    "landed_margin_w_omni_flow_path_prepared"."omni_flow_path_modified" AS "omni_flow_path_modified",
    "landed_margin_w_omni_flow_path_prepared"."omni_freight_flag" AS "omni_freight_flag",
    "store_freight_cost_per_article_latest_prepared"."lastest_freight_per_unit" AS "lastest_freight_per_unit"
  FROM "LANDED_MARGIN_W_OMNI_FLOW_PATH_PREPARED_TEMP" "landed_margin_w_omni_flow_path_prepared"
  LEFT JOIN "STORE_FREIGHT_COST_PER_ARTICLE_LATEST_PREPARED" "store_freight_cost_per_article_latest_prepared"
    ON ("landed_margin_w_omni_flow_path_prepared"."VENDOR_ID" = "store_freight_cost_per_article_latest_prepared"."VENDOR_ID")
      AND ("landed_margin_w_omni_flow_path_prepared"."ARTICLE_NO" = "store_freight_cost_per_article_latest_prepared"."ARTICLE_NO")
      AND ("landed_margin_w_omni_flow_path_prepared"."YEAR_PERIOD" = "store_freight_cost_per_article_latest_prepared"."YEAR_PERIOD")
      AND ("landed_margin_w_omni_flow_path_prepared"."omni_freight_flag" = "store_freight_cost_per_article_latest_prepared"."freight_flag")
)

SELECT DISTINCT
    "MERCH_YEAR",
    "PARENT_VENDOR_ID",
    "VENDOR_ID",
    "scandown_flag",
    "markdown_flag",
    "ARTICLE_NO",
    "YEAR_PERIOD",
    "freight_flag",
    "CO_CODE",
    "SUM_TYPE_CODE",
    "PRICE_OVRD_FLAG",
    "SCAN_ITEM_FLAG",
    "PRICE_TYPE_CODE",
    "BUY_ONLINE_CODE",
    "YEAR_WEEK",
    "YEAR_QUARTER",
    "ARTICLE_DESC",
    "ARTICLE_FULL",
    "ARTICLE_TYPE_NAME",
    "ARTICLE_TYPE_FULL",
    "LABEL_TYPE_CODE",
    "UOM_CODE",
    "UOM_DESC",
    "UOM_FULL",
    "ARTICLE_LENGTH",
    "ARTICLE_WIDTH",
    "ARTICLE_HEIGHT",
    "ARTICLE_WEIGHT",
    "ARTICLE_SIZE",
    "ARTICLE_COUNT",
    "LOC_TYPE_CODE",
    "ARTICLE_TYPE_CODE",
    "PRIMARY_VENDOR_ID",
    "ARTICLE_CAT_CODE",
    "ARTICLE_CAT_NAME",
    "ARTICLE_CAT_FULL",
    "DEPT_CODE",
    "DEPT_NAME",
    "DEPT_FULL",
    "LOB_NO",
    "LOB_DESC",
    "LOB_FULL",
    "MAJ_DEPT_NO",
    "MAJ_DEPT_DESC",
    "MAJ_DEPT_FULL",
    "PVT_LABEL_DESC",
    "BRAND_NAME",
    "VENDOR_NAME",
    "VENDOR_FULL",
    "VENDOR_CO_NAME",
    "REGION_CODE",
    "channel",
    "ORDER_LINE_TYPE",
    "SCAC",
    "OMS_ORDER_LINE_SCHEDULE_SHIP_NODE",
    "SALES_QTY",
    "SALES_AMT",
    "COST_AMT",
    COALESCE((COALESCE("SALES_AMT", 0) + COALESCE("ACTUAL_SHIPPING_REVENUE", 0)) - COALESCE("COST_AMT", 0), "margin") AS "margin",
    "DISC_AMT",
    "OVRD_AMT",
    "TRANS_TAX_AMT",
    "GROSS_SALES",
    "SALES_ALLOCATE",
    "COST_ALLOCATE",
    "unsellables",
    "defectives",
    "dc_shrink",
    "theft",
    "defective_allowance",
    "battery_core",
    "mixing_center",
    "new_store_discounts",
    "cash_discounts",
    "pop",
    "pdq",
    "marketing",
    "vendor_compliance",
    "booth_rental",
    "funding_outside_lm",
    "inventory_shrink",
    "total_shrink",
    "weighted_domestic_inbound",
    "weighted_ocean_freight",
    "weighted_domestic_outbound",
    "weighted_freight_cost",
    "freight_cost",
    "weighted_agency_fee",
    "agency_fee",
    "weighted_duty_rate",
    "duty_tarrifs",
    "domestic_inbound",
    COALESCE(COALESCE("domestic_inbound", 0) + COALESCE("domestic_outbound", 0) + COALESCE("freight_other_charges", 0) + COALESCE("ocean_freight", 0) + COALESCE("agency_fee", 0) + COALESCE("duty_tarrifs", 0) + COALESCE("lastest_freight_per_unit", 0) * COALESCE("SALES_QTY", 0) + COALESCE("small_parcel_ltl_cost", 0) + COALESCE("last_mile_delivery_combined", 0), "total_freight") AS "total_freight",
    "domestic_outbound",
    "SHIPPING_REVENUE",
    "SHIPPING_DISC",
    "ACTUAL_SHIPPING_REVENUE",
    "omni_tsc_delivery",
    "omni_small_package_ltl",
    "omni_roadie",
    "0mni_multi_salvage" as "omni_multi_salvage",
    "vendor_support_dol",
    "vendor_support_fast_dol",
    "freight_other_charges_final",
    "freight_other_charges",
    "volume_rebate",
    "price_cuts",
    "scanbacks",
    "markdowns",
    "vsf_exception",
    COALESCE(((((((((((((((COALESCE("SALES_AMT", 0) + COALESCE("ACTUAL_SHIPPING_REVENUE", 0)) - COALESCE("COST_AMT", 0)) + COALESCE("volume_rebate", 0) + COALESCE("vendor_support_dol", 0) + COALESCE("vendor_support_fast_dol", 0) + COALESCE("vsf_exception", 0) + COALESCE("price_cuts", 0) + COALESCE("defective_allowance", 0)) - COALESCE("battery_core", 0)) - COALESCE("mixing_center", 0)) + COALESCE("scanbacks", 0) + COALESCE("markdowns", 0) + COALESCE("unsellables", 0) + COALESCE("defectives", 0) + COALESCE("dc_shrink", 0) + COALESCE("theft", 0) + COALESCE("inventory_shrink", 0) + COALESCE("0mni_multi_salvage", 0)) - COALESCE("domestic_inbound", 0)) - COALESCE("domestic_outbound", 0)) - COALESCE("freight_other_charges", 0)) - COALESCE("ocean_freight", 0)) - COALESCE("agency_fee", 0)) - COALESCE("duty_tarrifs", 0)) - COALESCE("lastest_freight_per_unit", 0) * COALESCE("SALES_QTY", 0)) - COALESCE("small_parcel_ltl_cost", 0)) - COALESCE("last_mile_delivery_combined", 0), "landed_margin") AS "landed_margin",
    "funding_in_lm",
    "total_funding",
    "OMNI_FLOW_PATH",
    "last_mile_cost",
    "last_mile_delivery_combined",
    "small_parcel_ltl_cost",
    "Facility_Type",
    TRIM("omni_flow_path_modified") AS "omni_flow_path_modified",
    "omni_freight_flag",
    "lastest_freight_per_unit",
     "ocean_freight",
    "omni_freight"
  FROM (
    SELECT 
        "MERCH_YEAR",
        "PARENT_VENDOR_ID",
        "VENDOR_ID",
        "scandown_flag",
        "markdown_flag",
        "ARTICLE_NO",
        "YEAR_PERIOD",
        "freight_flag",
        "CO_CODE",
        "SUM_TYPE_CODE",
        "PRICE_OVRD_FLAG",
        "SCAN_ITEM_FLAG",
        "PRICE_TYPE_CODE",
        "BUY_ONLINE_CODE",
        "YEAR_WEEK",
        "YEAR_QUARTER",
        "ARTICLE_DESC",
        "ARTICLE_FULL",
        "ARTICLE_TYPE_NAME",
        "ARTICLE_TYPE_FULL",
        "LABEL_TYPE_CODE",
        "UOM_CODE",
        "UOM_DESC",
        "UOM_FULL",
        "ARTICLE_LENGTH",
        "ARTICLE_WIDTH",
        "ARTICLE_HEIGHT",
        "ARTICLE_WEIGHT",
        "ARTICLE_SIZE",
        "ARTICLE_COUNT",
        "LOC_TYPE_CODE",
        "ARTICLE_TYPE_CODE",
        "PRIMARY_VENDOR_ID",
        "ARTICLE_CAT_CODE",
        "ARTICLE_CAT_NAME",
        "ARTICLE_CAT_FULL",
        "DEPT_CODE",
        "DEPT_NAME",
        "DEPT_FULL",
        "LOB_NO",
        "LOB_DESC",
        "LOB_FULL",
        "MAJ_DEPT_NO",
        "MAJ_DEPT_DESC",
        "MAJ_DEPT_FULL",
        "PVT_LABEL_DESC",
        "BRAND_NAME",
        "VENDOR_NAME",
        "VENDOR_FULL",
        "VENDOR_CO_NAME",
        "REGION_CODE",
        "channel",
        "ORDER_LINE_TYPE",
        "SCAC",
        "OMS_ORDER_LINE_SCHEDULE_SHIP_NODE",
        "SALES_QTY",
        "SALES_AMT",
        "COST_AMT",
        "margin",
        "DISC_AMT",
        "OVRD_AMT",
        "TRANS_TAX_AMT",
        "GROSS_SALES",
        "SALES_ALLOCATE",
        "COST_ALLOCATE",
        "unsellables",
        "defectives",
        "dc_shrink",
        "theft",
        "defective_allowance",
        "battery_core",
        "mixing_center",
        "new_store_discounts",
        "cash_discounts",
        "pop",
        "pdq",
        "marketing",
        "vendor_compliance",
        "booth_rental",
        "funding_outside_lm",
        "inventory_shrink",
        "total_shrink",
        "weighted_domestic_inbound",
        "weighted_ocean_freight",
        REPLACE(CAST("ocean_freight" AS VARCHAR), 'NaN', '0') AS "ocean_freight",
        "weighted_domestic_outbound",
        "weighted_freight_cost",
        "freight_cost",
        "weighted_agency_fee",
        "agency_fee",
        "weighted_duty_rate",
        "duty_tarrifs",
        "domestic_inbound",
        "total_freight",
        "domestic_outbound",
        "SHIPPING_REVENUE",
        "SHIPPING_DISC",
        "ACTUAL_SHIPPING_REVENUE",
        "omni_tsc_delivery",
        "omni_small_package_ltl",
        "omni_roadie",
        "0mni_multi_salvage",
        "vendor_support_dol",
        "vendor_support_fast_dol",
        "freight_other_charges_final",
        "freight_other_charges",
        "volume_rebate",
        "price_cuts",
        "scanbacks",
        "markdowns",
        "vsf_exception",
        "landed_margin",
        "funding_in_lm",
        "total_funding",
        try_cast("OMNI_FLOW_PATH" as varchar) as "OMNI_FLOW_PATH",
        "last_mile_cost",
        "last_mile_delivery_combined",
        "small_parcel_ltl_cost",
        "Facility_Type",
        "omni_flow_path_modified",
        "omni_freight_flag",
        "lastest_freight_per_unit",
        COALESCE("lastest_freight_per_unit", 0) * COALESCE("SALES_QTY", 0) AS "omni_freight"
      FROM CTE
    ) A;

<br>
-- CREATING A TEMP TABLE FOR ALLOCATION_W_BUYER
</br>

create or replace TEMP TABLE ALLOCATION_W_BUYER_TEMP (
	MERCH_YEAR NUMBER(38,0),
	PARENT_VENDOR_ID NUMBER(38,0),
	VENDOR_ID NUMBER(38,0),
	ARTICLE_NO NUMBER(38,0),
	YEAR_PERIOD NUMBER(38,0),
	CO_CODE VARCHAR(16777216),
	SUM_TYPE_CODE VARCHAR(16777216),
	PRICE_OVRD_FLAG BOOLEAN,
	SCAN_ITEM_FLAG BOOLEAN,
	PRICE_TYPE_CODE VARCHAR(16777216),
	BUY_ONLINE_CODE VARCHAR(16777216),
	YEAR_WEEK NUMBER(38,0),
	YEAR_QUARTER NUMBER(38,0),
	ARTICLE_DESC VARCHAR(16777216),
	ARTICLE_FULL VARCHAR(16777216),
	ARTICLE_TYPE_NAME VARCHAR(16777216),
	ARTICLE_TYPE_FULL VARCHAR(16777216),
	LABEL_TYPE_CODE NUMBER(38,0),
	UOM_CODE VARCHAR(16777216),
	UOM_DESC VARCHAR(16777216),
	UOM_FULL VARCHAR(16777216),
	ARTICLE_LENGTH FLOAT,
	ARTICLE_WIDTH FLOAT,
	ARTICLE_HEIGHT FLOAT,
	ARTICLE_WEIGHT FLOAT,
	ARTICLE_SIZE VARCHAR(16777216),
	ARTICLE_COUNT VARCHAR(16777216),
	LOC_TYPE_CODE VARCHAR(16777216),
	ARTICLE_TYPE_CODE VARCHAR(16777216),
	PRIMARY_VENDOR_ID NUMBER(38,0),
	ARTICLE_CAT_CODE NUMBER(38,0),
	ARTICLE_CAT_NAME VARCHAR(16777216),
	ARTICLE_CAT_FULL VARCHAR(16777216),
	DEPT_CODE NUMBER(38,0),
	DEPT_NAME VARCHAR(16777216),
	DEPT_FULL VARCHAR(16777216),
	LOB_NO NUMBER(38,0),
	LOB_DESC VARCHAR(16777216),
	LOB_FULL VARCHAR(16777216),
	MAJ_DEPT_NO NUMBER(38,0),
	MAJ_DEPT_DESC VARCHAR(16777216),
	MAJ_DEPT_FULL VARCHAR(16777216),
	PVT_LABEL_DESC VARCHAR(16777216),
	BRAND_NAME VARCHAR(16777216),
	VENDOR_NAME VARCHAR(16777216),
	VENDOR_FULL VARCHAR(16777216),
	VENDOR_CO_NAME VARCHAR(16777216),
	REGION_CODE VARCHAR(16777216),
	"channel" VARCHAR(16777216),
	ORDER_LINE_TYPE VARCHAR(16777216),
	SCAC VARCHAR(16777216),
	OMS_ORDER_LINE_SCHEDULE_SHIP_NODE NUMBER(38,0),
	SALES_QTY FLOAT,
	SALES_AMT FLOAT,
	COST_AMT FLOAT,
	"margin" FLOAT,
	DISC_AMT FLOAT,
	OVRD_AMT FLOAT,
	TRANS_TAX_AMT FLOAT,
	GROSS_SALES FLOAT,
	SALES_ALLOCATE FLOAT,
	COST_ALLOCATE FLOAT,
	"unsellables" FLOAT,
	"defectives" FLOAT,
	"dc_shrink" FLOAT,
	"theft" FLOAT,
	"defective_allowance" FLOAT,
	"battery_core" FLOAT,
	"mixing_center" FLOAT,
	"new_store_discounts" FLOAT,
	"cash_discounts" FLOAT,
	"pop" FLOAT,
	"pdq" FLOAT,
	"marketing" FLOAT,
	"vendor_compliance" FLOAT,
	"booth_rental" FLOAT,
	"funding_outside_lm" FLOAT,
	"inventory_shrink" FLOAT,
	"total_shrink" FLOAT,
	"weighted_domestic_inbound" FLOAT,
	"weighted_ocean_freight" FLOAT,
	"ocean_freight" FLOAT,
	"weighted_domestic_outbound" FLOAT,
	"weighted_freight_cost" FLOAT,
	"freight_cost" FLOAT,
	"weighted_agency_fee" FLOAT,
	"agency_fee" FLOAT,
	"weighted_duty_rate" FLOAT,
	"duty_tarrifs" FLOAT,
	"domestic_inbound" FLOAT,
	"total_freight" FLOAT,
	"domestic_outbound" FLOAT,
	SHIPPING_REVENUE FLOAT,
	SHIPPING_DISC FLOAT,
	ACTUAL_SHIPPING_REVENUE FLOAT,
	OMNI_TSC_DELIVERY FLOAT,
	OMNI_SMALL_PACKAGE_LTL FLOAT,
	OMNI_ROADIE FLOAT,
	"omni_multi_salvage" FLOAT,
	"vendor_support_dol" FLOAT,
	"vendor_support_fast_dol" FLOAT,
	"freight_other_charges" FLOAT,
	"volume_rebate" NUMBER(38,0),
	"price_cuts" NUMBER(38,0),
	"scanbacks" NUMBER(38,0),
	"markdowns" NUMBER(38,0),
	"vsf_exception" NUMBER(38,0),
	"landed_margin" FLOAT,
	"funding_in_lm" FLOAT,
	"total_funding" FLOAT,
	"omni_flow_path" VARCHAR(16777216),
	"last_mile_cost" FLOAT,
	"last_mile_delivery_combined" VARCHAR(16777216),
	"small_parcel_ltl_cost" FLOAT,
	"Facility_Type" VARCHAR(16777216),
	"omni_flow_path_modified" VARCHAR(16777216),
	"omni_freight" FLOAT,
	BUYER_CODE VARCHAR(16777216),
	BUYER_NAME VARCHAR(16777216),
	BUYER_FULL VARCHAR(16777216),
	ARTICLE_COLOR VARCHAR(16777216),
	PARENT_ARTICLE_NO NUMBER(38,0),
	PARENT_ARTICLE_DESC VARCHAR(16777216),
	PARENT_VENDOR_FULL VARCHAR(16777216)
);

<br>
-- ALLOCATION_W_BUYER_TEMP
</br>
TRUNCATE TABLE ALLOCATION_W_BUYER_TEMP;

<br>
-- ALLOCATION_W_BUYER
</br>
insert into ALLOCATION_W_BUYER_TEMP
SELECT DISTINCT
    "landed_margin_w_freight_prepared"."MERCH_YEAR" AS "MERCH_YEAR",
    "landed_margin_w_freight_prepared"."PARENT_VENDOR_ID" AS "PARENT_VENDOR_ID",
    "landed_margin_w_freight_prepared"."VENDOR_ID" AS "VENDOR_ID",
    "landed_margin_w_freight_prepared"."ARTICLE_NO" AS "ARTICLE_NO",
    "landed_margin_w_freight_prepared"."YEAR_PERIOD" AS "YEAR_PERIOD",
    "landed_margin_w_freight_prepared"."CO_CODE" AS "CO_CODE",
    "landed_margin_w_freight_prepared"."SUM_TYPE_CODE" AS "SUM_TYPE_CODE",
    "landed_margin_w_freight_prepared"."PRICE_OVRD_FLAG" AS "PRICE_OVRD_FLAG",
    "landed_margin_w_freight_prepared"."SCAN_ITEM_FLAG" AS "SCAN_ITEM_FLAG",
    "landed_margin_w_freight_prepared"."PRICE_TYPE_CODE" AS "PRICE_TYPE_CODE",
    "landed_margin_w_freight_prepared"."BUY_ONLINE_CODE" AS "BUY_ONLINE_CODE",
    "landed_margin_w_freight_prepared"."YEAR_WEEK" AS "YEAR_WEEK",
    "landed_margin_w_freight_prepared"."YEAR_QUARTER" AS "YEAR_QUARTER",
    "landed_margin_w_freight_prepared"."ARTICLE_DESC" AS "ARTICLE_DESC",
    "landed_margin_w_freight_prepared"."ARTICLE_FULL" AS "ARTICLE_FULL",
    "landed_margin_w_freight_prepared"."ARTICLE_TYPE_NAME" AS "ARTICLE_TYPE_NAME",
    "landed_margin_w_freight_prepared"."ARTICLE_TYPE_FULL" AS "ARTICLE_TYPE_FULL",
    "landed_margin_w_freight_prepared"."LABEL_TYPE_CODE" AS "LABEL_TYPE_CODE",
    "landed_margin_w_freight_prepared"."UOM_CODE" AS "UOM_CODE",
    "landed_margin_w_freight_prepared"."UOM_DESC" AS "UOM_DESC",
    "landed_margin_w_freight_prepared"."UOM_FULL" AS "UOM_FULL",
    "landed_margin_w_freight_prepared"."ARTICLE_LENGTH" AS "ARTICLE_LENGTH",
    "landed_margin_w_freight_prepared"."ARTICLE_WIDTH" AS "ARTICLE_WIDTH",
    "landed_margin_w_freight_prepared"."ARTICLE_HEIGHT" AS "ARTICLE_HEIGHT",
    "landed_margin_w_freight_prepared"."ARTICLE_WEIGHT" AS "ARTICLE_WEIGHT",
    "landed_margin_w_freight_prepared"."ARTICLE_SIZE" AS "ARTICLE_SIZE",
    "landed_margin_w_freight_prepared"."ARTICLE_COUNT" AS "ARTICLE_COUNT",
    "landed_margin_w_freight_prepared"."LOC_TYPE_CODE" AS "LOC_TYPE_CODE",
    "landed_margin_w_freight_prepared"."ARTICLE_TYPE_CODE" AS "ARTICLE_TYPE_CODE",
    "landed_margin_w_freight_prepared"."PRIMARY_VENDOR_ID" AS "PRIMARY_VENDOR_ID",
    "landed_margin_w_freight_prepared"."ARTICLE_CAT_CODE" AS "ARTICLE_CAT_CODE",
    "landed_margin_w_freight_prepared"."ARTICLE_CAT_NAME" AS "ARTICLE_CAT_NAME",
    "landed_margin_w_freight_prepared"."ARTICLE_CAT_FULL" AS "ARTICLE_CAT_FULL",
    "landed_margin_w_freight_prepared"."DEPT_CODE" AS "DEPT_CODE",
    "landed_margin_w_freight_prepared"."DEPT_NAME" AS "DEPT_NAME",
    "landed_margin_w_freight_prepared"."DEPT_FULL" AS "DEPT_FULL",
    "landed_margin_w_freight_prepared"."LOB_NO" AS "LOB_NO",
    "landed_margin_w_freight_prepared"."LOB_DESC" AS "LOB_DESC",
    "landed_margin_w_freight_prepared"."LOB_FULL" AS "LOB_FULL",
    "landed_margin_w_freight_prepared"."MAJ_DEPT_NO" AS "MAJ_DEPT_NO",
    "landed_margin_w_freight_prepared"."MAJ_DEPT_DESC" AS "MAJ_DEPT_DESC",
    "landed_margin_w_freight_prepared"."MAJ_DEPT_FULL" AS "MAJ_DEPT_FULL",
    "landed_margin_w_freight_prepared"."PVT_LABEL_DESC" AS "PVT_LABEL_DESC",
    "landed_margin_w_freight_prepared"."BRAND_NAME" AS "BRAND_NAME",
    "landed_margin_w_freight_prepared"."VENDOR_NAME" AS "VENDOR_NAME",
    "landed_margin_w_freight_prepared"."VENDOR_FULL" AS "VENDOR_FULL",
    "landed_margin_w_freight_prepared"."VENDOR_CO_NAME" AS "VENDOR_CO_NAME",
    "landed_margin_w_freight_prepared"."REGION_CODE" AS "REGION_CODE",
    "landed_margin_w_freight_prepared"."channel" AS "channel",
    "landed_margin_w_freight_prepared"."ORDER_LINE_TYPE" AS "ORDER_LINE_TYPE",
    "landed_margin_w_freight_prepared"."SCAC" AS "SCAC",
    "landed_margin_w_freight_prepared"."OMS_ORDER_LINE_SCHEDULE_SHIP_NODE" AS "OMS_ORDER_LINE_SCHEDULE_SHIP_NODE",
    "landed_margin_w_freight_prepared"."SALES_QTY" AS "SALES_QTY",
    "landed_margin_w_freight_prepared"."SALES_AMT" AS "SALES_AMT",
    "landed_margin_w_freight_prepared"."COST_AMT" AS "COST_AMT",
    "landed_margin_w_freight_prepared"."margin" AS "margin",
    "landed_margin_w_freight_prepared"."DISC_AMT" AS "DISC_AMT",
    "landed_margin_w_freight_prepared"."OVRD_AMT" AS "OVRD_AMT",
    "landed_margin_w_freight_prepared"."TRANS_TAX_AMT" AS "TRANS_TAX_AMT",
    "landed_margin_w_freight_prepared"."GROSS_SALES" AS "GROSS_SALES",
    "landed_margin_w_freight_prepared"."SALES_ALLOCATE" AS "SALES_ALLOCATE",
    "landed_margin_w_freight_prepared"."COST_ALLOCATE" AS "COST_ALLOCATE",
    "landed_margin_w_freight_prepared"."unsellables" AS "unsellables",
    "landed_margin_w_freight_prepared"."defectives" AS "defectives",
    "landed_margin_w_freight_prepared"."dc_shrink" AS "dc_shrink",
    "landed_margin_w_freight_prepared"."theft" AS "theft",
    "landed_margin_w_freight_prepared"."defective_allowance" AS "defective_allowance",
    "landed_margin_w_freight_prepared"."battery_core" AS "battery_core",
    "landed_margin_w_freight_prepared"."mixing_center" AS "mixing_center",
    "landed_margin_w_freight_prepared"."new_store_discounts" AS "new_store_discounts",
    "landed_margin_w_freight_prepared"."cash_discounts" AS "cash_discounts",
    "landed_margin_w_freight_prepared"."pop" AS "pop",
    "landed_margin_w_freight_prepared"."pdq" AS "pdq",
    "landed_margin_w_freight_prepared"."marketing" AS "marketing",
    "landed_margin_w_freight_prepared"."vendor_compliance" AS "vendor_compliance",
    "landed_margin_w_freight_prepared"."booth_rental" AS "booth_rental",
    "landed_margin_w_freight_prepared"."funding_outside_lm" AS "funding_outside_lm",
    "landed_margin_w_freight_prepared"."inventory_shrink" AS "inventory_shrink",
    "landed_margin_w_freight_prepared"."total_shrink" AS "total_shrink",
    "landed_margin_w_freight_prepared"."weighted_domestic_inbound" AS "weighted_domestic_inbound",
    "landed_margin_w_freight_prepared"."weighted_ocean_freight" AS "weighted_ocean_freight",
    "landed_margin_w_freight_prepared"."OCEAN_FREIGHT" AS "ocean_freight",
    "landed_margin_w_freight_prepared"."weighted_domestic_outbound" AS "weighted_domestic_outbound",
    "landed_margin_w_freight_prepared"."weighted_freight_cost" AS "weighted_freight_cost",
    "landed_margin_w_freight_prepared"."freight_cost" AS "freight_cost",
    "landed_margin_w_freight_prepared"."weighted_agency_fee" AS "weighted_agency_fee",
    "landed_margin_w_freight_prepared"."agency_fee" AS "agency_fee",
    "landed_margin_w_freight_prepared"."weighted_duty_rate" AS "weighted_duty_rate",
    "landed_margin_w_freight_prepared"."duty_tarrifs" AS "duty_tarrifs",
    "landed_margin_w_freight_prepared"."domestic_inbound" AS "domestic_inbound",
    "landed_margin_w_freight_prepared"."total_freight" AS "total_freight",
    "landed_margin_w_freight_prepared"."domestic_outbound" AS "domestic_outbound",
    "landed_margin_w_freight_prepared"."SHIPPING_REVENUE" AS "SHIPPING_REVENUE",
    "landed_margin_w_freight_prepared"."SHIPPING_DISC" AS "SHIPPING_DISC",
    "landed_margin_w_freight_prepared"."ACTUAL_SHIPPING_REVENUE" AS "ACTUAL_SHIPPING_REVENUE",
    "landed_margin_w_freight_prepared".omni_tsc_delivery AS "omni_tsc_delivery",
    "landed_margin_w_freight_prepared".omni_small_package_ltl AS "omni_small_package_ltl",
    "landed_margin_w_freight_prepared".omni_roadie AS "omni_roadie",
    "landed_margin_w_freight_prepared"."omni_multi_salvage" AS "omni_multi_salvage",
    "landed_margin_w_freight_prepared"."vendor_support_dol" AS "vendor_support_dol",
    "landed_margin_w_freight_prepared"."vendor_support_fast_dol" AS "vendor_support_fast_dol",
    "landed_margin_w_freight_prepared"."freight_other_charges" AS "freight_other_charges",
    "landed_margin_w_freight_prepared"."volume_rebate" AS "volume_rebate",
    "landed_margin_w_freight_prepared"."price_cuts" AS "price_cuts",
    "landed_margin_w_freight_prepared"."scanbacks" AS "scanbacks",
    "landed_margin_w_freight_prepared"."markdowns" AS "markdowns",
    "landed_margin_w_freight_prepared"."vsf_exception" AS "vsf_exception",
    "landed_margin_w_freight_prepared"."landed_margin" AS "landed_margin",
    "landed_margin_w_freight_prepared"."funding_in_lm" AS "funding_in_lm",
    "landed_margin_w_freight_prepared"."total_funding" AS "total_funding",
    "landed_margin_w_freight_prepared".omni_flow_path AS "omni_flow_path",
    "landed_margin_w_freight_prepared".last_mile_cost AS "last_mile_cost",
    "landed_margin_w_freight_prepared".last_mile_delivery_combined AS "last_mile_delivery_combined",
    "landed_margin_w_freight_prepared".small_parcel_ltl_cost AS "small_parcel_ltl_cost",
    "landed_margin_w_freight_prepared".Facility_Type AS "Facility_Type",
    "landed_margin_w_freight_prepared".omni_flow_path_modified AS "omni_flow_path_modified",
    "landed_margin_w_freight_prepared".omni_freight AS "omni_freight",
    "company_article_w_buyer"."BUYER_CODE" AS "BUYER_CODE",
    "company_article_w_buyer"."BUYER_NAME" AS "BUYER_NAME",
    "company_article_w_buyer"."BUYER_FULL" AS "BUYER_FULL",
    "article"."ARTICLE_COLOR" AS "ARTICLE_COLOR",
    "article"."PARENT_ARTICLE_NO" AS "PARENT_ARTICLE_NO",
    "article"."PARENT_ARTICLE_DESC" AS "PARENT_ARTICLE_DESC",
    "vendor"."VENDOR_FULL" AS "PARENT_VENDOR_FULL"
  FROM "LANDED_MARGIN_W_FREIGHT_PREPARED_TEMP" "landed_margin_w_freight_prepared"
  LEFT JOIN COMPANY_ARTICLE_W_BUYER "company_article_w_buyer"
    ON TRY_TO_NUMBER("landed_margin_w_freight_prepared"."ARTICLE_NO") = TRY_TO_NUMBER("company_article_w_buyer"."ARTICLE_NO")
  LEFT JOIN ARTICLE_W_SHP_DIM_PREP "article"
    ON TRY_TO_NUMBER("landed_margin_w_freight_prepared"."ARTICLE_NO") = TRY_TO_NUMBER("article"."ARTICLE_NO")
  LEFT JOIN "EDW"."VW_VENDOR" "vendor"
    ON "landed_margin_w_freight_prepared"."PARENT_VENDOR_ID" = "vendor"."VENDOR_ID"


<br> </br>
TRUNCATE TABLE VISUAL_SALES_TABLE_SHARE;

<br>
--9 final
</br>
insert into VISUAL_SALES_TABLE_SHARE 
SELECT DISTINCT
    "ARTICLE_NO" AS ARTICLE_NO,
    "MERCH_YEAR" AS MERCH_YEAR,
    "VENDOR_ID" AS VENDOR_ID,
    "YEAR_PERIOD" AS YEAR_PERIOD,
    "ARTICLE_DESC" AS ARTICLE_DESC,
    "ARTICLE_SIZE" AS ARTICLE_SIZE,
    "MAJ_DEPT_DESC" AS MAJ_DEPT_DESC,
    "BRAND_NAME" AS BRAND_NAME,
    "BUYER_CODE" AS BUYER_CODE,
    "ARTICLE_COLOR" AS ARTICLE_COLOR,
    "PARENT_ARTICLE_NO" AS PARENT_ARTICLE_NO,
    "PARENT_ARTICLE_DESC" AS PARENT_ARTICLE_DESC,
    "ARTICLE_FULL" AS ARTICLE_FULL,
    "BUYER_FULL" AS BUYER_NAME,
    "ARTICLE_CAT_FULL" AS ARTICLE_CAT_NAME,
    "LOB_FULL" AS LOB_DESC,
    "DEPT_FULL" AS DEPT_NAME,
    "VENDOR_FULL" AS VENDOR_NAME,
    "PARENT_VENDOR_FULL" AS PARENT_VENDOR_NAME,
    "channel" AS channel,
    "SALES_QTY_sum" AS SALES_QTY,
    "SALES_AMT_sum" AS SALES_AMT,
    "COST_AMT_sum" AS COST_AMT,
    "margin_sum" AS margin,
    "unsellables_sum" AS unsellables,
    "defectives_sum" AS defectives_shrink,
    "dc_shrink_sum" AS dc_shrink,
    "theft_sum" AS theft,
    "defective_allowance_sum" AS defective_allowance,
    -1*coalesce("battery_core_sum",0) AS battery_core,
    -1*coalesce("mixing_center_sum",0) AS mixing_center,
    "new_store_discounts_sum" AS new_store_discounts,
    -1*coalesce("cash_discounts_sum",0) AS cash_discounts,
    "pop_sum" AS pop,
    "pdq_sum" AS pdq,
    -1*coalesce("marketing_sum",0) AS marketing,
    "vendor_compliance_sum" AS vendor_compliance,
    "booth_rental_sum" AS booth_rental,
    "funding_outside_lm_sum" AS funding_outside_lm,
    "inventory_shrink_sum" AS inventory_shrink,
    "total_shrink_sum" AS total_shrink,
    -1*coalesce("ocean_freight_sum",0) AS ocean_freight,
    -1*coalesce("freight_cost_sum",0) AS freight_cost,
    -1*coalesce("agency_fee_sum",0) AS agency_fee,
    -1*coalesce("duty_tarrifs_sum",0) AS duty_tarrifs,
    -1*coalesce("domestic_inbound_sum",0) AS domestic_inbound,
    -1*coalesce("total_freight_sum",0) AS total_freight,
    -1*coalesce("domestic_outbound_sum",0) AS domestic_outbound,
    -1*coalesce("freight_other_charges_sum",0) AS freight_other_charges,
    "volume_rebate_sum" AS volume_rebate,
    "price_cuts_sum" AS price_cuts,
    "scanbacks_sum" AS scanbacks,
    "markdowns_sum" AS markdowns,
    "landed_margin_sum" AS landed_margin,
    "funding_in_lm_sum" AS funding_in_lm,
    "total_funding_sum" AS total_funding,
    "vendor_support_new_sum" AS vendor_support,
    "vendor_support_fast_new_sum" AS vendor_support_fast,
    SUBSTRING("YEAR_PERIOD",5,2) AS PERIOD
FROM (
    SELECT 
        "ARTICLE_NO" AS "ARTICLE_NO",
        "MERCH_YEAR" AS "MERCH_YEAR",
        "VENDOR_ID" AS "VENDOR_ID",
        "YEAR_PERIOD" AS "YEAR_PERIOD",
        "ARTICLE_DESC" AS "ARTICLE_DESC",
        "ARTICLE_SIZE" AS "ARTICLE_SIZE",
        "MAJ_DEPT_DESC" AS "MAJ_DEPT_DESC",
        "BRAND_NAME" AS "BRAND_NAME",
        "BUYER_CODE" AS "BUYER_CODE",
        "ARTICLE_COLOR" AS "ARTICLE_COLOR",
        "PARENT_ARTICLE_NO" AS "PARENT_ARTICLE_NO",
        "PARENT_ARTICLE_DESC" AS "PARENT_ARTICLE_DESC",
        "ARTICLE_FULL" AS "ARTICLE_FULL",
        "BUYER_FULL" AS "BUYER_FULL",
        "ARTICLE_CAT_FULL" AS "ARTICLE_CAT_FULL",
        "LOB_FULL" AS "LOB_FULL",
        "DEPT_FULL" AS "DEPT_FULL",
        "VENDOR_FULL" AS "VENDOR_FULL",
        "PARENT_VENDOR_FULL" AS "PARENT_VENDOR_FULL",
        "channel" AS "channel",
        SUM("SALES_QTY") AS "SALES_QTY_sum",
        SUM("SALES_AMT") AS "SALES_AMT_sum",
        SUM("COST_AMT") AS "COST_AMT_sum",
        SUM("margin") AS "margin_sum",
        SUM("unsellables") AS "unsellables_sum",
        SUM("defectives") AS "defectives_sum",
        SUM("dc_shrink") AS "dc_shrink_sum",
        SUM("theft") AS "theft_sum",
        SUM("defective_allowance") AS "defective_allowance_sum",
        SUM("battery_core") AS "battery_core_sum",
        SUM("mixing_center") AS "mixing_center_sum",
        SUM("new_store_discounts") AS "new_store_discounts_sum",
        SUM("cash_discounts") AS "cash_discounts_sum",
        SUM("pop") AS "pop_sum",
        SUM("pdq") AS "pdq_sum",
        SUM("marketing") AS "marketing_sum",
        SUM("vendor_compliance") AS "vendor_compliance_sum",
        SUM("booth_rental") AS "booth_rental_sum",
        SUM("funding_outside_lm") AS "funding_outside_lm_sum",
        SUM("inventory_shrink") AS "inventory_shrink_sum",
        SUM("total_shrink") AS "total_shrink_sum",
        SUM("ocean_freight") AS "ocean_freight_sum",
        SUM("freight_cost") AS "freight_cost_sum",
        SUM("agency_fee") AS "agency_fee_sum",
        SUM("duty_tarrifs") AS "duty_tarrifs_sum",
        SUM("domestic_inbound") AS "domestic_inbound_sum",
        SUM("total_freight") AS "total_freight_sum",
        SUM("domestic_outbound") AS "domestic_outbound_sum",
        SUM("freight_other_charges") AS "freight_other_charges_sum",
        SUM("volume_rebate") AS "volume_rebate_sum",
        SUM("price_cuts") AS "price_cuts_sum",
        SUM("scanbacks") AS "scanbacks_sum",
        SUM("markdowns") AS "markdowns_sum",
        SUM("landed_margin") AS "landed_margin_sum",
        SUM("funding_in_lm") AS "funding_in_lm_sum",
        SUM("total_funding") AS "total_funding_sum",
        SUM("vendor_support_new") AS "vendor_support_new_sum",
        SUM("vendor_support_fast_new") AS "vendor_support_fast_new_sum"
      FROM (
        SELECT DISTINCT
            "MERCH_YEAR" AS "MERCH_YEAR",
            "PARENT_VENDOR_ID" AS "PARENT_VENDOR_ID",
            "VENDOR_ID" AS "VENDOR_ID",
            "ARTICLE_NO" AS "ARTICLE_NO",
            "YEAR_PERIOD" AS "YEAR_PERIOD",
            "CO_CODE" AS "CO_CODE",
            "SUM_TYPE_CODE" AS "SUM_TYPE_CODE",
            "PRICE_OVRD_FLAG" AS "PRICE_OVRD_FLAG",
            "SCAN_ITEM_FLAG" AS "SCAN_ITEM_FLAG",
            "PRICE_TYPE_CODE" AS "PRICE_TYPE_CODE",
            "BUY_ONLINE_CODE" AS "BUY_ONLINE_CODE",
            "YEAR_WEEK" AS "YEAR_WEEK",
            "YEAR_QUARTER" AS "YEAR_QUARTER",
            "ARTICLE_DESC" AS "ARTICLE_DESC",
            "ARTICLE_FULL" AS "ARTICLE_FULL",
            "ARTICLE_TYPE_NAME" AS "ARTICLE_TYPE_NAME",
            "ARTICLE_TYPE_FULL" AS "ARTICLE_TYPE_FULL",
            "LABEL_TYPE_CODE" AS "LABEL_TYPE_CODE",
            "UOM_CODE" AS "UOM_CODE",
            "UOM_DESC" AS "UOM_DESC",
            "UOM_FULL" AS "UOM_FULL",
            "ARTICLE_LENGTH" AS "ARTICLE_LENGTH",
            "ARTICLE_WIDTH" AS "ARTICLE_WIDTH",
            "ARTICLE_HEIGHT" AS "ARTICLE_HEIGHT",
            "ARTICLE_WEIGHT" AS "ARTICLE_WEIGHT",
            "ARTICLE_SIZE" AS "ARTICLE_SIZE",
            "ARTICLE_COUNT" AS "ARTICLE_COUNT",
            "LOC_TYPE_CODE" AS "LOC_TYPE_CODE",
            "ARTICLE_TYPE_CODE" AS "ARTICLE_TYPE_CODE",
            "PRIMARY_VENDOR_ID" AS "PRIMARY_VENDOR_ID",
            "ARTICLE_CAT_CODE" AS "ARTICLE_CAT_CODE",
            "ARTICLE_CAT_NAME" AS "ARTICLE_CAT_NAME",
            "ARTICLE_CAT_FULL" AS "ARTICLE_CAT_FULL",
            "DEPT_CODE" AS "DEPT_CODE",
            "DEPT_NAME" AS "DEPT_NAME",
            "DEPT_FULL" AS "DEPT_FULL",
            "LOB_NO" AS "LOB_NO",
            "LOB_DESC" AS "LOB_DESC",
            "LOB_FULL" AS "LOB_FULL",
            "MAJ_DEPT_NO" AS "MAJ_DEPT_NO",
            "MAJ_DEPT_DESC" AS "MAJ_DEPT_DESC",
            "MAJ_DEPT_FULL" AS "MAJ_DEPT_FULL",
            "PVT_LABEL_DESC" AS "PVT_LABEL_DESC",
            "BRAND_NAME" AS "BRAND_NAME",
            "VENDOR_NAME" AS "VENDOR_NAME",
            "VENDOR_FULL" AS "VENDOR_FULL",
            "VENDOR_CO_NAME" AS "VENDOR_CO_NAME",
            "REGION_CODE" AS "REGION_CODE",
            "channel" AS "channel",
            "ORDER_LINE_TYPE" AS "ORDER_LINE_TYPE",
            "SCAC" AS "SCAC",
            "OMS_ORDER_LINE_SCHEDULE_SHIP_NODE" AS "OMS_ORDER_LINE_SCHEDULE_SHIP_NODE",
            "SALES_QTY" AS "SALES_QTY",
            "SALES_AMT" AS "SALES_AMT",
            "COST_AMT" AS "COST_AMT",
            "margin" AS "margin",
            "DISC_AMT" AS "DISC_AMT",
            "OVRD_AMT" AS "OVRD_AMT",
            "TRANS_TAX_AMT" AS "TRANS_TAX_AMT",
            "GROSS_SALES" AS "GROSS_SALES",
            "SALES_ALLOCATE" AS "SALES_ALLOCATE",
            "COST_ALLOCATE" AS "COST_ALLOCATE",
            "unsellables" AS "unsellables",
            "defectives" AS "defectives",
            "dc_shrink" AS "dc_shrink",
            "theft" AS "theft",
            "defective_allowance" AS "defective_allowance",
            "battery_core" AS "battery_core",
            "mixing_center" AS "mixing_center",
            "new_store_discounts" AS "new_store_discounts",
            "cash_discounts" AS "cash_discounts",
            "pop" AS "pop",
            "pdq" AS "pdq",
            "marketing" AS "marketing",
            "vendor_compliance" AS "vendor_compliance",
            "booth_rental" AS "booth_rental",
            "funding_outside_lm" AS "funding_outside_lm",
            "inventory_shrink" AS "inventory_shrink",
            "total_shrink" AS "total_shrink",
            "weighted_domestic_inbound" AS "weighted_domestic_inbound",
            "weighted_ocean_freight" AS "weighted_ocean_freight",
            "ocean_freight" AS "ocean_freight",
            "weighted_domestic_outbound" AS "weighted_domestic_outbound",
            "weighted_freight_cost" AS "weighted_freight_cost",
            "freight_cost" AS "freight_cost",
            "weighted_agency_fee" AS "weighted_agency_fee",
            "agency_fee" AS "agency_fee",
            "weighted_duty_rate" AS "weighted_duty_rate",
            "duty_tarrifs" AS "duty_tarrifs",
            "domestic_inbound" AS "domestic_inbound",
            "total_freight" AS "total_freight",
            "domestic_outbound" AS "domestic_outbound",
            "SHIPPING_REVENUE" AS "SHIPPING_REVENUE",
            "SHIPPING_DISC" AS "SHIPPING_DISC",
            "ACTUAL_SHIPPING_REVENUE" AS "ACTUAL_SHIPPING_REVENUE",
            omni_tsc_delivery AS "omni_tsc_delivery",
            omni_small_package_ltl AS "omni_small_package_ltl",
            omni_roadie AS "omni_roadie",
            "omni_multi_salvage" AS "omni_multi_salvage",
            "vendor_support_dol" AS "vendor_support_dol",
            "vendor_support_fast_dol" AS "vendor_support_fast_dol",
            "freight_other_charges" AS "freight_other_charges",
            "volume_rebate" AS "volume_rebate",
            "price_cuts" AS "price_cuts",
            "scanbacks" AS "scanbacks",
            "markdowns" AS "markdowns",
            "vsf_exception" AS "vsf_exception",
            "landed_margin" AS "landed_margin",
            "funding_in_lm" AS "funding_in_lm",
            "total_funding" AS "total_funding",
            "omni_flow_path" AS "omni_flow_path",
            "last_mile_cost" AS "last_mile_cost",
            "last_mile_delivery_combined" AS "last_mile_delivery_combined",
            "small_parcel_ltl_cost" AS "small_parcel_ltl_cost",
            "Facility_Type" AS "Facility_Type",
            "omni_flow_path_modified" AS "omni_flow_path_modified",
            "omni_freight" AS "omni_freight",
            "BUYER_CODE" AS "BUYER_CODE",
            "BUYER_NAME" AS "BUYER_NAME",
            "BUYER_FULL" AS "BUYER_FULL",
            cast("ARTICLE_COLOR" as varchar) AS "ARTICLE_COLOR",
            "PARENT_ARTICLE_NO" AS "PARENT_ARTICLE_NO",
            "PARENT_ARTICLE_DESC" AS "PARENT_ARTICLE_DESC",
            "PARENT_VENDOR_FULL" AS "PARENT_VENDOR_FULL",
            COALESCE("vendor_support_dol", 0) AS "vendor_support_new",
            COALESCE("vendor_support_fast_dol", 0) + COALESCE("vsf_exception", 0) AS "vendor_support_fast_new"
          FROM ALLOCATION_W_BUYER_TEMP
        ) "dku__beforegrouping"
      GROUP BY "ARTICLE_NO", "MERCH_YEAR", "VENDOR_ID", "YEAR_PERIOD", "ARTICLE_DESC", "ARTICLE_SIZE", "MAJ_DEPT_DESC", "BRAND_NAME", "BUYER_CODE", "ARTICLE_COLOR", "PARENT_ARTICLE_NO", "PARENT_ARTICLE_DESC", "ARTICLE_FULL", "BUYER_FULL", "ARTICLE_CAT_FULL", "LOB_FULL", "DEPT_FULL", "VENDOR_FULL", "PARENT_VENDOR_FULL", "channel"
) "pristine_query";

<br>
	--VISUAL_TABLES_GRP_W_CHANNEL_1
</br>
CREATE OR REPLACE TEMPORARY TABLE VISUAL_TABLES_GRP_W_CHANNEL_1
AS
WITH CTE AS (
 SELECT 
        "ARTICLE_NO" AS "ARTICLE_NO",
        "MERCH_YEAR" AS "MERCH_YEAR",
        "VENDOR_ID" AS "VENDOR_ID",
        "YEAR_PERIOD" AS "YEAR_PERIOD",
        "ARTICLE_DESC" AS "ARTICLE_DESC",
        "ARTICLE_SIZE" AS "ARTICLE_SIZE",
        "MAJ_DEPT_DESC" AS "MAJ_DEPT_DESC",
        "BRAND_NAME" AS "BRAND_NAME",
        "BUYER_CODE" AS "BUYER_CODE",
        "ARTICLE_COLOR" AS "ARTICLE_COLOR",
        "PARENT_ARTICLE_NO" AS "PARENT_ARTICLE_NO",
        "PARENT_ARTICLE_DESC" AS "PARENT_ARTICLE_DESC",
        "ARTICLE_FULL" AS "ARTICLE_FULL",
        "BUYER_FULL" AS "BUYER_FULL",
        "ARTICLE_CAT_FULL" AS "ARTICLE_CAT_FULL",
        "LOB_FULL" AS "LOB_FULL",
        "DEPT_FULL" AS "DEPT_FULL",
        "VENDOR_FULL" AS "VENDOR_FULL",
        "PARENT_VENDOR_FULL" AS "PARENT_VENDOR_FULL",
        "channel" AS "channel",
        SUM("SALES_QTY") AS "SALES_QTY_sum",
        SUM("SALES_AMT") AS "SALES_AMT_sum",
        SUM("COST_AMT") AS "COST_AMT_sum",
        SUM("margin") AS "margin_sum",
        SUM("unsellables") AS "unsellables_sum",
        SUM("defectives") AS "defectives_sum",
        SUM("dc_shrink") AS "dc_shrink_sum",
        SUM("theft") AS "theft_sum",
        SUM("defective_allowance") AS "defective_allowance_sum",
        SUM("battery_core") AS "battery_core_sum",
        SUM("mixing_center") AS "mixing_center_sum",
        SUM("new_store_discounts") AS "new_store_discounts_sum",
        SUM("cash_discounts") AS "cash_discounts_sum",
        SUM("pop") AS "pop_sum",
        SUM("pdq") AS "pdq_sum",
        SUM("marketing") AS "marketing_sum",
        SUM("vendor_compliance") AS "vendor_compliance_sum",
        SUM("booth_rental") AS "booth_rental_sum",
        SUM("funding_outside_lm") AS "funding_outside_lm_sum",
        SUM("inventory_shrink") AS "inventory_shrink_sum",
        SUM("total_shrink") AS "total_shrink_sum",
        SUM("ocean_freight") AS "ocean_freight_sum",
        SUM("freight_cost") AS "freight_cost_sum",
        SUM("agency_fee") AS "agency_fee_sum",
        SUM("duty_tarrifs") AS "duty_tarrifs_sum",
        SUM("domestic_inbound") AS "domestic_inbound_sum",
        SUM("total_freight") AS "total_freight_sum",
        SUM("domestic_outbound") AS "domestic_outbound_sum",
        SUM("SHIPPING_REVENUE") AS "SHIPPING_REVENUE_sum",
        SUM("SHIPPING_DISC") AS "SHIPPING_DISC_sum",
        SUM("ACTUAL_SHIPPING_REVENUE") AS "ACTUAL_SHIPPING_REVENUE_sum",
        SUM("freight_other_charges") AS "freight_other_charges_sum",
        SUM("volume_rebate") AS "volume_rebate_sum",
        SUM("price_cuts") AS "price_cuts_sum",
        SUM("scanbacks") AS "scanbacks_sum",
        SUM("markdowns") AS "markdowns_sum",
        SUM("landed_margin") AS "landed_margin_sum",
        SUM("funding_in_lm") AS "funding_in_lm_sum",
        SUM("total_funding") AS "total_funding_sum",
        SUM("last_mile_delivery_combined") AS "last_mile_delivery_combined_sum",
        SUM("small_parcel_ltl_cost") AS "small_parcel_ltl_cost_sum",
        SUM("omni_freight") AS "omni_freight_sum",
        SUM("vendor_support_new") AS "vendor_support_new_sum",
        SUM("vendor_support_fast_new") AS "vendor_support_fast_new_sum",
        COUNT(*) AS "count"
      FROM (
        SELECT DISTINCT
            "MERCH_YEAR" AS "MERCH_YEAR",
            "PARENT_VENDOR_ID" AS "PARENT_VENDOR_ID",
            "VENDOR_ID" AS "VENDOR_ID",
            "ARTICLE_NO" AS "ARTICLE_NO",
            "YEAR_PERIOD" AS "YEAR_PERIOD",
            "CO_CODE" AS "CO_CODE",
            "SUM_TYPE_CODE" AS "SUM_TYPE_CODE",
            "PRICE_OVRD_FLAG" AS "PRICE_OVRD_FLAG",
            "SCAN_ITEM_FLAG" AS "SCAN_ITEM_FLAG",
            "PRICE_TYPE_CODE" AS "PRICE_TYPE_CODE",
            "BUY_ONLINE_CODE" AS "BUY_ONLINE_CODE",
            "YEAR_WEEK" AS "YEAR_WEEK",
            "YEAR_QUARTER" AS "YEAR_QUARTER",
            "ARTICLE_DESC" AS "ARTICLE_DESC",
            "ARTICLE_FULL" AS "ARTICLE_FULL",
            "ARTICLE_TYPE_NAME" AS "ARTICLE_TYPE_NAME",
            "ARTICLE_TYPE_FULL" AS "ARTICLE_TYPE_FULL",
            "LABEL_TYPE_CODE" AS "LABEL_TYPE_CODE",
            "UOM_CODE" AS "UOM_CODE",
            "UOM_DESC" AS "UOM_DESC",
            "UOM_FULL" AS "UOM_FULL",
            "ARTICLE_LENGTH" AS "ARTICLE_LENGTH",
            "ARTICLE_WIDTH" AS "ARTICLE_WIDTH",
            "ARTICLE_HEIGHT" AS "ARTICLE_HEIGHT",
            "ARTICLE_WEIGHT" AS "ARTICLE_WEIGHT",
            "ARTICLE_SIZE" AS "ARTICLE_SIZE",
            "ARTICLE_COUNT" AS "ARTICLE_COUNT",
            "LOC_TYPE_CODE" AS "LOC_TYPE_CODE",
            "ARTICLE_TYPE_CODE" AS "ARTICLE_TYPE_CODE",
            "PRIMARY_VENDOR_ID" AS "PRIMARY_VENDOR_ID",
            "ARTICLE_CAT_CODE" AS "ARTICLE_CAT_CODE",
            "ARTICLE_CAT_NAME" AS "ARTICLE_CAT_NAME",
            "ARTICLE_CAT_FULL" AS "ARTICLE_CAT_FULL",
            "DEPT_CODE" AS "DEPT_CODE",
            "DEPT_NAME" AS "DEPT_NAME",
            "DEPT_FULL" AS "DEPT_FULL",
            "LOB_NO" AS "LOB_NO",
            "LOB_DESC" AS "LOB_DESC",
            "LOB_FULL" AS "LOB_FULL",
            "MAJ_DEPT_NO" AS "MAJ_DEPT_NO",
            "MAJ_DEPT_DESC" AS "MAJ_DEPT_DESC",
            "MAJ_DEPT_FULL" AS "MAJ_DEPT_FULL",
            "PVT_LABEL_DESC" AS "PVT_LABEL_DESC",
            "BRAND_NAME" AS "BRAND_NAME",
            "VENDOR_NAME" AS "VENDOR_NAME",
            "VENDOR_FULL" AS "VENDOR_FULL",
            "VENDOR_CO_NAME" AS "VENDOR_CO_NAME",
            "REGION_CODE" AS "REGION_CODE",
            "channel" AS "channel",
            "ORDER_LINE_TYPE" AS "ORDER_LINE_TYPE",
            "SCAC" AS "SCAC",
            "OMS_ORDER_LINE_SCHEDULE_SHIP_NODE" AS "OMS_ORDER_LINE_SCHEDULE_SHIP_NODE",
            "SALES_QTY" AS "SALES_QTY",
            "SALES_AMT" AS "SALES_AMT",
            "COST_AMT" AS "COST_AMT",
            "margin" AS "margin",
            "DISC_AMT" AS "DISC_AMT",
            "OVRD_AMT" AS "OVRD_AMT",
            "TRANS_TAX_AMT" AS "TRANS_TAX_AMT",
            "GROSS_SALES" AS "GROSS_SALES",
            "SALES_ALLOCATE" AS "SALES_ALLOCATE",
            "COST_ALLOCATE" AS "COST_ALLOCATE",
            "unsellables" AS "unsellables",
            "defectives" AS "defectives",
            "dc_shrink" AS "dc_shrink",
            "theft" AS "theft",
            "defective_allowance" AS "defective_allowance",
            "battery_core" AS "battery_core",
            "mixing_center" AS "mixing_center",
            "new_store_discounts" AS "new_store_discounts",
            "cash_discounts" AS "cash_discounts",
            "pop" AS "pop",
            "pdq" AS "pdq",
            "marketing" AS "marketing",
            "vendor_compliance" AS "vendor_compliance",
            "booth_rental" AS "booth_rental",
            "funding_outside_lm" AS "funding_outside_lm",
            "inventory_shrink" AS "inventory_shrink",
            "total_shrink" AS "total_shrink",
            "weighted_domestic_inbound" AS "weighted_domestic_inbound",
            "weighted_ocean_freight" AS "weighted_ocean_freight",
            "ocean_freight" AS "ocean_freight",
            "weighted_domestic_outbound" AS "weighted_domestic_outbound",
            "weighted_freight_cost" AS "weighted_freight_cost",
            "freight_cost" AS "freight_cost",
            "weighted_agency_fee" AS "weighted_agency_fee",
            "agency_fee" AS "agency_fee",
            "weighted_duty_rate" AS "weighted_duty_rate",
            "duty_tarrifs" AS "duty_tarrifs",
            "domestic_inbound" AS "domestic_inbound",
            "total_freight" AS "total_freight",
            "domestic_outbound" AS "domestic_outbound",
            "SHIPPING_REVENUE" AS "SHIPPING_REVENUE",
            "SHIPPING_DISC" AS "SHIPPING_DISC",
            "ACTUAL_SHIPPING_REVENUE" AS "ACTUAL_SHIPPING_REVENUE",
            omni_tsc_delivery AS "omni_tsc_delivery",
            omni_small_package_ltl AS "omni_small_package_ltl",
            omni_roadie AS "omni_roadie",
            "omni_multi_salvage" AS "Omni_multi_salvage",
            "vendor_support_dol" AS "vendor_support_dol",
            "vendor_support_fast_dol" AS "vendor_support_fast_dol",
            "freight_other_charges" AS "freight_other_charges",
            "volume_rebate" AS "volume_rebate",
            "price_cuts" AS "price_cuts",
            "scanbacks" AS "scanbacks",
            "markdowns" AS "markdowns",
            "vsf_exception" AS "vsf_exception",
            "landed_margin" AS "landed_margin",
            "funding_in_lm" AS "funding_in_lm",
            "total_funding" AS "total_funding",
            "omni_flow_path" AS "omni_flow_path",
            "last_mile_cost" AS "last_mile_cost",
            "last_mile_delivery_combined" AS "last_mile_delivery_combined",
            "small_parcel_ltl_cost" AS "small_parcel_ltl_cost",
            "Facility_Type" AS "Facility_Type",
            "omni_flow_path_modified" AS "omni_flow_path_modified",
            "omni_freight" AS "omni_freight",
            "BUYER_CODE" AS "BUYER_CODE",
            "BUYER_NAME" AS "BUYER_NAME",
            "BUYER_FULL" AS "BUYER_FULL",
            "ARTICLE_COLOR" AS "ARTICLE_COLOR",
            "PARENT_ARTICLE_NO" AS "PARENT_ARTICLE_NO",
            "PARENT_ARTICLE_DESC" AS "PARENT_ARTICLE_DESC",
            "PARENT_VENDOR_FULL" AS "PARENT_VENDOR_FULL",
            COALESCE("vendor_support_dol", 0) AS "vendor_support_new",
            COALESCE("vendor_support_fast_dol", 0) + COALESCE("vsf_exception", 0) AS "vendor_support_fast_new"
          FROM "ALLOCATION_W_BUYER_TEMP"
        ) "dku__beforegrouping"
      GROUP BY "ARTICLE_NO", "MERCH_YEAR", "VENDOR_ID", "YEAR_PERIOD", "ARTICLE_DESC", "ARTICLE_SIZE", "MAJ_DEPT_DESC", "BRAND_NAME", "BUYER_CODE", "ARTICLE_COLOR", "PARENT_ARTICLE_NO", "PARENT_ARTICLE_DESC", "ARTICLE_FULL", "BUYER_FULL", "ARTICLE_CAT_FULL", "LOB_FULL", "DEPT_FULL", "VENDOR_FULL", "PARENT_VENDOR_FULL", "channel"
    )
    SELECT DISTINCT
    "ARTICLE_NO" AS "ARTICLE_NO",
    "MERCH_YEAR" AS "MERCH_YEAR",
    "VENDOR_ID" AS "VENDOR_ID",
    "YEAR_PERIOD" AS "YEAR_PERIOD",
    "ARTICLE_DESC" AS "ARTICLE_DESC",
    "ARTICLE_SIZE" AS "ARTICLE_SIZE",
    "MAJ_DEPT_DESC" AS "MAJ_DEPT_DESC",
    "BRAND_NAME" AS "BRAND_NAME",
    "BUYER_CODE" AS "BUYER_CODE",
    "ARTICLE_COLOR" AS "ARTICLE_COLOR",
    "PARENT_ARTICLE_NO" AS "PARENT_ARTICLE_NO",
    "PARENT_ARTICLE_DESC" AS "PARENT_ARTICLE_DESC",
    "ARTICLE_FULL" AS "ARTICLE_FULL",
    "BUYER_FULL" AS "BUYER_NAME",
    "ARTICLE_CAT_FULL" AS "ARTICLE_CAT_NAME",
    "LOB_FULL" AS "LOB_DESC",
    "DEPT_FULL" AS "DEPT_NAME",
    "VENDOR_FULL" AS "VENDOR_NAME",
    "PARENT_VENDOR_FULL" AS "PARENT_VENDOR_NAME",
    "channel" AS "channel",
    "SALES_QTY_sum" AS "SALES_QTY",
    "SALES_AMT_sum" AS "SALES_AMT",
    "COST_AMT_sum" AS "COST_AMT",
    "margin_sum" AS "margin",
    "unsellables_sum" AS "unsellables",
    "defectives_sum" AS "defectives_shrink",
    "dc_shrink_sum" AS "dc_shrink",
    "theft_sum" AS "theft",
    "defective_allowance_sum" AS "defective_allowance",
    "battery_core_sum" AS "battery_core",
    "mixing_center_sum" AS "mixing_center",
    "new_store_discounts_sum" AS "new_store_discounts",
    "cash_discounts_sum" AS "cash_discounts",
    "pop_sum" AS "pop",
    "pdq_sum" AS "pdq",
    "marketing_sum" AS "marketing",
    "vendor_compliance_sum" AS "vendor_compliance",
    "booth_rental_sum" AS "booth_rental",
    "funding_outside_lm_sum" AS "funding_outside_lm",
    "inventory_shrink_sum" AS "inventory_shrink",
    "total_shrink_sum" AS "total_shrink",
    "ocean_freight_sum" AS "ocean_freight",
    "freight_cost_sum" AS "freight_cost",
    "agency_fee_sum" AS "agency_fee",
    "duty_tarrifs_sum" AS "duty_tarrifs",
    "domestic_inbound_sum" AS "domestic_inbound",
    "total_freight_sum" AS "total_freight",
    "domestic_outbound_sum" AS "domestic_outbound",
    "SHIPPING_REVENUE_sum" AS "SHIPPING_REVENUE_sum",
    "SHIPPING_DISC_sum" AS "SHIPPING_DISC_sum",
    "ACTUAL_SHIPPING_REVENUE_sum" AS "ACTUAL_SHIPPING_REVENUE_sum",
    "freight_other_charges_sum" AS "freight_other_charges",
    "volume_rebate_sum" AS "volume_rebate",
    "price_cuts_sum" AS "price_cuts",
    "scanbacks_sum" AS "scanbacks",
    "markdowns_sum" AS "markdowns",
    "landed_margin_sum" AS "landed_margin",
    "funding_in_lm_sum" AS "funding_in_lm",
    "total_funding_sum" AS "total_funding",
    "last_mile_delivery_combined_sum" AS "last_mile_delivery_combined_sum",
    "small_parcel_ltl_cost_sum" AS "small_parcel_ltl_cost_sum",
    "omni_freight_sum" AS "omni_freight_sum",
    "vendor_support_new_sum" AS "vendor_support",
    "vendor_support_fast_new_sum" AS "vendor_support_fast",
    "count" AS "count"
  FROM CTE;


<br>
--compute_DC_3PL_allocation_joined_stacked
--STEP 6
</br>
insert into  ALLOCATION_V3_SF_GROUPED  
SELECT DISTINCT
  "MERCH_YEAR" AS "MERCH_YEAR",
  "STORE_NO" AS "STORE_NO",
  "YEAR_PERIOD" AS "YEAR_PERIOD",
  "ARTICLE_NO" AS "ARTICLE_NO",
  "VENDOR_ID" AS "VENDOR_ID",
  'Store' AS "channel",
  SUM("total_variable_cost_allocated") AS total_variable_cost_allocated
FROM (
  SELECT
      "MERCH_YEAR" AS "MERCH_YEAR",
      "STORE_NO" AS "STORE_NO",
      "CHANNEL" AS "channel",
      "YEAR_PERIOD" AS "YEAR_PERIOD",
      "ARTICLE_NO" AS "ARTICLE_NO",
      "VENDOR_ID" AS "VENDOR_ID",
      variable_cost_combined AS "variable_cost_combined",
      dc_3pl_variable_cost_prep_joined AS "dc_3pl_variable_cost_prep_joined",
      dc_3pl_variable_cost_prep_joined_l2 AS "dc_3pl_variable_cost_prep_joined_l2",
      COALESCE(dc_3pl_variable_cost_prep_joined, 0) + COALESCE(dc_3pl_variable_cost_prep_joined_l2, 0) AS "total_variable_cost_allocated"
    FROM ALLOCATION_V3_SP_SF
  ) "dku__beforegrouping"
GROUP BY 1,2,3,4,5,6;

<br>
--compute_DC_3PL_allocation_joined_stacked
--STEP 6
</br>
insert into  ALLOCATION_DC_VARIABLE_FIXED_STACKED_GRP 
SELECT DISTINCT
    "MERCH_YEAR" AS "MERCH_YEAR",
    "STORE_NO" AS "STORE_NO",
    "YEAR_PERIOD" AS "YEAR_PERIOD",
    "ARTICLE_NO" AS "ARTICLE_NO",
    "VENDOR_ID" AS "VENDOR_ID",
    "channel" AS "channel",
    SUM("total_variable_cost_allocated") AS "total_variable_cost_allocated",
    SUM("total_fixed_cost_allocated") AS "total_fixed_cost_allocated"
  FROM (
    SELECT 
    "MERCH_YEAR" AS "MERCH_YEAR",
    "STORE_NO" AS "STORE_NO",
    "YEAR_PERIOD" AS "YEAR_PERIOD",
    "ARTICLE_NO" AS "ARTICLE_NO",
    "VENDOR_ID" AS "VENDOR_ID",
    COALESCE('Store', channel) AS "channel",
    total_variable_cost_allocated AS "total_variable_cost_allocated",
    NULL AS "total_fixed_cost_allocated"
  FROM ALLOCATION_V3_SF_GROUPED
UNION ALL
(SELECT DISTINCT
  "MERCH_YEAR" AS "MERCH_YEAR",
  "STORE_NO" AS "STORE_NO",
  "YEAR_PERIOD" AS "YEAR_PERIOD",
  "ARTICLE_NO" AS "ARTICLE_NO",
  "VENDOR_ID" AS "VENDOR_ID",
  'Store' AS "channel",
  NULL AS "total_variable_cost_allocated",
  SUM("total_fixed_cost_allocated") AS "total_fixed_cost_allocated"
FROM (
 SELECT "MERCH_YEAR",
             "STORE_NO",
             "YEAR_PERIOD",
              "ARTICLE_NO",
              "VENDOR_ID",
              "VOLUME",
              "dc_3pl_pandl_fixed_cost_final",
              "dc_3pl_pandl_fixed_cost_final_l2",
              COALESCE("dc_3pl_pandl_fixed_cost_final", 0) + COALESCE("dc_3pl_pandl_fixed_cost_final_l2", 0) AS "total_fixed_cost_allocated"
    FROM 
     (
     SELECT 
      "MERCH_YEAR" AS "MERCH_YEAR",
      "STORE_NO" AS "STORE_NO",
      "YEAR_PERIOD" AS "YEAR_PERIOD",
      "ARTICLE_NO" AS "ARTICLE_NO",
      "VENDOR_ID" AS "VENDOR_ID",
      "VOLUME" AS "VOLUME",
      CASE WHEN  dc_3pl_pandl_fixed_cost_final IS NULL  OR TRIM(dc_3pl_pandl_fixed_cost_final) = ''THEN 0  ELSE dc_3pl_pandl_fixed_cost_final END AS 
      "dc_3pl_pandl_fixed_cost_final",
      CASE WHEN  dc_3pl_pandl_fixed_cost_final_l2 IS NULL OR TRIM(dc_3pl_pandl_fixed_cost_final_l2)='' THEN  0 ELSE dc_3pl_pandl_fixed_cost_final_l2 
      END AS "dc_3pl_pandl_fixed_cost_final_l2"
    FROM ALLOCATION_V2)ALLOCATION_V2
  ) "unioned"
GROUP BY 1,2,3,4,5,6)
) "dku__beforegrouping"
  GROUP BY 1,2,3,4,5,6;

<br>
--DC_3PL_ALLOCATION_JOINED
</br>
CREATE OR REPLACE TEMP  TABLE DC_3PL_ALLOCATION_JOINED
AS
SELECT DISTINCT 
    "varfixed"."MERCH_YEAR" AS "MERCH_YEAR",
    "varfixed"."STORE_NO" AS "STORE_NO",
    "varfixed"."YEAR_PERIOD" AS "YEAR_PERIOD",
    "varfixed"."ARTICLE_NO" AS "ARTICLE_NO",
    CAST("varfixed"."VENDOR_ID" AS NUMBER) AS "VENDOR_ID",
     "varfixed".CHANNEL AS "channel",
	 "varfixed".total_variable_cost_allocated AS "total_variable_cost_allocated",
    "varfixed".total_fixed_cost_allocated AS "total_fixed_cost_allocated",
    "prepj"."AVG_FIN_INV_QTY_SUM" AS "Avg_inventory_qty",
    "facinb".VENDOR_RCPT_UNIT_QTY_sum AS "inbound_unit",
    "facout"."VENDOR_RCPT_UNIT_QTY_sum" AS "outbound_unit",
    "locmap".Facility AS "Facility",
    "locmap"."DC_3PL" AS "Facility_Type"
  FROM ALLOCATION_DC_VARIABLE_FIXED_STACKED_GRP "varfixed"
  LEFT JOIN FINANCIAL_INVENTORY_YP_PREP_JOINED_QTY "prepj"
    ON ("varfixed"."STORE_NO" = "prepj"."STORE_NO")
      AND ("varfixed"."ARTICLE_NO" = "prepj"."ARTICLE_NO")
      AND ("varfixed"."VENDOR_ID" = "prepj"."VENDOR_ID")
      AND ("varfixed"."YEAR_PERIOD" = "prepj"."FIN_INV_YEAR_PERIOD")
  LEFT JOIN FACILITY_INBOUND_DC_ONLY_QTY "facinb"
      ON    ("varfixed"."STORE_NO" = "facinb"."STORE_NO")
      AND ("varfixed"."YEAR_PERIOD" = "facinb"."YEAR_PERIOD")
      AND ("varfixed"."ARTICLE_NO" = "facinb"."ARTICLE_NO")
      AND ("varfixed"."VENDOR_ID" = "facinb"."VENDOR_ID")
  LEFT JOIN FACILITY_OUTBOUND_DC_ONLY_QTY "facout"
    ON ("varfixed"."STORE_NO" = "facout"."STORE_NO")
      AND ("varfixed"."YEAR_PERIOD" = "facout"."YEAR_PERIOD")
      AND ("varfixed"."ARTICLE_NO" = "facout"."ARTICLE_NO")
      AND ("varfixed"."VENDOR_ID" = "facout"."VENDOR_ID")
  LEFT JOIN DC_3PL_LOCATION_MAPPING_PREPARED "locmap"
    ON "varfixed"."STORE_NO" = "locmap".Facility_Code;

<br>
--DC_3PL_ALLOCATION_JOINED_PREPARED
</br>
CREATE OR REPLACE TEMP TABLE DC_3PL_ALLOCATION_JOINED_PREPARED
	 AS
	 SELECT  "MERCH_YEAR",
	         "STORE_NO",
			 "YEAR_PERIOD",
			 "ARTICLE_NO",
			 "VENDOR_ID",
	 IFF(contains("Facility",'Omni'),'Omni-Shipped','Store') AS "channel", 
            "total_fixed_cost_allocated",
    	    "Avg_inventory_qty",
		    "inbound_unit",
		    "outbound_unit",
			"Facility",
			"Facility_Type",
	 CASE WHEN "Facility_Type" !='DC'  THEN coalesce("total_variable_cost_allocated",0) + coalesce("total_fixed_cost_allocated",0)/2  ELSE 0 END AS "3PL_total_variable_cost_allocated",
	 CASE WHEN "Facility_Type" !='DC'  THEN coalesce("total_fixed_cost_allocated",0)/2 ELSE 0 END AS "3PL_total_fixed_cost_allocated",
	 CASE WHEN "Facility_Type" ='DC'   THEN coalesce("total_variable_cost_allocated",0)  ELSE 0 END AS "DC_total_variable_cost_allocated",
	 CASE WHEN "Facility_Type" ='DC'   THEN coalesce("total_fixed_cost_allocated",0) ELSE 0 END AS "DC_total_fixed_cost_allocated",
	 NULL AS "BOOKED_SALES_QTY"	 
    FROM DC_3PL_ALLOCATION_JOINED;

<br>
--compute_DC_3PL_allocation_joined_stacked
--STEP 6
</br>
INSERT INTO  DC_3PL_ALLOCATION_JOINED_STACKED 
SELECT  
      "MERCH_YEAR",
      "STORE_NO",
      "YEAR_PERIOD",
      "ARTICLE_NO",
      "VENDOR_ID",
      "channel",
      "total_fixed_cost_allocated",
      "Avg_inventory_qty",
      "inbound_unit",
      "outbound_unit",
			"Facility",
			"Facility_Type",
			"3PL_total_variable_cost_allocated",
			"3PL_total_fixed_cost_allocated",
			"DC_total_variable_cost_allocated",
			"DC_total_fixed_cost_allocated",
			"BOOKED_SALES_QTY"
	 FROM 
	 DC_3PL_ALLOCATION_JOINED_PREPARED
	 WHERE UPPER("channel") ='STORE'
 
     UNION ALL
SELECT DISTINCT
    "MERCH_YEAR" AS "MERCH_YEAR",
    "STORE_NO" AS "STORE_NO",
    "YEAR_PERIOD" AS "YEAR_PERIOD",
    "OMS_ITEM_ID" AS "ARTICLE_NO",
    "OMS_ITEM_PRIMARY_VENDOR_ID" AS "VENDOR_ID",
    "channel" AS "channel",
    NULL AS "total_fixed_cost_allocated",
    NULL AS "Avg_inventory_qty",
    NULL AS "inbound_unit",
    NULL AS "outbound_unit",
    NULL AS "Facility",
    NULL AS "Facility_Type",
    NULL AS "3PL_total_variable_cost_allocated",
    NULL AS "3PL_total_fixed_cost_allocated",
    SUM("DC3PL_omni_fixed_cost") AS "DC_total_fixed_cost_allocated",
    SUM("DC3PL_omni_variable_cost") AS "DC_total_variable_cost_allocated",
    SUM("BOOKED_SALES_QTY") AS "BOOKED_SALES_QTY"
  FROM 
  (
    SELECT 
    "YEAR_WEEK" AS "YEAR_WEEK",
    "ACCT_YEAR" AS "ACCT_YEAR",
    "OMS_ORDER_NO" AS "OMS_ORDER_NO",
    "DC_3PL_PNL_OMNI_FULFILLMENT_VARIABLE_L2" AS "DC_3PL_PNL_OMNI_FULFILLMENT_VARIABLE_L2",
    COALESCE("DC_3PL_PNL_OMNI_FULFILLMENT_VARIABLE", 0) + COALESCE("DC_3PL_PNL_OMNI_FULFILLMENT_VARIABLE_L2", 0) AS "DC3PL_omni_variable_cost",
    "OMS_ORDER_LINE_KEY" AS "OMS_ORDER_LINE_KEY",
    "SHIPPING_DISC" AS "SHIPPING_DISC",
    "BOOKED_COST_AMT" AS "BOOKED_COST_AMT",
    "DC_3PL_PNL_OMNI_FULFILLMENT_FIXED" AS "DC_3PL_PNL_OMNI_FULFILLMENT_FIXED",
    "DC_3PL_PNL_OMNI_FULFILLMENT_FIXED_L2" AS "DC_3PL_PNL_OMNI_FULFILLMENT_FIXED_L2",
    COALESCE("DC_3PL_PNL_OMNI_FULFILLMENT_FIXED", 0) + COALESCE("DC_3PL_PNL_OMNI_FULFILLMENT_FIXED_L2", 0) AS "DC3PL_omni_fixed_cost",
    "SHIP_NODE_TYPE" AS "SHIP_NODE_TYPE",
    "BOOKED_SALES_QTY" AS "BOOKED_SALES_QTY",
    "SCAC" AS "SCAC",
    CASE WHEN ("ORDER_LINE_TYPE" = 'Return') OR ("SCAC" = 'Return') THEN 'Omni-Return' ELSE 'Omni-Shipped' END AS "channel",
    "OMS_ITEM_PRIMARY_VENDOR_ID" AS "OMS_ITEM_PRIMARY_VENDOR_ID",
    "STORE_NO" AS "STORE_NO",
    "ACTUAL_SHIPPING_REVENUE" AS "ACTUAL_SHIPPING_REVENUE",
    "OMS_SHIP_NODE_DESC" AS "OMS_SHIP_NODE_DESC",
    "ORDER_LINE_TYPE" AS "ORDER_LINE_TYPE",
    "MERCH_YEAR" AS "MERCH_YEAR",
    "ARTICLE_CAT_FULL" AS "ARTICLE_CAT_FULL",
    "OMS_ORDER_LINE_SCHEDULE_SHIP_NODE" AS "OMS_ORDER_LINE_SCHEDULE_SHIP_NODE",
    "YEAR_PERIOD" AS "YEAR_PERIOD",
    "DC_3PL_PNL_OMNI_FULFILLMENT_VARIABLE" AS "DC_3PL_PNL_OMNI_FULFILLMENT_VARIABLE",
    "DEMAND_QTY" AS "DEMAND_QTY",
    "OMS_ITEM_ID" AS "OMS_ITEM_ID",
    "BOOKED_SALES_AMT" AS "BOOKED_SALES_AMT",
    "SHIPPING_REVENUE" AS "SHIPPING_REVENUE",
    "DEMAND_SALES" AS "DEMAND_SALES"
FROM ALLOCATION_DC_3PL_OMNI_FULFILLMENT
WHERE "ORDER_LINE_TYPE" NOT IN ('BOPIS', 'BODFS')
  ) "pregroup"
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14;

<br>
--STEP 7
</br>
insert into  DC_3PL_ALLOCATION_JOINED_STACKED_JOINED_GRP 
SELECT DISTINCT
    "allocj"."YEAR_PERIOD" AS "YEAR_PERIOD",
    "allocj"."MERCH_YEAR" AS "MERCH_YEAR",
    "allocj"."STORE_NO" AS "STORE_NO",
    "allocj"."ARTICLE_NO" AS "ARTICLE_NO",
    "allocj"."VENDOR_ID" AS "VENDOR_ID",
    "allocj"."channel" AS "channel",
    "allocj"."Facility" AS "Facility",
    "locmap".Facility AS "Facility_Name",
    SUM("allocj"."Avg_inventory_qty") AS "Avg_inventory_qty",
    SUM("allocj"."inbound_unit") AS "inbound_unit",
    SUM("allocj"."outbound_unit") AS "outbound_unit",
    SUM("allocj"."3PL_total_variable_cost_allocated") AS "3PL_TOTAL_VARIABLE_COST_ALLOCATED",
    SUM("allocj"."3PL_total_fixed_cost_allocated") AS "3PL_TOTAL_FIXED_COST_ALLOCATED",
    SUM("allocj"."DC_total_variable_cost_allocated") AS "DC_total_variable_cost_allocated",
    SUM("allocj"."DC_total_fixed_cost_allocated") AS "DC_total_fixed_cost_allocated",
    SUM("allocj"."BOOKED_SALES_QTY") AS "BOOKED_SALES_QTY_SUM"
FROM "DC_3PL_ALLOCATION_JOINED_STACKED" "allocj"
LEFT JOIN "DC_3PL_LOCATION_MAPPING_PREPARED" "locmap"
 ON "allocj"."STORE_NO" = "locmap".Facility_Code
 GROUP BY 1,2,3,4,5,6,7,8;

<br>
--DC_3PL_ALLOCATION_COST_UNION

--STEP 8
</br>
insert into DC_3PL_ALLOCATION_COST_UNION 
SELECT DISTINCT
    MERCH_YEAR AS MERCH_YEAR,
    YEAR_PERIOD AS YEAR_PERIOD,
    ARTICLE_NO AS ARTICLE_NO,
    VENDOR_ID AS VENDOR_ID,
    channel AS channel,
    SUM("3PL_TOTAL_VARIABLE_COST_ALLOCATED") AS "3PL_TOTAL_VARIABLE_COST_ALLOCATED",
    SUM("3PL_TOTAL_FIXED_COST_ALLOCATED") AS "3PL_TOTAL_FIXED_COST_ALLOCATED",
    SUM(DC_total_variable_cost_allocated) AS DC_total_variable_cost_allocated,
    SUM(DC_total_fixed_cost_allocated) AS DC_total_fixed_cost_allocated
FROM DC_3PL_ALLOCATION_JOINED_STACKED_JOINED_GRP
GROUP BY 1,2,3,4,5;

<br>
--VISUAL_TABLES_GRP_PREP_CHANNEL_1
</br>
CREATE OR REPLACE TEMPORARY TABLE VISUAL_TABLES_GRP_PREP_CHANNEL_1
AS
SELECT DISTINCT
    "ARTICLE_NO",
    "MERCH_YEAR",
    "VENDOR_ID",
    "YEAR_PERIOD",
    "ARTICLE_DESC",
    "ARTICLE_SIZE",
    "MAJ_DEPT_DESC",
    "BRAND_NAME",
    "BUYER_CODE",
    "ARTICLE_COLOR",
    "PARENT_ARTICLE_NO",
    "PARENT_ARTICLE_DESC",
    "ARTICLE_FULL",
    "BUYER_NAME",
    "ARTICLE_CAT_NAME",
    "LOB_DESC",
    "DEPT_NAME",
    "VENDOR_NAME",
    "PARENT_VENDOR_NAME",
    "channel",
    "SALES_QTY",
    "SALES_AMT",
    "COST_AMT",
    "margin",
    "unsellables",
    "defectives_shrink",
    "dc_shrink",
    "theft",
    "defective_allowance",
    COALESCE(-1.0 * COALESCE("battery_core", 0), "battery_core") AS "battery_core",
    COALESCE(-1.0 * COALESCE("mixing_center", 0), "mixing_center") AS "mixing_center",
    "new_store_discounts",
    COALESCE(-1.0 * COALESCE("cash_discounts", 0), "cash_discounts") AS "cash_discounts",
    "pop",
    "pdq",
    COALESCE(-1.0 * COALESCE("marketing", 0), "marketing") AS "marketing",
    "vendor_compliance",
    "booth_rental",
    "funding_outside_lm",
    "inventory_shrink",
    "total_shrink",
    COALESCE(-1.0 * COALESCE("ocean_freight", 0), "ocean_freight") AS "ocean_freight",
    COALESCE(-1.0 * COALESCE("freight_cost", 0), "freight_cost") AS "freight_cost",
    COALESCE(-1.0 * COALESCE("agency_fee", 0), "agency_fee") AS "agency_fee",
    COALESCE(-1.0 * COALESCE("duty_tarrifs", 0), "duty_tarrifs") AS "duty_tarrifs",
    COALESCE(-1.0 * COALESCE("domestic_inbound", 0), "domestic_inbound") AS "domestic_inbound",
    COALESCE(-1.0 * COALESCE("total_freight", 0), "total_freight") AS "total_freight",
    COALESCE(-1.0 * COALESCE("domestic_outbound", 0), "domestic_outbound") AS "domestic_outbound",
    "SHIPPING_REVENUE_sum",
    "SHIPPING_DISC_sum",
    "ACTUAL_SHIPPING_REVENUE_sum",
    COALESCE(-1.0 * COALESCE("freight_other_charges", 0), "freight_other_charges") AS "freight_other_charges",
    "volume_rebate",
    "price_cuts",
    "scanbacks",
    "markdowns",
    "landed_margin",
    "funding_in_lm",
    "total_funding",
    "last_mile_delivery_combined_sum",
    "small_parcel_ltl_cost_sum",
    "omni_freight_sum",
    "vendor_support",
    "vendor_support_fast",
    "count",
    CAST(right("YEAR_PERIOD",2) AS BIGINT) AS "Period"
  FROM (
    SELECT 
        "ARTICLE_NO",
        "MERCH_YEAR",
        "VENDOR_ID",
        "YEAR_PERIOD",
        "ARTICLE_DESC",
        "ARTICLE_SIZE",
        "MAJ_DEPT_DESC",
        "BRAND_NAME",
        "BUYER_CODE",
        "ARTICLE_COLOR",
        "PARENT_ARTICLE_NO",
        "PARENT_ARTICLE_DESC",
        "ARTICLE_FULL",
        "BUYER_NAME",
        "ARTICLE_CAT_NAME",
        "LOB_DESC",
        "DEPT_NAME",
        "VENDOR_NAME",
        "PARENT_VENDOR_NAME",
        "channel",
        "SALES_QTY",
        "SALES_AMT",
        "COST_AMT",
        "margin",
        "unsellables",
        "defectives_shrink",
        "dc_shrink",
        "theft",
        "defective_allowance",
        "battery_core",
        "mixing_center",
        "new_store_discounts",
        "cash_discounts",
        "pop",
        "pdq",
        "marketing",
        "vendor_compliance",
        "booth_rental",
        "funding_outside_lm",
        "inventory_shrink",
        "total_shrink",
        "ocean_freight",
        "freight_cost",
        "agency_fee",
        "duty_tarrifs",
        "domestic_inbound",
        "total_freight",
        "domestic_outbound",
        "SHIPPING_REVENUE_sum",
        "SHIPPING_DISC_sum",
        "ACTUAL_SHIPPING_REVENUE_sum",
        "freight_other_charges",
        "volume_rebate",
        "price_cuts",
        "scanbacks",
        "markdowns",
        "landed_margin",
        "funding_in_lm",
        "total_funding",
        "last_mile_delivery_combined_sum",
        "small_parcel_ltl_cost_sum",
        "omni_freight_sum",
        "vendor_support",
        "vendor_support_fast",
        "count"
      FROM "VISUAL_TABLES_GRP_W_CHANNEL_1" )A;

<br> </br>
TRUNCATE TABLE VISUAL_TABLES_W_CHANNEL;

<br>
--FINAL QUERY FOR VISUAL_TABLES_W_CHANNEL
</br>
INSERT INTO VISUAL_TABLES_W_CHANNEL
WITH Base AS 
(
  SELECT 
          TO_DATE(  CONCAT(  LEFT( cast(visual_tables_grp_w_channel_dc3pl.YEAR_PERIOD AS VARCHAR) , 4 ) , '-' , RIGHT( 
                  cast(visual_tables_grp_w_channel_dc3pl.YEAR_PERIOD AS VARCHAR) , 2 ) , '-01'  ) ) as PeriodDate
        , visual_tables_grp_w_channel_dc3pl.*
 FROM (
SELECT 
    "visual_tables_grp_prep_channel_1"."ARTICLE_NO" AS "ARTICLE_NO",
    "visual_tables_grp_prep_channel_1"."MERCH_YEAR" AS "MERCH_YEAR",
    "visual_tables_grp_prep_channel_1"."VENDOR_ID" AS "VENDOR_ID",
    "visual_tables_grp_prep_channel_1"."YEAR_PERIOD" AS "YEAR_PERIOD",
    "visual_tables_grp_prep_channel_1"."Period" AS "Period",
    "visual_tables_grp_prep_channel_1"."ARTICLE_DESC" AS "ARTICLE_DESC",
    "visual_tables_grp_prep_channel_1"."ARTICLE_SIZE" AS "ARTICLE_SIZE",
    "visual_tables_grp_prep_channel_1"."MAJ_DEPT_DESC" AS "MAJ_DEPT_DESC",
    "visual_tables_grp_prep_channel_1"."BRAND_NAME" AS "BRAND_NAME",
    "visual_tables_grp_prep_channel_1"."BUYER_CODE" AS "BUYER_CODE",
    "visual_tables_grp_prep_channel_1"."ARTICLE_COLOR" AS "ARTICLE_COLOR",
    "visual_tables_grp_prep_channel_1"."PARENT_ARTICLE_NO" AS "PARENT_ARTICLE_NO",
    "visual_tables_grp_prep_channel_1"."PARENT_ARTICLE_DESC" AS "PARENT_ARTICLE_DESC",
    "visual_tables_grp_prep_channel_1"."ARTICLE_FULL" AS "ARTICLE_FULL",
    "visual_tables_grp_prep_channel_1"."BUYER_NAME" AS "BUYER_NAME",
    "visual_tables_grp_prep_channel_1"."ARTICLE_CAT_NAME" AS "ARTICLE_CAT_NAME",
    "visual_tables_grp_prep_channel_1"."LOB_DESC" AS "LOB_DESC",
    "visual_tables_grp_prep_channel_1"."DEPT_NAME" AS "DEPT_NAME",
    "visual_tables_grp_prep_channel_1"."VENDOR_NAME" AS "VENDOR_NAME",
    "visual_tables_grp_prep_channel_1"."PARENT_VENDOR_NAME" AS "PARENT_VENDOR_NAME",
    "visual_tables_grp_prep_channel_1"."channel" AS "channel",
    "visual_tables_grp_prep_channel_1"."SALES_QTY" AS "SALES_QTY",
    "visual_tables_grp_prep_channel_1"."SALES_AMT" AS "SALES_AMT",
    "visual_tables_grp_prep_channel_1"."COST_AMT" AS "COST_AMT",
    "visual_tables_grp_prep_channel_1"."margin" AS "margin",
    "visual_tables_grp_prep_channel_1"."unsellables" AS "unsellables",
    "visual_tables_grp_prep_channel_1"."defectives_shrink" AS "defectives_shrink",
    "visual_tables_grp_prep_channel_1"."dc_shrink" AS "dc_shrink",
    "visual_tables_grp_prep_channel_1"."theft" AS "theft",
    "visual_tables_grp_prep_channel_1"."defective_allowance" AS "defective_allowance",
    "visual_tables_grp_prep_channel_1"."battery_core" AS "battery_core",
    "visual_tables_grp_prep_channel_1"."mixing_center" AS "mixing_center",
    "visual_tables_grp_prep_channel_1"."new_store_discounts" AS "new_store_discounts",
    "visual_tables_grp_prep_channel_1"."cash_discounts" AS "cash_discounts",
    "visual_tables_grp_prep_channel_1"."pop" AS "pop",
    "visual_tables_grp_prep_channel_1"."pdq" AS "pdq",
    "visual_tables_grp_prep_channel_1"."marketing" AS "marketing",
    "visual_tables_grp_prep_channel_1"."vendor_compliance" AS "vendor_compliance",
    "visual_tables_grp_prep_channel_1"."booth_rental" AS "booth_rental",
    "visual_tables_grp_prep_channel_1"."funding_outside_lm" AS "funding_outside_lm",
    "visual_tables_grp_prep_channel_1"."inventory_shrink" AS "inventory_shrink",
    "visual_tables_grp_prep_channel_1"."total_shrink" AS "total_shrink",
    "visual_tables_grp_prep_channel_1"."ocean_freight" AS "ocean_freight",
    "visual_tables_grp_prep_channel_1"."freight_cost" AS "freight_cost",
    "visual_tables_grp_prep_channel_1"."agency_fee" AS "agency_fee",
    "visual_tables_grp_prep_channel_1"."duty_tarrifs" AS "duty_tarrifs",
    "visual_tables_grp_prep_channel_1"."domestic_inbound" AS "domestic_inbound",
    "visual_tables_grp_prep_channel_1"."total_freight" AS "total_freight",
    "visual_tables_grp_prep_channel_1"."domestic_outbound" AS "domestic_outbound",
    "visual_tables_grp_prep_channel_1"."SHIPPING_REVENUE_sum" AS "SHIPPING_REVENUE_sum",
    "visual_tables_grp_prep_channel_1"."SHIPPING_DISC_sum" AS "SHIPPING_DISC_sum",
    "visual_tables_grp_prep_channel_1"."ACTUAL_SHIPPING_REVENUE_sum" AS "ACTUAL_SHIPPING_REVENUE_sum",
    "visual_tables_grp_prep_channel_1"."freight_other_charges" AS "freight_other_charges",
    "visual_tables_grp_prep_channel_1"."volume_rebate" AS "volume_rebate",
    "visual_tables_grp_prep_channel_1"."price_cuts" AS "price_cuts",
    "visual_tables_grp_prep_channel_1"."scanbacks" AS "scanbacks",
    "visual_tables_grp_prep_channel_1"."markdowns" AS "markdowns",
    "visual_tables_grp_prep_channel_1"."landed_margin" AS "landed_margin",
    "visual_tables_grp_prep_channel_1"."funding_in_lm" AS "funding_in_lm",
    "visual_tables_grp_prep_channel_1"."total_funding" AS "total_funding",
    "visual_tables_grp_prep_channel_1"."last_mile_delivery_combined_sum" AS "last_mile_delivery_combined_sum",
    "visual_tables_grp_prep_channel_1"."small_parcel_ltl_cost_sum" AS "small_parcel_ltl_cost_sum",
    "visual_tables_grp_prep_channel_1"."omni_freight_sum" AS "omni_freight_sum",
    "visual_tables_grp_prep_channel_1"."vendor_support" AS "vendor_support",
    "visual_tables_grp_prep_channel_1"."vendor_support_fast" AS "vendor_support_fast",
    "DC_3PL_allocation_cost_union"."3PL_TOTAL_VARIABLE_COST_ALLOCATED" AS "3PL_total_variable_cost",
    "DC_3PL_allocation_cost_union"."3PL_TOTAL_FIXED_COST_ALLOCATED" AS "3PL_total_fixed_cost",
    "DC_3PL_allocation_cost_union"."DC_TOTAL_VARIABLE_COST_ALLOCATED" AS "DC_total_variable_cost",
    "DC_3PL_allocation_cost_union"."DC_TOTAL_FIXED_COST_ALLOCATED" AS "DC_total_fixed_cost"
  FROM "VISUAL_TABLES_GRP_PREP_CHANNEL_1" "visual_tables_grp_prep_channel_1"
  LEFT JOIN "DC_3PL_ALLOCATION_COST_UNION" "DC_3PL_allocation_cost_union" 
    ON ("visual_tables_grp_prep_channel_1"."ARTICLE_NO" = "DC_3PL_allocation_cost_union"."ARTICLE_NO")
      AND ("visual_tables_grp_prep_channel_1"."VENDOR_ID" = "DC_3PL_allocation_cost_union"."VENDOR_ID")
      AND ("visual_tables_grp_prep_channel_1"."YEAR_PERIOD" = "DC_3PL_allocation_cost_union"."YEAR_PERIOD")
      AND ("visual_tables_grp_prep_channel_1"."channel" = "DC_3PL_allocation_cost_union"."CHANNEL")
      )visual_tables_grp_w_channel_dc3pl
    )

 SELECT DISTINCT
    Base."ARTICLE_NO"
  , Base.MERCH_YEAR
  , Base.VENDOR_ID
  , Base.YEAR_PERIOD
  , Base."Period"
  , Base.ARTICLE_DESC
  , Base.ARTICLE_SIZE
  , Base.MAJ_DEPT_DESC
  , Base.BRAND_NAME
  , Base.BUYER_CODE
  , Base.ARTICLE_COLOR
  , Base.PARENT_ARTICLE_NO
  , Base.PARENT_ARTICLE_DESC
  , Base.ARTICLE_FULL
  , Base.BUYER_NAME
  , Base.ARTICLE_CAT_NAME
  , Base.LOB_DESC
  , Base.DEPT_NAME
  , Base.VENDOR_NAME
  , Base.PARENT_VENDOR_NAME
  , Base."channel"
  , Base.SALES_QTY
  , Base.SALES_AMT
  , Base.COST_AMT
  , Base."margin"
  , Base."unsellables"
  , Base."defectives_shrink"
  , Base."dc_shrink"
  , Base."theft"
  , Base."defective_allowance"
  , Base."battery_core"
  , Base."mixing_center"
  , Base."new_store_discounts"
  , Base."cash_discounts"
  , Base."pop"
  , Base."pdq"
  , Base."marketing"
  , Base."vendor_compliance"
  , Base."booth_rental"
  , Base."funding_outside_lm"
  , Base."inventory_shrink"
  , Base."total_shrink"
  , Base."ocean_freight"
  , Base."freight_cost"
  , Base."agency_fee"
  , Base."duty_tarrifs"
  , Base."domestic_inbound"
  , Base."total_freight"
  , Base."domestic_outbound"
  , Base."SHIPPING_REVENUE_sum"
  , Base."SHIPPING_DISC_sum"
  , Base."ACTUAL_SHIPPING_REVENUE_sum"
  , Base."freight_other_charges"
  , Base."volume_rebate"
  , Base."price_cuts"
  , Base."scanbacks"
  , Base."markdowns"
  , Base."landed_margin"
  , Base."funding_in_lm"
  , Base."total_funding"
  , Base."last_mile_delivery_combined_sum"
  , Base."small_parcel_ltl_cost_sum"
  , Base."omni_freight_sum"
  , Base."vendor_support"
  , Base."vendor_support_fast"
  , Base."3PL_total_variable_cost"
  , Base."3PL_total_fixed_cost"
  , Base."DC_total_variable_cost"
  , Base."DC_total_fixed_cost"
  , SUM( Agg."3PL_total_variable_cost" ) as "3PL_total_variable_cost_TTM"
  , SUM( Agg."3PL_total_fixed_cost" ) as "3PL_total_fixed_cost_TTM"
  , SUM( Agg."DC_total_variable_cost" ) as "DC_total_variable_cost_TTM"
  , SUM( Agg."DC_total_fixed_cost" ) as "DC_total_fixed_cost_TTM"
  , SUM( Agg.sales_amt ) as sales_amt_TTM  
FROM
    Base
    LEFT OUTER JOIN Base Agg ON
            Base."ARTICLE_NO"         = Agg.Article_No
        AND ifnull(Base.BUYER_CODE,-11111)     = ifnull(Agg.BUYER_CODE,-11111)
        AND ifnull(Base.BUYER_NAME,'')         = ifnull(Agg.BUYER_NAME,'')
        AND ifnull(Base.ARTICLE_CAT_NAME,'')   = ifnull(Agg.ARTICLE_CAT_NAME,'')
        AND ifnull(Base.PARENT_VENDOR_NAME,'') = ifnull(Agg.PARENT_VENDOR_NAME   ,'')     
        AND ifnull(Base.Vendor_ID,-11111 )     = ifnull(Agg.Vendor_ID,-11111)
        AND ifnull(Base.LOB_DESC,'')           = ifnull(Agg.LOB_DESC,'')
        AND ifnull(Base.DEPT_NAME,'')          = ifnull(Agg.DEPT_NAME,'')
        AND ifnull(Base.BRAND_NAME,'')         = ifnull(Agg.BRAND_NAME,'')
        AND ifnull(Base."channel", '')         = ifnull(Agg."channel",'')
        AND ifnull(Base.ARTICLE_SIZE, '')      = ifnull(Agg.ARTICLE_SIZE,'')
        AND ifnull(Base.ARTICLE_COLOR , '')    = ifnull(Agg.ARTICLE_COLOR,'')
        AND Agg.PeriodDate between DateAdd( month, -11 , Base.PeriodDate ) and Base.PeriodDate    
GROUP BY 
    Base."ARTICLE_NO"
  , Base.MERCH_YEAR
  , Base.VENDOR_ID
  , Base.YEAR_PERIOD
  , Base."Period"
  , Base.ARTICLE_DESC
  , Base.ARTICLE_SIZE
  , Base.MAJ_DEPT_DESC
  , Base.BRAND_NAME
  , Base.BUYER_CODE
  , Base.ARTICLE_COLOR
  , Base.PARENT_ARTICLE_NO
  , Base.PARENT_ARTICLE_DESC
  , Base.ARTICLE_FULL
  , Base.BUYER_NAME
  , Base.ARTICLE_CAT_NAME
  , Base.LOB_DESC
  , Base.DEPT_NAME
  , Base.VENDOR_NAME
  , Base.PARENT_VENDOR_NAME
  , Base."channel"
  , Base.SALES_QTY
  , Base.SALES_AMT
  , Base.COST_AMT
  , Base."margin"
  , Base."unsellables"
  , Base."defectives_shrink"
  , Base."dc_shrink"
  , Base."theft"
  , Base."defective_allowance"
  , Base."battery_core"
  , Base."mixing_center"
  , Base."new_store_discounts"
  , Base."cash_discounts"
  , Base."pop"
  , Base."pdq"
  , Base."marketing"
  , Base."vendor_compliance"
  , Base."booth_rental"
  , Base."funding_outside_lm"
  , Base."inventory_shrink"
  , Base."total_shrink"
  , Base."ocean_freight"
  , Base."freight_cost"
  , Base."agency_fee"
  , Base."duty_tarrifs"
  , Base."domestic_inbound"
  , Base."total_freight"
  , Base."domestic_outbound"
  , Base."SHIPPING_REVENUE_sum"
  , Base."SHIPPING_DISC_sum"
  , Base."ACTUAL_SHIPPING_REVENUE_sum"
  , Base."freight_other_charges"
  , Base."volume_rebate"
  , Base."price_cuts"
  , Base."scanbacks"
  , Base."markdowns"
  , Base."landed_margin"
  , Base."funding_in_lm"
  , Base."total_funding"
  , Base."last_mile_delivery_combined_sum"
  , Base."small_parcel_ltl_cost_sum"
  , Base."omni_freight_sum"
  , Base."vendor_support"
  , Base."vendor_support_fast"
  , Base."3PL_total_variable_cost"
  , Base."3PL_total_fixed_cost"
  , Base."DC_total_variable_cost"
  , Base."DC_total_fixed_cost";

<br>
--FLOW_PATH_PREPAID_LATEST_FLOWPATH
</br>
insert into FLOW_PATH_PREPAID_LATEST_FLOWPATH
WITH Base AS 
    (
        SELECT 
            SUM(case when ifnull( "flow_path" ,'-999') = '-999' then 0 else 1 end) over (Partition By ARTICLE_NO order by YEAR_PERIOD ) as value_partition
            , * 
        FROM 
            "FLOW_PATH_PREPARED_STACKED"
        ORDER BY 
                ARTICLE_NO
            , YEAR_PERIOD     
    ) 
    SELECT DISTINCT
        Base.YEAR_PERIOD
        , Base.ARTICLE_NO
        , CASE WHEN Base."flow_path" IS NOT NULL THEN Base."flow_path"	            ELSE LR."flow_path"              END AS flow_path
        , CASE WHEN Base."flow_path" IS NOT NULL THEN Base."leg_2"	                ELSE LR."leg_2"                  END AS leg_2
        , CASE WHEN Base."flow_path" IS NOT NULL THEN Base."order_qty"	            ELSE LR."order_qty"              END AS order_qty
        , CASE WHEN Base."flow_path" IS NOT NULL THEN Base."order_qty_sum"	        ELSE LR."order_qty_sum"          END AS order_qty_sum
        , CASE WHEN Base."flow_path" IS NOT NULL THEN Base."share"	                ELSE LR."share"                  END AS share
        , CASE WHEN Base."flow_path" IS NOT NULL THEN Base."leg_3"	                ELSE LR."leg_3"                  END AS leg_3
        , CASE WHEN Base."flow_path" IS NOT NULL THEN Base."leg_2_latest_unit_cost"	ELSE LR."leg_2_latest_unit_cost" END AS leg_2_latest_unit_cost
        , CASE WHEN Base."flow_path" IS NOT NULL THEN Base."leg_3_latest_unit_cost"  ELSE LR."leg_3_latest_unit_cost" END AS leg_3_latest_unit_cost
        , Base.value_partition
        , case when Base."flow_path" IS NULL THEN 'E' ELSE 'NE' END as Flag
    FROM 
        Base
        LEFT OUTER JOIN Base LR
            ON  Base.Article_No      = LR.Article_No
            AND Base.value_partition = LR.value_partition
            AND Base.Year_Period     > LR.Year_Period
            AND LR."flow_path" IS NOT NULL;

<br>
--FLOW_PATH_PREPAID_LATEST_FLOWPATH_PREPARED
</br>
insert into FLOW_PATH_PREPAID_LATEST_FLOWPATH_PREPARED
SELECT DISTINCT
    YEAR_PERIOD,
    ARTICLE_NO,
    flow_path,
    leg_2,
    order_qty,
    order_qty_sum,
    share,
    leg_3,
    leg_2_latest_unit_cost,
    leg_3_latest_unit_cost,
    VALUE_PARTITION,
    FLAG,
    prepaid_freight,
    COALESCE(share, 0) * COALESCE(prepaid_freight, 0) AS weighted_prepaid_freight
  FROM (
    SELECT 
        YEAR_PERIOD,
        ARTICLE_NO,
        flow_path,
        leg_2,
        order_qty,
        order_qty_sum,
        share,
        leg_3,
        leg_2_latest_unit_cost,
        leg_3_latest_unit_cost,
        VALUE_PARTITION,
        FLAG,
        COALESCE(leg_2_latest_unit_cost, 0) + COALESCE(leg_3_latest_unit_cost, 0) AS prepaid_freight
      FROM FLOW_PATH_PREPAID_LATEST_FLOWPATH 
    ) A;

<br>
--Sales_for_freight_table
</br>
Insert into Sales_for_freight_table
SELECT DISTINCT
    ARTICLE_NO AS ARTICLE_NO,
    YEAR_PERIOD AS YEAR_PERIOD,
    SUM(SALES_QTY) AS SALES_QTY,
    SUM(SALES_AMT) AS SALES_AMT,
    SUM(COST_AMT) AS COST_AMT,
    SUM(total_shrink) AS total_shrink,
    SUM(freight_other_charges) AS freight_other_charges,
    SUM(funding_in_lm) AS total_funding
    FROM (
    SELECT 
        ARTICLE_NO,
        YEAR_PERIOD,
        SALES_QTY,
        SALES_AMT,
        COST_AMT,
        "total_shrink" AS total_shrink,
        "freight_other_charges" AS freight_other_charges,
        "funding_in_lm" AS funding_in_lm
        FROM LANDED_MARGIN_CALC
        WHERE "channel" = 'Store'
    ) "dku__beforegrouping"
    GROUP BY ARTICLE_NO, YEAR_PERIOD;

<br>
--FLOW_PATH_FINAL_STACKED_AGG
</br>
insert into FLOW_PATH_FINAL_STACKED_AGG 
SELECT DISTINCT
     YEAR_PERIOD  AS  YEAR_PERIOD ,
     ARTICLE_NO  AS  ARTICLE_NO ,
     VENDOR_ID  AS  VENDOR_ID ,
     flow_path  AS  flow_path ,
    SUM( order_qty ) AS  order_qty ,
    SUM( domestic_inbound_freight ) AS  domestic_inbound ,
    SUM( ocean_freight ) AS  ocean_freight ,
    SUM( domestic_outbound_freight ) AS  domestic_outbound ,
    AVG( lastest_agency_fee_rate ) AS  agency_fee_rate ,
    AVG( latest_duty_rate ) AS  duty_rate ,
    SUM( prepaid_freight ) AS  prepaid_freight 
       from (select  * 
       from (
            SELECT distinct
                 YEAR_PERIOD  AS  YEAR_PERIOD ,
                 ARTICLE_NO  AS  ARTICLE_NO ,
                 VENDOR_ID  AS  VENDOR_ID ,
                 leg  AS  leg ,
                 flow_path  AS  flow_path ,
                 import_flag  AS  import_flag ,
                 leg_1  AS  leg_1 ,
                 order_qty  AS  order_qty ,
                 order_qty_sum  AS  order_qty_sum ,
                 share  AS  share ,
                 share_sum  AS  share_sum ,
                 leg_2  AS  leg_2 ,
                 leg_3  AS  leg_3 ,
                 leg_1_latest_unit_cost  AS  leg_1_latest_unit_cost ,
                 domestic_inbound_freight  AS  domestic_inbound_freight ,
                 weighted_domestic_inbound  AS  weighted_domestic_inbound ,
                 ocean_freight  AS  ocean_freight ,
                 weighted_ocean_freight  AS  weighted_ocean_freight ,
                 leg_2_latest_unit_cost  AS  leg_2_latest_unit_cost ,
                 leg_3_latest_unit_cost  AS  leg_3_latest_unit_cost ,
                 domestic_outbound_freight  AS  domestic_outbound_freight ,
                 weighted_domestic_outbound  AS  weighted_domestic_outbound ,
                 total_freight_cost  AS  total_freight_cost ,
                 weighted_freight_cost  AS  weighted_freight_cost ,
                 lastest_agency_fee_rate  AS  lastest_agency_fee_rate ,
                 weighted_agency_fee  AS  weighted_agency_fee ,
                 latest_duty_rate  AS  latest_duty_rate ,
                 weighted_duty_rate  AS  weighted_duty_rate ,
                 VALUE_PARTITION  AS  VALUE_PARTITION ,
                 FLAG  AS  FLAG ,
                NULL AS  prepaid_freight ,
                NULL AS  weighted_prepaid_freight ,
                NULL AS  prepaid_share 
            FROM FLOW_PATH_WITH_LATEST_COST_PREPARED
            UNION ALL
            SELECT distinct
                 YEAR_PERIOD  AS  YEAR_PERIOD ,
                 ARTICLE_NO  AS  ARTICLE_NO ,
                NULL AS  VENDOR_ID ,
                NULL AS  leg ,
                 flow_path  AS  flow_path ,
                NULL AS  import_flag ,
                NULL AS  leg_1 ,
                 order_qty  AS  order_qty ,
                 order_qty_sum  AS  order_qty_sum ,
                 share  AS  share ,
                NULL AS  share_sum ,
                 leg_2  AS  leg_2 ,
                 leg_3  AS  leg_3 ,
                NULL AS  leg_1_latest_unit_cost ,
                NULL AS  domestic_inbound_freight ,
                NULL AS  weighted_domestic_inbound ,
                NULL AS  ocean_freight ,
                NULL AS  weighted_ocean_freight ,
                 leg_2_latest_unit_cost  AS  leg_2_latest_unit_cost ,
                 leg_3_latest_unit_cost  AS  leg_3_latest_unit_cost ,
                NULL AS  domestic_outbound_freight ,
                NULL AS  weighted_domestic_outbound ,
                NULL AS  total_freight_cost ,
                NULL AS  weighted_freight_cost ,
                NULL AS  lastest_agency_fee_rate ,
                NULL AS  weighted_agency_fee ,
                NULL AS  latest_duty_rate ,
                NULL AS  weighted_duty_rate ,
                 VALUE_PARTITION  AS  VALUE_PARTITION ,
                 FLAG  AS  FLAG ,
                 prepaid_freight  AS  prepaid_freight ,
                 weighted_prepaid_freight  AS  weighted_prepaid_freight ,
                 prepaid_share  AS  prepaid_share 
            FROM (               
                select 
                     YEAR_PERIOD ,
                     ARTICLE_NO ,
                     flow_path ,
                     leg_2 ,
                     order_qty_sum ,
                     share ,
                     leg_3 ,
                     leg_2_latest_unit_cost ,
                     leg_3_latest_unit_cost ,
                     prepaid_freight ,
                     weighted_prepaid_freight ,
                     VALUE_PARTITION ,
                     FLAG ,
                     prepaid_share  ,
                    coalesce(prepaid_share,0)*coalesce(order_qty,0) as order_qty
                from ( 
                SELECT 
                    FLOW_PATH_PREPAID_latest_flowpath_prepared.YEAR_PERIOD AS YEAR_PERIOD,
                    FLOW_PATH_PREPAID_latest_flowpath_prepared.ARTICLE_NO AS ARTICLE_NO,
                    FLOW_PATH_PREPAID_latest_flowpath_prepared.flow_path AS flow_path,
                    FLOW_PATH_PREPAID_latest_flowpath_prepared.leg_2 AS leg_2,
                    FLOW_PATH_PREPAID_latest_flowpath_prepared.order_qty AS order_qty,
                    FLOW_PATH_PREPAID_latest_flowpath_prepared.order_qty_sum AS order_qty_sum,
                    FLOW_PATH_PREPAID_latest_flowpath_prepared.share AS share,
                    FLOW_PATH_PREPAID_latest_flowpath_prepared.leg_3 AS leg_3,
                    FLOW_PATH_PREPAID_latest_flowpath_prepared.leg_2_latest_unit_cost AS leg_2_latest_unit_cost,
                    FLOW_PATH_PREPAID_latest_flowpath_prepared.leg_3_latest_unit_cost AS leg_3_latest_unit_cost,
                    FLOW_PATH_PREPAID_latest_flowpath_prepared.prepaid_freight AS prepaid_freight,
                    FLOW_PATH_PREPAID_latest_flowpath_prepared.weighted_prepaid_freight AS weighted_prepaid_freight,
                    FLOW_PATH_PREPAID_latest_flowpath_prepared.VALUE_PARTITION AS VALUE_PARTITION,
                    FLOW_PATH_PREPAID_latest_flowpath_prepared.FLAG AS FLAG,
                     purchase_order_line_prepaid_share . prepaid_share  AS  prepaid_share 
                FROM FLOW_PATH_PREPAID_LATEST_FLOWPATH_PREPARED  FLOW_PATH_PREPAID_latest_flowpath_prepared 
                LEFT JOIN PURCHASE_ORDER_LINE_PREPAID_SHARE  purchase_order_line_prepaid_share 
                    ON ( FLOW_PATH_PREPAID_latest_flowpath_prepared . YEAR_PERIOD  =  purchase_order_line_prepaid_share . YEAR_PERIOD )
                    AND ( FLOW_PATH_PREPAID_latest_flowpath_prepared . ARTICLE_NO  =  purchase_order_line_prepaid_share . ARTICLE_NO )
                )dku
                where prepaid_share != '0'
            )dku
            )dku1
            where  flow_path  IS NOT NULL)dku_2
GROUP BY  YEAR_PERIOD ,  ARTICLE_NO ,  VENDOR_ID ,  flow_path ;

<br>
--SALES_FOR_FREIGHT_TABLE_JOINED_AGG_LAST
</br>
insert into  SALES_FOR_FREIGHT_TABLE_JOINED_AGG_LAST
SELECT DISTINCT
    R.Year_Period
    , R.Article_no
    , R.Sales_QTY
    , R.SALES_AMT
    , R.COST_AMT
    , R.total_shrink
    , R.total_funding
    , R.freight_other_charges
    , R.po_order_qty
    , R.po_receipt
    , R.turn_period
    , R.po_order_qty_sum
    , R.po_receipt_sum
    , R.value_partition_order
    , R.value_partition_receipt          
    , first_Value(R.po_order_qty_sum)over (partition by R.ARTICLE_NO, R.value_partition_order order by R.YEAR_PERIOD ASC)  as last_po_order_qty_sum
    , first_Value(R.po_receipt_sum)over (partition by R.ARTICLE_NO, value_partition_receipt order by R.YEAR_PERIOD ASC)  as last_po_receipt_sum
    FROM
    (
        SELECT   
            I.Year_Period
            , I.Article_no
            , I.Sales_QTY
            , I.SALES_AMT
            , I.COST_AMT
            , I.total_shrink
            , I.total_funding
            , I.freight_other_charges
            , I.po_order_qty
            , I.po_receipt
            , I.turn_period
            , I.po_order_qty_sum
            , I.po_receipt_sum
            , sum(case when ifnull( I.po_order_qty_sum ,0) = 0 then 0 else 1 end) over (Partition By I.ARTICLE_NO order by I.YEAR_PERIOD ) as value_partition_order
            , sum(case when ifnull( I.po_receipt_sum ,0) = 0 then 0 else 1 end)   over (Partition By I.ARTICLE_NO order by I.YEAR_PERIOD ) as value_partition_receipt
    FROM 
        (
    With Base as
    ( 
    SELECT 
        TO_DATE(  CONCAT(  LEFT( cast(B.Year_Period AS VARCHAR) , 4 ) , '-' , RIGHT( cast(B.Year_Period AS VARCHAR) , 2 ) , '-01'  ) )  as Period 
        , B.Year_Period
        , B.Article_no
        , B.Sales_QTY
        , B.SALES_AMT
        , B.COST_AMT
        , B.total_shrink
        , B.total_funding
        , B.freight_other_charges
        , B.po_order_qty
        , B.po_receipt
        , B.turn_period
        , ( B.turn_period * -1  ) as negativeturn_period    
    FROM 
        (SELECT 
            "sales_for_freight_table".ARTICLE_NO AS ARTICLE_NO,
            "sales_for_freight_table".YEAR_PERIOD AS YEAR_PERIOD,
            "sales_for_freight_table".SALES_QTY AS SALES_QTY,
            "sales_for_freight_table".SALES_AMT AS SALES_AMT,
            "sales_for_freight_table".COST_AMT AS COST_AMT,
            "sales_for_freight_table".total_shrink AS total_shrink,
            "sales_for_freight_table".freight_other_charges AS freight_other_charges,
            "sales_for_freight_table".total_funding AS total_funding,
            "purchase_order_line_cost".po_order_qty AS po_order_qty,
            "purchase_order_line_cost".po_receipt AS po_receipt,
            "turn_period_master"."turn_period" AS turn_period
        FROM SALES_FOR_FREIGHT_TABLE "sales_for_freight_table"
        LEFT JOIN PURCHASE_ORDER_LINE_COST "purchase_order_line_cost"
            ON ("sales_for_freight_table".ARTICLE_NO = "purchase_order_line_cost".ARTICLE_NO)
            AND ("sales_for_freight_table".YEAR_PERIOD = "purchase_order_line_cost".YEAR_PERIOD)
        LEFT JOIN TURN_PERIOD_MASTER "turn_period_master"
            ON "sales_for_freight_table".ARTICLE_NO = "turn_period_master".ARTICLE_NO) B
    )    
    SELECT 
        Base.Year_Period
        , Base.Article_no
        , Base.Sales_QTY
        , Base.SALES_AMT
        , Base.COST_AMT
        , Base.total_shrink
        , Base.total_funding
        , Base.freight_other_charges
        , Base.po_order_qty
        , Base.po_receipt
        , Base.turn_period
        , Sum( B.po_order_qty  ) as po_order_qty_sum
        , Sum( B.po_receipt    ) as po_receipt_sum
    FROM  Base 
    LEFT OUTER JOIN Base B on 
        Base.Article_no = B.Article_No
        AND B.Period BETWEEN  DateAdd( Month, Base.negativeturn_period + 1 , Base.Period  ) AND Base.Period
    GROUP BY 
        Base.Year_Period
        , Base.Article_no
        , Base.Sales_QTY
        , Base.SALES_AMT
        , Base.COST_AMT
        , Base.total_shrink
        , Base.total_funding
        , Base.freight_other_charges
        , Base.po_order_qty
        , Base.po_receipt
        , Base.turn_period
    ) I
    ORDER BY 
        I.Year_Period
        , I.Article_no
        , I.Sales_QTY
        , I.SALES_AMT
        , I.COST_AMT
        , I.total_shrink
        , I.total_funding
        , I.freight_other_charges
        , I.po_order_qty
        , I.po_receipt
        , I.turn_period
        , I.po_order_qty_sum
        , I.po_receipt_sum
    ) as R    
    ORDER BY 
        R.Year_Period
        , R.Article_no
        , R.Sales_QTY
        , R.SALES_AMT
        , R.COST_AMT
        , R.total_shrink
        , R.total_funding
        , R.freight_other_charges
        , R.po_order_qty
        , R.po_receipt
        , R.turn_period
        , R.po_order_qty_sum
        , R.po_receipt_sum
        , R.value_partition_order
        , R.value_partition_receipt  ;

<br>
--FLOW_PATH_FINAL_STACKED_AGG_JOINED
</br>
insert into FLOW_PATH_FINAL_STACKED_AGG_JOINED
SELECT DISTINCT
    flow_path.YEAR_PERIOD AS YEAR_PERIOD,
    flow_path.ARTICLE_NO AS ARTICLE_NO,
    flow_path.VENDOR_ID AS VENDOR_ID,
    flow_path.flow_path AS flow_path,
    flow_path.order_qty AS order_qty,
    flow_path.domestic_inbound AS domestic_inbound,
    flow_path.ocean_freight AS ocean_freight,
    flow_path.domestic_outbound AS domestic_outbound,
    flow_path.agency_fee_rate AS agency_fee_rate,
    flow_path.duty_rate AS duty_rate,
    flow_path.prepaid_freight AS prepaid_freight,
    sales.SALES_QTY AS SALES_QTY,
    sales.SALES_AMT AS SALES_AMT,
    sales.COST_AMT AS COST_AMT,
    sales.total_shrink AS total_shrink,
    sales.total_funding AS total_funding,
    sales.freight_other_charges AS freight_other_charges,
    sales.last_po_order_qty_sum AS po_order_qty,
    sales.last_po_receipt_sum AS po_receipt
FROM FLOW_PATH_FINAL_STACKED_AGG flow_path
LEFT JOIN SALES_FOR_FREIGHT_TABLE_JOINED_AGG_LAST sales
ON (flow_path.YEAR_PERIOD = sales.YEAR_PERIOD)
AND (flow_path.ARTICLE_NO = sales.ARTICLE_NO);

<br>
--FLOW_PATH_FINAL_STACKED_W_ARTICLE
</br>
insert into FLOW_PATH_FINAL_STACKED_W_ARTICLE
SELECT DISTINCT
    flow_path.YEAR_PERIOD AS YEAR_PERIOD,
    flow_path.ARTICLE_NO AS ARTICLE_NO,
    flow_path.VENDOR_ID AS VENDOR_ID,
    flow_path.flow_path AS flow_path,
    flow_path.order_qty AS order_qty,
    flow_path.domestic_inbound AS domestic_inbound,
    flow_path.ocean_freight AS ocean_freight,
    flow_path.domestic_outbound AS domestic_outbound,
    flow_path.agency_fee_rate AS agency_fee_rate,
    flow_path.duty_rate AS duty_rate,
    flow_path.prepaid_freight AS prepaid_freight,
    flow_path.SALES_QTY AS SALES_QTY,
    flow_path.SALES_AMT AS SALES_AMT,
    flow_path.COST_AMT AS COST_AMT,
    flow_path.total_shrink AS total_shrink,
    flow_path.total_funding AS total_funding,
    flow_path.freight_other_charges AS freight_other_charges,
    flow_path.po_order_qty AS po_order_qty,
    flow_path.po_receipt AS po_receipt,
    artical.ARTICLE_FULL AS ARTICLE_FULL,
    artical.ARTICLE_SIZE AS ARTICLE_SIZE,
    artical.ARTICLE_COLOR AS ARTICLE_COLOR,
    artical.PARENT_ARTICLE_NO AS PARENT_ARTICLE_NO,
    artical.PARENT_ARTICLE_DESC AS PARENT_ARTICLE_DESC,
    artical.ARTICLE_CAT_FULL AS ARTICLE_CAT_FULL,
    artical.DEPT_FULL AS DEPT_FULL,
    artical.LOB_FULL AS LOB_FULL,
    artical.BRAND_NAME AS BRAND_NAME,
    vendor.VENDOR_FULL AS VENDOR_FULL,
    company_article_w_buyer.BUYER_FULL AS BUYER_FULL,
    company_parent_vendor_prepared.PARENT_VENDOR_ID AS PARENT_VENDOR_ID,
    vendor_2.VENDOR_FULL AS PARENT_VENDOR_NAME
FROM FLOW_PATH_FINAL_STACKED_AGG_JOINED flow_path
LEFT JOIN ARTICLE_W_SHP_DIM_PREP artical
    ON flow_path.ARTICLE_NO = artical.ARTICLE_NO
LEFT JOIN EDW.VW_VENDOR vendor
    ON flow_path.VENDOR_ID = vendor.VENDOR_ID
LEFT JOIN COMPANY_ARTICLE_W_BUYER company_article_w_buyer
    ON flow_path.ARTICLE_NO = company_article_w_buyer.ARTICLE_NO
LEFT JOIN COMPANY_PARENT_VENDOR_PREPARED company_parent_vendor_prepared
    ON flow_path.VENDOR_ID = company_parent_vendor_prepared.VENDOR_ID
LEFT JOIN EDW.VW_VENDOR vendor_2
    ON company_parent_vendor_prepared.PARENT_VENDOR_ID = vendor_2.VENDOR_ID;

<br>
--FLOW_PATH_FINAL_STACKED_W_ARTICLE_PREPARED
</br>
insert into FLOW_PATH_FINAL_STACKED_W_ARTICLE_PREPARED(
    YEAR_PERIOD,
    MERCH_YEAR,
    ARTICLE_NO,
    VENDOR_ID,
    flow_path,
    order_qty,
    domestic_inbound,
    ocean_freight,
    domestic_outbound,
    agency_fee_rate,
    duty_rate,
    SALES_QTY,
    SALES_AMT,
    COST_AMT,
    cogs_per_unit,
    agency_fee_per_unit,
    duty_per_unit,
    margin,
    total_shrink,
    shrink_per_unit,
    total_funding,
    funding_per_unit,
    freight_other_charges,
    po_order_qty,
    po_receipt,
    ARTICLE_FULL,
    ARTICLE_SIZE,
    ARTICLE_COLOR,
    PARENT_ARTICLE_NO,
    PARENT_ARTICLE_DESC,
    ARTICLE_CAT_FULL,
    DEPT_FULL,
    LOB_FULL,
    BRAND_NAME,
    VENDOR_FULL,
    BUYER_FULL,
    PARENT_VENDOR_ID,
    PARENT_VENDOR_NAME,
    order_qty_sum
)
SELECT DISTINCT
    YEAR_PERIOD,
    SUBSTRING(YEAR_PERIOD,1,4) AS MERCH_YEAR,
    ARTICLE_NO,
    VENDOR_ID,
    flow_path,
    order_qty,
    CASE
        WHEN COALESCE(domestic_inbound, 0) != 0 THEN COALESCE(domestic_inbound, 0)
        ELSE NULL
    END AS domestic_inbound,
    CASE
        WHEN COALESCE(ocean_freight, 0) != 0 THEN COALESCE(ocean_freight, 0)
        ELSE NULL
    END AS ocean_freight,
    CASE
        WHEN COALESCE(domestic_outbound_temp, 0) != 0 THEN COALESCE(domestic_outbound_temp, 0)
        ELSE NULL
    END AS domestic_outbound,
    CASE
        WHEN COALESCE(agency_fee_rate, 0) != 0 THEN COALESCE(agency_fee_rate, 0)
        ELSE NULL
    END AS agency_fee_rate,
    CASE
        WHEN COALESCE(duty_rate, 0) != 0 THEN COALESCE(duty_rate, 0)
        ELSE NULL
    END AS duty_rate,
    COALESCE(SALES_QTY, 0) AS SALES_QTY,
    COALESCE(SALES_AMT, 0) AS SALES_AMT,
    COALESCE(COST_AMT, 0) AS COST_AMT,
    CASE WHEN (SALES_QTY = 0 OR (SALES_QTY is null)) THEN 0
        ELSE coalesce(COST_AMT,0)/coalesce(SALES_QTY,0)
    END AS cogs_per_unit,
    coalesce(agency_fee_rate,0)*coalesce(cogs_per_unit,0) AS agency_fee_per_unit,
    coalesce(duty_rate,0)*coalesce(cogs_per_unit,0) AS duty_per_unit,
    coalesce(SALES_AMT,0)- coalesce(COST_AMT,0) AS margin,
    COALESCE(total_shrink, 0) AS total_shrink,
    CASE 
       WHEN (SALES_QTY = 0 OR (SALES_QTY is null)) THEN 0
       ELSE coalesce(total_shrink,0)/coalesce(SALES_QTY,0)
    END AS shrink_per_unit,
    COALESCE(total_funding, 0) AS total_funding,
    CASE WHEN (SALES_QTY = 0 OR (SALES_QTY is Null)) THEN 0
    ELSE coalesce(total_funding,0)/coalesce(SALES_QTY,0)
    END AS funding_per_unit,
    COALESCE(freight_other_charges, 0) AS freight_other_charges,
    po_order_qty,
    po_receipt,
    ARTICLE_FULL,
    ARTICLE_SIZE,
    ARTICLE_COLOR,
    PARENT_ARTICLE_NO,
    PARENT_ARTICLE_DESC,
    ARTICLE_CAT_FULL,
    DEPT_FULL,
    LOB_FULL,
    BRAND_NAME,
    VENDOR_FULL,
    BUYER_FULL,
    PARENT_VENDOR_ID,
    PARENT_VENDOR_NAME,
    order_qty_sum
from(
      SELECT 
        YEAR_PERIOD AS YEAR_PERIOD,
        ARTICLE_NO AS ARTICLE_NO,
        VENDOR_ID AS VENDOR_ID,
        flow_path AS flow_path,
        order_qty AS order_qty,
        domestic_inbound AS domestic_inbound,
        ocean_freight AS ocean_freight,
        domestic_outbound AS domestic_outbound,
        CASE
            WHEN ARTICLE_NO = 3195163 OR ARTICLE_NO = 1115622 THEN 0
            ELSE COALESCE(domestic_outbound, 0) + COALESCE(prepaid_freight, 0)
        END AS domestic_outbound_temp,
        agency_fee_rate AS agency_fee_rate,
        duty_rate AS duty_rate,
        prepaid_freight AS prepaid_freight,
        SALES_QTY AS SALES_QTY,
        SALES_AMT AS SALES_AMT,
        COST_AMT AS COST_AMT,
        total_shrink AS total_shrink,
        total_funding AS total_funding,
        freight_other_charges AS freight_other_charges,
        po_order_qty AS po_order_qty,
        po_receipt AS po_receipt,
        ARTICLE_FULL AS ARTICLE_FULL,
        ARTICLE_SIZE AS ARTICLE_SIZE,
        ARTICLE_COLOR AS ARTICLE_COLOR,
        PARENT_ARTICLE_NO AS PARENT_ARTICLE_NO,
        PARENT_ARTICLE_DESC AS PARENT_ARTICLE_DESC,
        ARTICLE_CAT_FULL AS ARTICLE_CAT_FULL,
        DEPT_FULL AS DEPT_FULL,
        LOB_FULL AS LOB_FULL,
        BRAND_NAME AS BRAND_NAME,
        VENDOR_FULL AS VENDOR_FULL,
        BUYER_FULL AS BUYER_FULL,
        PARENT_VENDOR_ID AS PARENT_VENDOR_ID,
        PARENT_VENDOR_NAME AS PARENT_VENDOR_NAME,
        SUM(order_qty) OVER (PARTITION BY YEAR_PERIOD, ARTICLE_NO) AS order_qty_sum
    FROM FLOW_PATH_FINAL_STACKED_W_ARTICLE)dku ;


<br>
--FLOW_PATH_FINAL_STACKED_W_ARTICLE_DC3PL
</br>
insert into FLOW_PATH_FINAL_STACKED_W_ARTICLE_DC3PL
SELECT DISTINCT
    flow_path.YEAR_PERIOD AS YEAR_PERIOD,
    flow_path.MERCH_YEAR AS MERCH_YEAR,
    flow_path.ARTICLE_NO AS ARTICLE_NO,
    flow_path.VENDOR_ID AS VENDOR_ID,
    flow_path.flow_path AS flow_path,
    flow_path.order_qty AS order_qty,
    flow_path.domestic_inbound AS domestic_inbound,
    flow_path.ocean_freight AS ocean_freight,
    flow_path.domestic_outbound AS domestic_outbound,
    flow_path.agency_fee_rate AS agency_fee_rate,
    flow_path.duty_rate AS duty_rate,
    flow_path.SALES_QTY AS SALES_QTY,
    flow_path.SALES_AMT AS SALES_AMT,
    flow_path.COST_AMT AS COST_AMT,
    flow_path.cogs_per_unit AS cogs_per_unit,
    flow_path.agency_fee_per_unit AS agency_fee_per_unit,
    flow_path.duty_per_unit AS duty_per_unit,
    flow_path.margin AS margin,
    flow_path.total_shrink AS total_shrink,
    flow_path.shrink_per_unit AS shrink_per_unit,
    flow_path.total_funding AS total_funding,
    flow_path.funding_per_unit AS funding_per_unit,
    flow_path.freight_other_charges AS freight_other_charges,
    flow_path.po_order_qty AS po_order_qty,
    flow_path.po_receipt AS po_receipt,
    flow_path.ARTICLE_FULL AS ARTICLE_FULL,
    flow_path.ARTICLE_SIZE AS ARTICLE_SIZE,
    flow_path.ARTICLE_COLOR AS ARTICLE_COLOR,
    flow_path.PARENT_ARTICLE_NO AS PARENT_ARTICLE_NO,
    flow_path.PARENT_ARTICLE_DESC AS PARENT_ARTICLE_DESC,
    flow_path.ARTICLE_CAT_FULL AS ARTICLE_CAT_FULL,
    flow_path.DEPT_FULL AS DEPT_FULL,
    flow_path.LOB_FULL AS LOB_FULL,
    flow_path.BRAND_NAME AS BRAND_NAME,
    flow_path.VENDOR_FULL AS VENDOR_FULL,
    flow_path.BUYER_FULL AS BUYER_FULL,
    flow_path.PARENT_VENDOR_ID AS PARENT_VENDOR_ID,
    flow_path.PARENT_VENDOR_NAME AS PARENT_VENDOR_NAME,
    flow_path.order_qty_sum AS order_qty_sum,
    dc_3pl."3PL_TOTAL_VARIABLE_COST_ALLOCATED" AS "3PL_TOTAL_VARIABLE_COST_ALLOCATED",
    dc_3pl."3PL_TOTAL_FIXED_COST_ALLOCATED" AS "3PL_TOTAL_FIXED_COST_ALLOCATED",
    dc_3pl.DC_total_variable_cost_allocated AS DC_total_variable_cost_allocated,
    dc_3pl.DC_total_fixed_cost_allocated AS DC_total_fixed_cost_allocated
  FROM FLOW_PATH_FINAL_STACKED_W_ARTICLE_PREPARED flow_path
  LEFT JOIN (
    SELECT dc_3pl.*
      FROM DC_3PL_ALLOCATION_COST_UNION dc_3pl
      WHERE channel = 'Store'
    ) dc_3pl
    ON (flow_path.YEAR_PERIOD = dc_3pl.YEAR_PERIOD)
      AND (flow_path.ARTICLE_NO = dc_3pl.ARTICLE_NO)
      AND (flow_path.VENDOR_ID = dc_3pl.VENDOR_ID)
      AND (flow_path.MERCH_YEAR = dc_3pl.MERCH_YEAR);

<br> </br>
TRUNCATE TABLE FLOW_PATH_W_DC3PL;

<br>
--FLOW_PATH_FINAL_STACKED_W_ARTICLE_DC3PL
</br>
insert into FLOW_PATH_W_DC3PL
select DISTINCT
   YEAR_PERIOD,
   MERCH_YEAR,
   ARTICLE_NO,
   VENDOR_ID,
   flow_path,
   order_qty,
   domestic_inbound,
   ocean_freight,
   domestic_outbound,
   agency_fee_rate,
   duty_rate,
   SALES_QTY,
   SALES_AMT,
   COST_AMT,
   cogs_per_unit,
   agency_fee_per_unit,
   duty_per_unit,
   margin,
   total_shrink,
   shrink_per_unit,
   total_funding,
   funding_per_unit,
   freight_other_charges,
   po_order_qty,
   po_receipt,
   ARTICLE_FULL,
   ARTICLE_SIZE,
   ARTICLE_COLOR,
   PARENT_ARTICLE_NO,
   PARENT_ARTICLE_DESC,
   ARTICLE_CAT_FULL,
   DEPT_FULL,
   LOB_FULL,
   BRAND_NAME,
   VENDOR_FULL,
   BUYER_FULL,
   PARENT_VENDOR_ID,
   PARENT_VENDOR_NAME,
   order_qty_sum,
   "3PL_TOTAL_VARIABLE_COST_ALLOCATED" as "DC3PL_TOTAL_VARIABLE_COST_ALLOCATED",
   "3PL_TOTAL_FIXED_COST_ALLOCATED" AS "DC3PL_TOTAL_FIXED_COST_ALLOCATED",
   DC_total_variable_cost_allocated,
   DC_total_fixed_cost_allocated
FROM FLOW_PATH_FINAL_STACKED_W_ARTICLE_DC3PL;

<br>
--allocation_v2_sf_grouped_v
</br>
insert into allocation_v2_sf_grouped_v
SELECT DISTINCT
  MERCH_YEAR AS MERCH_YEAR,
  STORE_NO AS STORE_NO,
  YEAR_PERIOD AS YEAR_PERIOD,
  ARTICLE_NO AS ARTICLE_NO,
  VENDOR_ID AS VENDOR_ID,
  'Store' AS channel,
  NULL AS total_variable_cost_allocated,
  SUM(total_fixed_cost_allocated) AS total_fixed_cost_allocated
FROM (
          SELECT 
             MERCH_YEAR,
             STORE_NO,
             YEAR_PERIOD,
              ARTICLE_NO,
              VENDOR_ID,
              VOLUME,
              dc_3pl_pandl_fixed_cost_final,
              dc_3pl_pandl_fixed_cost_final_l2,
              COALESCE(dc_3pl_pandl_fixed_cost_final, 0) + COALESCE(dc_3pl_pandl_fixed_cost_final_l2, 0) AS total_fixed_cost_allocated
           FROM 
               (
              SELECT 
                MERCH_YEAR AS MERCH_YEAR,
                STORE_NO AS STORE_NO,
                YEAR_PERIOD AS YEAR_PERIOD,
                ARTICLE_NO AS ARTICLE_NO,
                "VENDOR_ID" AS VENDOR_ID,
                "VOLUME" AS VOLUME,
                CASE WHEN  dc_3pl_pandl_fixed_cost_final IS NULL  OR TRIM(dc_3pl_pandl_fixed_cost_final) = ''THEN 0  ELSE dc_3pl_pandl_fixed_cost_final END AS 
                dc_3pl_pandl_fixed_cost_final,
                CASE WHEN  dc_3pl_pandl_fixed_cost_final_l2 IS NULL OR TRIM(dc_3pl_pandl_fixed_cost_final_l2)='' THEN  0 ELSE dc_3pl_pandl_fixed_cost_final_l2 
                END AS dc_3pl_pandl_fixed_cost_final_l2
              FROM ALLOCATION_V2)ALLOCATION_V2
     ) "dku"
GROUP BY 1,2,3,4,5,6;

<br>
--ALLOCATION_V2_SF_GROUPED_3PL
</br>
insert into ALLOCATION_V2_SF_GROUPED_3PL
select DISTINCT
    MERCH_YEAR,
    STORE_NO,
    YEAR_PERIOD,
    ARTICLE_NO,
    VENDOR_ID,
    channel,
    total_variable_cost_allocated,
    total_fixed_cost_allocated
FROM allocation_v2_sf_grouped_v
where STORE_NO= 770
or (STORE_NO >= 860 AND STORE_NO <= 872)
OR (STORE_NO <= 867 AND STORE_NO >=851);

<br>
--ALLOCATION_V2_SF_GROUPED_3PL_BY_STORE_NO
</br>
INSERT INTO ALLOCATION_V2_SF_GROUPED_3PL_BY_STORE_NO
SELECT 
     STORE_NO AS STORE_NO
FROM ALLOCATION_V2_SF_GROUPED_3PL
GROUP BY STORE_NO;

<br>
--DC_3PL_Location_Mapping_channel
</br>
insert into DC_3PL_Location_Mapping_channel
SELECT DISTINCT
    Facility_Code as Facility_Code,
    Facility as Facility ,
    DC_Hierchy AS DC_Hierchy,
    DC_3PL as DC_3PL,
    CASE
        WHEN Facility LIKE '%Omni%' THEN 'Omni'
        ELSE 'Store'
    END AS channel
FROM DC_3PL_Location_Mapping_prepared;

<br>
--ALLOCATION_V2_SF_GROUPED_V_DC
</br>
create or replace temp table ALLOCATION_V2_SF_GROUPED_V_DC as 
SELECT DISTINCT
    MERCH_YEAR AS MERCH_YEAR,
    STORE_NO AS STORE_NO,
    YEAR_PERIOD AS YEAR_PERIOD,
    ARTICLE_NO AS ARTICLE_NO,
    VENDOR_ID AS VENDOR_ID,
    total_fixed_cost_allocated AS total_fixed_cost_allocated,
    dc3pl_STORE_NO AS dc3pl_STORE_NO,
    channel AS channel,
    CASE WHEN dc3pl_STORE_NO IS NULL THEN 1 ELSE 0 END AS DC_flag
FROM (
    SELECT 
        allocation_v2_sf_grouped_v.MERCH_YEAR AS MERCH_YEAR,
        allocation_v2_sf_grouped_v.STORE_NO AS STORE_NO,
        allocation_v2_sf_grouped_v.YEAR_PERIOD AS YEAR_PERIOD,
        allocation_v2_sf_grouped_v.ARTICLE_NO AS ARTICLE_NO,
        allocation_v2_sf_grouped_v.VENDOR_ID AS VENDOR_ID,
        allocation_v2_sf_grouped_v.total_fixed_cost_allocated AS total_fixed_cost_allocated,
        allocation_v2_sf_grouped_3PL_by_STORE_NO.STORE_NO AS dc3pl_STORE_NO,
        DC_3PL_Location_Mapping_channel.channel AS channel
      FROM ALLOCATION_V2_SF_GROUPED_V allocation_v2_sf_grouped_v
      LEFT JOIN ALLOCATION_V2_SF_GROUPED_3PL_BY_STORE_NO allocation_v2_sf_grouped_3PL_by_STORE_NO
        ON allocation_v2_sf_grouped_v.STORE_NO = allocation_v2_sf_grouped_3PL_by_STORE_NO.STORE_NO
      LEFT JOIN DC_3PL_LOCATION_MAPPING_CHANNEL DC_3PL_Location_Mapping_channel
        ON allocation_v2_sf_grouped_v.STORE_NO = DC_3PL_Location_Mapping_channel.Facility_Code
    ) withoutcomputedcols_query;

<br>
--ALLOCATION_FIXED_COST_SF_GROUPED_V_DC_PREPARED
</br>
insert into ALLOCATION_FIXED_COST_SF_GROUPED_V_DC_PREPARED
SELECT DISTINCT
  MERCH_YEAR AS MERCH_YEAR,
  YEAR_PERIOD AS YEAR_PERIOD,
  ARTICLE_NO AS ARTICLE_NO,
  VENDOR_ID AS VENDOR_ID,
  DC_flag AS DC_flag,
  channel AS channel,
  sum(total_fixed_cost_allocated) AS DC_total_fixed_cost_allocated
FROM (
  SELECT 
      MERCH_YEAR,
      STORE_NO,
      YEAR_PERIOD,
      ARTICLE_NO,
      VENDOR_ID,
      total_fixed_cost_allocated,
      dc3pl_STORE_NO,
      channel,
      DC_flag
    FROM ALLOCATION_V2_SF_GROUPED_V_DC
    WHERE (DC_flag = 1) AND (channel = 'Store')
  ) dku__beforegrouping
GROUP BY MERCH_YEAR, YEAR_PERIOD, ARTICLE_NO, VENDOR_ID, DC_flag, channel;

<br>
--ALLOCATION_DC_3PL_OMNI_FULFILLMENT_SF
</br>
CREATE OR REPLACE TEMP TABLE ALLOCATION_DC_3PL_OMNI_FULFILLMENT_SF
AS 
SELECT DISTINCT
    oms_order_line_key AS OMS_ORDER_LINE_KEY,
    demand_sales AS DEMAND_SALES,
    shipping_revenue AS SHIPPING_REVENUE,
    year_week AS YEAR_WEEK,
    dc_3pl_pnl_omni_fulfillment_fixed_l2 as dc_3pl_pnl_omni_fulfillment_fixed_l2,
    demand_qty AS DEMAND_QTY,
    oms_order_no AS OMS_ORDER_NO,
    oms_item_primary_vendor_id AS OMS_ITEM_PRIMARY_VENDOR_ID,
    booked_sales_qty AS BOOKED_SALES_QTY,
    oms_order_line_schedule_ship_node AS OMS_ORDER_LINE_SCHEDULE_SHIP_NODE,
    article_cat_full AS ARTICLE_CAT_FULL,
    booked_cost_amt AS BOOKED_COST_AMT,
    dc_3pl_pnl_omni_fulfillment_fixed as dc_3pl_pnl_omni_fulfillment_fixed,
    store_no AS STORE_NO,
    acct_year AS ACCT_YEAR,
    actual_shipping_revenue AS ACTUAL_SHIPPING_REVENUE,
    ship_node_type AS SHIP_NODE_TYPE,
    channel as channel,
    dc_3pl_pnl_omni_fulfillment_variable as dc_3pl_pnl_omni_fulfillment_variable,
    booked_sales_amt AS BOOKED_SALES_AMT,
    oms_item_id AS OMS_ITEM_ID,
    dc_3pl_pnl_omni_fulfillment_variable_l2 as dc_3pl_pnl_omni_fulfillment_variable_l2,
    merch_year AS MERCH_YEAR,
    oms_ship_node_desc AS OMS_SHIP_NODE_DESC,
    shipping_disc AS SHIPPING_DISC,
    year_period AS YEAR_PERIOD,
    order_line_type AS ORDER_LINE_TYPE,
    scac AS SCAC
FROM  ALLOCATION_DC_3PL_OMNI_FULFILLMENT_STEP1_SF ;

<br>
--ALLOCATION_DC_3PL_OMNI_FULFILLMENT_GRP
</br>
INSERT INTO ALLOCATION_DC_3PL_OMNI_FULFILLMENT_GRP
WITH ALLOCATION_DC_3PL_OMNI_FULFILLMENT_SF_PREPARED AS 
(
SELECT 
    YEAR_WEEK AS YEAR_WEEK,
    ACCT_YEAR AS ACCT_YEAR,
    OMS_ORDER_NO AS OMS_ORDER_NO,
    dc_3pl_pnl_omni_fulfillment_variable_l2 AS dc_3pl_pnl_omni_fulfillment_variable_l2,
    DC3PL_omni_variable_cost AS DC3PL_omni_variable_cost,
    OMS_ORDER_LINE_KEY AS OMS_ORDER_LINE_KEY,
    SHIPPING_DISC AS SHIPPING_DISC,
    BOOKED_COST_AMT AS BOOKED_COST_AMT,
    dc_3pl_pnl_omni_fulfillment_fixed AS dc_3pl_pnl_omni_fulfillment_fixed,
    dc_3pl_pnl_omni_fulfillment_fixed_l2 AS dc_3pl_pnl_omni_fulfillment_fixed_l2,
    DC3PL_omni_fixed_cost AS DC3PL_omni_fixed_cost,
    SHIP_NODE_TYPE AS SHIP_NODE_TYPE,
    BOOKED_SALES_QTY AS BOOKED_SALES_QTY,
    SCAC AS SCAC,
    channel AS channel,
    OMS_ITEM_PRIMARY_VENDOR_ID AS OMS_ITEM_PRIMARY_VENDOR_ID,
    STORE_NO AS STORE_NO,
    ACTUAL_SHIPPING_REVENUE AS ACTUAL_SHIPPING_REVENUE,
    OMS_SHIP_NODE_DESC AS OMS_SHIP_NODE_DESC,
    ORDER_LINE_TYPE AS ORDER_LINE_TYPE,
    MERCH_YEAR AS MERCH_YEAR,
    ARTICLE_CAT_FULL AS ARTICLE_CAT_FULL,
    OMS_ORDER_LINE_SCHEDULE_SHIP_NODE AS OMS_ORDER_LINE_SCHEDULE_SHIP_NODE,
    YEAR_PERIOD AS YEAR_PERIOD,
    dc_3pl_pnl_omni_fulfillment_variable AS dc_3pl_pnl_omni_fulfillment_variable,
    DEMAND_QTY AS DEMAND_QTY,
    OMS_ITEM_ID AS OMS_ITEM_ID,
    BOOKED_SALES_AMT AS BOOKED_SALES_AMT,
    SHIPPING_REVENUE AS SHIPPING_REVENUE,
    DEMAND_SALES AS DEMAND_SALES
  FROM (
    SELECT 
        YEAR_WEEK,
        ACCT_YEAR,
        OMS_ORDER_NO,
        dc_3pl_pnl_omni_fulfillment_variable_l2,
        OMS_ORDER_LINE_KEY,
        SHIPPING_DISC,
        BOOKED_COST_AMT,
        dc_3pl_pnl_omni_fulfillment_fixed,
        dc_3pl_pnl_omni_fulfillment_fixed_l2,
        SHIP_NODE_TYPE,
        BOOKED_SALES_QTY,
        SCAC,
        channel AS channel_old,
        OMS_ITEM_PRIMARY_VENDOR_ID,
        STORE_NO,
        ACTUAL_SHIPPING_REVENUE,
        OMS_SHIP_NODE_DESC,
        ORDER_LINE_TYPE,
        MERCH_YEAR,
        ARTICLE_CAT_FULL,
        OMS_ORDER_LINE_SCHEDULE_SHIP_NODE,
        YEAR_PERIOD,
        dc_3pl_pnl_omni_fulfillment_variable,
        DEMAND_QTY,
        OMS_ITEM_ID,
        BOOKED_SALES_AMT,
        SHIPPING_REVENUE,
        DEMAND_SALES,
        COALESCE(dc_3pl_pnl_omni_fulfillment_variable, 0) + COALESCE(dc_3pl_pnl_omni_fulfillment_variable_l2, 0) AS DC3PL_omni_variable_cost,
        COALESCE(dc_3pl_pnl_omni_fulfillment_fixed, 0) + COALESCE(dc_3pl_pnl_omni_fulfillment_fixed_l2, 0) AS DC3PL_omni_fixed_cost,
        CASE WHEN (ORDER_LINE_TYPE = 'Return') OR (SCAC = 'Return') THEN 'Omni-Return' ELSE 'Omni-Shipped' END AS channel
      FROM ALLOCATION_DC_3PL_OMNI_FULFILLMENT_SF __input_table
      WHERE NOT (ORDER_LINE_TYPE IS NOT NULL AND (ORDER_LINE_TYPE = 'BOPIS') OR ORDER_LINE_TYPE IS NOT NULL AND (ORDER_LINE_TYPE = 'BODFS')) OR ((ORDER_LINE_TYPE IS NOT NULL AND (ORDER_LINE_TYPE = 'BOPIS') OR ORDER_LINE_TYPE IS NOT NULL AND (ORDER_LINE_TYPE = 'BODFS')) IS NULL)
    ) A
    )
    SELECT DISTINCT
    MERCH_YEAR AS MERCH_YEAR,
    STORE_NO AS STORE_NO,
    YEAR_PERIOD AS YEAR_PERIOD,
    channel AS channel,
    OMS_ITEM_ID AS OMS_ITEM_ID,
    OMS_ITEM_PRIMARY_VENDOR_ID AS OMS_ITEM_PRIMARY_VENDOR_ID,
    BOOKED_SALES_QTY_sum AS BOOKED_SALES_QTY,
    DC3PL_omni_fixed_cost_sum AS DC3PL_omni_fixed_cost_sum,
    DC3PL_omni_variable_cost_sum AS DC3PL_omni_variable_cost_sum
  FROM (
    SELECT 
        MERCH_YEAR AS MERCH_YEAR,
        STORE_NO AS STORE_NO,
        YEAR_PERIOD AS YEAR_PERIOD,
        channel AS channel,
        OMS_ITEM_ID AS OMS_ITEM_ID,
        OMS_ITEM_PRIMARY_VENDOR_ID AS OMS_ITEM_PRIMARY_VENDOR_ID,
        SUM(BOOKED_SALES_QTY) AS BOOKED_SALES_QTY_sum,
        SUM(DC3PL_omni_fixed_cost) AS DC3PL_omni_fixed_cost_sum,
        SUM(DC3PL_omni_variable_cost) AS DC3PL_omni_variable_cost_sum
      FROM (
        SELECT DISTINCT 
            MERCH_YEAR AS MERCH_YEAR,
            STORE_NO AS STORE_NO,
            YEAR_PERIOD AS YEAR_PERIOD,
            ACCT_YEAR AS ACCT_YEAR,
            YEAR_WEEK AS YEAR_WEEK,
            OMS_ORDER_NO AS OMS_ORDER_NO,
            OMS_ITEM_ID AS OMS_ITEM_ID,
            OMS_ORDER_LINE_KEY AS OMS_ORDER_LINE_KEY,
            OMS_ITEM_PRIMARY_VENDOR_ID AS OMS_ITEM_PRIMARY_VENDOR_ID,
            OMS_ORDER_LINE_SCHEDULE_SHIP_NODE AS OMS_ORDER_LINE_SCHEDULE_SHIP_NODE,
            OMS_SHIP_NODE_DESC AS OMS_SHIP_NODE_DESC,
            ARTICLE_CAT_FULL AS ARTICLE_CAT_FULL,
            DEMAND_QTY AS DEMAND_QTY,
            DEMAND_SALES AS DEMAND_SALES,
            BOOKED_SALES_QTY AS BOOKED_SALES_QTY,
            BOOKED_SALES_AMT AS BOOKED_SALES_AMT,
            BOOKED_COST_AMT AS BOOKED_COST_AMT,
            SHIPPING_REVENUE AS SHIPPING_REVENUE,
            SHIPPING_DISC AS SHIPPING_DISC,
            ACTUAL_SHIPPING_REVENUE AS ACTUAL_SHIPPING_REVENUE,
            ORDER_LINE_TYPE AS ORDER_LINE_TYPE,
            SCAC AS SCAC,
            channel AS channel,
            SHIP_NODE_TYPE AS SHIP_NODE_TYPE,
            dc_3pl_pnl_omni_fulfillment_fixed AS dc_3pl_pnl_omni_fulfillment_fixed,
            dc_3pl_pnl_omni_fulfillment_variable AS dc_3pl_pnl_omni_fulfillment_variable,
            dc_3pl_pnl_omni_fulfillment_fixed_l2 AS dc_3pl_pnl_omni_fulfillment_fixed_l2,
            DC3PL_omni_fixed_cost AS DC3PL_omni_fixed_cost,
            dc_3pl_pnl_omni_fulfillment_variable_l2 AS dc_3pl_pnl_omni_fulfillment_variable_l2,
            DC3PL_omni_variable_cost AS DC3PL_omni_variable_cost
          FROM ALLOCATION_DC_3PL_OMNI_FULFILLMENT_SF_PREPARED
        ) WITHOUT_GROUPING 
      GROUP BY MERCH_YEAR, STORE_NO, YEAR_PERIOD, channel, OMS_ITEM_ID, OMS_ITEM_PRIMARY_VENDOR_ID
    ) FINAL ;

<br>
--ALLOCATION_V3_SF_GROUPED_grp
</br>
create or replace temp table  ALLOCATION_V3_SF_GROUPED_grp as
SELECT DISTINCT
    MERCH_YEAR AS MERCH_YEAR,
    YEAR_PERIOD AS YEAR_PERIOD,
    ARTICLE_NO AS ARTICLE_NO,
    VENDOR_ID AS VENDOR_ID,
    channel AS channel,
    DC_3PL AS DC_3PL,
    SUM(total_variable_cost_allocated) AS total_variable_cost_allocated
  FROM (
    SELECT 
      "allocation_v3_sf_grouped"."MERCH_YEAR" AS MERCH_YEAR,
      "allocation_v3_sf_grouped"."STORE_NO" AS STORE_NO,
      "allocation_v3_sf_grouped"."YEAR_PERIOD" AS YEAR_PERIOD,
      "allocation_v3_sf_grouped"."ARTICLE_NO" AS ARTICLE_NO,
      "allocation_v3_sf_grouped"."VENDOR_ID" AS VENDOR_ID,
      "allocation_v3_sf_grouped".total_variable_cost_allocated AS total_variable_cost_allocated,
      "DC_3PL_Location_Mapping_channel".channel AS channel,
      "DC_3PL_Location_Mapping_channel"."DC_3PL" AS DC_3PL
    FROM ALLOCATION_V3_SF_GROUPED "allocation_v3_sf_grouped"
    LEFT JOIN DC_3PL_LOCATION_MAPPING_CHANNEL "DC_3PL_Location_Mapping_channel"
      ON "allocation_v3_sf_grouped"."STORE_NO" = "DC_3PL_Location_Mapping_channel".Facility_Code
    )dku
  GROUP BY MERCH_YEAR, YEAR_PERIOD, ARTICLE_NO, VENDOR_ID, channel, DC_3PL;

<br>
--ALLOCATION_V3_SF_GROUPED_GRP_PREPARED
</br>
insert into ALLOCATION_V3_SF_GROUPED_GRP_PREPARED
SELECT DISTINCT
    MERCH_YEAR,
    YEAR_PERIOD,
    ARTICLE_NO,
    VENDOR_ID,
    total_variable_cost_allocated,
    channel,
    DC_3PL,
    CASE WHEN DC_3PL = 'DC' THEN total_variable_cost_allocated ELSE 0 END AS DC_total_variable_cost_allocated,
    CASE WHEN DC_3PL != 'DC' THEN total_variable_cost_allocated ELSE 0 END AS dc3PL_total_variable_cost_allocated
FROM ALLOCATION_V3_SF_GROUPED_grp;

<br>
--ALLOCATION_VARIABLE_STACKED
</br>
insert into ALLOCATION_VARIABLE_STACKED
SELECT DISTINCT
    MERCH_YEAR AS MERCH_YEAR,
    YEAR_PERIOD AS YEAR_PERIOD,
    ARTICLE_NO AS ARTICLE_NO,
    CAST(VENDOR_ID AS VARCHAR) AS VENDOR_ID,
    channel AS channel,
    DC_3PL AS DC_3PL,
    dc3PL_total_variable_cost_allocated AS dc3PL_total_variable_cost_allocated,
    DC_total_variable_cost_allocated AS DC_total_variable_cost_allocated
  FROM ALLOCATION_V3_SF_GROUPED_GRP_PREPARED
UNION ALL
SELECT DISTINCT
    MERCH_YEAR AS MERCH_YEAR,
    YEAR_PERIOD AS YEAR_PERIOD,
    OMS_ITEM_ID AS ARTICLE_NO,
    OMS_ITEM_PRIMARY_VENDOR_ID AS VENDOR_ID,
    channel AS channel,
    NULL AS DC_3PL,
    NULL AS dc3PL_total_variable_cost_allocated,
    DC3PL_omni_variable_cost_sum AS DC_total_variable_cost_allocated
FROM ALLOCATION_DC_3PL_OMNI_FULFILLMENT_GRP;

<br>
--ALLOCATION_VARIABLE_STACKED_GRP
</br>
insert into ALLOCATION_VARIABLE_STACKED_GRP
SELECT DISTINCT
    MERCH_YEAR AS MERCH_YEAR,
    YEAR_PERIOD AS YEAR_PERIOD,
    ARTICLE_NO AS ARTICLE_NO,
    VENDOR_ID,
    channel AS channel,
    SUM(dc3PL_total_variable_cost_allocated) AS dc3PL_total_variable_cost_allocated_sum,
    SUM(DC_total_variable_cost_allocated) AS DC_total_variable_cost_allocated_sum
FROM ALLOCATION_VARIABLE_STACKED
GROUP BY YEAR_PERIOD, MERCH_YEAR, ARTICLE_NO, VENDOR_ID, channel;

<br>
--ALLOCATION_FIXED_COST_SF_GROUPED_V_DC_PREPARED_STACKED
</br>
INSERT INTO ALLOCATION_FIXED_COST_SF_GROUPED_V_DC_PREPARED_STACKED
SELECT DISTINCT
    MERCH_YEAR AS MERCH_YEAR,
    YEAR_PERIOD AS YEAR_PERIOD,
    ARTICLE_NO AS ARTICLE_NO,
    CAST(VENDOR_ID AS VARCHAR) AS VENDOR_ID,
    DC_flag AS DC_flag,
    channel AS channel,
    DC_total_fixed_cost_allocated AS DC_total_fixed_cost_allocated
FROM ALLOCATION_FIXED_COST_SF_GROUPED_V_DC_PREPARED
UNION ALL
SELECT DISTINCT
    MERCH_YEAR AS MERCH_YEAR,
    YEAR_PERIOD AS YEAR_PERIOD,
    OMS_ITEM_ID AS ARTICLE_NO,
    OMS_ITEM_PRIMARY_VENDOR_ID AS VENDOR_ID,
    NULL AS DC_flag,
    channel AS channel,
    DC3PL_omni_fixed_cost_sum AS DC_total_fixed_cost_allocated
FROM ALLOCATION_DC_3PL_OMNI_FULFILLMENT_GRP;

<br>
--ALLOCATION_FIXED_COST_SF_GROUPED_V_DC_PREPARED_STACKED_GRP
</br>
INSERT INTO ALLOCATION_FIXED_COST_SF_GROUPED_V_DC_PREPARED_STACKED_GRP
SELECT DISTINCT
    MERCH_YEAR AS MERCH_YEAR,
    YEAR_PERIOD AS YEAR_PERIOD,
    ARTICLE_NO AS ARTICLE_NO,
    VENDOR_ID AS VENDOR_ID,
    channel AS channel,
    SUM(DC_total_fixed_cost_allocated) AS DC_total_fixed_cost_allocated
FROM ALLOCATION_FIXED_COST_SF_GROUPED_V_DC_PREPARED_STACKED
GROUP BY YEAR_PERIOD, MERCH_YEAR, ARTICLE_NO, VENDOR_ID, channel;

<br>
--ALLOCATION_FIXED_COST_SF_GROUPED_V_3PL_PREPARED
</br>
INSERT INTO ALLOCATION_FIXED_COST_SF_GROUPED_V_3PL_PREPARED
SELECT DISTINCT
    MERCH_YEAR AS MERCH_YEAR,
    YEAR_PERIOD AS YEAR_PERIOD,
    ARTICLE_NO AS ARTICLE_NO,
    VENDOR_ID AS VENDOR_ID,
    channel AS channel,
    SUM(DC3PL_total_variable_cost_allocated) AS DC3PL_total_variable_cost_allocated,
    SUM(DC3PL_total_fixed_cost_allocated) AS DC3PL_total_fixed_cost_allocated   
FROM (SELECT 
            MERCH_YEAR,
            STORE_NO,
            YEAR_PERIOD,
            ARTICLE_NO,
            VENDOR_ID,
            total_variable_cost_allocated,
            total_fixed_cost_allocated, 
            COALESCE(total_fixed_cost_allocated / 2, 0) AS DC3PL_total_fixed_cost_allocated,
            COALESCE(total_fixed_cost_allocated / 2, 0) AS DC3PL_total_variable_cost_allocated,
            'Store' AS channel
        FROM ALLOCATION_V2_SF_GROUPED_3PL)DKU
GROUP BY MERCH_YEAR, YEAR_PERIOD, ARTICLE_NO, VENDOR_ID, channel;


<br>
--VISUAL_TABLES_GRP_JOINED
</br>
INSERT INTO VISUAL_TABLES_GRP_JOINED
SELECT DISTINCT
    ARTICLE_NO AS ARTICLE_NO,
    VENDOR_ID AS VENDOR_ID,
    YEAR_PERIOD AS YEAR_PERIOD,
    ARTICLE_DESC AS ARTICLE_DESC,
    ARTICLE_SIZE AS ARTICLE_SIZE,
    MAJ_DEPT_DESC AS MAJ_DEPT_DESC,
    BRAND_NAME AS BRAND_NAME,
    BUYER_CODE AS BUYER_CODE,
    ARTICLE_COLOR AS ARTICLE_COLOR,
    PARENT_ARTICLE_NO AS PARENT_ARTICLE_NO,
    PARENT_ARTICLE_DESC AS PARENT_ARTICLE_DESC,
    ARTICLE_FULL AS ARTICLE_FULL,
    BUYER_NAME AS BUYER_NAME,
    ARTICLE_CAT_NAME AS ARTICLE_CAT_NAME,
    LOB_DESC AS LOB_DESC,
    DEPT_NAME AS DEPT_NAME,
    VENDOR_NAME AS VENDOR_NAME,
    PARENT_VENDOR_NAME AS PARENT_VENDOR_NAME,
    channel AS channel,
    SALES_QTY AS SALES_QTY,
    SALES_AMT AS SALES_AMT,
    COST_AMT AS COST_AMT,
    margin AS margin,
    unsellables AS unsellables,
    defectives_shrink AS defectives_shrink,
    dc_shrink AS dc_shrink,
    theft AS theft,
    defective_allowance AS defective_allowance,
    battery_core AS battery_core,
    mixing_center AS mixing_center,
    new_store_discounts AS new_store_discounts,
    cash_discounts AS cash_discounts,
    pop AS pop,
    pdq AS pdq,
    marketing AS marketing,
    vendor_compliance AS vendor_compliance,
    booth_rental AS booth_rental,
    funding_outside_lm AS funding_outside_lm,
    inventory_shrink AS inventory_shrink,
    total_shrink AS total_shrink,
    ocean_freight AS ocean_freight,
    freight_cost AS freight_cost,
    agency_fee AS agency_fee,
    duty_tarrifs AS duty_tarrifs,
    domestic_inbound AS domestic_inbound,
    total_freight AS total_freight,
    domestic_outbound AS domestic_outbound,
    freight_other_charges AS freight_other_charges,
    volume_rebate AS volume_rebate,
    price_cuts AS price_cuts,
    scanbacks AS scanbacks,
    markdowns AS markdowns,
    landed_margin AS landed_margin,
    funding_in_lm AS funding_in_lm,
    total_funding AS total_funding,
    vendor_support AS vendor_support,
    vendor_support_fast AS vendor_support_fast,
    DC3PL_total_variable_cost_allocated_sum AS DC3PL_total_variable_cost_allocated_sum,
    DC_total_variable_cost_allocated_sum AS DC_total_variable_cost_allocated_sum,
    DC_total_fixed_cost_allocated AS DC_total_fixed_cost_allocated,
    DC3PL_total_variable_cost_allocated AS DC3PL_total_variable_cost_allocated,
    DC3PL_total_fixed_cost_allocated AS DC3PL_total_fixed_cost_allocated,
    COALESCE(DC3PL_total_variable_cost_allocated_sum, 0) + COALESCE(DC3PL_total_variable_cost_allocated, 0) AS Total_3PL_total_variable_cost_allocated
FROM (
    SELECT 
        VISUAL."ARTICLE_NO" AS ARTICLE_NO,
        VISUAL."VENDOR_ID" AS VENDOR_ID,
        VISUAL."YEAR_PERIOD" AS YEAR_PERIOD,
        VISUAL."ARTICLE_DESC" AS ARTICLE_DESC,
        VISUAL."ARTICLE_SIZE" AS ARTICLE_SIZE,
        VISUAL."MAJ_DEPT_DESC" AS MAJ_DEPT_DESC,
        VISUAL."BRAND_NAME" AS BRAND_NAME,
        VISUAL."BUYER_CODE" AS BUYER_CODE,
        VISUAL."ARTICLE_COLOR" AS ARTICLE_COLOR,
        VISUAL."PARENT_ARTICLE_NO" AS PARENT_ARTICLE_NO,
        VISUAL."PARENT_ARTICLE_DESC" AS PARENT_ARTICLE_DESC,
        VISUAL."ARTICLE_FULL" AS ARTICLE_FULL,
        VISUAL."BUYER_NAME" AS BUYER_NAME,
        VISUAL."ARTICLE_CAT_NAME" AS ARTICLE_CAT_NAME,
        VISUAL."LOB_DESC" AS LOB_DESC,
        VISUAL."DEPT_NAME" AS DEPT_NAME,
        VISUAL."VENDOR_NAME" AS VENDOR_NAME,
        VISUAL."PARENT_VENDOR_NAME" AS PARENT_VENDOR_NAME,
        VISUAL.channel AS channel,
        VISUAL.SALES_QTY AS SALES_QTY,
        VISUAL."SALES_AMT" AS SALES_AMT,
        VISUAL."COST_AMT" AS COST_AMT,
        VISUAL.margin AS margin,
        VISUAL.unsellables AS unsellables,
        VISUAL.defectives_shrink AS defectives_shrink,
        VISUAL.dc_shrink AS dc_shrink,
        VISUAL.theft AS theft,
        VISUAL.defective_allowance AS defective_allowance,
        VISUAL. battery_core AS battery_core,
        VISUAL. mixing_center AS mixing_center,
        VISUAL. new_store_discounts AS new_store_discounts,
        VISUAL. cash_discounts AS cash_discounts,
        VISUAL. pop AS pop,
        VISUAL. pdq AS pdq,
        VISUAL. marketing AS marketing,
        VISUAL. vendor_compliance AS vendor_compliance,
        VISUAL. booth_rental AS booth_rental,
        VISUAL. funding_outside_lm AS funding_outside_lm,
        VISUAL. inventory_shrink AS inventory_shrink,
        VISUAL. total_shrink AS total_shrink,
        VISUAL. ocean_freight AS ocean_freight,
        VISUAL. freight_cost AS freight_cost,
        VISUAL. agency_fee AS agency_fee,
        VISUAL. duty_tarrifs AS duty_tarrifs,
        VISUAL. domestic_inbound AS domestic_inbound,
        VISUAL. total_freight AS total_freight,
        VISUAL. domestic_outbound AS domestic_outbound,
        VISUAL. freight_other_charges AS freight_other_charges,
        VISUAL. volume_rebate AS volume_rebate,
        VISUAL. price_cuts AS price_cuts,
        VISUAL. scanbacks AS scanbacks,
        VISUAL. markdowns AS markdowns,
        VISUAL. landed_margin AS landed_margin,
        VISUAL. funding_in_lm AS funding_in_lm,
        VISUAL. total_funding AS total_funding,
        VISUAL. vendor_support AS vendor_support,
        VISUAL. vendor_support_fast AS vendor_support_fast,
        ALLO_VARI.DC3PL_total_variable_cost_allocated_sum AS DC3PL_total_variable_cost_allocated_sum,
        ALLO_VARI.DC_total_variable_cost_allocated_sum AS DC_total_variable_cost_allocated_sum,
        FIXED_GRP.DC_total_fixed_cost_allocated AS DC_total_fixed_cost_allocated,
        FIXED_PREP.DC3PL_total_variable_cost_allocated AS DC3PL_total_variable_cost_allocated,
        FIXED_PREP.DC3PL_total_fixed_cost_allocated AS DC3PL_total_fixed_cost_allocated
      FROM VISUAL_SALES_TABLE_SHARE VISUAL
      LEFT JOIN ALLOCATION_VARIABLE_STACKED_GRP ALLO_VARI
        ON (VISUAL."ARTICLE_NO" = ALLO_VARI.ARTICLE_NO)
          AND (VISUAL."MERCH_YEAR" = ALLO_VARI.MERCH_YEAR)
          AND (VISUAL."VENDOR_ID" = ALLO_VARI.VENDOR_ID)
          AND (VISUAL."YEAR_PERIOD" = ALLO_VARI.YEAR_PERIOD)
          AND (VISUAL.channel = ALLO_VARI.channel)
      LEFT JOIN ALLOCATION_FIXED_COST_SF_GROUPED_V_DC_PREPARED_STACKED_GRP FIXED_GRP
        ON (VISUAL."ARTICLE_NO" = FIXED_GRP.ARTICLE_NO)
          AND (VISUAL."VENDOR_ID" = FIXED_GRP.VENDOR_ID)
          AND (VISUAL."MERCH_YEAR" = FIXED_GRP.MERCH_YEAR)
          AND (VISUAL."YEAR_PERIOD" = FIXED_GRP.YEAR_PERIOD)
          AND (VISUAL.channel = FIXED_GRP.channel)
      LEFT JOIN ALLOCATION_FIXED_COST_SF_GROUPED_V_3PL_PREPARED FIXED_PREP
        ON (VISUAL."ARTICLE_NO" = FIXED_PREP.ARTICLE_NO)
          AND (VISUAL."MERCH_YEAR" = FIXED_PREP.MERCH_YEAR)
          AND (VISUAL."VENDOR_ID" = FIXED_PREP.VENDOR_ID)
          AND (VISUAL."YEAR_PERIOD" = FIXED_PREP.YEAR_PERIOD)
          AND (VISUAL.channel = FIXED_PREP.channel)
    ) "withoutcomputedcols_query";

<br>
--VISUAL_TABLES_GRP_JOINED_AGG
</br>
INSERT INTO VISUAL_TABLES_GRP_JOINED_AGG
SELECT DISTINCT
    YEAR_PERIOD,
    ARTICLE_NO,
    VENDOR_ID,
    ARTICLE_DESC,
    ARTICLE_SIZE,
    MAJ_DEPT_DESC,
    BRAND_NAME,
    BUYER_CODE,
    ARTICLE_COLOR,
    PARENT_ARTICLE_NO,
    PARENT_ARTICLE_DESC,
    ARTICLE_FULL,
    BUYER_NAME,
    ARTICLE_CAT_NAME,
    LOB_DESC,
    DEPT_NAME,
    VENDOR_NAME,
    PARENT_VENDOR_NAME,
    channel,
    SUM(SALES_QTY) AS SALES_QTY,
    SUM(SALES_AMT) AS SALES_AMT,
    SUM(COST_AMT) AS COST_AMT,
    SUM(margin) AS margin,
    SUM(unsellables) AS unsellables,
    SUM(defectives_shrink) AS defectives_shrink,
    SUM(dc_shrink) AS dc_shrink,
    SUM(theft) AS theft,
    SUM(defective_allowance) AS defective_allowance,
    SUM(battery_core) AS battery_core,
    SUM(mixing_center) AS mixing_center,
    SUM(new_store_discounts) AS new_store_discounts,
    SUM(cash_discounts) AS cash_discounts,
    SUM(pop) AS pop,
    SUM(pdq) AS pdq,
    SUM(marketing) AS marketing,
    SUM(vendor_compliance) AS vendor_compliance,
    SUM(booth_rental) AS booth_rental,
    SUM(funding_outside_lm) AS funding_outside_lm,
    SUM(inventory_shrink) AS inventory_shrink,
    SUM(total_shrink) AS total_shrink,
    SUM(ocean_freight) AS ocean_freight,
    SUM(freight_cost) AS freight_cost,
    SUM(agency_fee) AS agency_fee,
    SUM(duty_tarrifs) AS duty_tarrifs,
    SUM(domestic_inbound) AS domestic_inbound,
    SUM(total_freight) AS total_freight,
    SUM(domestic_outbound) AS domestic_outbound,
    SUM(freight_other_charges) AS freight_other_charges,
    SUM(volume_rebate) AS volume_rebate,
    SUM(price_cuts) AS price_cuts,
    SUM(scanbacks) AS scanbacks,
    SUM(markdowns) AS markdowns,
    SUM(landed_margin) AS landed_margin,
    SUM(funding_in_lm) AS funding_in_lm,
    SUM(total_funding) AS total_funding,
    SUM(vendor_support) AS vendor_support,
    SUM(vendor_support_fast) AS vendor_support_fast,
    SUM(DC_total_variable_cost_allocated_sum) AS DC_total_variable_cost_allocated,
    SUM(DC_total_fixed_cost_allocated) AS DC_total_fixed_cost_allocated,
    SUM(DC3PL_total_fixed_cost_allocated) AS DC3PL_total_fixed_cost_allocated,
    SUM(Total_3PL_total_variable_cost_allocated) AS DC3PL_total_variable_cost_allocated
FROM VISUAL_TABLES_GRP_JOINED
GROUP BY
    YEAR_PERIOD,
    ARTICLE_NO,
    VENDOR_ID,
    ARTICLE_DESC,
    ARTICLE_SIZE,
    MAJ_DEPT_DESC,
    BRAND_NAME,
    BUYER_CODE,
    ARTICLE_COLOR,
    PARENT_ARTICLE_NO,
    PARENT_ARTICLE_DESC,
    ARTICLE_FULL,
    BUYER_NAME,
    ARTICLE_CAT_NAME,
    LOB_DESC,
    DEPT_NAME,
    VENDOR_NAME,
    PARENT_VENDOR_NAME,
    channel;

<br>
--VISUAL_TABLES_GRP_AGGREGATED
</br>
INSERT INTO VISUAL_TABLES_GRP_AGGREGATED
WITH Base AS
(
  SELECT 
        TO_DATE(  CONCAT(  LEFT( cast(S.YEAR_PERIOD AS VARCHAR) , 4 ) , '-' , RIGHT( cast(S.YEAR_PERIOD AS VARCHAR) , 2 ) , '-01'  ) ) as Period
      , S.* 
  FROM 
      VISUAL_TABLES_GRP_JOINED_AGG S
)
SELECT DISTINCT
    Base.ARTICLE_NO
  , Base.VENDOR_ID
  , Base.YEAR_PERIOD
  , Base.ARTICLE_DESC
  , Base.ARTICLE_SIZE
  ,Base.ARTICLE_SIZE as ARTICLE_SIZE_1
  , Base.MAJ_DEPT_DESC
  , Base.BRAND_NAME
  , Base.BUYER_CODE
  , Base.ARTICLE_COLOR
  , Base.ARTICLE_COLOR as ARTICLE_COLOR_1
  , Base.PARENT_ARTICLE_NO
  , Base.PARENT_ARTICLE_DESC
  , Base.ARTICLE_FULL
  , Base.BUYER_NAME
  , Base.ARTICLE_CAT_NAME
  , Base.LOB_DESC
  , Base.DEPT_NAME
  , Base.VENDOR_NAME
  , Base.PARENT_VENDOR_NAME
  , Base.channel
  , Base.SALES_QTY
  , Base.SALES_AMT
  , Base.COST_AMT
  , Base.margin
  , Base.volume_rebate
  , Base.unsellables
  , Base.defectives_shrink
  , Base.dc_shrink
  , Base.price_cuts
  , Base.theft
  , Base.defective_allowance
  , Base.battery_core
  , Base.mixing_center
  , Base.scanbacks
  , Base.markdowns
  , Base.new_store_discounts
  , Base.cash_discounts
  , Base.pop
  , Base.pdq
  , Base.marketing
  , Base.vendor_compliance
  , Base.booth_rental
  , Base.funding_outside_lm
  , Base.inventory_shrink
  , Base.total_shrink
  , Base.ocean_freight
  , Base.freight_cost
  , Base.agency_fee
  , Base.duty_tarrifs
  , Base.funding_in_lm
  , Base.total_funding
  , Base.domestic_inbound
  , Base.landed_margin
  , Base.total_freight
  , Base.domestic_outbound
  , Base.freight_other_charges
  , Base.vendor_support
  , Base.vendor_support_fast
  , Base.DC_total_variable_cost_allocated
  , Base.DC_total_fixed_cost_allocated
  , Base.DC3PL_total_variable_cost_allocated
  , Base.DC3PL_total_fixed_cost_allocated  
  , SUM( Agg.DC_total_variable_cost_allocated   ) as DC_total_variable_cost_allocated_TTM
  , SUM( Agg.DC_total_fixed_cost_allocated      ) as DC_total_fixed_cost_allocated_TTM
  , SUM( Agg.DC3PL_total_fixed_cost_allocated     ) as DC3PL_total_fixed_cost_allocated_TTM
  , SUM( Agg.DC3PL_total_variable_cost_allocated  ) as DC3PL_total_variable_cost_allocated_TTM
  , SUM( Agg.sales_amt )       as sales_amt_TTM
FROM
    Base
    LEFT OUTER JOIN Base Agg ON            
            Base.Article_No         = Agg.Article_No
        AND ifnull(Base.BUYER_CODE,-11111)     = ifnull(Agg.BUYER_CODE,-11111)
        AND ifnull(Base.BUYER_NAME,'')         = ifnull(Agg.BUYER_NAME,'')
        AND ifnull(Base.ARTICLE_CAT_NAME,'')   = ifnull(Agg.ARTICLE_CAT_NAME,'')
        AND ifnull(Base.PARENT_VENDOR_NAME,'') = ifnull(Agg.PARENT_VENDOR_NAME   ,'')     
        AND ifnull(Base.Vendor_ID,-11111 )     = ifnull(Agg.Vendor_ID,-11111)
        AND ifnull(Base.LOB_DESC,'')           = ifnull(Agg.LOB_DESC,'')
        AND ifnull(Base.DEPT_NAME,'')          = ifnull(Agg.DEPT_NAME,'')
        AND ifnull(Base.BRAND_NAME,'')         = ifnull(Agg.BRAND_NAME,'')
        AND ifnull(Base.channel, '')         = ifnull(Agg.channel,'')
        AND ifnull(Base.ARTICLE_SIZE, '')      = ifnull(Agg.ARTICLE_SIZE,'')
        AND ifnull(Base.ARTICLE_COLOR , '')    = ifnull(Agg.ARTICLE_COLOR,'')
        AND Agg.Period between DateAdd( month, -11 , Base.Period ) and Base.Period      
GROUP BY 
    Base.PERIOD
  , Base.ARTICLE_NO
  , Base.VENDOR_ID
  , Base.YEAR_PERIOD
  , Base.ARTICLE_DESC
  , Base.ARTICLE_SIZE
  , Base.ARTICLE_SIZE
  , Base.MAJ_DEPT_DESC
  , Base.BRAND_NAME
  , Base.BUYER_CODE
  , Base.ARTICLE_COLOR
  , Base.ARTICLE_COLOR
  , Base.PARENT_ARTICLE_NO
  , Base.PARENT_ARTICLE_DESC
  , Base.ARTICLE_FULL
  , Base.BUYER_NAME
  , Base.ARTICLE_CAT_NAME
  , Base.LOB_DESC
  , Base.DEPT_NAME
  , Base.VENDOR_NAME
  , Base.PARENT_VENDOR_NAME
  , Base.channel
  , Base.SALES_QTY
  , Base.SALES_AMT
  , Base.COST_AMT
  , Base.margin
  , Base.volume_rebate
  , Base.unsellables
  , Base.defectives_shrink
  , Base.dc_shrink
  , Base.price_cuts
  , Base.theft
  , Base.defective_allowance
  , Base.battery_core
  , Base.mixing_center
  , Base.scanbacks
  , Base.markdowns
  , Base.new_store_discounts
  , Base.cash_discounts
  , Base.pop
  , Base.pdq
  , Base.marketing
  , Base.vendor_compliance
  , Base.booth_rental
  , Base.funding_outside_lm
  , Base.inventory_shrink
  , Base.total_shrink
  , Base.ocean_freight
  , Base.freight_cost
  , Base.agency_fee
  , Base.duty_tarrifs
  , Base.funding_in_lm
  , Base.total_funding
  , Base.domestic_inbound
  , Base.landed_margin
  , Base.total_freight
  , Base.domestic_outbound
  , Base.freight_other_charges
  , Base.vendor_support
  , Base.vendor_support_fast
  , Base.DC_total_variable_cost_allocated
  , Base.DC_total_fixed_cost_allocated
  , Base.DC3PL_total_variable_cost_allocated
  , Base.DC3PL_total_fixed_cost_allocated  ;

<br>
--VISUAL_TABLES_GRP_AGGREGATED_JOINED
</br>
INSERT INTO VISUAL_TABLES_GRP_AGGREGATED_JOINED
SELECT DISTINCT
    VISUAL.ARTICLE_NO AS ARTICLE_NO,
    VISUAL.VENDOR_ID AS VENDOR_ID,
    VISUAL.YEAR_PERIOD AS YEAR_PERIOD,
    VISUAL.ARTICLE_DESC AS ARTICLE_DESC,
    VISUAL.ARTICLE_SIZE AS ARTICLE_SIZE,
    VISUAL.ARTICLE_SIZE_1 AS ARTICLE_SIZE_1,
    VISUAL.MAJ_DEPT_DESC AS MAJ_DEPT_DESC,
    VISUAL.BRAND_NAME AS BRAND_NAME,
    VISUAL.BUYER_CODE AS BUYER_CODE,
    VISUAL.ARTICLE_COLOR AS ARTICLE_COLOR,
    VISUAL.ARTICLE_COLOR_1 AS ARTICLE_COLOR_1,
    VISUAL.PARENT_ARTICLE_NO AS PARENT_ARTICLE_NO,
    VISUAL.PARENT_ARTICLE_DESC AS PARENT_ARTICLE_DESC,
    VISUAL.ARTICLE_FULL AS ARTICLE_FULL,
    VISUAL.BUYER_NAME AS BUYER_NAME,
    VISUAL.ARTICLE_CAT_NAME AS ARTICLE_CAT_NAME,
    VISUAL.LOB_DESC AS LOB_DESC,
    VISUAL.DEPT_NAME AS DEPT_NAME,
    VISUAL.VENDOR_NAME AS VENDOR_NAME,
    VISUAL.PARENT_VENDOR_NAME AS PARENT_VENDOR_NAME,
    VISUAL.channel AS channel,
    VISUAL.SALES_QTY AS SALES_QTY,
    VISUAL.SALES_AMT AS SALES_AMT,
    VISUAL.COST_AMT AS COST_AMT,
    VISUAL.margin AS margin,
    VISUAL.volume_rebate AS volume_rebate,
    VISUAL.unsellables AS unsellables,
    VISUAL.defectives_shrink AS defectives_shrink,
    VISUAL.dc_shrink AS dc_shrink,
    VISUAL.price_cuts AS price_cuts,
    VISUAL.theft AS theft,
    VISUAL.defective_allowance AS defective_allowance,
    VISUAL.battery_core AS battery_core,
    VISUAL.mixing_center AS mixing_center,
    VISUAL.scanbacks AS scanbacks,
    VISUAL.markdowns AS markdowns,
    VISUAL.new_store_discounts AS new_store_discounts,
    VISUAL.cash_discounts AS cash_discounts,
    VISUAL.pop AS pop,
    VISUAL.pdq AS pdq,
    VISUAL.marketing AS marketing,
    VISUAL.vendor_compliance AS vendor_compliance,
    VISUAL.booth_rental AS booth_rental,
    VISUAL.funding_outside_lm AS funding_outside_lm,
    VISUAL.inventory_shrink AS inventory_shrink,
    VISUAL.total_shrink AS total_shrink,
    VISUAL.ocean_freight AS ocean_freight,
    VISUAL.freight_cost AS freight_cost,
    VISUAL.agency_fee AS agency_fee,
    VISUAL.duty_tarrifs AS duty_tarrifs,
    VISUAL.funding_in_lm AS funding_in_lm,
    VISUAL.total_funding AS total_funding,
    VISUAL.domestic_inbound AS domestic_inbound,
    VISUAL.landed_margin AS landed_margin,
    VISUAL.total_freight AS total_freight,
    VISUAL.domestic_outbound AS domestic_outbound,
    VISUAL.freight_other_charges AS freight_other_charges,
    VISUAL.vendor_support AS vendor_support,
    VISUAL.vendor_support_fast AS vendor_support_fast,
    VISUAL.DC_total_variable_cost_allocated AS DC_total_variable_cost_allocated,
    VISUAL.DC_total_fixed_cost_allocated AS DC_total_fixed_cost_allocated,
    VISUAL.DC3PL_total_variable_cost_allocated AS DC3PL_total_variable_cost_allocated,
    VISUAL.DC3PL_total_fixed_cost_allocated AS DC3PL_total_fixed_cost_allocated,
    VISUAL.DC_total_variable_cost_allocated_TTM AS DC_total_variable_cost_allocated_TTM,
    VISUAL.DC_total_fixed_cost_allocated_TTM AS DC_total_fixed_cost_allocated_TTM,
    VISUAL.DC3PL_total_fixed_cost_allocated_TTM AS DC3PL_total_fixed_cost_allocated_TTM,
    VISUAL.DC3PL_total_variable_cost_allocated_TTM AS DC3PL_total_variable_cost_allocated_TTM,
    VISUAL.SALES_AMT_TTM AS SALES_AMT_TTM,
    MERCH_Y.MERCH_YEAR AS MERCH_YEAR,
    MERCH_Y.PERIOD AS PERIOD
FROM VISUAL_TABLES_GRP_AGGREGATED VISUAL
LEFT JOIN MERCH_YEAR_YEAR_PERIOD MERCH_Y
ON VISUAL.YEAR_PERIOD = MERCH_Y.YEAR_PERIOD;

<br> </br>
TRUNCATE TABLE DC_3PL_VISUAL_TABLES_GRP_JOINED;

<br>
--DC_3PL_VISUAL_TABLES_GRP_JOINED
</br>
INSERT INTO DC_3PL_VISUAL_TABLES_GRP_JOINED
SELECT DISTINCT
  ARTICLE_NO,
  VENDOR_ID,
  YEAR_PERIOD,
  ARTICLE_DESC,
  ARTICLE_SIZE,
  ARTICLE_SIZE_1,
  MAJ_DEPT_DESC,
  BRAND_NAME,
  BUYER_CODE,
  ARTICLE_COLOR,
  ARTICLE_COLOR_1,
  PARENT_ARTICLE_NO,
  PARENT_ARTICLE_DESC,
  ARTICLE_FULL,
  BUYER_NAME,
  ARTICLE_CAT_NAME,
  LOB_DESC,
  DEPT_NAME,
  VENDOR_NAME,
  PARENT_VENDOR_NAME,
  channel,
  SALES_QTY,
  SALES_AMT,
  COST_AMT,
  margin,
  volume_rebate,
  unsellables,
  defectives_shrink,
  dc_shrink,
  price_cuts,
  theft,
  defective_allowance,
  battery_core,
  mixing_center,
  scanbacks,
  markdowns,
  new_store_discounts,
  cash_discounts,
  pop,
  pdq,
  marketing,
  vendor_compliance,
  booth_rental,
  funding_outside_lm,
  inventory_shrink,
  total_shrink,
  ocean_freight,
  freight_cost,
  agency_fee,
  duty_tarrifs,
  funding_in_lm,
  total_funding,
  domestic_inbound,
  landed_margin,
  total_freight,
  domestic_outbound,
  freight_other_charges,
  vendor_support,
  vendor_support_fast,
  DC_total_variable_cost_allocated,
  DC_total_fixed_cost_allocated,
  DC3PL_total_variable_cost_allocated,
  DC3PL_total_fixed_cost_allocated,
  DC_total_variable_cost_allocated_TTM,
  DC_total_fixed_cost_allocated_TTM,
  DC3PL_total_fixed_cost_allocated_TTM,
  DC3PL_total_variable_cost_allocated_TTM,
  SALES_AMT_TTM,
  MERCH_YEAR,
  PERIOD
FROM VISUAL_TABLES_GRP_AGGREGATED_JOINED;




