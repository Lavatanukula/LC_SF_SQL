
<br>
	--VISUAL_SALES_TABLE
</br> 
CREATE OR REPLACE TEMP TABLE VISUAL_SALES_TABLE
 AS
 WITH CTE AS 
  (
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
        SELECT  DISTINCT 
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
            "OMNI_TSC_DELIVERY" AS "omni_tsc_delivery",
            "OMNI_SMALL_PACKAGE_LTL" AS "omni_small_package_ltl",
            "OMNI_ROADIE" AS "omni_roadie",
            "omni_multi_salvage" AS "0mni_multi_salvage",
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
        ) Without_Grouping
      GROUP BY "ARTICLE_NO", "MERCH_YEAR", "VENDOR_ID", "YEAR_PERIOD", "ARTICLE_DESC", "ARTICLE_SIZE", "MAJ_DEPT_DESC", "BRAND_NAME", "BUYER_CODE", "ARTICLE_COLOR", "PARENT_ARTICLE_NO", "PARENT_ARTICLE_DESC", "ARTICLE_FULL", "BUYER_FULL", "ARTICLE_CAT_FULL", "LOB_FULL", "DEPT_FULL", "VENDOR_FULL", "PARENT_VENDOR_FULL", "channel"
      )
      SELECT  DISTINCT 
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
    "freight_other_charges_sum" AS "freight_other_charges",
    "volume_rebate_sum" AS "volume_rebate",
    "price_cuts_sum" AS "price_cuts",
    "scanbacks_sum" AS "scanbacks",
    "markdowns_sum" AS "markdowns",
    "landed_margin_sum" AS "landed_margin",
    "funding_in_lm_sum" AS "funding_in_lm",
    "total_funding_sum" AS "total_funding",
    "vendor_support_new_sum" AS "vendor_support",
    "vendor_support_fast_new_sum" AS "vendor_support_fast"
  FROM CTE ;

<br>
	--VISUAL_SALES_TABLE_SHARE
-- CREATE OR REPLACE TEMP TABLE VISUAL_SALES_TABLE_SHARE
-- AS 
--  SELECT 
-- "ARTICLE_NO"
-- ,"MERCH_YEAR"
-- ,"VENDOR_ID"
-- ,"YEAR_PERIOD"
-- ,SUBSTR("YEAR_PERIOD", 5, 2) AS "Period"
-- ,"ARTICLE_DESC"
-- ,"ARTICLE_SIZE"
-- ,"MAJ_DEPT_DESC"
-- ,"BRAND_NAME"
-- ,"BUYER_CODE"
-- ,"ARTICLE_COLOR"
-- ,"PARENT_ARTICLE_NO"
-- ,"PARENT_ARTICLE_DESC"
-- ,"ARTICLE_FULL"
-- ,"BUYER_NAME"
-- ,"ARTICLE_CAT_NAME"
-- ,"LOB_DESC"
-- ,"DEPT_NAME"
-- ,"VENDOR_NAME"
-- ,"PARENT_VENDOR_NAME"
-- ,"channel"
-- ,"SALES_QTY"
-- ,"SALES_AMT"
-- ,"COST_AMT"
-- ,"margin"
-- ,"unsellables"
-- ,"defectives_shrink"
-- ,"dc_shrink"
-- ,"theft"
-- ,"defective_allowance"
-- ,-1*coalesce("battery_core",0) AS "battery_core"
-- ,-1*coalesce("mixing_center",0) AS "mixing_center"
-- ,"new_store_discounts"
-- ,-1*coalesce("cash_discounts",0) AS "cash_discounts"
-- ,"pop"
-- ,"pdq"
-- ,-1*coalesce("marketing",0) AS "marketing"
-- ,"vendor_compliance"
-- ,"booth_rental"
-- ,"funding_outside_lm"
-- ,"inventory_shrink"
-- ,"total_shrink"
-- ,-1*coalesce("ocean_freight",0) AS "ocean_freight"
-- ,"freight_cost"
-- ,-1*coalesce("agency_fee",0) AS "agency_fee"
-- ,-1*coalesce("duty_tarrifs",0) AS "duty_tarrifs"
-- ,-1*coalesce("domestic_inbound",0) AS "domestic_inbound"
-- ,-1*coalesce("total_freight",0) AS "total_freight"
-- ,-1*coalesce("domestic_outbound",0) AS "domestic_outbound"
-- ,-1*coalesce("freight_other_charges",0) AS "freight_other_charges"
-- ,"volume_rebate"
-- ,"price_cuts"
-- ,"scanbacks"
-- ,"markdowns"
-- ,"landed_margin"
-- ,"funding_in_lm"
-- ,"total_funding"
-- ,"vendor_support"
-- ,"vendor_support_fast"
--  FROM VISUAL_SALES_TABLE ;

	--DC_3PL_LANDED_MARGIN_JOINED
</br> CREATE OR REPLACE TEMP TABLE DC_3PL_LANDED_MARGIN_JOINED
AS
SELECT DISTINCT
    "DC_3PL_allocation_joined_stacked_joined_grp"."YEAR_PERIOD" AS "YEAR_PERIOD",
    "DC_3PL_allocation_joined_stacked_joined_grp"."MERCH_YEAR" AS "MERCH_YEAR",
    "DC_3PL_allocation_joined_stacked_joined_grp"."STORE_NO" AS "STORE_NO",
    "DC_3PL_allocation_joined_stacked_joined_grp"."ARTICLE_NO" AS "ARTICLE_NO",
    "DC_3PL_allocation_joined_stacked_joined_grp"."VENDOR_ID" AS "VENDOR_ID",
    "DC_3PL_allocation_joined_stacked_joined_grp"."CHANNEL" AS "channel",
    "DC_3PL_allocation_joined_stacked_joined_grp"."FACILITY" AS "Facility",
    "DC_3PL_allocation_joined_stacked_joined_grp"."FACILITY_NAME" AS "Facility_Name",
    "DC_3PL_allocation_joined_stacked_joined_grp"."AVG_INVENTORY_QTY" AS "Avg_inventory_qty",
    "DC_3PL_allocation_joined_stacked_joined_grp"."INBOUND_UNIT" AS "inbound_unit",
    "DC_3PL_allocation_joined_stacked_joined_grp"."OUTBOUND_UNIT" AS "outbound_unit",
    "DC_3PL_allocation_joined_stacked_joined_grp"."3PL_TOTAL_VARIABLE_COST_ALLOCATED" AS "3PL_total_variable_cost_allocated",
    "DC_3PL_allocation_joined_stacked_joined_grp"."3PL_TOTAL_FIXED_COST_ALLOCATED" AS "3PL_total_fixed_cost_allocated",
    "DC_3PL_allocation_joined_stacked_joined_grp"."DC_TOTAL_VARIABLE_COST_ALLOCATED" AS "DC_total_variable_cost_allocated",
    "DC_3PL_allocation_joined_stacked_joined_grp"."DC_TOTAL_FIXED_COST_ALLOCATED" AS "DC_total_fixed_cost_allocated",
    "DC_3PL_allocation_joined_stacked_joined_grp"."BOOKED_SALES_QTY_SUM" AS "BOOKED_SALES_QTY_sum",
    "article"."ARTICLE_FULL" AS "ARTICLE_FULL",
    "article"."ARTICLE_SIZE" AS "ARTICLE_SIZE",
    "article"."ARTICLE_COLOR" AS "ARTICLE_COLOR",
    "article"."PARENT_ARTICLE_NO" AS "PARENT_ARTICLE_NO",
    "article"."PARENT_ARTICLE_DESC" AS "PARENT_ARTICLE_DESC",
    "article"."ARTICLE_CAT_FULL" AS "ARTICLE_CAT_FULL",
    "article"."DEPT_FULL" AS "DEPT_FULL",
    "article"."LOB_FULL" AS "LOB_FULL",
    "article"."BRAND_NAME" AS "BRAND_NAME",
    "vendor"."VENDOR_FULL" AS "VENDOR_FULL",
    "company_article_w_buyer"."BUYER_FULL" AS "BUYER_FULL",
    "company_parent_vendor_prepared"."PARENT_VENDOR_ID" AS "PARENT_VENDOR_ID",
    "vendor_2"."VENDOR_FULL" AS "PARENT_VENDOR_NAME"
   FROM "DC_3PL_ALLOCATION_JOINED_STACKED_JOINED_GRP" "DC_3PL_allocation_joined_stacked_joined_grp"
LEFT JOIN EDW.VW_ARTICLE "article"
    ON CAST("DC_3PL_allocation_joined_stacked_joined_grp"."ARTICLE_NO" AS NUMBER) = CAST("article"."ARTICLE_NO" AS NUMBER)
  LEFT JOIN EDW.VW_VENDOR "vendor"
    ON CAST("DC_3PL_allocation_joined_stacked_joined_grp"."VENDOR_ID" AS NUMBER) = CAST("vendor"."VENDOR_ID" AS NUMBER)
  LEFT JOIN COMPANY_ARTICLE_W_BUYER "company_article_w_buyer"
    ON CAST("DC_3PL_allocation_joined_stacked_joined_grp"."ARTICLE_NO" AS NUMBER) = CAST("company_article_w_buyer"."ARTICLE_NO" AS NUMBER)
  LEFT JOIN COMPANY_PARENT_VENDOR "company_parent_vendor_prepared"
    ON "DC_3PL_allocation_joined_stacked_joined_grp"."VENDOR_ID" = "company_parent_vendor_prepared"."VENDOR_ID"
  LEFT JOIN EDW.VW_VENDOR "vendor_2"
    ON CAST("company_parent_vendor_prepared"."PARENT_VENDOR_ID" AS NUMBER) = CAST("vendor_2"."VENDOR_ID" AS NUMBER);

<br>
	--Truncating DC_3PL_LANDED_MARGIN_JOINED_W_FACILITY_CODE
</br> 
TRUNCATE TABLE DC_3PL_LANDED_MARGIN_JOINED_W_FACILITY_CODE ;
<br>
	--Loading fresh data into DC_3PL_LANDED_MARGIN_JOINED_W_FACILITY_CODE
</br>
INSERT INTO  DC_3PL_LANDED_MARGIN_JOINED_W_FACILITY_CODE 
WITH  DC_3PL_ALLOCATION_JOINED_LM AS (
SELECT 
    distinct
    "DC_3PL_landed_margin_joined"."YEAR_PERIOD" AS "YEAR_PERIOD",
    "DC_3PL_landed_margin_joined"."MERCH_YEAR" AS "MERCH_YEAR",
    "DC_3PL_landed_margin_joined"."STORE_NO" AS "STORE_NO",
    "DC_3PL_landed_margin_joined"."ARTICLE_NO" AS "ARTICLE_NO",
    "DC_3PL_landed_margin_joined"."VENDOR_ID" AS "VENDOR_ID",
    "DC_3PL_landed_margin_joined"."channel" AS "channel",
    "DC_3PL_landed_margin_joined"."Facility" AS "Facility",
    "DC_3PL_landed_margin_joined"."Facility_Name" AS "Facility_Name",
    "DC_3PL_landed_margin_joined"."Avg_inventory_qty" AS "Avg_inventory_qty",
    "DC_3PL_landed_margin_joined"."inbound_unit" AS "inbound_unit",
    "DC_3PL_landed_margin_joined"."outbound_unit" AS "outbound_unit",
    "DC_3PL_landed_margin_joined"."3PL_total_variable_cost_allocated" AS "3PL_total_variable_cost_allocated",
    "DC_3PL_landed_margin_joined"."3PL_total_fixed_cost_allocated" AS "3PL_total_fixed_cost_allocated",
    "DC_3PL_landed_margin_joined"."DC_total_variable_cost_allocated" AS "DC_total_variable_cost_allocated",
    "DC_3PL_landed_margin_joined"."DC_total_fixed_cost_allocated" AS "DC_total_fixed_cost_allocated",
    "DC_3PL_landed_margin_joined"."BOOKED_SALES_QTY_sum" AS "BOOKED_SALES_QTY_sum",
    "DC_3PL_landed_margin_joined"."ARTICLE_FULL" AS "ARTICLE_FULL",
    "DC_3PL_landed_margin_joined"."ARTICLE_SIZE" AS "ARTICLE_SIZE",
    "DC_3PL_landed_margin_joined"."ARTICLE_COLOR" AS "ARTICLE_COLOR",
    "DC_3PL_landed_margin_joined"."PARENT_ARTICLE_NO" AS "PARENT_ARTICLE_NO",
    "DC_3PL_landed_margin_joined"."PARENT_ARTICLE_DESC" AS "PARENT_ARTICLE_DESC",
    "DC_3PL_landed_margin_joined"."ARTICLE_CAT_FULL" AS "ARTICLE_CAT_FULL",
    "DC_3PL_landed_margin_joined"."DEPT_FULL" AS "DEPT_FULL",
    "DC_3PL_landed_margin_joined"."LOB_FULL" AS "LOB_FULL",
    "DC_3PL_landed_margin_joined"."BRAND_NAME" AS "BRAND_NAME",
    "DC_3PL_landed_margin_joined"."VENDOR_FULL" AS "VENDOR_FULL",
    "DC_3PL_landed_margin_joined"."BUYER_FULL" AS "BUYER_FULL",
    "DC_3PL_landed_margin_joined"."PARENT_VENDOR_ID" AS "PARENT_VENDOR_ID",
    "DC_3PL_landed_margin_joined"."PARENT_VENDOR_NAME" AS "PARENT_VENDOR_NAME",
    "visual_sales_table_share"."SALES_QTY" AS "SALES_QTY",
    "visual_sales_table_share"."SALES_AMT" AS "SALES_AMT",
    "visual_sales_table_share"."COST_AMT" AS "COST_AMT",
    "visual_sales_table_share".margin AS "margin",
    "visual_sales_table_share".landed_margin AS "landed_margin"
  FROM "DC_3PL_LANDED_MARGIN_JOINED" "DC_3PL_landed_margin_joined"
  LEFT JOIN "VISUAL_SALES_TABLE_SHARE" "visual_sales_table_share"
    ON  (CAST("DC_3PL_landed_margin_joined"."YEAR_PERIOD" AS NUMBER) = CAST("visual_sales_table_share"."YEAR_PERIOD" AS NUMBER))
      AND ("DC_3PL_landed_margin_joined"."ARTICLE_NO" = "visual_sales_table_share"."ARTICLE_NO")
      AND (CAST("DC_3PL_landed_margin_joined"."VENDOR_ID" AS NUMBER) = CAST("visual_sales_table_share"."VENDOR_ID" AS NUMBER))
      AND (UPPER("DC_3PL_landed_margin_joined"."channel") = UPPER("visual_sales_table_share".channel))
      )
      SELECT *
      FROM DC_3PL_ALLOCATION_JOINED_LM ;
	  

<br>
	--ALLOCATION_DC_3PL_OMNI_FULFILLMENT_SF
</br>	  
CREATE OR REPLACE TEMP TABLE ALLOCATION_DC_3PL_OMNI_FULFILLMENT_SF
AS 
SELECT  DISTINCT 
 "OMS_ORDER_LINE_KEY" AS OMS_ORDER_LINE_KEY
,"DEMAND_SALES" AS DEMAND_SALES
,"SHIPPING_REVENUE" AS SHIPPING_REVENUE
,"YEAR_WEEK" AS YEAR_WEEK
,"DC_3PL_PNL_OMNI_FULFILLMENT_FIXED_L2"
,"DEMAND_QTY" AS DEMAND_QTY
,"OMS_ORDER_NO" AS OMS_ORDER_NO
,"OMS_ITEM_PRIMARY_VENDOR_ID" AS OMS_ITEM_PRIMARY_VENDOR_ID
,"BOOKED_SALES_QTY" AS BOOKED_SALES_QTY
,"OMS_ORDER_LINE_SCHEDULE_SHIP_NODE" AS OMS_ORDER_LINE_SCHEDULE_SHIP_NODE
,"ARTICLE_CAT_FULL" AS ARTICLE_CAT_FULL
,"BOOKED_COST_AMT" AS BOOKED_COST_AMT
,"DC_3PL_PNL_OMNI_FULFILLMENT_FIXED"
,"STORE_NO" AS STORE_NO
,"ACCT_YEAR" AS ACCT_YEAR
,"ACTUAL_SHIPPING_REVENUE" AS ACTUAL_SHIPPING_REVENUE
,"SHIP_NODE_TYPE" AS SHIP_NODE_TYPE
,"CHANNEL"
,"DC_3PL_PNL_OMNI_FULFILLMENT_VARIABLE"
,"BOOKED_SALES_AMT" AS BOOKED_SALES_AMT
,"OMS_ITEM_ID" AS OMS_ITEM_ID
,"DC_3PL_PNL_OMNI_FULFILLMENT_VARIABLE_L2" 
,"MERCH_YEAR" AS MERCH_YEAR
,"OMS_SHIP_NODE_DESC" AS OMS_SHIP_NODE_DESC
,"SHIPPING_DISC" AS SHIPPING_DISC
,"YEAR_PERIOD" AS YEAR_PERIOD
,"ORDER_LINE_TYPE" AS ORDER_LINE_TYPE
,"SCAC" AS SCAC
FROM  ALLOCATION_DC_3PL_OMNI_FULFILLMENT_STEP1_SF ;

<br>
	--ALLOCATION_DC_3PL_OMNI_FULFILLMENT_GRP_JOINED
</br>

CREATE OR REPLACE TEMP TABLE ALLOCATION_DC_3PL_OMNI_FULFILLMENT_GRP_JOINED
AS 
SELECT  DISTINCT
    "allocation_DC_3PL_omni_fulfillment_grp"."MERCH_YEAR" AS "MERCH_YEAR",
    "allocation_DC_3PL_omni_fulfillment_grp"."STORE_NO" AS "STORE_NO",
    "allocation_DC_3PL_omni_fulfillment_grp"."YEAR_PERIOD" AS "YEAR_PERIOD",
    "allocation_DC_3PL_omni_fulfillment_grp"."CHANNEL" AS "channel",
    "allocation_DC_3PL_omni_fulfillment_grp"."OMS_ITEM_ID" AS "OMS_ITEM_ID",
    "allocation_DC_3PL_omni_fulfillment_grp"."OMS_ITEM_PRIMARY_VENDOR_ID" AS "OMS_ITEM_PRIMARY_VENDOR_ID",
    "allocation_DC_3PL_omni_fulfillment_grp"."DC3PL_OMNI_FIXED_COST_SUM" AS "DC3PL_omni_fixed_cost_sum",
    "allocation_DC_3PL_omni_fulfillment_grp"."DC3PL_OMNI_VARIABLE_COST_SUM" AS "DC3PL_omni_variable_cost_sum",
    "MERCH_YEAR_YEAR_PERIOD"."PERIOD" AS "PERIOD"
  FROM "ALLOCATION_DC_3PL_OMNI_FULFILLMENT_GRP" "allocation_DC_3PL_omni_fulfillment_grp"
  LEFT JOIN "MERCH_YEAR_YEAR_PERIOD" "MERCH_YEAR_YEAR_PERIOD"
    ON "allocation_DC_3PL_omni_fulfillment_grp"."YEAR_PERIOD" = "MERCH_YEAR_YEAR_PERIOD"."YEAR_PERIOD";

<br>
	--ALLOCATION_DC_3PL_OMNI_FULFILLMENT_TOTAL_SHIPPED
</br>

 CREATE OR REPLACE TEMP TABLE ALLOCATION_DC_3PL_OMNI_FULFILLMENT_TOTAL_SHIPPED
 AS 
 WITH ALLOCATION_DC_3PL_OMNI_FULFILLMENT_GRP_TOTAL_COST AS 
 (
 SELECT DISTINCT 
    "MERCH_YEAR",
    "STORE_NO",
    "YEAR_PERIOD",
    "channel",
    "OMS_ITEM_ID",
    "OMS_ITEM_PRIMARY_VENDOR_ID",
    "DC3PL_omni_fixed_cost_sum",
    "DC3PL_omni_variable_cost_sum",
    "PERIOD",
    COALESCE("DC3PL_omni_fixed_cost_sum", 0) + COALESCE("DC3PL_omni_variable_cost_sum", 0) AS "DC3PL_total_cost"
  FROM "ALLOCATION_DC_3PL_OMNI_FULFILLMENT_GRP_JOINED"
  )
  SELECT 
    "MERCH_YEAR" AS "MERCH_YEAR",
    "YEAR_PERIOD" AS "YEAR_PERIOD",
    "OMS_ITEM_ID" AS "OMS_ITEM_ID",
    "OMS_ITEM_PRIMARY_VENDOR_ID" AS "OMS_ITEM_PRIMARY_VENDOR_ID",
    "PERIOD" AS "PERIOD",
    "DC3PL_total_cost_sum" AS "DC3PL_total_cost"
  FROM (
    SELECT  
        "MERCH_YEAR" AS "MERCH_YEAR",
        "YEAR_PERIOD" AS "YEAR_PERIOD",
        "OMS_ITEM_ID" AS "OMS_ITEM_ID",
        "OMS_ITEM_PRIMARY_VENDOR_ID" AS "OMS_ITEM_PRIMARY_VENDOR_ID",
        "PERIOD" AS "PERIOD",
        SUM("DC3PL_total_cost") AS "DC3PL_total_cost_sum"
      FROM (
        SELECT DISTINCT
            "MERCH_YEAR" AS "MERCH_YEAR",
            "STORE_NO" AS "STORE_NO",
            "YEAR_PERIOD" AS "YEAR_PERIOD",
            "channel" AS "channel",
            "OMS_ITEM_ID" AS "OMS_ITEM_ID",
            "OMS_ITEM_PRIMARY_VENDOR_ID" AS "OMS_ITEM_PRIMARY_VENDOR_ID",
            "DC3PL_omni_fixed_cost_sum" AS "DC3PL_omni_fixed_cost_sum",
            "DC3PL_omni_variable_cost_sum" AS "DC3PL_omni_variable_cost_sum",
            "DC3PL_total_cost" AS "DC3PL_total_cost",
            "PERIOD" AS "PERIOD"
          FROM "ALLOCATION_DC_3PL_OMNI_FULFILLMENT_GRP_TOTAL_COST"
        )BEFORE_GROUPING
      GROUP BY "MERCH_YEAR", "YEAR_PERIOD", "OMS_ITEM_ID", "OMS_ITEM_PRIMARY_VENDOR_ID", "PERIOD"
    ) FINAL ;

<br>
	--DC_3PL_ALLOCATION_STACKED_STORE_CHANNEL
</br>

CREATE OR REPLACE TEMP TABLE DC_3PL_ALLOCATION_STACKED_STORE_CHANNEL
AS 
SELECT 
    "YEAR_PERIOD" AS "YEAR_PERIOD",
    "MERCH_YEAR" AS "MERCH_YEAR",
    "STORE_NO" AS "STORE_NO",
    "ARTICLE_NO" AS "ARTICLE_NO",
    "VENDOR_ID" AS "VENDOR_ID",
    "channel" AS "channel",
    SUM("DC3PL_total_cost") AS "DC3PL_total_cost_sum"
  FROM (
    SELECT 
        "YEAR_PERIOD" AS "YEAR_PERIOD",
        "MERCH_YEAR" AS "MERCH_YEAR",
        "STORE_NO" AS "STORE_NO",
        "ARTICLE_NO" AS "ARTICLE_NO",
        "VENDOR_ID" AS "VENDOR_ID",
        "channel" AS "channel",
        "Facility" AS "Facility",
        "Facility_Name" AS "Facility_Name",
        "Avg_inventory_qty" AS "Avg_inventory_qty",
        "inbound_unit" AS "inbound_unit",
        "outbound_unit" AS "outbound_unit",
        "3PL_total_variable_cost_allocated" AS "3PL_total_variable_cost_allocated",
        "3PL_total_fixed_cost_allocated" AS "3PL_total_fixed_cost_allocated",
        "DC_total_variable_cost_allocated" AS "DC_total_variable_cost_allocated",
        "DC_total_fixed_cost_allocated" AS "DC_total_fixed_cost_allocated",
        "BOOKED_SALES_QTY_sum" AS "BOOKED_SALES_QTY_sum",
        COALESCE("3PL_total_variable_cost_allocated", 0) + COALESCE("3PL_total_fixed_cost_allocated", 0) + COALESCE("DC_total_variable_cost_allocated", 0) + COALESCE("DC_total_fixed_cost_allocated", 0) AS "DC3PL_total_cost"
      FROM (
        SELECT  DISTINCT 
            "YEAR_PERIOD",
            "MERCH_YEAR",
            "STORE_NO",
            "ARTICLE_NO",
            "VENDOR_ID",
             CHANNEL AS "channel",
             FACILITY AS "Facility",
             FACILITY_NAME AS "Facility_Name",
             AVG_INVENTORY_QTY AS "Avg_inventory_qty",
             INBOUND_UNIT "inbound_unit",
             OUTBOUND_UNIT "outbound_unit",
             "3PL_TOTAL_VARIABLE_COST_ALLOCATED" AS "3PL_total_variable_cost_allocated",
            "3PL_TOTAL_FIXED_COST_ALLOCATED" AS  "3PL_total_fixed_cost_allocated",
            DC_TOTAL_VARIABLE_COST_ALLOCATED AS "DC_total_variable_cost_allocated",
            DC_TOTAL_FIXED_COST_ALLOCATED AS "DC_total_fixed_cost_allocated",
            BOOKED_SALES_QTY_SUM "BOOKED_SALES_QTY_sum"
          FROM "DC_3PL_ALLOCATION_JOINED_STACKED_JOINED_GRP"
          WHERE "channel" = 'Store'
        ) A
    ) BEFORE_GROUPING
  GROUP BY "YEAR_PERIOD", "MERCH_YEAR", "STORE_NO", "ARTICLE_NO", "VENDOR_ID", "channel";

<br>
	--OMNI_TABLE_GRP
</br>

CREATE OR REPLACE TEMP TABLE OMNI_TABLE_GRP
AS
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
    "BUYER_FULL" AS "BUYER_NAME",
    "ARTICLE_CAT_FULL" AS "ARTICLE_CAT_NAME",
    "LOB_FULL" AS "LOB_DESC",
    "DEPT_FULL" AS "DEPT_NAME",
    "VENDOR_FULL" AS "VENDOR_NAME",
    "PARENT_VENDOR_FULL" AS "PARENT_VENDOR_NAME",
    "omni_flow_path" AS "omni_flow_path",
    "omni_flow_path_modified" AS "omni_flow_path_modified",
    "SALES_QTY_sum" AS "SALES_QTY",
    "SALES_AMT_sum" AS "SALES_AMT",
    "COST_AMT_sum" AS "COST_AMT",
    "margin_sum" AS "margin",
    "total_shrink_sum" AS "total_shrink",
    "SHIPPING_REVENUE_sum" AS "SHIPPING_REVENUE",
    "SHIPPING_DISC_sum" AS "SHIPPING_DISC",
    "ACTUAL_SHIPPING_REVENUE_sum" AS "ACTUAL_SHIPPING_REVENUE",
    "omni_tsc_delivery_sum" AS "omni_tsc_delivery",
    "omni_small_package_ltl_sum" AS "omni_small_package_ltl",
    "omni_roadie_sum" AS "omni_roadie",
    "omni_multi_salvage_sum" AS "omni_multi_salvage",
    "volume_rebate_sum" AS "volume_rebate",
    "price_cuts_sum" AS "price_cuts",
    "scanbacks_sum" AS "scanbacks",
    "markdowns_sum" AS "markdowns",
    "landed_margin_sum" AS "landed_margin",
    "funding_in_lm_sum" AS "funding_in_lm",
    "total_funding_sum" AS "total_funding",
    "last_mile_cost_sum" AS "last_mile_cost",
    "last_mile_delivery_combined_sum" AS "last_mile_delivery_combined",
    "small_parcel_ltl_cost_sum" AS "small_parcel_ltl_cost",
    "omni_freight_sum" AS "omni_freight",
    "vendor_support_new_sum" AS "vendor_support",
    "vendor_support_fast_new_sum" AS "vendor_support_fast"
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
        "omni_flow_path" AS "omni_flow_path",
        "omni_flow_path_modified" AS "omni_flow_path_modified",
        SUM("SALES_QTY") AS "SALES_QTY_sum",
        SUM("SALES_AMT") AS "SALES_AMT_sum",
        SUM("COST_AMT") AS "COST_AMT_sum",
        SUM("margin") AS "margin_sum",
        SUM("total_shrink") AS "total_shrink_sum",
        SUM("SHIPPING_REVENUE") AS "SHIPPING_REVENUE_sum",
        SUM("SHIPPING_DISC") AS "SHIPPING_DISC_sum",
        SUM("ACTUAL_SHIPPING_REVENUE") AS "ACTUAL_SHIPPING_REVENUE_sum",
        SUM("omni_tsc_delivery") AS "omni_tsc_delivery_sum",
        SUM("omni_small_package_ltl") AS "omni_small_package_ltl_sum",
        SUM("omni_roadie") AS "omni_roadie_sum",
        SUM("omni_multi_salvage") AS "omni_multi_salvage_sum",
        SUM("volume_rebate") AS "volume_rebate_sum",
        SUM("price_cuts") AS "price_cuts_sum",
        SUM("scanbacks") AS "scanbacks_sum",
        SUM("markdowns") AS "markdowns_sum",
        SUM("landed_margin") AS "landed_margin_sum",
        SUM("funding_in_lm") AS "funding_in_lm_sum",
        SUM("total_funding") AS "total_funding_sum",
        SUM("last_mile_cost") AS "last_mile_cost_sum",
        SUM("last_mile_delivery_combined") AS "last_mile_delivery_combined_sum",
        SUM("small_parcel_ltl_cost") AS "small_parcel_ltl_cost_sum",
        SUM("omni_freight") AS "omni_freight_sum",
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
            "omni_tsc_delivery" AS "omni_tsc_delivery",
            "omni_small_package_ltl" AS "omni_small_package_ltl",
            "omni_roadie" AS "omni_roadie",
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
            "ARTICLE_COLOR" AS "ARTICLE_COLOR",
            "PARENT_ARTICLE_NO" AS "PARENT_ARTICLE_NO",
            "PARENT_ARTICLE_DESC" AS "PARENT_ARTICLE_DESC",
            "PARENT_VENDOR_FULL" AS "PARENT_VENDOR_FULL",
            COALESCE("vendor_support_dol", 0) AS "vendor_support_new",
            COALESCE("vendor_support_fast_dol", 0) + COALESCE("vsf_exception", 0) AS "vendor_support_fast_new"
          FROM (
            SELECT  DISTINCT 
                "MERCH_YEAR",
                "PARENT_VENDOR_ID",
                "VENDOR_ID",
                "ARTICLE_NO",
                "YEAR_PERIOD",
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
                OMNI_TSC_DELIVERY AS "omni_tsc_delivery",
                OMNI_SMALL_PACKAGE_LTL "omni_small_package_ltl",
                OMNI_ROADIE "omni_roadie",
                "omni_multi_salvage" AS "omni_multi_salvage",
                "vendor_support_dol",
                "vendor_support_fast_dol",
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
                "last_mile_cost",
                "last_mile_delivery_combined",
                "small_parcel_ltl_cost",
                "Facility_Type",
                "omni_flow_path_modified",
                "omni_freight",
                "BUYER_CODE",
                "BUYER_NAME",
                "BUYER_FULL",
                "ARTICLE_COLOR",
                "PARENT_ARTICLE_NO",
                "PARENT_ARTICLE_DESC",
                "PARENT_VENDOR_FULL"
              FROM "ALLOCATION_W_BUYER_TEMP"
              WHERE "channel" LIKE CONCAT('%', IFNULL(TO_VARCHAR(REPLACE(REPLACE(REPLACE('Omni', '\\', '\\\\'), '%', '\\%'), '_', '\\_')), ''), '%') ESCAPE '\\'
            ) _DERIVE_TABLE
        ) BEFORE_GROUPING
      GROUP BY "ARTICLE_NO", "MERCH_YEAR", "VENDOR_ID", "YEAR_PERIOD", "ARTICLE_DESC", "ARTICLE_SIZE", "MAJ_DEPT_DESC", "BRAND_NAME", "BUYER_CODE", "ARTICLE_COLOR", "PARENT_ARTICLE_NO", "PARENT_ARTICLE_DESC", "ARTICLE_FULL", "BUYER_FULL", "ARTICLE_CAT_FULL", "LOB_FULL", "DEPT_FULL", "VENDOR_FULL", "PARENT_VENDOR_FULL", "omni_flow_path", "omni_flow_path_modified"
    ) FINAL ;

<br>
	--OMNI_TABLE_GRP_PREP
</br>
CREATE OR REPLACE TEMP TABLE OMNI_TABLE_GRP_PREP
AS
 WITH CTE AS (
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
        "omni_flow_path",
        "omni_flow_path_modified",
        "SALES_QTY",
        "SALES_AMT",
        "COST_AMT",
        "margin",
        "total_shrink",
        "SHIPPING_REVENUE",
        "SHIPPING_DISC",
        "ACTUAL_SHIPPING_REVENUE",
        "omni_tsc_delivery",
        "omni_small_package_ltl",
        "omni_roadie",
        "omni_multi_salvage",
        "volume_rebate",
        "price_cuts",
        "scanbacks",
        "markdowns",
        "landed_margin",
        "funding_in_lm",
        "total_funding",
        "last_mile_cost",
        "last_mile_delivery_combined",
        "small_parcel_ltl_cost",
        "omni_freight",
        "vendor_support",
        "vendor_support_fast"
      FROM "OMNI_TABLE_GRP" 
    )
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
    "omni_flow_path",
    "omni_flow_path_modified",
    "SALES_QTY",
    "SALES_AMT",
    "COST_AMT",
    "margin",
    "total_shrink",
    "SHIPPING_REVENUE",
    "SHIPPING_DISC",
    "ACTUAL_SHIPPING_REVENUE",
    "omni_tsc_delivery",
    "omni_small_package_ltl",
    "omni_roadie",
    "omni_multi_salvage",
    "volume_rebate",
    "price_cuts",
    "scanbacks",
    "markdowns",
    "landed_margin",
    "funding_in_lm",
    "total_funding",
    "last_mile_cost",
    "last_mile_delivery_combined",
    "small_parcel_ltl_cost",
    "omni_freight",
    "vendor_support",
    "vendor_support_fast",
    CAST(right("YEAR_PERIOD",2) AS BIGINT) AS "Period"
    FROM CTE;
<br>
	--OMNI_TABLE_GRP_SHIPPED
</br>

CREATE OR REPLACE TEMP TABLE OMNI_TABLE_GRP_SHIPPED
AS 
SELECT 
    "ARTICLE_NO" AS "ARTICLE_NO",
    "MERCH_YEAR" AS "MERCH_YEAR",
    "VENDOR_ID" AS "VENDOR_ID",
    "YEAR_PERIOD" AS "YEAR_PERIOD",
    "Period" AS "Period",
    "ARTICLE_DESC" AS "ARTICLE_DESC",
    "ARTICLE_SIZE" AS "ARTICLE_SIZE",
    "MAJ_DEPT_DESC" AS "MAJ_DEPT_DESC",
    "BRAND_NAME" AS "BRAND_NAME",
    "BUYER_CODE" AS "BUYER_CODE",
    "ARTICLE_COLOR" AS "ARTICLE_COLOR",
    "PARENT_ARTICLE_NO" AS "PARENT_ARTICLE_NO",
    "PARENT_ARTICLE_DESC" AS "PARENT_ARTICLE_DESC",
    "ARTICLE_FULL" AS "ARTICLE_FULL",
    "BUYER_NAME" AS "BUYER_NAME",
    "ARTICLE_CAT_NAME" AS "ARTICLE_CAT_NAME",
    "LOB_DESC" AS "LOB_DESC",
    "DEPT_NAME" AS "DEPT_NAME",
    "VENDOR_NAME" AS "VENDOR_NAME",
    "PARENT_VENDOR_NAME" AS "PARENT_VENDOR_NAME",
    "omni_flow_path" AS "omni_flow_path",
    "omni_flow_path_modified" AS "omni_flow_path_modified",
    "SALES_QTY" AS "SALES_QTY",
    "SALES_AMT" AS "SALES_AMT",
    "COST_AMT" AS "COST_AMT",
    "margin" AS "margin",
    "total_shrink" AS "total_shrink",
    "SHIPPING_REVENUE" AS "SHIPPING_REVENUE",
    "SHIPPING_DISC" AS "SHIPPING_DISC",
    "ACTUAL_SHIPPING_REVENUE" AS "ACTUAL_SHIPPING_REVENUE",
    "omni_tsc_delivery" AS "omni_tsc_delivery",
    "omni_small_package_ltl" AS "omni_small_package_ltl",
    "omni_roadie" AS "omni_roadie",
    "omni_multi_salvage" AS "omni_multi_salvage",
    "volume_rebate" AS "volume_rebate",
    "price_cuts" AS "price_cuts",
    "scanbacks" AS "scanbacks",
    "markdowns" AS "markdowns",
    "landed_margin" AS "landed_margin",
    "funding_in_lm" AS "funding_in_lm",
    "total_funding" AS "total_funding",
    "last_mile_cost" AS "last_mile_cost",
    "last_mile_delivery_combined" AS "last_mile_delivery_combined",
    "small_parcel_ltl_cost" AS "small_parcel_ltl_cost",
    "omni_freight" AS "omni_freight",
    "vendor_support" AS "vendor_support",
    "vendor_support_fast" AS "vendor_support_fast"
  FROM "OMNI_TABLE_GRP_PREP"
  WHERE ("omni_flow_path_modified" != 'BODFS' OR "omni_flow_path_modified" IS NULL AND 'BODFS' IS NOT NULL OR "omni_flow_path_modified" IS NOT NULL AND 'BODFS' IS NULL) AND ("omni_flow_path_modified" != 'BODFS - Roadie' OR "omni_flow_path_modified" IS NULL AND 'BODFS - Roadie' IS NOT NULL OR "omni_flow_path_modified" IS NOT NULL AND 'BODFS - Roadie' IS NULL) AND ("omni_flow_path_modified" != 'BODFS - TSC Delivery' OR "omni_flow_path_modified" IS NULL AND 'BODFS - TSC Delivery' IS NOT NULL OR "omni_flow_path_modified" IS NOT NULL AND 'BODFS - TSC Delivery' IS NULL) AND ("omni_flow_path_modified" != 'BOPIS' OR "omni_flow_path_modified" IS NULL AND 'BOPIS' IS NOT NULL OR "omni_flow_path_modified" IS NOT NULL AND 'BOPIS' IS NULL);

<br>
	--OMNI_TABLE_GRP_PREP_BODFS_BOPIS
</br>
CREATE OR  REPLACE TEMP TABLE OMNI_TABLE_GRP_PREP_BODFS_BOPIS
AS
SELECT 
    "ARTICLE_NO" AS "ARTICLE_NO",
    "MERCH_YEAR" AS "MERCH_YEAR",
    "VENDOR_ID" AS "VENDOR_ID",
    "YEAR_PERIOD" AS "YEAR_PERIOD",
    "Period" AS "Period",
    "ARTICLE_DESC" AS "ARTICLE_DESC",
    "ARTICLE_SIZE" AS "ARTICLE_SIZE",
    "MAJ_DEPT_DESC" AS "MAJ_DEPT_DESC",
    "BRAND_NAME" AS "BRAND_NAME",
    "BUYER_CODE" AS "BUYER_CODE",
    "ARTICLE_COLOR" AS "ARTICLE_COLOR",
    "PARENT_ARTICLE_NO" AS "PARENT_ARTICLE_NO",
    "PARENT_ARTICLE_DESC" AS "PARENT_ARTICLE_DESC",
    "ARTICLE_FULL" AS "ARTICLE_FULL",
    "BUYER_NAME" AS "BUYER_NAME",
    "ARTICLE_CAT_NAME" AS "ARTICLE_CAT_NAME",
    "LOB_DESC" AS "LOB_DESC",
    "DEPT_NAME" AS "DEPT_NAME",
    "VENDOR_NAME" AS "VENDOR_NAME",
    "PARENT_VENDOR_NAME" AS "PARENT_VENDOR_NAME",
    "omni_flow_path" AS "omni_flow_path",
    "omni_flow_path_modified" AS "omni_flow_path_modified",
    "SALES_QTY" AS "SALES_QTY",
    "SALES_AMT" AS "SALES_AMT",
    "COST_AMT" AS "COST_AMT",
    "margin" AS "margin",
    "total_shrink" AS "total_shrink",
    "SHIPPING_REVENUE" AS "SHIPPING_REVENUE",
    "SHIPPING_DISC" AS "SHIPPING_DISC",
    "ACTUAL_SHIPPING_REVENUE" AS "ACTUAL_SHIPPING_REVENUE",
    "omni_tsc_delivery" AS "omni_tsc_delivery",
    "omni_small_package_ltl" AS "omni_small_package_ltl",
    "omni_roadie" AS "omni_roadie",
    "omni_multi_salvage" AS "omni_multi_salvage",
    "volume_rebate" AS "volume_rebate",
    "price_cuts" AS "price_cuts",
    "scanbacks" AS "scanbacks",
    "markdowns" AS "markdowns",
    "landed_margin" AS "landed_margin",
    "funding_in_lm" AS "funding_in_lm",
    "total_funding" AS "total_funding",
    "last_mile_cost" AS "last_mile_cost",
    "last_mile_delivery_combined" AS "last_mile_delivery_combined",
    "small_parcel_ltl_cost" AS "small_parcel_ltl_cost",
    "omni_freight" AS "omni_freight",
    "vendor_support" AS "vendor_support",
    "vendor_support_fast" AS "vendor_support_fast"
  FROM "OMNI_TABLE_GRP_PREP"
  WHERE "omni_flow_path_modified" LIKE CONCAT('%', IFNULL(TO_VARCHAR(REPLACE(REPLACE(REPLACE('BODFS', '\\', '\\\\'), '%', '\\%'), '_', '\\_')), ''), '%') ESCAPE '\\' OR ("omni_flow_path_modified" = 'BODFS - Roadie') OR ("omni_flow_path_modified" = 'BODFS - TSC Delivery') OR "omni_flow_path_modified" LIKE CONCAT('%', IFNULL(TO_VARCHAR(REPLACE(REPLACE(REPLACE('BOPIS', '\\', '\\\\'), '%', '\\%'), '_', '\\_')), ''), '%') ESCAPE '\\'  ;


<br>
	--OMNI_TABLE_GRP_PREP_BODFS_BOPIS_DC3PL
</br>
CREATE OR REPLACE TEMP TABLE OMNI_TABLE_GRP_PREP_BODFS_BOPIS_DC3PL
AS
SELECT 
    "omni_table_grp_prep_bodfs_bopis"."ARTICLE_NO" AS "ARTICLE_NO",
    "omni_table_grp_prep_bodfs_bopis"."MERCH_YEAR" AS "MERCH_YEAR",
    "omni_table_grp_prep_bodfs_bopis"."VENDOR_ID" AS "VENDOR_ID",
    "omni_table_grp_prep_bodfs_bopis"."YEAR_PERIOD" AS "YEAR_PERIOD",
    "omni_table_grp_prep_bodfs_bopis"."Period" AS "Period",
    "omni_table_grp_prep_bodfs_bopis"."ARTICLE_DESC" AS "ARTICLE_DESC",
    "omni_table_grp_prep_bodfs_bopis"."ARTICLE_SIZE" AS "ARTICLE_SIZE",
    "omni_table_grp_prep_bodfs_bopis"."MAJ_DEPT_DESC" AS "MAJ_DEPT_DESC",
    "omni_table_grp_prep_bodfs_bopis"."BRAND_NAME" AS "BRAND_NAME",
    "omni_table_grp_prep_bodfs_bopis"."BUYER_CODE" AS "BUYER_CODE",
    "omni_table_grp_prep_bodfs_bopis"."ARTICLE_COLOR" AS "ARTICLE_COLOR",
    "omni_table_grp_prep_bodfs_bopis"."PARENT_ARTICLE_NO" AS "PARENT_ARTICLE_NO",
    "omni_table_grp_prep_bodfs_bopis"."PARENT_ARTICLE_DESC" AS "PARENT_ARTICLE_DESC",
    "omni_table_grp_prep_bodfs_bopis"."ARTICLE_FULL" AS "ARTICLE_FULL",
    "omni_table_grp_prep_bodfs_bopis"."BUYER_NAME" AS "BUYER_NAME",
    "omni_table_grp_prep_bodfs_bopis"."ARTICLE_CAT_NAME" AS "ARTICLE_CAT_NAME",
    "omni_table_grp_prep_bodfs_bopis"."LOB_DESC" AS "LOB_DESC",
    "omni_table_grp_prep_bodfs_bopis"."DEPT_NAME" AS "DEPT_NAME",
    "omni_table_grp_prep_bodfs_bopis"."VENDOR_NAME" AS "VENDOR_NAME",
    "omni_table_grp_prep_bodfs_bopis"."PARENT_VENDOR_NAME" AS "PARENT_VENDOR_NAME",
    "omni_table_grp_prep_bodfs_bopis"."omni_flow_path" AS "omni_flow_path",
    "omni_table_grp_prep_bodfs_bopis"."omni_flow_path_modified" AS "omni_flow_path_modified",
    "omni_table_grp_prep_bodfs_bopis"."SALES_QTY" AS "SALES_QTY",
    "omni_table_grp_prep_bodfs_bopis"."SALES_AMT" AS "SALES_AMT",
    "omni_table_grp_prep_bodfs_bopis"."COST_AMT" AS "COST_AMT",
    "omni_table_grp_prep_bodfs_bopis"."margin" AS "margin",
    "omni_table_grp_prep_bodfs_bopis"."total_shrink" AS "total_shrink",
    "omni_table_grp_prep_bodfs_bopis"."SHIPPING_REVENUE" AS "SHIPPING_REVENUE",
    "omni_table_grp_prep_bodfs_bopis"."SHIPPING_DISC" AS "SHIPPING_DISC",
    "omni_table_grp_prep_bodfs_bopis"."ACTUAL_SHIPPING_REVENUE" AS "ACTUAL_SHIPPING_REVENUE",
    "omni_table_grp_prep_bodfs_bopis"."omni_tsc_delivery" AS "omni_tsc_delivery",
    "omni_table_grp_prep_bodfs_bopis"."omni_small_package_ltl" AS "omni_small_package_ltl",
    "omni_table_grp_prep_bodfs_bopis"."omni_roadie" AS "omni_roadie",
    "omni_table_grp_prep_bodfs_bopis"."omni_multi_salvage" AS "omni_multi_salvage",
    "omni_table_grp_prep_bodfs_bopis"."volume_rebate" AS "volume_rebate",
    "omni_table_grp_prep_bodfs_bopis"."price_cuts" AS "price_cuts",
    "omni_table_grp_prep_bodfs_bopis"."scanbacks" AS "scanbacks",
    "omni_table_grp_prep_bodfs_bopis"."markdowns" AS "markdowns",
    "omni_table_grp_prep_bodfs_bopis"."landed_margin" AS "landed_margin",
    "omni_table_grp_prep_bodfs_bopis"."funding_in_lm" AS "funding_in_lm",
    "omni_table_grp_prep_bodfs_bopis"."total_funding" AS "total_funding",
    "omni_table_grp_prep_bodfs_bopis"."last_mile_cost" AS "last_mile_cost",
    "omni_table_grp_prep_bodfs_bopis"."last_mile_delivery_combined" AS "last_mile_delivery_combined",
    "omni_table_grp_prep_bodfs_bopis"."small_parcel_ltl_cost" AS "small_parcel_ltl_cost",
    "omni_table_grp_prep_bodfs_bopis"."omni_freight" AS "omni_freight",
    "omni_table_grp_prep_bodfs_bopis"."vendor_support" AS "vendor_support",
    "omni_table_grp_prep_bodfs_bopis"."vendor_support_fast" AS "vendor_support_fast",
    "DC_3PL_allocation_stacked_store_channel"."DC3PL_total_cost_sum" AS "DC3PL_total_cost"
  FROM "OMNI_TABLE_GRP_PREP_BODFS_BOPIS" "omni_table_grp_prep_bodfs_bopis"
  LEFT JOIN "DC_3PL_ALLOCATION_STACKED_STORE_CHANNEL" "DC_3PL_allocation_stacked_store_channel"
    ON ("omni_table_grp_prep_bodfs_bopis"."ARTICLE_NO" = "DC_3PL_allocation_stacked_store_channel"."ARTICLE_NO")
      AND ("omni_table_grp_prep_bodfs_bopis"."MERCH_YEAR" = "DC_3PL_allocation_stacked_store_channel"."MERCH_YEAR")
      AND ("omni_table_grp_prep_bodfs_bopis"."VENDOR_ID" = "DC_3PL_allocation_stacked_store_channel"."VENDOR_ID")
      AND ("omni_table_grp_prep_bodfs_bopis"."YEAR_PERIOD" = "DC_3PL_allocation_stacked_store_channel"."YEAR_PERIOD")
      AND ("omni_table_grp_prep_bodfs_bopis"."ARTICLE_NO" = "DC_3PL_allocation_stacked_store_channel"."MERCH_YEAR");

<br>
	--OMNI_TABLE_GRP_SHIPPED_DC3PL
</br>

CREATE OR REPLACE TEMP TABLE OMNI_TABLE_GRP_SHIPPED_DC3PL
AS
SELECT 
    "omni_table_grp_shipped"."ARTICLE_NO" AS "ARTICLE_NO",
    "omni_table_grp_shipped"."MERCH_YEAR" AS "MERCH_YEAR",
    "omni_table_grp_shipped"."VENDOR_ID" AS "VENDOR_ID",
    "omni_table_grp_shipped"."YEAR_PERIOD" AS "YEAR_PERIOD",
    "omni_table_grp_shipped"."Period" AS "Period",
    "omni_table_grp_shipped"."ARTICLE_DESC" AS "ARTICLE_DESC",
    "omni_table_grp_shipped"."ARTICLE_SIZE" AS "ARTICLE_SIZE",
    "omni_table_grp_shipped"."MAJ_DEPT_DESC" AS "MAJ_DEPT_DESC",
    "omni_table_grp_shipped"."BRAND_NAME" AS "BRAND_NAME",
    "omni_table_grp_shipped"."BUYER_CODE" AS "BUYER_CODE",
    "omni_table_grp_shipped"."ARTICLE_COLOR" AS "ARTICLE_COLOR",
    "omni_table_grp_shipped"."PARENT_ARTICLE_NO" AS "PARENT_ARTICLE_NO",
    "omni_table_grp_shipped"."PARENT_ARTICLE_DESC" AS "PARENT_ARTICLE_DESC",
    "omni_table_grp_shipped"."ARTICLE_FULL" AS "ARTICLE_FULL",
    "omni_table_grp_shipped"."BUYER_NAME" AS "BUYER_NAME",
    "omni_table_grp_shipped"."ARTICLE_CAT_NAME" AS "ARTICLE_CAT_NAME",
    "omni_table_grp_shipped"."LOB_DESC" AS "LOB_DESC",
    "omni_table_grp_shipped"."DEPT_NAME" AS "DEPT_NAME",
    "omni_table_grp_shipped"."VENDOR_NAME" AS "VENDOR_NAME",
    "omni_table_grp_shipped"."PARENT_VENDOR_NAME" AS "PARENT_VENDOR_NAME",
    "omni_table_grp_shipped"."omni_flow_path" AS "omni_flow_path",
    "omni_table_grp_shipped"."omni_flow_path_modified" AS "omni_flow_path_modified",
    "omni_table_grp_shipped"."SALES_QTY" AS "SALES_QTY",
    "omni_table_grp_shipped"."SALES_AMT" AS "SALES_AMT",
    "omni_table_grp_shipped"."COST_AMT" AS "COST_AMT",
    "omni_table_grp_shipped"."margin" AS "margin",
    "omni_table_grp_shipped"."total_shrink" AS "total_shrink",
    "omni_table_grp_shipped"."SHIPPING_REVENUE" AS "SHIPPING_REVENUE",
    "omni_table_grp_shipped"."SHIPPING_DISC" AS "SHIPPING_DISC",
    "omni_table_grp_shipped"."ACTUAL_SHIPPING_REVENUE" AS "ACTUAL_SHIPPING_REVENUE",
    "omni_table_grp_shipped"."omni_tsc_delivery" AS "omni_tsc_delivery",
    "omni_table_grp_shipped"."omni_small_package_ltl" AS "omni_small_package_ltl",
    "omni_table_grp_shipped"."omni_roadie" AS "omni_roadie",
    "omni_table_grp_shipped"."omni_multi_salvage" AS "omni_multi_salvage",
    "omni_table_grp_shipped"."volume_rebate" AS "volume_rebate",
    "omni_table_grp_shipped"."price_cuts" AS "price_cuts",
    "omni_table_grp_shipped"."scanbacks" AS "scanbacks",
    "omni_table_grp_shipped"."markdowns" AS "markdowns",
    "omni_table_grp_shipped"."landed_margin" AS "landed_margin",
    "omni_table_grp_shipped"."funding_in_lm" AS "funding_in_lm",
    "omni_table_grp_shipped"."total_funding" AS "total_funding",
    "omni_table_grp_shipped"."last_mile_cost" AS "last_mile_cost",
    "omni_table_grp_shipped"."last_mile_delivery_combined" AS "last_mile_delivery_combined",
    "omni_table_grp_shipped"."small_parcel_ltl_cost" AS "small_parcel_ltl_cost",
    "omni_table_grp_shipped"."omni_freight" AS "omni_freight",
    "omni_table_grp_shipped"."vendor_support" AS "vendor_support",
    "omni_table_grp_shipped"."vendor_support_fast" AS "vendor_support_fast",
    "allocation_DC_3PL_omni_fulfillment_total_shipped"."DC3PL_total_cost" AS "DC3PL_total_cost"
  FROM (
    SELECT 
        "ARTICLE_NO" AS "ARTICLE_NO",
        "MERCH_YEAR" AS "MERCH_YEAR",
        "VENDOR_ID" AS "VENDOR_ID",
        "YEAR_PERIOD" AS "YEAR_PERIOD",
        "Period" AS "Period",
        "ARTICLE_DESC" AS "ARTICLE_DESC",
        "ARTICLE_SIZE" AS "ARTICLE_SIZE",
        "MAJ_DEPT_DESC" AS "MAJ_DEPT_DESC",
        "BRAND_NAME" AS "BRAND_NAME",
        "BUYER_CODE" AS "BUYER_CODE",
        "ARTICLE_COLOR" AS "ARTICLE_COLOR",
        "PARENT_ARTICLE_NO" AS "PARENT_ARTICLE_NO",
        "PARENT_ARTICLE_DESC" AS "PARENT_ARTICLE_DESC",
        "ARTICLE_FULL" AS "ARTICLE_FULL",
        "BUYER_NAME" AS "BUYER_NAME",
        "ARTICLE_CAT_NAME" AS "ARTICLE_CAT_NAME",
        "LOB_DESC" AS "LOB_DESC",
        "DEPT_NAME" AS "DEPT_NAME",
        "VENDOR_NAME" AS "VENDOR_NAME",
        "PARENT_VENDOR_NAME" AS "PARENT_VENDOR_NAME",
        "omni_flow_path" AS "omni_flow_path",
        "omni_flow_path_modified" AS "omni_flow_path_modified",
        "SALES_QTY" AS "SALES_QTY",
        "SALES_AMT" AS "SALES_AMT",
        "COST_AMT" AS "COST_AMT",
        "margin" AS "margin",
        "total_shrink" AS "total_shrink",
        "SHIPPING_REVENUE" AS "SHIPPING_REVENUE",
        "SHIPPING_DISC" AS "SHIPPING_DISC",
        "ACTUAL_SHIPPING_REVENUE" AS "ACTUAL_SHIPPING_REVENUE",
        "omni_tsc_delivery" AS "omni_tsc_delivery",
        "omni_small_package_ltl" AS "omni_small_package_ltl",
        "omni_roadie" AS "omni_roadie",
        "omni_multi_salvage" AS "omni_multi_salvage",
        "volume_rebate" AS "volume_rebate",
        "price_cuts" AS "price_cuts",
        "scanbacks" AS "scanbacks",
        "markdowns" AS "markdowns",
        "landed_margin" AS "landed_margin",
        "funding_in_lm" AS "funding_in_lm",
        "total_funding" AS "total_funding",
        "last_mile_cost" AS "last_mile_cost",
        "last_mile_delivery_combined" AS "last_mile_delivery_combined",
        "small_parcel_ltl_cost" AS "small_parcel_ltl_cost",
        "omni_freight" AS "omni_freight",
        "vendor_support" AS "vendor_support",
        "vendor_support_fast" AS "vendor_support_fast",
        CASE WHEN "omni_flow_path_modified" LIKE CONCAT('%', IFNULL(TO_VARCHAR(REPLACE(REPLACE(REPLACE('Vendor', '\\', '\\\\'), '%', '\\%'), '_', '\\_')), ''), '%') ESCAPE '\\' THEN 0 ELSE 1 END AS "omni_ship_flag_exclude_vendor"
      FROM (
        SELECT *
          FROM "OMNI_TABLE_GRP_SHIPPED" "omni_table_grp_shipped"
        ) "withoutcomputedcols_query"
    ) "omni_table_grp_shipped"
  LEFT JOIN (
    SELECT 
        "MERCH_YEAR" AS "MERCH_YEAR",
        "YEAR_PERIOD" AS "YEAR_PERIOD",
        "OMS_ITEM_ID" AS "OMS_ITEM_ID",
        "OMS_ITEM_PRIMARY_VENDOR_ID" AS "OMS_ITEM_PRIMARY_VENDOR_ID",
        "PERIOD" AS "PERIOD",
        "DC3PL_total_cost" AS "DC3PL_total_cost",
        1 AS "omni_ship_flag"
      FROM (
        SELECT *
          FROM "ALLOCATION_DC_3PL_OMNI_FULFILLMENT_TOTAL_SHIPPED" "allocation_DC_3PL_omni_fulfillment_total_shipped"
        ) "withoutcomputedcols_query"
    ) "allocation_DC_3PL_omni_fulfillment_total_shipped"
    ON ("omni_table_grp_shipped"."MERCH_YEAR" = "allocation_DC_3PL_omni_fulfillment_total_shipped"."MERCH_YEAR")
      AND ("omni_table_grp_shipped"."YEAR_PERIOD" = "allocation_DC_3PL_omni_fulfillment_total_shipped"."YEAR_PERIOD")
      AND ("omni_table_grp_shipped"."ARTICLE_NO" = "allocation_DC_3PL_omni_fulfillment_total_shipped"."OMS_ITEM_ID")
      AND ("omni_table_grp_shipped"."VENDOR_ID" = "allocation_DC_3PL_omni_fulfillment_total_shipped"."OMS_ITEM_PRIMARY_VENDOR_ID")
      AND ("omni_table_grp_shipped"."Period" = "allocation_DC_3PL_omni_fulfillment_total_shipped"."PERIOD")
      AND ("omni_table_grp_shipped"."omni_ship_flag_exclude_vendor" = "allocation_DC_3PL_omni_fulfillment_total_shipped"."omni_ship_flag");

<br>
	--OMNI_TABLE_GRP_SHIPPED_DC3PL_PREPARED
</br>

CREATE OR REPLACE TEMP TABLE OMNI_TABLE_GRP_SHIPPED_DC3PL_PREPARED
AS
SELECT 
    "ARTICLE_NO",
    "MERCH_YEAR",
    "VENDOR_ID",
    "YEAR_PERIOD",
    "Period",
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
    "omni_flow_path",
    "omni_flow_path_modified",
    "SALES_QTY",
    "SALES_AMT",
    "COST_AMT",
    "margin",
    "total_shrink",
    "SHIPPING_REVENUE",
    "SHIPPING_DISC",
    "ACTUAL_SHIPPING_REVENUE",
    "omni_tsc_delivery",
    "omni_small_package_ltl",
    "omni_roadie",
    "omni_multi_salvage",
    "volume_rebate",
    "price_cuts",
    "scanbacks",
    "markdowns",
    "landed_margin",
    "funding_in_lm",
    "total_funding",
    "last_mile_cost",
    "last_mile_delivery_combined",
    "small_parcel_ltl_cost",
    "omni_freight",
    "vendor_support",
    "vendor_support_fast",
    "DC3PL_total_cost",
    CASE WHEN "omni_flow_path_modified" LIKE CONCAT('%', IFNULL(TO_VARCHAR(REPLACE(REPLACE(REPLACE('Vendor', '\\', '\\\\'), '%', '\\%'), '_', '\\_')), ''), '%') ESCAPE '\\' THEN 1 ELSE 0 END AS "omni_vendor_flag"
  FROM "OMNI_TABLE_GRP_SHIPPED_DC3PL" ;

<br>
	--OMNI_TABLE_GRP_SHIPPED_DC3PL_PREPARED_FILTERED
</br>

CREATE OR REPLACE TEMP TABLE OMNI_TABLE_GRP_SHIPPED_DC3PL_PREPARED_FILTERED
AS
SELECT 
    "ARTICLE_NO" AS "ARTICLE_NO",
    "MERCH_YEAR" AS "MERCH_YEAR",
    "VENDOR_ID" AS "VENDOR_ID",
    "YEAR_PERIOD" AS "YEAR_PERIOD",
    "Period" AS "Period",
    "ARTICLE_DESC" AS "ARTICLE_DESC",
    "ARTICLE_SIZE" AS "ARTICLE_SIZE",
    "MAJ_DEPT_DESC" AS "MAJ_DEPT_DESC",
    "BRAND_NAME" AS "BRAND_NAME",
    "BUYER_CODE" AS "BUYER_CODE",
    "ARTICLE_COLOR" AS "ARTICLE_COLOR",
    "PARENT_ARTICLE_NO" AS "PARENT_ARTICLE_NO",
    "PARENT_ARTICLE_DESC" AS "PARENT_ARTICLE_DESC",
    "ARTICLE_FULL" AS "ARTICLE_FULL",
    "BUYER_NAME" AS "BUYER_NAME",
    "ARTICLE_CAT_NAME" AS "ARTICLE_CAT_NAME",
    "LOB_DESC" AS "LOB_DESC",
    "DEPT_NAME" AS "DEPT_NAME",
    "VENDOR_NAME" AS "VENDOR_NAME",
    "PARENT_VENDOR_NAME" AS "PARENT_VENDOR_NAME",
    "omni_flow_path" AS "omni_flow_path",
    "omni_flow_path_modified" AS "omni_flow_path_modified",
    "omni_vendor_flag" AS "omni_vendor_flag",
    "SALES_QTY" AS "SALES_QTY",
    "SALES_AMT" AS "SALES_AMT",
    "COST_AMT" AS "COST_AMT",
    "margin" AS "margin",
    "total_shrink" AS "total_shrink",
    "SHIPPING_REVENUE" AS "SHIPPING_REVENUE",
    "SHIPPING_DISC" AS "SHIPPING_DISC",
    "ACTUAL_SHIPPING_REVENUE" AS "ACTUAL_SHIPPING_REVENUE",
    "omni_tsc_delivery" AS "omni_tsc_delivery",
    "omni_small_package_ltl" AS "omni_small_package_ltl",
    "omni_roadie" AS "omni_roadie",
    "omni_multi_salvage" AS "omni_multi_salvage",
    "volume_rebate" AS "volume_rebate",
    "price_cuts" AS "price_cuts",
    "scanbacks" AS "scanbacks",
    "markdowns" AS "markdowns",
    "landed_margin" AS "landed_margin",
    "funding_in_lm" AS "funding_in_lm",
    "total_funding" AS "total_funding",
    "last_mile_cost" AS "last_mile_cost",
    "last_mile_delivery_combined" AS "last_mile_delivery_combined",
    "small_parcel_ltl_cost" AS "small_parcel_ltl_cost",
    "omni_freight" AS "omni_freight",
    "vendor_support" AS "vendor_support",
    "vendor_support_fast" AS "vendor_support_fast",
    "DC3PL_total_cost" AS "DC3PL_total_cost"
  FROM "OMNI_TABLE_GRP_SHIPPED_DC3PL_PREPARED"
  WHERE "omni_vendor_flag" = 1;
<br>
	--OMNI_TABLE_GRP_SHIPPED_DC3PL_WINDOWS
</br>
CREATE OR REPLACE TEMP TABLE OMNI_TABLE_GRP_SHIPPED_DC3PL_WINDOWS
AS
SELECT 
    "ARTICLE_NO" AS "ARTICLE_NO",
    "MERCH_YEAR" AS "MERCH_YEAR",
    "VENDOR_ID" AS "VENDOR_ID",
    "YEAR_PERIOD" AS "YEAR_PERIOD",
    "Period" AS "Period",
    "ARTICLE_DESC" AS "ARTICLE_DESC",
    "ARTICLE_SIZE" AS "ARTICLE_SIZE",
    "MAJ_DEPT_DESC" AS "MAJ_DEPT_DESC",
    "BRAND_NAME" AS "BRAND_NAME",
    "BUYER_CODE" AS "BUYER_CODE",
    "ARTICLE_COLOR" AS "ARTICLE_COLOR",
    "PARENT_ARTICLE_NO" AS "PARENT_ARTICLE_NO",
    "PARENT_ARTICLE_DESC" AS "PARENT_ARTICLE_DESC",
    "ARTICLE_FULL" AS "ARTICLE_FULL",
    "BUYER_NAME" AS "BUYER_NAME",
    "ARTICLE_CAT_NAME" AS "ARTICLE_CAT_NAME",
    "LOB_DESC" AS "LOB_DESC",
    "DEPT_NAME" AS "DEPT_NAME",
    "VENDOR_NAME" AS "VENDOR_NAME",
    "PARENT_VENDOR_NAME" AS "PARENT_VENDOR_NAME",
    "omni_flow_path" AS "omni_flow_path",
    "omni_flow_path_modified" AS "omni_flow_path_modified",
    "omni_vendor_flag" AS "omni_vendor_flag",
    "SALES_QTY" AS "SALES_QTY",
    "SALES_AMT" AS "SALES_AMT",
    "COST_AMT" AS "COST_AMT",
    "margin" AS "margin",
    "total_shrink" AS "total_shrink",
    "SHIPPING_REVENUE" AS "SHIPPING_REVENUE",
    "SHIPPING_DISC" AS "SHIPPING_DISC",
    "ACTUAL_SHIPPING_REVENUE" AS "ACTUAL_SHIPPING_REVENUE",
    "omni_tsc_delivery" AS "omni_tsc_delivery",
    "omni_small_package_ltl" AS "omni_small_package_ltl",
    "omni_roadie" AS "omni_roadie",
    "omni_multi_salvage" AS "omni_multi_salvage",
    "volume_rebate" AS "volume_rebate",
    "price_cuts" AS "price_cuts",
    "scanbacks" AS "scanbacks",
    "markdowns" AS "markdowns",
    "landed_margin" AS "landed_margin",
    "funding_in_lm" AS "funding_in_lm",
    "total_funding" AS "total_funding",
    "last_mile_cost" AS "last_mile_cost",
    "last_mile_delivery_combined" AS "last_mile_delivery_combined",
    "small_parcel_ltl_cost" AS "small_parcel_ltl_cost",
    "omni_freight" AS "omni_freight",
    "vendor_support" AS "vendor_support",
    "vendor_support_fast" AS "vendor_support_fast",
    "DC3PL_total_cost" AS "DC3PL_total_cost",
    SUM("SALES_QTY") OVER (PARTITION BY "ARTICLE_NO", "MERCH_YEAR", "VENDOR_ID", "YEAR_PERIOD", "Period") AS "SALES_QTY_sum"
  FROM "OMNI_TABLE_GRP_SHIPPED_DC3PL_PREPARED"
  WHERE "omni_vendor_flag" = 0;
<br>
	--OMNI_TABLE_GRP_SHIPPED_DC3PL_STACKED
</br>

   CREATE OR REPLACE TEMP TABLE OMNI_TABLE_GRP_SHIPPED_DC3PL_STACKED
   AS
   WITH  OMNI_TABLE_GRP_SHIPPED_DC3PL_WINDOWS_STACKED  AS
   (
   SELECT  
    "ARTICLE_NO" AS "ARTICLE_NO",
    "MERCH_YEAR" AS "MERCH_YEAR",
    "VENDOR_ID" AS "VENDOR_ID",
    "YEAR_PERIOD" AS "YEAR_PERIOD",
    "Period" AS "Period",
    "ARTICLE_DESC" AS "ARTICLE_DESC",
    "ARTICLE_SIZE" AS "ARTICLE_SIZE",
    "MAJ_DEPT_DESC" AS "MAJ_DEPT_DESC",
    "BRAND_NAME" AS "BRAND_NAME",
    "BUYER_CODE" AS "BUYER_CODE",
    "ARTICLE_COLOR" AS "ARTICLE_COLOR",
    "PARENT_ARTICLE_NO" AS "PARENT_ARTICLE_NO",
    "PARENT_ARTICLE_DESC" AS "PARENT_ARTICLE_DESC",
    "ARTICLE_FULL" AS "ARTICLE_FULL",
    "BUYER_NAME" AS "BUYER_NAME",
    "ARTICLE_CAT_NAME" AS "ARTICLE_CAT_NAME",
    "LOB_DESC" AS "LOB_DESC",
    "DEPT_NAME" AS "DEPT_NAME",
    "VENDOR_NAME" AS "VENDOR_NAME",
    "PARENT_VENDOR_NAME" AS "PARENT_VENDOR_NAME",
    "omni_flow_path" AS "omni_flow_path",
    "omni_flow_path_modified" AS "omni_flow_path_modified",
    "omni_vendor_flag" AS "omni_vendor_flag",
    "SALES_QTY" AS "SALES_QTY",
    "SALES_AMT" AS "SALES_AMT",
    "COST_AMT" AS "COST_AMT",
    "margin" AS "margin",
    "total_shrink" AS "total_shrink",
    "SHIPPING_REVENUE" AS "SHIPPING_REVENUE",
    "SHIPPING_DISC" AS "SHIPPING_DISC",
    "ACTUAL_SHIPPING_REVENUE" AS "ACTUAL_SHIPPING_REVENUE",
    "omni_tsc_delivery" AS "omni_tsc_delivery",
    "omni_small_package_ltl" AS "omni_small_package_ltl",
    "omni_roadie" AS "omni_roadie",
    "omni_multi_salvage" AS "omni_multi_salvage",
    "volume_rebate" AS "volume_rebate",
    "price_cuts" AS "price_cuts",
    "scanbacks" AS "scanbacks",
    "markdowns" AS "markdowns",
    "landed_margin" AS "landed_margin",
    "funding_in_lm" AS "funding_in_lm",
    "total_funding" AS "total_funding",
    "last_mile_cost" AS "last_mile_cost",
    "last_mile_delivery_combined" AS "last_mile_delivery_combined",
    "small_parcel_ltl_cost" AS "small_parcel_ltl_cost",
    "omni_freight" AS "omni_freight",
    "vendor_support" AS "vendor_support",
    "vendor_support_fast" AS "vendor_support_fast",
    "DC3PL_total_cost" AS "DC3PL_total_cost",
    "SALES_QTY_sum" AS "SALES_QTY_sum",
    NULL AS "abs_sales_qty_sum"
  FROM "OMNI_TABLE_GRP_SHIPPED_DC3PL_WINDOWS"
  WHERE "SALES_QTY_sum" != 0 OR "SALES_QTY_sum" IS NULL AND 0 IS NOT NULL OR "SALES_QTY_sum" IS NOT NULL AND 0 IS NULL
  UNION ALL 
  SELECT 
    "ARTICLE_NO" AS "ARTICLE_NO",
    "MERCH_YEAR" AS "MERCH_YEAR",
    "VENDOR_ID" AS "VENDOR_ID",
    "YEAR_PERIOD" AS "YEAR_PERIOD",
    "Period" AS "Period",
    "ARTICLE_DESC" AS "ARTICLE_DESC",
    "ARTICLE_SIZE" AS "ARTICLE_SIZE",
    "MAJ_DEPT_DESC" AS "MAJ_DEPT_DESC",
    "BRAND_NAME" AS "BRAND_NAME",
    "BUYER_CODE" AS "BUYER_CODE",
    "ARTICLE_COLOR" AS "ARTICLE_COLOR",
    "PARENT_ARTICLE_NO" AS "PARENT_ARTICLE_NO",
    "PARENT_ARTICLE_DESC" AS "PARENT_ARTICLE_DESC",
    "ARTICLE_FULL" AS "ARTICLE_FULL",
    "BUYER_NAME" AS "BUYER_NAME",
    "ARTICLE_CAT_NAME" AS "ARTICLE_CAT_NAME",
    "LOB_DESC" AS "LOB_DESC",
    "DEPT_NAME" AS "DEPT_NAME",
    "VENDOR_NAME" AS "VENDOR_NAME",
    "PARENT_VENDOR_NAME" AS "PARENT_VENDOR_NAME",
    "omni_flow_path" AS "omni_flow_path",
    "omni_flow_path_modified" AS "omni_flow_path_modified",
    "omni_vendor_flag" AS "omni_vendor_flag",
    "SALES_QTY" AS "SALES_QTY",
    "SALES_AMT" AS "SALES_AMT",
    "COST_AMT" AS "COST_AMT",
    "margin" AS "margin",
    "total_shrink" AS "total_shrink",
    "SHIPPING_REVENUE" AS "SHIPPING_REVENUE",
    "SHIPPING_DISC" AS "SHIPPING_DISC",
    "ACTUAL_SHIPPING_REVENUE" AS "ACTUAL_SHIPPING_REVENUE",
    "omni_tsc_delivery" AS "omni_tsc_delivery",
    "omni_small_package_ltl" AS "omni_small_package_ltl",
    "omni_roadie" AS "omni_roadie",
    "omni_multi_salvage" AS "omni_multi_salvage",
    "volume_rebate" AS "volume_rebate",
    "price_cuts" AS "price_cuts",
    "scanbacks" AS "scanbacks",
    "markdowns" AS "markdowns",
    "landed_margin" AS "landed_margin",
    "funding_in_lm" AS "funding_in_lm",
    "total_funding" AS "total_funding",
    "last_mile_cost" AS "last_mile_cost",
    "last_mile_delivery_combined" AS "last_mile_delivery_combined",
    "small_parcel_ltl_cost" AS "small_parcel_ltl_cost",
    "omni_freight" AS "omni_freight",
    "vendor_support" AS "vendor_support",
    "vendor_support_fast" AS "vendor_support_fast",
    "DC3PL_total_cost" AS "DC3PL_total_cost",
    "SALES_QTY_sum" AS "SALES_QTY_sum",
    SUM("abs_sales_qty") OVER (PARTITION BY "ARTICLE_NO", "MERCH_YEAR", "VENDOR_ID", "YEAR_PERIOD", "Period") AS "abs_sales_qty_sum"
  FROM (
    SELECT DISTINCT
        "ARTICLE_NO" AS "ARTICLE_NO",
        "MERCH_YEAR" AS "MERCH_YEAR",
        "VENDOR_ID" AS "VENDOR_ID",
        "YEAR_PERIOD" AS "YEAR_PERIOD",
        "Period" AS "Period",
        "ARTICLE_DESC" AS "ARTICLE_DESC",
        "ARTICLE_SIZE" AS "ARTICLE_SIZE",
        "MAJ_DEPT_DESC" AS "MAJ_DEPT_DESC",
        "BRAND_NAME" AS "BRAND_NAME",
        "BUYER_CODE" AS "BUYER_CODE",
        "ARTICLE_COLOR" AS "ARTICLE_COLOR",
        "PARENT_ARTICLE_NO" AS "PARENT_ARTICLE_NO",
        "PARENT_ARTICLE_DESC" AS "PARENT_ARTICLE_DESC",
        "ARTICLE_FULL" AS "ARTICLE_FULL",
        "BUYER_NAME" AS "BUYER_NAME",
        "ARTICLE_CAT_NAME" AS "ARTICLE_CAT_NAME",
        "LOB_DESC" AS "LOB_DESC",
        "DEPT_NAME" AS "DEPT_NAME",
        "VENDOR_NAME" AS "VENDOR_NAME",
        "PARENT_VENDOR_NAME" AS "PARENT_VENDOR_NAME",
        "omni_flow_path" AS "omni_flow_path",
        "omni_flow_path_modified" AS "omni_flow_path_modified",
        "omni_vendor_flag" AS "omni_vendor_flag",
        "SALES_QTY" AS "SALES_QTY",
        "SALES_AMT" AS "SALES_AMT",
        "COST_AMT" AS "COST_AMT",
        "margin" AS "margin",
        "total_shrink" AS "total_shrink",
        "SHIPPING_REVENUE" AS "SHIPPING_REVENUE",
        "SHIPPING_DISC" AS "SHIPPING_DISC",
        "ACTUAL_SHIPPING_REVENUE" AS "ACTUAL_SHIPPING_REVENUE",
        "omni_tsc_delivery" AS "omni_tsc_delivery",
        "omni_small_package_ltl" AS "omni_small_package_ltl",
        "omni_roadie" AS "omni_roadie",
        "omni_multi_salvage" AS "omni_multi_salvage",
        "volume_rebate" AS "volume_rebate",
        "price_cuts" AS "price_cuts",
        "scanbacks" AS "scanbacks",
        "markdowns" AS "markdowns",
        "landed_margin" AS "landed_margin",
        "funding_in_lm" AS "funding_in_lm",
        "total_funding" AS "total_funding",
        "last_mile_cost" AS "last_mile_cost",
        "last_mile_delivery_combined" AS "last_mile_delivery_combined",
        "small_parcel_ltl_cost" AS "small_parcel_ltl_cost",
        "omni_freight" AS "omni_freight",
        "vendor_support" AS "vendor_support",
        "vendor_support_fast" AS "vendor_support_fast",
        "DC3PL_total_cost" AS "DC3PL_total_cost",
        "SALES_QTY_sum" AS "SALES_QTY_sum",
        CASE WHEN "SALES_QTY_sum" = 0 THEN ABS("SALES_QTY") ELSE "SALES_QTY" END AS "abs_sales_qty"
      FROM (
        SELECT *
          FROM "OMNI_TABLE_GRP_SHIPPED_DC3PL_WINDOWS"
          WHERE "SALES_QTY_sum" = 0
        ) B
    ) A
    )
SELECT 
    "ARTICLE_NO" AS "ARTICLE_NO",
    "MERCH_YEAR" AS "MERCH_YEAR",
    "VENDOR_ID" AS "VENDOR_ID",
    "YEAR_PERIOD" AS "YEAR_PERIOD",
    "Period" AS "Period",
    "ARTICLE_DESC" AS "ARTICLE_DESC",
    "ARTICLE_SIZE" AS "ARTICLE_SIZE",
    "MAJ_DEPT_DESC" AS "MAJ_DEPT_DESC",
    "BRAND_NAME" AS "BRAND_NAME",
    "BUYER_CODE" AS "BUYER_CODE",
    "ARTICLE_COLOR" AS "ARTICLE_COLOR",
    "PARENT_ARTICLE_NO" AS "PARENT_ARTICLE_NO",
    "PARENT_ARTICLE_DESC" AS "PARENT_ARTICLE_DESC",
    "ARTICLE_FULL" AS "ARTICLE_FULL",
    "BUYER_NAME" AS "BUYER_NAME",
    "ARTICLE_CAT_NAME" AS "ARTICLE_CAT_NAME",
    "LOB_DESC" AS "LOB_DESC",
    "DEPT_NAME" AS "DEPT_NAME",
    "VENDOR_NAME" AS "VENDOR_NAME",
    "PARENT_VENDOR_NAME" AS "PARENT_VENDOR_NAME",
    "omni_flow_path" AS "omni_flow_path",
    "omni_flow_path_modified" AS "omni_flow_path_modified",
    "omni_vendor_flag" AS "omni_vendor_flag",
    "SALES_QTY" AS "SALES_QTY",
    "SALES_AMT" AS "SALES_AMT",
    "COST_AMT" AS "COST_AMT",
    "margin" AS "margin",
    "total_shrink" AS "total_shrink",
    "SHIPPING_REVENUE" AS "SHIPPING_REVENUE",
    "SHIPPING_DISC" AS "SHIPPING_DISC",
    "ACTUAL_SHIPPING_REVENUE" AS "ACTUAL_SHIPPING_REVENUE",
    "omni_tsc_delivery" AS "omni_tsc_delivery",
    "omni_small_package_ltl" AS "omni_small_package_ltl",
    "omni_roadie" AS "omni_roadie",
    "omni_multi_salvage" AS "omni_multi_salvage",
    "volume_rebate" AS "volume_rebate",
    "price_cuts" AS "price_cuts",
    "scanbacks" AS "scanbacks",
    "markdowns" AS "markdowns",
    "landed_margin" AS "landed_margin",
    "funding_in_lm" AS "funding_in_lm",
    "total_funding" AS "total_funding",
    "last_mile_cost" AS "last_mile_cost",
    "last_mile_delivery_combined" AS "last_mile_delivery_combined",
    "small_parcel_ltl_cost" AS "small_parcel_ltl_cost",
    "omni_freight" AS "omni_freight",
    "vendor_support" AS "vendor_support",
    "vendor_support_fast" AS "vendor_support_fast",
    "DC3PL_total_cost" AS "DC3PL_total_cost",
    NULL AS "SALES_QTY_sum",
    NULL AS "abs_sales_qty_sum"
  FROM "OMNI_TABLE_GRP_SHIPPED_DC3PL_PREPARED_FILTERED"
UNION ALL
SELECT 
    "ARTICLE_NO" AS "ARTICLE_NO",
    "MERCH_YEAR" AS "MERCH_YEAR",
    "VENDOR_ID" AS "VENDOR_ID",
    "YEAR_PERIOD" AS "YEAR_PERIOD",
    "Period" AS "Period",
    "ARTICLE_DESC" AS "ARTICLE_DESC",
    "ARTICLE_SIZE" AS "ARTICLE_SIZE",
    "MAJ_DEPT_DESC" AS "MAJ_DEPT_DESC",
    "BRAND_NAME" AS "BRAND_NAME",
    "BUYER_CODE" AS "BUYER_CODE",
    "ARTICLE_COLOR" AS "ARTICLE_COLOR",
    "PARENT_ARTICLE_NO" AS "PARENT_ARTICLE_NO",
    "PARENT_ARTICLE_DESC" AS "PARENT_ARTICLE_DESC",
    "ARTICLE_FULL" AS "ARTICLE_FULL",
    "BUYER_NAME" AS "BUYER_NAME",
    "ARTICLE_CAT_NAME" AS "ARTICLE_CAT_NAME",
    "LOB_DESC" AS "LOB_DESC",
    "DEPT_NAME" AS "DEPT_NAME",
    "VENDOR_NAME" AS "VENDOR_NAME",
    "PARENT_VENDOR_NAME" AS "PARENT_VENDOR_NAME",
    "omni_flow_path" AS "omni_flow_path",
    "omni_flow_path_modified" AS "omni_flow_path_modified",
    "omni_vendor_flag" AS "omni_vendor_flag",
    "SALES_QTY" AS "SALES_QTY",
    "SALES_AMT" AS "SALES_AMT",
    "COST_AMT" AS "COST_AMT",
    "margin" AS "margin",
    "total_shrink" AS "total_shrink",
    "SHIPPING_REVENUE" AS "SHIPPING_REVENUE",
    "SHIPPING_DISC" AS "SHIPPING_DISC",
    "ACTUAL_SHIPPING_REVENUE" AS "ACTUAL_SHIPPING_REVENUE",
    "omni_tsc_delivery" AS "omni_tsc_delivery",
    "omni_small_package_ltl" AS "omni_small_package_ltl",
    "omni_roadie" AS "omni_roadie",
    "omni_multi_salvage" AS "omni_multi_salvage",
    "volume_rebate" AS "volume_rebate",
    "price_cuts" AS "price_cuts",
    "scanbacks" AS "scanbacks",
    "markdowns" AS "markdowns",
    "landed_margin" AS "landed_margin",
    "funding_in_lm" AS "funding_in_lm",
    "total_funding" AS "total_funding",
    "last_mile_cost" AS "last_mile_cost",
    "last_mile_delivery_combined" AS "last_mile_delivery_combined",
    "small_parcel_ltl_cost" AS "small_parcel_ltl_cost",
    "omni_freight" AS "omni_freight",
    "vendor_support" AS "vendor_support",
    "vendor_support_fast" AS "vendor_support_fast",
    "DC3PL_total_cost" AS "DC3PL_total_cost",
    "SALES_QTY_sum" AS "SALES_QTY_sum",
    "abs_sales_qty_sum" AS "abs_sales_qty_sum"
  FROM "OMNI_TABLE_GRP_SHIPPED_DC3PL_WINDOWS_STACKED";

<br>
	--OMNI_TABLE_GRP_SHIPPED_DC3PL_WINDOWS_PREPARED
</br>
 CREATE OR REPLACE TEMP TABLE OMNI_TABLE_GRP_SHIPPED_DC3PL_WINDOWS_PREPARED
 AS
 WITH CTE AS (
    SELECT 
        "ARTICLE_NO" AS "ARTICLE_NO",
        "MERCH_YEAR" AS "MERCH_YEAR",
        "VENDOR_ID" AS "VENDOR_ID",
        "YEAR_PERIOD" AS "YEAR_PERIOD",
        "Period" AS "Period",
        "ARTICLE_DESC" AS "ARTICLE_DESC",
        "ARTICLE_SIZE" AS "ARTICLE_SIZE",
        "MAJ_DEPT_DESC" AS "MAJ_DEPT_DESC",
        "BRAND_NAME" AS "BRAND_NAME",
        "BUYER_CODE" AS "BUYER_CODE",
        "ARTICLE_COLOR" AS "ARTICLE_COLOR",
        "PARENT_ARTICLE_NO" AS "PARENT_ARTICLE_NO",
        "PARENT_ARTICLE_DESC" AS "PARENT_ARTICLE_DESC",
        "ARTICLE_FULL" AS "ARTICLE_FULL",
        "BUYER_NAME" AS "BUYER_NAME",
        "ARTICLE_CAT_NAME" AS "ARTICLE_CAT_NAME",
        "LOB_DESC" AS "LOB_DESC",
        "DEPT_NAME" AS "DEPT_NAME",
        "VENDOR_NAME" AS "VENDOR_NAME",
        "PARENT_VENDOR_NAME" AS "PARENT_VENDOR_NAME",
        "omni_flow_path" AS "omni_flow_path",
        "omni_flow_path_modified" AS "omni_flow_path_modified",
        "omni_vendor_flag" AS "omni_vendor_flag",
        "SALES_QTY" AS "SALES_QTY",
        "SALES_AMT" AS "SALES_AMT",
        "COST_AMT" AS "COST_AMT",
        "margin" AS "margin",
        "total_shrink" AS "total_shrink",
        "SHIPPING_REVENUE" AS "SHIPPING_REVENUE",
        "SHIPPING_DISC" AS "SHIPPING_DISC",
        "ACTUAL_SHIPPING_REVENUE" AS "ACTUAL_SHIPPING_REVENUE",
        "omni_tsc_delivery" AS "omni_tsc_delivery",
        "omni_small_package_ltl" AS "omni_small_package_ltl",
        "omni_roadie" AS "omni_roadie",
        "omni_multi_salvage" AS "omni_multi_salvage",
        "volume_rebate" AS "volume_rebate",
        "price_cuts" AS "price_cuts",
        "scanbacks" AS "scanbacks",
        "markdowns" AS "markdowns",
        "landed_margin" AS "landed_margin",
        "funding_in_lm" AS "funding_in_lm",
        "total_funding" AS "total_funding",
        "last_mile_cost" AS "last_mile_cost",
        "last_mile_delivery_combined" AS "last_mile_delivery_combined",
        "small_parcel_ltl_cost" AS "small_parcel_ltl_cost",
        "omni_freight" AS "omni_freight",
        "vendor_support" AS "vendor_support",
        "vendor_support_fast" AS "vendor_support_fast",
        "SALES_QTY_sum" AS "SALES_QTY_sum",
        "abs_sales_qty_sum" AS "abs_sales_qty_sum",
        "DC3PL_total_cost_new" AS "DC3PL_total_cost"
      FROM (
        SELECT 
            "ARTICLE_NO",
            "MERCH_YEAR",
            "VENDOR_ID",
            "YEAR_PERIOD",
            "Period",
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
            "omni_flow_path",
            "omni_flow_path_modified",
            "omni_vendor_flag",
            "SALES_QTY",
            "SALES_AMT",
            "COST_AMT",
            "margin",
            "total_shrink",
            "SHIPPING_REVENUE",
            "SHIPPING_DISC",
            "ACTUAL_SHIPPING_REVENUE",
            "omni_tsc_delivery",
            "omni_small_package_ltl",
            "omni_roadie",
            "omni_multi_salvage",
            "volume_rebate",
            "price_cuts",
            "scanbacks",
            "markdowns",
            "landed_margin",
            "funding_in_lm",
            "total_funding",
            "last_mile_cost",
            "last_mile_delivery_combined",
            "small_parcel_ltl_cost",
            "omni_freight",
            "vendor_support",
            "vendor_support_fast",
            "DC3PL_total_cost",
            "SALES_QTY_sum",
            "abs_sales_qty_sum",
            CASE WHEN "SALES_QTY_sum" != 0 OR "SALES_QTY_sum" IS NULL AND 0 IS NOT NULL OR "SALES_QTY_sum" IS NOT NULL AND 0 IS NULL THEN COALESCE("DC3PL_total_cost",0) * ("SALES_QTY" / NULLIF("SALES_QTY_sum", 0)) ELSE COALESCE("DC3PL_total_cost",0) * ("SALES_QTY" / NULLIF("abs_sales_qty_sum", 0)) END AS "DC3PL_total_cost_new"
          FROM "OMNI_TABLE_GRP_SHIPPED_DC3PL_STACKED"
        ) A
  )
  SELECT 
    "ARTICLE_NO" AS "ARTICLE_NO",
    "MERCH_YEAR" AS "MERCH_YEAR",
    "VENDOR_ID" AS "VENDOR_ID",
    "YEAR_PERIOD" AS "YEAR_PERIOD",
    "Period" AS "Period",
    "ARTICLE_DESC" AS "ARTICLE_DESC",
    "ARTICLE_SIZE" AS "ARTICLE_SIZE",
    "MAJ_DEPT_DESC" AS "MAJ_DEPT_DESC",
    "BRAND_NAME" AS "BRAND_NAME",
    "BUYER_CODE" AS "BUYER_CODE",
    "ARTICLE_COLOR" AS "ARTICLE_COLOR",
    "PARENT_ARTICLE_NO" AS "PARENT_ARTICLE_NO",
    "PARENT_ARTICLE_DESC" AS "PARENT_ARTICLE_DESC",
    "ARTICLE_FULL" AS "ARTICLE_FULL",
    "BUYER_NAME" AS "BUYER_NAME",
    "ARTICLE_CAT_NAME" AS "ARTICLE_CAT_NAME",
    "LOB_DESC" AS "LOB_DESC",
    "DEPT_NAME" AS "DEPT_NAME",
    "VENDOR_NAME" AS "VENDOR_NAME",
    "PARENT_VENDOR_NAME" AS "PARENT_VENDOR_NAME",
    "omni_flow_path" AS "omni_flow_path",
    "omni_flow_path_modified" AS "omni_flow_path_modified",
    "SALES_QTY" AS "SALES_QTY",
    "SALES_AMT" AS "SALES_AMT",
    "COST_AMT" AS "COST_AMT",
    "margin" AS "margin",
    "total_shrink" AS "total_shrink",
    "SHIPPING_REVENUE" AS "SHIPPING_REVENUE",
    "SHIPPING_DISC" AS "SHIPPING_DISC",
    "ACTUAL_SHIPPING_REVENUE" AS "ACTUAL_SHIPPING_REVENUE",
    "omni_tsc_delivery" AS "omni_tsc_delivery",
    "omni_small_package_ltl" AS "omni_small_package_ltl",
    "omni_roadie" AS "omni_roadie",
    "omni_multi_salvage" AS "omni_multi_salvage",
    "volume_rebate" AS "volume_rebate",
    "price_cuts" AS "price_cuts",
    "scanbacks" AS "scanbacks",
    "markdowns" AS "markdowns",
    "landed_margin" AS "landed_margin",
    "funding_in_lm" AS "funding_in_lm",
    "total_funding" AS "total_funding",
    "last_mile_cost" AS "last_mile_cost",
    "last_mile_delivery_combined" AS "last_mile_delivery_combined",
    "small_parcel_ltl_cost" AS "small_parcel_ltl_cost",
    "omni_freight" AS "omni_freight",
    "vendor_support" AS "vendor_support",
    "vendor_support_fast" AS "vendor_support_fast",
    "DC3PL_total_cost" AS "DC3PL_total_cost"
  FROM CTE;

<br>
	--truncating the table OMNI_TABLE
</br>
TRUNCATE TABLE OMNI_TABLE ;
<br>
	--INSERTING new data into table OMNI_TABLE
</br>
INSERT INTO  OMNI_TABLE
WITH OMNI_TABLE_GRP_DC3PL_STACKED AS 
(
SELECT 
    "ARTICLE_NO" AS "ARTICLE_NO",
    "MERCH_YEAR" AS "MERCH_YEAR",
    "VENDOR_ID" AS "VENDOR_ID",
    "YEAR_PERIOD" AS "YEAR_PERIOD",
    "Period" AS "Period",
    "ARTICLE_DESC" AS "ARTICLE_DESC",
    "ARTICLE_SIZE" AS "ARTICLE_SIZE",
    "MAJ_DEPT_DESC" AS "MAJ_DEPT_DESC",
    "BRAND_NAME" AS "BRAND_NAME",
    "BUYER_CODE" AS "BUYER_CODE",
    "ARTICLE_COLOR" AS "ARTICLE_COLOR",
    "PARENT_ARTICLE_NO" AS "PARENT_ARTICLE_NO",
    "PARENT_ARTICLE_DESC" AS "PARENT_ARTICLE_DESC",
    "ARTICLE_FULL" AS "ARTICLE_FULL",
    "BUYER_NAME" AS "BUYER_NAME",
    "ARTICLE_CAT_NAME" AS "ARTICLE_CAT_NAME",
    "LOB_DESC" AS "LOB_DESC",
    "DEPT_NAME" AS "DEPT_NAME",
    "VENDOR_NAME" AS "VENDOR_NAME",
    "PARENT_VENDOR_NAME" AS "PARENT_VENDOR_NAME",
    "omni_flow_path" AS "omni_flow_path",
    "omni_flow_path_modified" AS "omni_flow_path_modified",
    "SALES_QTY" AS "SALES_QTY",
    "SALES_AMT" AS "SALES_AMT",
    "COST_AMT" AS "COST_AMT",
    "margin" AS "margin",
    "total_shrink" AS "total_shrink",
    "SHIPPING_REVENUE" AS "SHIPPING_REVENUE",
    "SHIPPING_DISC" AS "SHIPPING_DISC",
    "ACTUAL_SHIPPING_REVENUE" AS "ACTUAL_SHIPPING_REVENUE",
    "omni_tsc_delivery" AS "omni_tsc_delivery",
    "omni_small_package_ltl" AS "omni_small_package_ltl",
    "omni_roadie" AS "omni_roadie",
    "omni_multi_salvage" AS "omni_multi_salvage",
    "volume_rebate" AS "volume_rebate",
    "price_cuts" AS "price_cuts",
    "scanbacks" AS "scanbacks",
    "markdowns" AS "markdowns",
    "landed_margin" AS "landed_margin",
    "funding_in_lm" AS "funding_in_lm",
    "total_funding" AS "total_funding",
    "last_mile_cost" AS "last_mile_cost",
    "last_mile_delivery_combined" AS "last_mile_delivery_combined",
    "small_parcel_ltl_cost" AS "small_parcel_ltl_cost",
    "omni_freight" AS "omni_freight",
    "vendor_support" AS "vendor_support",
    "vendor_support_fast" AS "vendor_support_fast",
    "DC3PL_total_cost" AS "DC3PL_total_cost"
  FROM "OMNI_TABLE_GRP_PREP_BODFS_BOPIS_DC3PL"
UNION ALL
SELECT DISTINCT
    "ARTICLE_NO" AS "ARTICLE_NO",
    "MERCH_YEAR" AS "MERCH_YEAR",
    "VENDOR_ID" AS "VENDOR_ID",
    "YEAR_PERIOD" AS "YEAR_PERIOD",
    "Period" AS "Period",
    "ARTICLE_DESC" AS "ARTICLE_DESC",
    "ARTICLE_SIZE" AS "ARTICLE_SIZE",
    "MAJ_DEPT_DESC" AS "MAJ_DEPT_DESC",
    "BRAND_NAME" AS "BRAND_NAME",
    "BUYER_CODE" AS "BUYER_CODE",
    "ARTICLE_COLOR" AS "ARTICLE_COLOR",
    "PARENT_ARTICLE_NO" AS "PARENT_ARTICLE_NO",
    "PARENT_ARTICLE_DESC" AS "PARENT_ARTICLE_DESC",
    "ARTICLE_FULL" AS "ARTICLE_FULL",
    "BUYER_NAME" AS "BUYER_NAME",
    "ARTICLE_CAT_NAME" AS "ARTICLE_CAT_NAME",
    "LOB_DESC" AS "LOB_DESC",
    "DEPT_NAME" AS "DEPT_NAME",
    "VENDOR_NAME" AS "VENDOR_NAME",
    "PARENT_VENDOR_NAME" AS "PARENT_VENDOR_NAME",
    "omni_flow_path" AS "omni_flow_path",
    "omni_flow_path_modified" AS "omni_flow_path_modified",
    "SALES_QTY" AS "SALES_QTY",
    "SALES_AMT" AS "SALES_AMT",
    "COST_AMT" AS "COST_AMT",
    "margin" AS "margin",
    "total_shrink" AS "total_shrink",
    "SHIPPING_REVENUE" AS "SHIPPING_REVENUE",
    "SHIPPING_DISC" AS "SHIPPING_DISC",
    "ACTUAL_SHIPPING_REVENUE" AS "ACTUAL_SHIPPING_REVENUE",
    "omni_tsc_delivery" AS "omni_tsc_delivery",
    "omni_small_package_ltl" AS "omni_small_package_ltl",
    "omni_roadie" AS "omni_roadie",
    "omni_multi_salvage" AS "omni_multi_salvage",
    "volume_rebate" AS "volume_rebate",
    "price_cuts" AS "price_cuts",
    "scanbacks" AS "scanbacks",
    "markdowns" AS "markdowns",
    "landed_margin" AS "landed_margin",
    "funding_in_lm" AS "funding_in_lm",
    "total_funding" AS "total_funding",
    "last_mile_cost" AS "last_mile_cost",
    "last_mile_delivery_combined" AS "last_mile_delivery_combined",
    "small_parcel_ltl_cost" AS "small_parcel_ltl_cost",
    "omni_freight" AS "omni_freight",
    "vendor_support" AS "vendor_support",
    "vendor_support_fast" AS "vendor_support_fast",
    "DC3PL_total_cost" AS "DC3PL_total_cost"
  FROM "OMNI_TABLE_GRP_SHIPPED_DC3PL_WINDOWS_PREPARED"
)
SELECT DISTINCT * FROM OMNI_TABLE_GRP_DC3PL_STACKED;

    
<br>
	--truncating the table DC_3PL_VARIABLE_COST_ALLOCATIOIN_CONTROL_PREPARED
</br>
TRUNCATE TABLE DC_3PL_VARIABLE_COST_ALLOCATIOIN_CONTROL_PREPARED;
<br>
	--INSERTING NEW DATA  DC_3PL_VARIABLE_COST_ALLOCATIOIN_CONTROL_PREPARED
</br>
INSERT INTO  DC_3PL_VARIABLE_COST_ALLOCATIOIN_CONTROL_PREPARED
SELECT  
 ALLOCATION_FILE
,GROUP_BY_PERIOD_1
,GROUP_BY_PERIOD_2
,L2_GROUP_BY_PERIOD_1
,L2_GROUP_BY_PERIOD_2
,GROUP_BY_1
,GROUP_BY_2
,GROUP_BY_3
,GROUP_BY_4
,GROUP_BY_5
,L2_GROUP_BY_1
,L2_GROUP_BY_2
,L2_GROUP_BY_3
,L2_GROUP_BY_4
,ALLOCATE_BY
,ALLOCATION_TYPE
,ROLLUP_BY_1
,ROLLUP_BY_2
FROM DC_3PL_variable_cost_allocatioin_control
WHERE allocate_flag ='Y';

<br>
	--truncating  DC_3PL_OMNI_FULFILLMENT_ALLOCATION_PREPARED
</br>

 TRUNCATE TABLE DC_3PL_OMNI_FULFILLMENT_ALLOCATIOIN_PREPARED ;
 
 <br>
	--INSERTING NEW DATA   DC_3PL_OMNI_FULFILLMENT_ALLOCATION_PREPARED
</br>

 INSERT INTO DC_3PL_OMNI_FULFILLMENT_ALLOCATIOIN_PREPARED
 SELECT  
 ALLOCATION_FILE
,GROUP_BY_PERIOD_1
,GROUP_BY_PERIOD_2
,L2_GROUP_BY_PERIOD_1
,L2_GROUP_BY_PERIOD_2
,GROUP_BY_1
,GROUP_BY_2
,GROUP_BY_3
,GROUP_BY_4
,GROUP_BY_5
,L2_GROUP_BY_1
,L2_GROUP_BY_2
,L2_GROUP_BY_3
,L2_GROUP_BY_4
,ALLOCATE_BY
,ALLOCATION_TYPE
,ROLLUP_BY_1
,ROLLUP_BY_2
 FROM  DC_3PL_OMNI_FULFILLMENT_ALLOCATIOIN
 WHERE allocate_flag ='Y';	

    







    

      
      




















  