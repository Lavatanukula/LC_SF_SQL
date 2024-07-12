<br> </br>
TRUNCATE TABLE FLOW_PATH_WITH_LATEST_COST_PREPARED;
<br> </br>
TRUNCATE TABLE MASTER_SALES_ROUND_P2;
<br> </br>
TRUNCATE TABLE DC_3PL_OMNI_FULFILLMENT_BASE_FINAL_SF;
<br> </br>
TRUNCATE TABLE ALLOCATION_W_INVENTORY_SHRINK_PREPARED_PRECOMPUTE;
<br> </br>
TRUNCATE TABLE ALLOCATION_W_INVENTORY_SHRINK_PREPARED_RENAMED;
<br> </br>
TRUNCATE TABLE allocation_control_pt2_sp;
<br> </br>
TRUNCATE TABLE DC_3PL_OMNI_fulfillment_allocatioin_prepared_sf;
<br>
--FLOW_PATH_WITH_LATEST_COST
</br>
CREATE OR REPLACE TEMPORARY TABLE "FLOW_PATH_WITH_LATEST_COST"
AS
SELECT DISTINCT
    fpal2."YEAR_PERIOD" AS YEAR_PERIOD,
    fpal2."ARTICLE_NO" AS ARTICLE_NO,
    fpal2."VENDOR_ID" AS VENDOR_ID,
    fpal2."leg" AS leg,
    fpal2."flow_path" AS flow_path,
    fpal2."import_flag" AS import_flag,
    fpal2."leg_1" AS leg_1,
    fpal2."order_qty" AS order_qty,
    fpal2."order_qty_sum" AS order_qty_sum,
    fpal2."share" AS share,
    fpal2."share_sum" AS share_sum,
    fpal2."leg_2" AS leg_2,
    fpal2."leg_3" AS leg_3,
    flc."last_unit_cost" AS leg_1_latest_unit_cost,
    flc_2."last_unit_cost" AS leg_2_latest_unit_cost,
    flc_3."last_unit_cost" AS leg_3_latest_unit_cost,
    afrp."last_agency_fee_rate" AS lastest_agency_fee_rate,
    drl."lastest_avg_per" AS latest_duty_rate
  FROM flow_path_agg_leg_2 fpal2
  LEFT JOIN freight_latest_cost flc
    ON (fpal2.YEAR_PERIOD = flc.YEAR_PERIOD)
      AND (fpal2."ARTICLE_NO" = flc.ARTICLE_NO)
      AND (fpal2."VENDOR_ID" = flc.VENDOR_ID)
      AND (upper(fpal2."leg_1") = upper(flc."leg"))
  LEFT JOIN freight_latest_cost flc_2
    ON (fpal2."YEAR_PERIOD" = flc_2.YEAR_PERIOD)
      AND (fpal2."ARTICLE_NO" = flc_2.ARTICLE_NO)
      AND (upper(fpal2."leg_2")= upper(flc_2."leg"))
  LEFT JOIN freight_latest_cost flc_3
    ON (fpal2.YEAR_PERIOD = flc_3.YEAR_PERIOD)
      AND (fpal2."ARTICLE_NO" = flc_3.ARTICLE_NO)
      AND (upper(fpal2."leg_3") = upper(flc_3."leg"))
  LEFT JOIN agency_fee_rate_prep afrp
    ON (fpal2.YEAR_PERIOD = afrp.YEAR_PERIOD)
      AND (fpal2."ARTICLE_NO" = afrp.ARTICLE_NO)
      AND (fpal2."VENDOR_ID" = afrp.VENDOR_ID)
      AND (fpal2."import_flag" = afrp.import_flag)
  LEFT JOIN duty_rate_latest drl
    ON (fpal2.YEAR_PERIOD = drl.YEAR_PERIOD)
      AND (fpal2."ARTICLE_NO" = drl.ARTICLE_NO)
      AND (fpal2."VENDOR_ID" = drl.VENDOR_ID)
      AND (fpal2."import_flag" = coalesce(drl.import_flag,0));

<br>
--FLOW_PATH_WITH_LATEST_COST_STACKED
</br>
CREATE OR REPLACE TEMPORARY TABLE "FLOW_PATH_WITH_LATEST_COST_STACKED"
AS
WITH CTE AS (
  SELECT DISTINCT flowpath.ARTICLE_NO,
  tdf."YEAR_PERIOD" 
  FROM 
  (
  SELECT DISTINCT ARTICLE_NO
  FROM FLOW_PATH_WITH_LATEST_COST 
  GROUP BY ARTICLE_NO
  ) flowpath
  CROSS JOIN TIME_DIMENSION_BY_YEAR_PERIOD_FILTERED tdf
  )
SELECT DISTINCT fpj.YEAR_PERIOD,
       fpj.ARTICLE_NO,
       NULL AS VENDOR_ID,
       NULL AS leg,
	   NULL AS flow_path,
	   NULL AS import_flag,
	   NULL AS leg_1,
	   NULL AS order_qty,
	   NULL AS order_qty_sum,
	   NULL AS share,
	   NULL AS share_sum,
	   NULL AS leg_2,
	   NULL AS leg_3,
	   NULL AS leg_1_latest_unit_cost,
	   NULL AS leg_2_latest_unit_cost,
	   NULL AS leg_3_latest_unit_cost,
	   NULL AS lastest_agency_fee_rate,
	   NULL AS latest_duty_rate 
   FROM CTE fpj
   LEFT JOIN 
   (
  SELECT DISTINCT YEAR_PERIOD,ARTICLE_NO,1 AS flag
  FROM FLOW_PATH_WITH_LATEST_COST
  GROUP BY YEAR_PERIOD,ARTICLE_NO
  )fpyp 
  ON fpj.ARTICLE_NO=fpyp.ARTICLE_NO AND fpj.YEAR_PERIOD=fpyp.YEAR_PERIOD
  WHERE fpyp.flag IS NULL
      UNION ALL 
   SELECT DISTINCT
     YEAR_PERIOD,
	   ARTICLE_NO,
	   VENDOR_ID,
	   leg,
	   flow_path,
	   import_flag,
	   leg_1,
	   order_qty,
	   order_qty_sum,
	   share,
	   share_sum,
	   leg_2,
	   leg_3,
	   leg_1_latest_unit_cost,
	   leg_2_latest_unit_cost,
	   leg_3_latest_unit_cost,
	   lastest_agency_fee_rate,
	   latest_duty_rate 
    FROM FLOW_PATH_WITH_LATEST_COST;


<br>
--DNP_FLOW_PATH_WITH_LATEST_COST_ALL
</br>
CREATE OR REPLACE TEMPORARY TABLE "DNP_FLOW_PATH_WITH_LATEST_COST_ALL"
AS
WITH Base AS 
(
      SELECT DISTINCT
          SUM(case when ifnull( Vendor_ID,0) = 0 then 0 else 1 end) over (Partition By ARTICLE_NO order by YEAR_PERIOD ) as value_partition
          , * 
      FROM 
          "FLOW_PATH_WITH_LATEST_COST_STACKED"
      ORDER BY 
            ARTICLE_NO
          , YEAR_PERIOD     
) 
SELECT DISTINCT
      Base.YEAR_PERIOD
    , Base.ARTICLE_NO
    , CASE WHEN Base.VENDOR_ID IS NOT NULL THEN Base.VENDOR_ID                  ELSE LR.VENDOR_ID                END AS VENDOR_ID
    , CASE WHEN Base.VENDOR_ID IS NOT NULL THEN Base.leg	                    ELSE LR.leg                      END AS "leg"
    , CASE WHEN Base.VENDOR_ID IS NOT NULL THEN Base.flow_path	                ELSE LR.flow_path              END AS "flow_path"
    , CASE WHEN Base.VENDOR_ID IS NOT NULL THEN Base.import_flag	            ELSE LR.import_flag            END AS "import_flag"
    , CASE WHEN Base.VENDOR_ID IS NOT NULL THEN Base.leg_1                     ELSE LR.leg_1                 END AS "leg_1"
    , CASE WHEN Base.VENDOR_ID IS NOT NULL THEN Base.order_qty	            ELSE LR.order_qty              END AS "order_qty"
    , CASE WHEN Base.VENDOR_ID IS NOT NULL THEN Base.order_qty_sum	        ELSE LR.order_qty_sum          END AS "order_qty_sum"
    , CASE WHEN Base.VENDOR_ID IS NOT NULL THEN Base.share	                ELSE LR.share                 END AS "share"
    , CASE WHEN Base.VENDOR_ID IS NOT NULL THEN Base.share_sum	            ELSE LR.share_sum              END AS "share_sum"
    , CASE WHEN Base.VENDOR_ID IS NOT NULL THEN Base.leg_2	                ELSE LR.leg_2                  END AS "leg_2"
    , CASE WHEN Base.VENDOR_ID IS NOT NULL THEN Base.leg_3	                ELSE LR.leg_3	                 END AS "leg_3"
    , CASE WHEN Base.VENDOR_ID IS NOT NULL THEN Base.leg_1_latest_unit_cost	ELSE LR.leg_1_latest_unit_cost    END AS "leg_1_latest_unit_cost"
    , CASE WHEN Base.VENDOR_ID IS NOT NULL THEN Base.leg_2_latest_unit_cost	ELSE LR.leg_2_latest_unit_cost    END AS "leg_2_latest_unit_cost"
    , CASE WHEN Base.VENDOR_ID IS NOT NULL THEN Base.leg_3_latest_unit_cost	ELSE LR.leg_3_latest_unit_cost   END AS "leg_3_latest_unit_cost"
    , CASE WHEN Base.VENDOR_ID IS NOT NULL THEN Base.lastest_agency_fee_rate  ELSE LR.lastest_agency_fee_rate END AS "lastest_agency_fee_rate"
    , CASE WHEN Base.VENDOR_ID IS NOT NULL THEN Base.latest_duty_rate         ELSE LR.latest_duty_rate       END AS "latest_duty_rate"
    , Base.value_partition
    , case when Base.VENDOR_ID IS NULL THEN 'E' ELSE 'NE' END as Flag
FROM 
    Base
    LEFT OUTER JOIN Base LR
        ON  Base.Article_No      = LR.Article_No
        AND Base.value_partition = LR.value_partition
        AND Base.Year_Period     > LR.Year_Period
        AND LR.Vendor_ID IS NOT NULL;


<br>      
--FLOW_PATH_WITH_LATEST_COST_PREPARED
</br>
insert into FLOW_PATH_WITH_LATEST_COST_PREPARED
SELECT DISTINCT
    YEAR_PERIOD,
    ARTICLE_NO,
    VENDOR_ID,
    leg,
    flow_path,
    import_flag,
    leg_1,
    order_qty,
    order_qty_sum,
    share,
    share_sum,
    leg_2,
    leg_3,
    leg_1_latest_unit_cost,
    leg_2_latest_unit_cost,
    leg_3_latest_unit_cost,
    lastest_agency_fee_rate,
    latest_duty_rate,
    VALUE_PARTITION,
    FLAG,
    total_freight_cost,
    weighted_freight_cost,
    weighted_agency_fee,
    ocean_freight,
    domestic_inbound_freight,
    domestic_outbound_freight,
    ocean_freight * COALESCE(share, 0) AS weighted_ocean_freight,
    domestic_inbound_freight * COALESCE(share, 0) AS weighted_domestic_inbound,
    domestic_outbound_freight * COALESCE(share, 0) AS weighted_domestic_outbound,
    COALESCE(latest_duty_rate, 0) * COALESCE(share, 0) AS weighted_duty_rate
  FROM (
    SELECT DISTINCT
        YEAR_PERIOD,
        ARTICLE_NO,
        VENDOR_ID,
        leg,
        flow_path,
        import_flag,
        leg_1,
        order_qty,
        order_qty_sum,
        share,
        share_sum,
        leg_2,
        leg_3,
        leg_1_latest_unit_cost,
        leg_2_latest_unit_cost,
        leg_3_latest_unit_cost,
        lastest_agency_fee_rate,
        latest_duty_rate,
        VALUE_PARTITION,
        FLAG,
        total_freight_cost,
        total_freight_cost * COALESCE(share, 0) AS weighted_freight_cost,
        COALESCE(lastest_agency_fee_rate, 0) * COALESCE(share, 0) AS weighted_agency_fee,
        CASE WHEN import_flag = 1 THEN COALESCE(leg_1_latest_unit_cost, 0) ELSE 0 END AS ocean_freight,
        CASE WHEN import_flag != 1 OR import_flag IS NULL AND 1 IS NOT NULL OR import_flag IS NOT NULL AND 1 IS NULL THEN COALESCE(leg_1_latest_unit_cost, 0) ELSE 0 END AS domestic_inbound_freight,
        COALESCE(leg_2_latest_unit_cost, 0) + COALESCE(leg_3_latest_unit_cost, 0) AS domestic_outbound_freight
      FROM (
        SELECT DISTINCT
            YEAR_PERIOD,
            ARTICLE_NO,
            VENDOR_ID,
            "leg" as leg ,
            "flow_path" as flow_path,
            "import_flag" as import_flag,
            "leg_1" as leg_1,
            "order_qty" as order_qty,
            "order_qty_sum" as order_qty_sum,
            "share" as share,
            "share_sum" as share_sum,
            "leg_2" as leg_2,
            "leg_3" as leg_3,
            "leg_1_latest_unit_cost" as leg_1_latest_unit_cost,
            "leg_2_latest_unit_cost" as leg_2_latest_unit_cost,
            "leg_3_latest_unit_cost" as leg_3_latest_unit_cost,
            "lastest_agency_fee_rate" as lastest_agency_fee_rate,
            "latest_duty_rate" as latest_duty_rate,
            VALUE_PARTITION,
            FLAG,
            COALESCE("leg_1_latest_unit_cost", 0) + COALESCE("leg_2_latest_unit_cost", 0) + COALESCE("leg_3_latest_unit_cost", 0) AS total_freight_cost
          FROM DNP_FLOW_PATH_WITH_LATEST_COST_ALL __input_table
        ) A
    ) B ;


<br>
--FREIGHT_COST_BY_ARTICLE
</br>
CREATE OR REPLACE TEMPORARY TABLE "FREIGHT_COST_BY_ARTICLE"
AS
SELECT DISTINCT
      YEAR_PERIOD AS YEAR_PERIOD,
      ARTICLE_NO AS ARTICLE_NO,
      SUM(weighted_domestic_inbound) AS weighted_domestic_inbound,
      SUM(weighted_ocean_freight) AS weighted_ocean_freight,
      SUM(weighted_domestic_outbound) AS weighted_domestic_outbound,
      SUM(weighted_freight_cost) AS weighted_freight_cost,
      SUM(weighted_agency_fee) AS weighted_agency_fee,
      SUM(weighted_duty_rate) AS weighted_duty_rate
    FROM  FLOW_PATH_WITH_LATEST_COST_PREPARED
    GROUP BY YEAR_PERIOD, ARTICLE_NO; 


<br> 
--FLOW_PATH_STACKED_PREPAID_ALL_LEGS_JOINED
</br>
CREATE OR REPLACE TEMPORARY TABLE "FLOW_PATH_STACKED_PREPAID_ALL_LEGS_JOINED"
AS
SELECT DISTINCT
    fpal.YEAR_PERIOD AS YEAR_PERIOD,
    fpal.ARTICLE_NO AS ARTICLE_NO,
    fpal."flow_path" AS flow_path,
    fpal."leg_2" AS leg_2,
    fpal."order_qty" AS order_qty,
    fpal."order_qty_sum" AS order_qty_sum,
    fpal."share" AS share,
    fpal."leg_3" AS leg_3,
    flc."last_unit_cost" AS leg_2_latest_unit_cost,
    flc2."last_unit_cost" AS leg_3_latest_unit_cost
  FROM flow_path_stacked_prepaid_all_legs fpal
  LEFT JOIN freight_latest_cost flc
    ON (fpal.YEAR_PERIOD = flc.YEAR_PERIOD)
      AND (fpal.ARTICLE_NO = flc.ARTICLE_NO)
      AND (upper(fpal."leg_2") = upper(flc."leg"))
  LEFT JOIN freight_latest_cost flc2
    ON (fpal.YEAR_PERIOD = flc2.YEAR_PERIOD)
      AND (fpal.ARTICLE_NO = flc2.ARTICLE_NO)
      AND (upper(fpal."leg_3") = upper(flc2."leg"));
      
<br>
--FLOW_PATH_PREPARED_STACKED	
</br>
CREATE OR REPLACE  TEMPORARY TABLE "FLOW_PATH_PREPARED_STACKED"
AS
WITH CTE AS 
(
SELECT fpsaan.ARTICLE_NO,td.YEAR_PERIOD
   FROM    
	(SELECT DISTINCT ARTICLE_NO
          FROM FLOW_PATH_STACKED_PREPAID_ALL_LEGS_JOINED	
	 GROUP BY ARTICLE_NO
	)fpsaan
	CROSS JOIN TIME_DIMENSION_BY_YEAR_PERIOD_FILTERED td
    )

    SELECT   DISTINCT
    "YEAR_PERIOD" AS "YEAR_PERIOD",
    "ARTICLE_NO" AS "ARTICLE_NO",
    NULL AS "flow_path",
    NULL AS "leg_2",
    NULL AS "order_qty",
    NULL AS "order_qty_sum",
    NULL AS "share",
    NULL AS "leg_3",
    NULL AS "leg_2_latest_unit_cost",
    NULL AS "leg_3_latest_unit_cost"
  FROM (
    SELECT DISTINCT
        "TENANT149_flow_path_stacked_prepaid_all_legs_joined_by_ARTICLE_NO_joined"."ARTICLE_NO" AS "ARTICLE_NO",
        "TENANT149_flow_path_stacked_prepaid_all_legs_joined_by_ARTICLE_NO_joined"."YEAR_PERIOD" AS "YEAR_PERIOD",
        "TENANT149_flow_path_stacked_prepaid_all_legs_joined_by_YEAR_PERIOD"."flag" AS "flag"
      FROM CTE "TENANT149_flow_path_stacked_prepaid_all_legs_joined_by_ARTICLE_NO_joined"
      LEFT JOIN (
        SELECT DISTINCT
            "YEAR_PERIOD" AS "YEAR_PERIOD",
            "ARTICLE_NO" AS "ARTICLE_NO",
            1 AS "flag"
          FROM (
            SELECT *
              FROM ( SELECT DISTINCT YEAR_PERIOD,ARTICLE_NO
	                 FROM FLOW_PATH_STACKED_PREPAID_ALL_LEGS_JOINED
	                 GROUP BY YEAR_PERIOD,ARTICLE_NO) "TENANT149_flow_path_stacked_prepaid_all_legs_joined_by_YEAR_PERIOD"
            ) "withoutcomputedcols_query"
        ) "TENANT149_flow_path_stacked_prepaid_all_legs_joined_by_YEAR_PERIOD"
        ON ("TENANT149_flow_path_stacked_prepaid_all_legs_joined_by_ARTICLE_NO_joined"."ARTICLE_NO" = "TENANT149_flow_path_stacked_prepaid_all_legs_joined_by_YEAR_PERIOD"."ARTICLE_NO")
          AND ("TENANT149_flow_path_stacked_prepaid_all_legs_joined_by_ARTICLE_NO_joined"."YEAR_PERIOD" = "TENANT149_flow_path_stacked_prepaid_all_legs_joined_by_YEAR_PERIOD"."YEAR_PERIOD")
    ) "unfiltered_query"
  WHERE "flag" IS NULL
  UNION ALL 
  SELECT DISTINCT
    "YEAR_PERIOD" AS "YEAR_PERIOD",
    "ARTICLE_NO" AS "ARTICLE_NO",
    flow_path AS "flow_path",
    leg_2 AS "leg_2",
    order_qty AS "order_qty",
    order_qty_sum AS "order_qty_sum",
    share AS "share",
    leg_3 AS "leg_3",
    leg_2_latest_unit_cost AS "leg_2_latest_unit_cost",
    leg_3_latest_unit_cost AS "leg_3_latest_unit_cost"
  FROM "FLOW_PATH_STACKED_PREPAID_ALL_LEGS_JOINED";
 
   
<br>
--PREPAID_FREIGHT_AGG
</br>
CREATE OR REPLACE TEMPORARY TABLE "PREPAID_FREIGHT_AGG"
AS
WITH Base AS 
(
      SELECT DISTINCT
          SUM(case when ifnull( "flow_path" ,'-999') = '-999' then 0 else 1 end) over (Partition By ARTICLE_NO order by YEAR_PERIOD ) as value_partition
          , * 
      FROM 
          "FLOW_PATH_PREPARED_STACKED"
      ORDER BY 
            ARTICLE_NO
          , YEAR_PERIOD     
) 
SELECT DISTINCT
    "YEAR_PERIOD" AS "YEAR_PERIOD",
    "ARTICLE_NO" AS "ARTICLE_NO",
    SUM("weighted_prepaid_freight") AS "weighted_prepaid_freight"
FROM (
SELECT DISTINCT
    "YEAR_PERIOD",
    "ARTICLE_NO",
    "flow_path",
    "leg_2",
    "order_qty",
    "order_qty_sum",
    "share",
    "leg_3",
    "leg_2_latest_unit_cost",
    "leg_3_latest_unit_cost",
    "VALUE_PARTITION",
    "FLAG",
    "prepaid_freight",
    COALESCE("share", 0) * COALESCE("prepaid_freight", 0) AS "weighted_prepaid_freight"
  FROM (
    SELECT DISTINCT
        "YEAR_PERIOD",
        "ARTICLE_NO",
        "flow_path",
        "leg_2",
        "order_qty",
        "order_qty_sum",
        "share",
        "leg_3",
        "leg_2_latest_unit_cost",
        "leg_3_latest_unit_cost",
        "VALUE_PARTITION",
        "FLAG",
        COALESCE("leg_2_latest_unit_cost", 0) + COALESCE("leg_3_latest_unit_cost", 0) AS "prepaid_freight"
    FROM (
SELECT DISTINCT
      Base.YEAR_PERIOD
    , Base.ARTICLE_NO
    , CASE WHEN Base."flow_path" IS NOT NULL THEN Base."flow_path"	            ELSE LR."flow_path"              END AS "flow_path"
    , CASE WHEN Base."flow_path" IS NOT NULL THEN Base."leg_2"	                ELSE LR."leg_2"                  END AS "leg_2"
    , CASE WHEN Base."flow_path" IS NOT NULL THEN Base."order_qty"	            ELSE LR."order_qty"              END AS "order_qty"
    , CASE WHEN Base."flow_path" IS NOT NULL THEN Base."order_qty_sum"	        ELSE LR."order_qty_sum"          END AS "order_qty_sum"
    , CASE WHEN Base."flow_path" IS NOT NULL THEN Base."share"	                ELSE LR."share"                  END AS "share"
    , CASE WHEN Base."flow_path" IS NOT NULL THEN Base."leg_3"	                ELSE LR."leg_3"                  END AS "leg_3"
    , CASE WHEN Base."flow_path" IS NOT NULL THEN Base."leg_2_latest_unit_cost"	ELSE LR."leg_2_latest_unit_cost" END AS "leg_2_latest_unit_cost"
    , CASE WHEN Base."flow_path" IS NOT NULL THEN Base."leg_3_latest_unit_cost"  ELSE LR."leg_3_latest_unit_cost" END AS "leg_3_latest_unit_cost"
    , Base.value_partition
    , case when Base."flow_path" IS NULL THEN 'E' ELSE 'NE' END as Flag
FROM 
    Base
    LEFT OUTER JOIN Base LR
        ON  Base.Article_No      = LR.Article_No
        AND Base.value_partition = LR.value_partition
        AND Base.Year_Period     > LR.Year_Period
        AND LR."flow_path" IS NOT NULL)A)B)FINAL 
        GROUP BY "YEAR_PERIOD", "ARTICLE_NO";

<br>
--ALLOCATION_SF
</br>
CREATE OR REPLACE TEMPORARY TABLE ALLOCATION_SF
AS
SELECT DISTINCT
       article_no as "ARTICLE_NO"
    ,  merch_year as "MERCH_YEAR"
    ,  vendor_id as "VENDOR_ID"
    ,  battery_core_flag as "battery_core_flag"
    ,  cash_discount_flag as "cash_discount_flag"
    ,  year_period as "YEAR_PERIOD"
    ,  year_quarter as "YEAR_QUARTER"
    ,  parent_vendor_id as "PARENT_VENDOR_ID"
    ,  co_code as "CO_CODE"
    ,  sum_type_code as "SUM_TYPE_CODE"
    ,  price_ovrd_flag as "PRICE_OVRD_FLAG"
    ,  scan_item_flag as "SCAN_ITEM_FLAG"
    ,  price_type_code as "PRICE_TYPE_CODE"
    ,  buy_online_code as "BUY_ONLINE_CODE"
    ,  year_week as "YEAR_WEEK"
    ,  article_desc as "ARTICLE_DESC"
    ,  article_full as "ARTICLE_FULL"
    ,  article_type_name as "ARTICLE_TYPE_NAME"
    ,  article_type_code as "ARTICLE_TYPE_CODE"
    ,  article_type_full as "ARTICLE_TYPE_FULL"
    ,  label_type_code as "LABEL_TYPE_CODE"
    ,  uom_code as "UOM_CODE"
    ,  uom_desc as "UOM_DESC"
    ,  uom_full as "UOM_FULL"
    ,  article_length as "ARTICLE_LENGTH"
    ,  article_width as "ARTICLE_WIDTH"
    ,  article_height as "ARTICLE_HEIGHT"
    ,  article_weight as "ARTICLE_WEIGHT"
    ,  article_size as "ARTICLE_SIZE"
    ,  article_count as "ARTICLE_COUNT"
    ,  loc_type_code as "LOC_TYPE_CODE"
    ,  primary_vendor_id as "PRIMARY_VENDOR_ID"
    ,  article_cat_code as "ARTICLE_CAT_CODE"
    ,  article_cat_name as "ARTICLE_CAT_NAME"
    ,  article_cat_full as "ARTICLE_CAT_FULL"
    ,  dept_code as "DEPT_CODE"
    ,  dept_name as "DEPT_NAME"
    ,  dept_full as "DEPT_FULL"
    ,  lob_no as "LOB_NO"
    ,  lob_desc as "LOB_DESC"
    ,  lob_full as "LOB_FULL"
    ,  maj_dept_no as "MAJ_DEPT_NO"
    ,  maj_dept_desc as "MAJ_DEPT_DESC"
    ,  maj_dept_full as "MAJ_DEPT_FULL"
    ,  brand_name as "BRAND_NAME"
    ,  pvt_label_desc as "PVT_LABEL_DESC"
    ,  vendor_name as "VENDOR_NAME"
    ,  vendor_full as "VENDOR_FULL"
    ,  vendor_co_name as "VENDOR_CO_NAME"
    ,  region_code as "REGION_CODE"
    ,  sales_qty as "SALES_QTY"
    ,  sales_amt as "SALES_AMT"
    ,  cost_amt as "COST_AMT"
    ,  disc_amt as "DISC_AMT"
    ,  ovrd_amt as "OVRD_AMT"
    ,  trans_tax_amt as "TRANS_TAX_AMT"
    ,  gross_sales as "GROSS_SALES"
    ,  markdown_perc as "markdown_perc"
    ,  markdown_flag as "markdown_flag"
    ,  order_line_type as "ORDER_LINE_TYPE"
    ,  scac as "SCAC"
    ,  oms_order_line_schedule_ship_node as "OMS_ORDER_LINE_SCHEDULE_SHIP_NODE"
    ,  shipping_revenue as "SHIPPING_REVENUE"
    ,  shipping_disc as "SHIPPING_DISC"
    ,  actual_shipping_revenue as "ACTUAL_SHIPPING_REVENUE"
    ,  omni_tsc_delivery as "omni_tsc_delivery"
    ,  omni_small_package_ltl as "omni_small_package_ltl"
    ,  omni_roadie as "omni_roadie"
    ,  _0mni_multi_salvage as "0mni_multi_salvage"
    ,  battery_cost as "battery_cost"
    ,  battery_cost_total as "battery_cost_total"
    ,  scandown_flag as "scandown_flag"
    ,  sales_allocate as "SALES_ALLOCATE"
    ,  cost_allocate as "COST_ALLOCATE"
    ,  dc_shrink_allocate as "dc_shrink_final"
    ,  pdq_allocate_filtered as "pdq_final"
    ,  vf_defective_allocate as "vf_defective_final"
    ,  mixing_center_credit_prep as "mixing_center_final"
    ,  defectives_allocate as "defectives_final"
    ,  pop_allocate as "pop_final"
    ,  vendor_compliance_allocate as "vendor_compliance_final"
    ,  booth_rental_final as "booth_rental_final"
    ,  tenant149_data_new_store_discount_prepared_prepared_by_nsd_sku as "new_store_discount_final"
    ,  cash_discounts_w_vendor as "cash_discounts_w_vendor_final"
    ,  cash_discounts_by_flag as "cash_discounts_by_flag_final"
    ,  marketing_allocate as "marketing_final"
    ,  battery_core_allocate as "battery_core_final"
    ,  shrink_allocate as "shrink_inventory_final"
    ,  unsellables_allocate as "unsellables_final"
    ,  theft_allocate as "theft_final"
    ,  dc_shrink_allocate_l2 as "dc_shrink_final_l2"
    ,  pdq_allocate_filtered_l2 as "pdq_final_l2"
    ,  vf_defective_allocate_l2 as "vf_defective_final_l2"
    ,  mixing_center_credit_prep_l2 as "mixing_center_final_l2"
    ,  defectives_allocate_l2 as "defectives_final_l2"
    ,  pop_allocate_l2 as "pop_final_l2"
    ,  vendor_compliance_allocate_l2 as "vendor_compliance_final_l2"
    ,  tenant149_data_new_store_discount_prepared_prepared_by_nsd_sku_l2 as "new_store_discount_final_l2"
    ,  cash_discounts_w_vendor_l2 as "cash_discounts_w_vendor_final_l2"
    ,  cash_discounts_by_flag_l2 as "cash_discounts_by_flag_final_l2"
    ,  marketing_allocate_l2 as "marketing_final_l2"
    ,  battery_core_allocate_l2 as "battery_core_final_l2"
    ,  shrink_allocate_l2 as "shrink_inventory_final_l2"
    ,  unsellables_allocate_l2 as "unsellables_final_l2"
    ,  theft_allocate_l2 as "theft_final_l2"
FROM 
    ALLOCATION_SP;

<br>
--ALLOCATION_ROLLUP 
</br>
CREATE OR REPLACE TEMPORARY TABLE "ALLOCATION_ROLLUP"
AS
    SELECT DISTINCT
        ARTICLE_NO AS ARTICLE_NO,
        MERCH_YEAR AS MERCH_YEAR,
        VENDOR_ID AS VENDOR_ID,
        PARENT_VENDOR_ID AS PARENT_VENDOR_ID,
        YEAR_PERIOD AS YEAR_PERIOD,
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
        SUM(SALES_QTY) AS SALES_QTY,
        SUM(SALES_AMT) AS SALES_AMT,
        SUM(COST_AMT) AS COST_AMT,
        SUM(DISC_AMT) AS DISC_AMT,
        SUM(OVRD_AMT) AS OVRD_AMT,
        SUM(TRANS_TAX_AMT) AS TRANS_TAX_AMT,
        SUM(GROSS_SALES) AS GROSS_SALES,
        SUM(SHIPPING_REVENUE) AS SHIPPING_REVENUE,
        SUM(SHIPPING_DISC) AS SHIPPING_DISC,
        SUM(ACTUAL_SHIPPING_REVENUE) AS ACTUAL_SHIPPING_REVENUE,
        SUM(omni_tsc_delivery) AS omni_tsc_delivery,
        SUM(omni_small_package_ltl) AS omni_small_package_ltl,
        SUM(omni_roadie) AS omni_roadie,
        SUM("omni_multi_salvage") AS "0mni_multi_salvage",
        SUM(SALES_ALLOCATE) AS SALES_ALLOCATE,
        SUM(COST_ALLOCATE) AS COST_ALLOCATE,
        SUM(unsellables_combined) AS unsellables,
        SUM(defectives_combined) AS defectives,
        SUM(dc_shrink_combined) AS dc_shrink,
        SUM(theft_combined) AS theft,
        SUM(defective_allowance_combined) AS defective_allowance,
        SUM(battery_core_combined) AS battery_core,
        SUM(mixing_center_combined) AS mixing_center,
        SUM(shrink_inventory_combined) AS shrink_inventory,
        SUM(new_store_discounts_combined) AS new_store_discounts,
        SUM(cash_discounts_combined) AS cash_discounts,
        SUM(pop_combined) AS pop,
        SUM(pdq_combined) AS pdq,
        SUM(marketing_combined) AS marketing,
        SUM(vendor_compliance_combined) AS vendor_compliance,
        SUM(booth_rental) AS booth_rental
      FROM (
         SELECT DISTINCT
            ARTICLE_NO AS ARTICLE_NO,
            MERCH_YEAR AS MERCH_YEAR,
            VENDOR_ID AS VENDOR_ID,
            "battery_core_flag" AS battery_core_flag,
            "cash_discount_flag" AS cash_discount_flag,
            YEAR_PERIOD AS YEAR_PERIOD,
            YEAR_QUARTER AS YEAR_QUARTER,
            PARENT_VENDOR_ID AS PARENT_VENDOR_ID,
            CO_CODE AS CO_CODE,
            SUM_TYPE_CODE AS SUM_TYPE_CODE,
            PRICE_OVRD_FLAG AS PRICE_OVRD_FLAG,
            SCAN_ITEM_FLAG AS SCAN_ITEM_FLAG,
            PRICE_TYPE_CODE AS PRICE_TYPE_CODE,
            BUY_ONLINE_CODE AS BUY_ONLINE_CODE,
            YEAR_WEEK AS YEAR_WEEK,
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
            SALES_QTY AS SALES_QTY,
            SALES_AMT AS SALES_AMT,
            COST_AMT AS COST_AMT,
            DISC_AMT AS DISC_AMT,
            OVRD_AMT AS OVRD_AMT,
            TRANS_TAX_AMT AS TRANS_TAX_AMT,
            GROSS_SALES AS GROSS_SALES,
            "markdown_perc" AS markdown_perc,
            "markdown_flag" AS markdown_flag,
            ORDER_LINE_TYPE AS ORDER_LINE_TYPE,
            SCAC AS SCAC,
            OMS_ORDER_LINE_SCHEDULE_SHIP_NODE AS OMS_ORDER_LINE_SCHEDULE_SHIP_NODE,
            SHIPPING_REVENUE AS SHIPPING_REVENUE,
            SHIPPING_DISC AS SHIPPING_DISC,
            ACTUAL_SHIPPING_REVENUE AS ACTUAL_SHIPPING_REVENUE,
            "omni_tsc_delivery" AS omni_tsc_delivery,
            "omni_small_package_ltl" AS omni_small_package_ltl,
            "omni_roadie" AS omni_roadie,
            "0mni_multi_salvage" AS "omni_multi_salvage",
            "battery_cost" AS battery_cost,
            "battery_cost_total" AS battery_cost_total,
            "scandown_flag" AS scandown_flag,
            SALES_ALLOCATE AS SALES_ALLOCATE,
            COST_ALLOCATE AS COST_ALLOCATE,
            "dc_shrink_final" AS dc_shrink_final,
            "pdq_final" AS pdq_final,
            "vf_defective_final" AS vf_defective_final,
            "mixing_center_final" AS mixing_center_final,
            "defectives_final" AS defectives_final,
            "pop_final" AS pop_final,
            "vendor_compliance_final" AS vendor_compliance_final,
            "booth_rental_final" AS booth_rental_final,
            "new_store_discount_final" AS new_store_discount_final,
            "cash_discounts_w_vendor_final" AS cash_discounts_w_vendor_final,
            "cash_discounts_by_flag_final" AS cash_discounts_by_flag_final,
            "marketing_final" AS marketing_final,
            "battery_core_final" AS battery_core_final,
            "shrink_inventory_final" AS shrink_inventory_final,
            "unsellables_final" AS unsellables_final,
            "theft_final" AS theft_final,
            "dc_shrink_final_l2" AS dc_shrink_final_l2,
            "pdq_final_l2" AS pdq_final_l2,
            "vf_defective_final_l2" AS vf_defective_final_l2,
            "mixing_center_final_l2" AS mixing_center_final_l2,
            "defectives_final_l2" AS defectives_final_l2,
            "pop_final_l2" AS pop_final_l2,
            "vendor_compliance_final_l2" AS vendor_compliance_final_l2,
            "new_store_discount_final_l2" AS new_store_discount_final_l2,
            "cash_discounts_w_vendor_final_l2" AS cash_discounts_w_vendor_final_l2,
            "cash_discounts_by_flag_final_l2" AS cash_discounts_by_flag_final_l2,
            "marketing_final_l2" AS marketing_final_l2,
            "battery_core_final_l2" AS battery_core_final_l2,
            "shrink_inventory_final_l2" AS shrink_inventory_final_l2,
            "unsellables_final_l2" AS unsellables_final_l2,
            "theft_final_l2" AS theft_final_l2,
            COALESCE("unsellables_final", 0) + COALESCE("unsellables_final_l2", 0) AS unsellables_combined,
            COALESCE("defectives_final", 0) + COALESCE("defectives_final_l2", 0) AS defectives_combined,
            COALESCE("dc_shrink_final", 0) + COALESCE("dc_shrink_final_l2", 0) AS dc_shrink_combined,
            COALESCE("theft_final", 0) + COALESCE("theft_final_l2", 0) AS theft_combined,
            COALESCE("vf_defective_final", 0) + COALESCE("vf_defective_final_l2", 0) AS defective_allowance_combined,
            COALESCE("battery_core_final", 0) + COALESCE("battery_core_final_l2", 0) AS battery_core_combined,
            COALESCE("mixing_center_final", 0) + COALESCE("mixing_center_final_l2", 0) AS mixing_center_combined,
            COALESCE("shrink_inventory_final", 0) + COALESCE("shrink_inventory_final_l2", 0) AS shrink_inventory_combined,
            COALESCE("new_store_discount_final", 0) + COALESCE("new_store_discount_final_l2", 0) AS new_store_discounts_combined,
            COALESCE("cash_discounts_w_vendor_final", 0) + COALESCE("cash_discounts_w_vendor_final_l2", 0) + COALESCE(cash_discounts_by_flag_final, 0) + COALESCE("cash_discounts_by_flag_final_l2", 0) AS cash_discounts_combined,
            COALESCE("pop_final", 0) + COALESCE("pop_final_l2", 0) AS pop_combined,
            COALESCE("pdq_final", 0) + COALESCE("pdq_final_l2", 0) AS pdq_combined,
            COALESCE("marketing_final", 0) + COALESCE("marketing_final_l2", 0) AS marketing_combined,
            COALESCE("vendor_compliance_final", 0) + COALESCE("vendor_compliance_final_l2", 0) AS vendor_compliance_combined,
            COALESCE("booth_rental_final", 0) AS booth_rental
          FROM "ALLOCATION_SF"
        )A  
 GROUP BY ARTICLE_NO, MERCH_YEAR, VENDOR_ID, PARENT_VENDOR_ID, YEAR_PERIOD, CO_CODE, SUM_TYPE_CODE, PRICE_OVRD_FLAG, SCAN_ITEM_FLAG, PRICE_TYPE_CODE, BUY_ONLINE_CODE, YEAR_WEEK, YEAR_QUARTER, ARTICLE_DESC, ARTICLE_FULL, ARTICLE_TYPE_NAME, ARTICLE_TYPE_CODE, ARTICLE_TYPE_FULL, LABEL_TYPE_CODE, UOM_CODE, UOM_DESC, UOM_FULL, ARTICLE_LENGTH, ARTICLE_WIDTH, ARTICLE_HEIGHT, ARTICLE_WEIGHT, ARTICLE_SIZE, ARTICLE_COUNT, LOC_TYPE_CODE, PRIMARY_VENDOR_ID, ARTICLE_CAT_CODE, ARTICLE_CAT_NAME, ARTICLE_CAT_FULL, DEPT_CODE, DEPT_NAME, DEPT_FULL, LOB_NO, LOB_DESC, LOB_FULL, MAJ_DEPT_NO, MAJ_DEPT_DESC, MAJ_DEPT_FULL, BRAND_NAME, PVT_LABEL_DESC, VENDOR_NAME, VENDOR_FULL, VENDOR_CO_NAME, REGION_CODE, ORDER_LINE_TYPE, SCAC, OMS_ORDER_LINE_SCHEDULE_SHIP_NODE;

 
<br>
--ALLOCATION_ROLLUP_JOINED
</br>
CREATE OR REPLACE TEMPORARY TABLE "ALLOCATION_ROLLUP_JOINED"
AS 
 WITH CTE AS (
 SELECT DISTINCT
        "ARTICLE_NO" AS "ARTICLE_NO",
        "MERCH_YEAR" AS "MERCH_YEAR",
        "VENDOR_ID" AS "VENDOR_ID",
        "PARENT_VENDOR_ID" AS "PARENT_VENDOR_ID",
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
        "ARTICLE_TYPE_CODE" AS "ARTICLE_TYPE_CODE",
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
        "BRAND_NAME" AS "BRAND_NAME",
        "PVT_LABEL_DESC" AS "PVT_LABEL_DESC",
        "VENDOR_NAME" AS "VENDOR_NAME",
        "VENDOR_FULL" AS "VENDOR_FULL",
        "VENDOR_CO_NAME" AS "VENDOR_CO_NAME",
        "REGION_CODE" AS "REGION_CODE",
        "ORDER_LINE_TYPE" AS "ORDER_LINE_TYPE",
        "SCAC" AS "SCAC",
        "OMS_ORDER_LINE_SCHEDULE_SHIP_NODE" AS "OMS_ORDER_LINE_SCHEDULE_SHIP_NODE",
        "SALES_QTY" AS "SALES_QTY",
        "SALES_AMT" AS "SALES_AMT",
        "COST_AMT" AS "COST_AMT",
        "DISC_AMT" AS "DISC_AMT",
        "OVRD_AMT" AS "OVRD_AMT",
        "TRANS_TAX_AMT" AS "TRANS_TAX_AMT",
        "GROSS_SALES" AS "GROSS_SALES",
        "SHIPPING_REVENUE" AS "SHIPPING_REVENUE",
        "SHIPPING_DISC" AS "SHIPPING_DISC",
        "ACTUAL_SHIPPING_REVENUE" AS "ACTUAL_SHIPPING_REVENUE",
        omni_tsc_delivery AS "omni_tsc_delivery",
        omni_small_package_ltl AS "omni_small_package_ltl",
        omni_roadie AS "omni_roadie",
        "0mni_multi_salvage" AS "0mni_multi_salvage",
        "SALES_ALLOCATE" AS "SALES_ALLOCATE",
        "COST_ALLOCATE" AS "COST_ALLOCATE",
        unsellables AS "unsellables",
        defectives AS "defectives",
        dc_shrink AS "dc_shrink",
        theft AS "theft",
        defective_allowance AS "defective_allowance",
        battery_core AS "battery_core",
        mixing_center AS "mixing_center",
        shrink_inventory AS "shrink_inventory",
        new_store_discounts AS "new_store_discounts",
        cash_discounts AS "cash_discounts",
        pop AS "pop",
        pdq AS "pdq",
        marketing AS "marketing",
        vendor_compliance AS "vendor_compliance",
        booth_rental AS "booth_rental",
        CASE WHEN ("ORDER_LINE_TYPE" = 'BOPIS') OR ("ORDER_LINE_TYPE" = 'BODFS') THEN 0 ELSE 1 END AS "flag_join"
        FROM "ALLOCATION_ROLLUP" "allocation_rollup"
    )
SELECT DISTINCT
    "allocation_rollup"."ARTICLE_NO" AS "ARTICLE_NO",
    "allocation_rollup"."MERCH_YEAR" AS "MERCH_YEAR",
    "allocation_rollup"."VENDOR_ID" AS "VENDOR_ID",
    "allocation_rollup"."PARENT_VENDOR_ID" AS "PARENT_VENDOR_ID",
    "allocation_rollup"."YEAR_PERIOD" AS "YEAR_PERIOD",
    "allocation_rollup"."CO_CODE" AS "CO_CODE",
    "allocation_rollup"."SUM_TYPE_CODE" AS "SUM_TYPE_CODE",
    "allocation_rollup"."PRICE_OVRD_FLAG" AS "PRICE_OVRD_FLAG",
    "allocation_rollup"."SCAN_ITEM_FLAG" AS "SCAN_ITEM_FLAG",
    "allocation_rollup"."PRICE_TYPE_CODE" AS "PRICE_TYPE_CODE",
    "allocation_rollup"."BUY_ONLINE_CODE" AS "BUY_ONLINE_CODE",
    "allocation_rollup"."YEAR_WEEK" AS "YEAR_WEEK",
    "allocation_rollup"."YEAR_QUARTER" AS "YEAR_QUARTER",
    "allocation_rollup"."ARTICLE_DESC" AS "ARTICLE_DESC",
    "allocation_rollup"."ARTICLE_FULL" AS "ARTICLE_FULL",
    "allocation_rollup"."ARTICLE_TYPE_NAME" AS "ARTICLE_TYPE_NAME",
    "allocation_rollup"."ARTICLE_TYPE_CODE" AS "ARTICLE_TYPE_CODE",
    "allocation_rollup"."ARTICLE_TYPE_FULL" AS "ARTICLE_TYPE_FULL",
    "allocation_rollup"."LABEL_TYPE_CODE" AS "LABEL_TYPE_CODE",
    "allocation_rollup"."UOM_CODE" AS "UOM_CODE",
    "allocation_rollup"."UOM_DESC" AS "UOM_DESC",
    "allocation_rollup"."UOM_FULL" AS "UOM_FULL",
    "allocation_rollup"."ARTICLE_LENGTH" AS "ARTICLE_LENGTH",
    "allocation_rollup"."ARTICLE_WIDTH" AS "ARTICLE_WIDTH",
    "allocation_rollup"."ARTICLE_HEIGHT" AS "ARTICLE_HEIGHT",
    "allocation_rollup"."ARTICLE_WEIGHT" AS "ARTICLE_WEIGHT",
    "allocation_rollup"."ARTICLE_SIZE" AS "ARTICLE_SIZE",
    "allocation_rollup"."ARTICLE_COUNT" AS "ARTICLE_COUNT",
    "allocation_rollup"."LOC_TYPE_CODE" AS "LOC_TYPE_CODE",
    "allocation_rollup"."PRIMARY_VENDOR_ID" AS "PRIMARY_VENDOR_ID",
    "allocation_rollup"."ARTICLE_CAT_CODE" AS "ARTICLE_CAT_CODE",
    "allocation_rollup"."ARTICLE_CAT_NAME" AS "ARTICLE_CAT_NAME",
    "allocation_rollup"."ARTICLE_CAT_FULL" AS "ARTICLE_CAT_FULL",
    "allocation_rollup"."DEPT_CODE" AS "DEPT_CODE",
    "allocation_rollup"."DEPT_NAME" AS "DEPT_NAME",
    "allocation_rollup"."DEPT_FULL" AS "DEPT_FULL",
    "allocation_rollup"."LOB_NO" AS "LOB_NO",
    "allocation_rollup"."LOB_DESC" AS "LOB_DESC",
    "allocation_rollup"."LOB_FULL" AS "LOB_FULL",
    "allocation_rollup"."MAJ_DEPT_NO" AS "MAJ_DEPT_NO",
    "allocation_rollup"."MAJ_DEPT_DESC" AS "MAJ_DEPT_DESC",
    "allocation_rollup"."MAJ_DEPT_FULL" AS "MAJ_DEPT_FULL",
    "allocation_rollup"."BRAND_NAME" AS "BRAND_NAME",
    "allocation_rollup"."PVT_LABEL_DESC" AS "PVT_LABEL_DESC",
    "allocation_rollup"."VENDOR_NAME" AS "VENDOR_NAME",
    "allocation_rollup"."VENDOR_FULL" AS "VENDOR_FULL",
    "allocation_rollup"."VENDOR_CO_NAME" AS "VENDOR_CO_NAME",
    "allocation_rollup"."REGION_CODE" AS "REGION_CODE",
    "allocation_rollup"."ORDER_LINE_TYPE" AS "ORDER_LINE_TYPE",
    "allocation_rollup"."SCAC" AS "SCAC",
    "allocation_rollup"."OMS_ORDER_LINE_SCHEDULE_SHIP_NODE" AS "OMS_ORDER_LINE_SCHEDULE_SHIP_NODE",
    "allocation_rollup"."SALES_QTY" AS "SALES_QTY",
    "allocation_rollup"."SALES_AMT" AS "SALES_AMT",
    "allocation_rollup"."COST_AMT" AS "COST_AMT",
    "allocation_rollup"."DISC_AMT" AS "DISC_AMT",
    "allocation_rollup"."OVRD_AMT" AS "OVRD_AMT",
    "allocation_rollup"."TRANS_TAX_AMT" AS "TRANS_TAX_AMT",
    "allocation_rollup"."GROSS_SALES" AS "GROSS_SALES",
    "allocation_rollup"."SHIPPING_REVENUE" AS "SHIPPING_REVENUE",
    "allocation_rollup"."SHIPPING_DISC" AS "SHIPPING_DISC",
    "allocation_rollup"."ACTUAL_SHIPPING_REVENUE" AS "ACTUAL_SHIPPING_REVENUE",
    "allocation_rollup"."omni_tsc_delivery" AS "omni_tsc_delivery",
    "allocation_rollup"."omni_small_package_ltl" AS "omni_small_package_ltl",
    "allocation_rollup"."omni_roadie" AS "omni_roadie",
    "allocation_rollup"."0mni_multi_salvage" AS "0mni_multi_salvage",
    "allocation_rollup"."SALES_ALLOCATE" AS "SALES_ALLOCATE",
    "allocation_rollup"."COST_ALLOCATE" AS "COST_ALLOCATE",
    "allocation_rollup"."unsellables" AS "unsellables",
    "allocation_rollup"."defectives" AS "defectives",
    "allocation_rollup"."dc_shrink" AS "dc_shrink",
    "allocation_rollup"."theft" AS "theft",
    "allocation_rollup"."defective_allowance" AS "defective_allowance",
    "allocation_rollup"."battery_core" AS "battery_core",
    "allocation_rollup"."mixing_center" AS "mixing_center",
    "allocation_rollup"."shrink_inventory" AS "shrink_inventory",
    "allocation_rollup"."new_store_discounts" AS "new_store_discounts",
    "allocation_rollup"."cash_discounts" AS "cash_discounts",
    "allocation_rollup"."pop" AS "pop",
    "allocation_rollup"."pdq" AS "pdq",
    "allocation_rollup"."marketing" AS "marketing",
    "allocation_rollup"."vendor_compliance" AS "vendor_compliance",
    "allocation_rollup"."booth_rental" AS "booth_rental",
    "TENANT149_shrink_r12_filter".shrink_rate AS "shrink_rate",
    "freight_cost_by_article"."weighted_domestic_inbound" AS "weighted_domestic_inbound",
    "freight_cost_by_article"."weighted_ocean_freight" AS "weighted_ocean_freight",
    "freight_cost_by_article"."weighted_domestic_outbound" AS "weighted_domestic_outbound",
    "freight_cost_by_article"."weighted_freight_cost" AS "weighted_freight_cost",
    "freight_cost_by_article"."weighted_agency_fee" AS "weighted_agency_fee",
    "freight_cost_by_article"."weighted_duty_rate" AS "weighted_duty_rate",
    "freight_cost_by_article"."freight_join_flag" AS "freight_join_flag",
    "purchase_order_line_prepaid_share".prepaid_share AS "prepaid_share",
    "prepaid_freight_agg"."weighted_prepaid_freight" AS "weighted_prepaid_freight",
    "prepaid_freight_agg"."prepaid_flag" AS "prepaid_flag"
 FROM CTE  "allocation_rollup"
  LEFT JOIN "SHRINK_R12_FILTER" "TENANT149_shrink_r12_filter"
    ON ("allocation_rollup"."YEAR_PERIOD" = "TENANT149_shrink_r12_filter".year_period)
      AND ("allocation_rollup"."ARTICLE_NO" = "TENANT149_shrink_r12_filter"."SKU")
  LEFT JOIN (
    SELECT DISTINCT
        "YEAR_PERIOD" AS "YEAR_PERIOD",
        "ARTICLE_NO" AS "ARTICLE_NO",
        weighted_domestic_inbound AS "weighted_domestic_inbound",
        weighted_ocean_freight AS "weighted_ocean_freight",
        weighted_domestic_outbound AS "weighted_domestic_outbound",
        weighted_freight_cost AS "weighted_freight_cost",
        weighted_agency_fee AS "weighted_agency_fee",
        weighted_duty_rate AS "weighted_duty_rate",
        1 AS "freight_join_flag"
      FROM (
        SELECT DISTINCT *
          FROM "FREIGHT_COST_BY_ARTICLE" "freight_cost_by_article"
        ) "withoutcomputedcols_query"
    ) "freight_cost_by_article"
    ON ("allocation_rollup"."ARTICLE_NO" = "freight_cost_by_article"."ARTICLE_NO")
      AND ("allocation_rollup"."YEAR_PERIOD" = "freight_cost_by_article"."YEAR_PERIOD")
      AND ("allocation_rollup"."flag_join" = "freight_cost_by_article"."freight_join_flag")
  LEFT JOIN "PURCHASE_ORDER_LINE_PREPAID_SHARE" "purchase_order_line_prepaid_share"
    ON ("allocation_rollup"."ARTICLE_NO" = "purchase_order_line_prepaid_share"."ARTICLE_NO")
      AND ("allocation_rollup"."YEAR_PERIOD" = "purchase_order_line_prepaid_share"."YEAR_PERIOD")
  LEFT JOIN (
    SELECT DISTINCT
        "YEAR_PERIOD" AS "YEAR_PERIOD",
        "ARTICLE_NO" AS "ARTICLE_NO",
        "weighted_prepaid_freight" AS "weighted_prepaid_freight",
        1 AS "prepaid_flag"
      FROM (
        SELECT *
          FROM "PREPAID_FREIGHT_AGG" "prepaid_freight_agg"
        ) "withoutcomputedcols_query"
    ) "prepaid_freight_agg"
    ON ("allocation_rollup"."ARTICLE_NO" = "prepaid_freight_agg"."ARTICLE_NO")
      AND ("allocation_rollup"."YEAR_PERIOD" = "prepaid_freight_agg"."YEAR_PERIOD")
      AND ("allocation_rollup"."flag_join" = "prepaid_freight_agg"."prepaid_flag");

<br>  
--ALLOCATION_ROLLUP_JOINED_STACKED
</br>
CREATE OR REPLACE TEMPORARY TABLE "ALLOCATION_ROLLUP_JOINED_STACKED"
AS
SELECT DISTINCT
    "ARTICLE_NO" AS "ARTICLE_NO",
    "MERCH_YEAR" AS "MERCH_YEAR",
    "VENDOR_ID" AS "VENDOR_ID",
    "PARENT_VENDOR_ID" AS "PARENT_VENDOR_ID",
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
    "ARTICLE_TYPE_CODE" AS "ARTICLE_TYPE_CODE",
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
    "BRAND_NAME" AS "BRAND_NAME",
    "PVT_LABEL_DESC" AS "PVT_LABEL_DESC",
    "VENDOR_NAME" AS "VENDOR_NAME",
    "VENDOR_FULL" AS "VENDOR_FULL",
    "VENDOR_CO_NAME" AS "VENDOR_CO_NAME",
    "REGION_CODE" AS "REGION_CODE",
    "ORDER_LINE_TYPE" AS "ORDER_LINE_TYPE",
    "SCAC" AS "SCAC",
    "OMS_ORDER_LINE_SCHEDULE_SHIP_NODE" AS "OMS_ORDER_LINE_SCHEDULE_SHIP_NODE",
    "SALES_QTY" AS "SALES_QTY",
    "SALES_AMT" AS "SALES_AMT",
    "COST_AMT" AS "COST_AMT",
    "DISC_AMT" AS "DISC_AMT",
    "OVRD_AMT" AS "OVRD_AMT",
    "TRANS_TAX_AMT" AS "TRANS_TAX_AMT",
    "GROSS_SALES" AS "GROSS_SALES",
    "SHIPPING_REVENUE" AS "SHIPPING_REVENUE",
    "SHIPPING_DISC" AS "SHIPPING_DISC",
    "ACTUAL_SHIPPING_REVENUE" AS "ACTUAL_SHIPPING_REVENUE",
    "omni_tsc_delivery" AS "omni_tsc_delivery",
    "omni_small_package_ltl" AS "omni_small_package_ltl",
    "omni_roadie" AS "omni_roadie",
    "0mni_multi_salvage" AS "0mni_multi_salvage",
    "SALES_ALLOCATE" AS "SALES_ALLOCATE",
    "COST_ALLOCATE" AS "COST_ALLOCATE",
    "unsellables" AS "unsellables",
    "defectives" AS "defectives",
    "dc_shrink" AS "dc_shrink",
    "theft" AS "theft",
    "defective_allowance" AS "defective_allowance",
    "battery_core" AS "battery_core",
    "mixing_center" AS "mixing_center",
    "shrink_inventory" AS "shrink_inventory",
    "new_store_discounts" AS "new_store_discounts",
    "cash_discounts" AS "cash_discounts",
    "pop" AS "pop",
    "pdq" AS "pdq",
    "marketing" AS "marketing",
    "vendor_compliance" AS "vendor_compliance",
    "booth_rental" AS "booth_rental",
    "shrink_rate" AS "shrink_rate",
    "weighted_domestic_inbound" AS "weighted_domestic_inbound",
    "weighted_ocean_freight" AS "weighted_ocean_freight",
    "weighted_domestic_outbound" AS "weighted_domestic_outbound",
    "weighted_freight_cost" AS "weighted_freight_cost",
    "weighted_agency_fee" AS "weighted_agency_fee",
    "weighted_duty_rate" AS "weighted_duty_rate",
    "freight_join_flag" AS "freight_join_flag",
    "prepaid_share" AS "prepaid_share",
    "weighted_prepaid_freight" AS "weighted_prepaid_freight",
    "prepaid_flag" AS "prepaid_flag",
    'Store' AS "channel"
  FROM "ALLOCATION_ROLLUP_JOINED"
  UNION ALL 
 SELECT DISTINCT
    "ARTICLE_NO" AS "ARTICLE_NO",
    "MERCH_YEAR" AS "MERCH_YEAR",
    "VENDOR_ID" AS "VENDOR_ID",
    "PARENT_VENDOR_ID" AS "PARENT_VENDOR_ID",
    "YEAR_PERIOD" AS "YEAR_PERIOD",
    NULL AS "CO_CODE",
    NULL AS "SUM_TYPE_CODE",
    NULL AS "PRICE_OVRD_FLAG",
    NULL AS "SCAN_ITEM_FLAG",
    NULL AS "PRICE_TYPE_CODE",
    NULL AS "BUY_ONLINE_CODE",
    "YEAR_WEEK" AS "YEAR_WEEK",
    "YEAR_QUARTER" AS "YEAR_QUARTER",
    "ARTICLE_DESC" AS "ARTICLE_DESC",
    "ARTICLE_FULL" AS "ARTICLE_FULL",
    NULL AS "ARTICLE_TYPE_NAME",
    NULL AS "ARTICLE_TYPE_CODE",
    NULL AS "ARTICLE_TYPE_FULL",
    NULL AS "LABEL_TYPE_CODE",
    NULL AS "UOM_CODE",
    NULL AS "UOM_DESC",
    NULL AS "UOM_FULL",
    NULL AS "ARTICLE_LENGTH",
    NULL AS "ARTICLE_WIDTH",
    NULL AS "ARTICLE_HEIGHT",
    NULL AS "ARTICLE_WEIGHT",
    "ARTICLE_SIZE" AS "ARTICLE_SIZE",
    NULL AS "ARTICLE_COUNT",
    NULL AS "LOC_TYPE_CODE",
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
    "BRAND_NAME" AS "BRAND_NAME",
    "PVT_LABEL_DESC" AS "PVT_LABEL_DESC",
    "VENDOR_NAME" AS "VENDOR_NAME",
    "VENDOR_FULL" AS "VENDOR_FULL",
    NULL AS "VENDOR_CO_NAME",
    NULL AS "REGION_CODE",
    "ORDER_LINE_TYPE" AS "ORDER_LINE_TYPE",
    "SCAC" AS "SCAC",
    "OMS_ORDER_LINE_SCHEDULE_SHIP_NODE" AS "OMS_ORDER_LINE_SCHEDULE_SHIP_NODE",
    "SALES_QTY" AS "SALES_QTY",
    "SALES_AMT" AS "SALES_AMT",
    "COST_AMT" AS "COST_AMT",
    NULL AS "DISC_AMT",
    NULL AS "OVRD_AMT",
    NULL AS "TRANS_TAX_AMT",
    NULL AS "GROSS_SALES",
    "SHIPPING_REVENUE" AS "SHIPPING_REVENUE",
    "SHIPPING_DISC_sum" AS "SHIPPING_DISC",
    "ACTUAL_SHIPPING_REVENUE" AS "ACTUAL_SHIPPING_REVENUE",
    "tsc_delivery_combined" AS "omni_tsc_delivery",
    "omni_small_package_ltl_comb" AS "omni_small_package_ltl",
    "roadie_combined" AS "omni_roadie",
    "multi_salvage_combined" AS "0mni_multi_salvage",
    "SALES_ALLOCATE" AS "SALES_ALLOCATE",
    "COST_ALLOCATE" AS "COST_ALLOCATE",
    NULL AS "unsellables",
    NULL AS "defectives",
    NULL AS "dc_shrink",
    NULL AS "theft",
    NULL AS "defective_allowance",
    NULL AS "battery_core",
    NULL AS "mixing_center",
    NULL AS "shrink_inventory",
    NULL AS "new_store_discounts",
    NULL AS "cash_discounts",
    NULL AS "pop",
    NULL AS "pdq",
    NULL AS "marketing",
    NULL AS "vendor_compliance",
    NULL AS "booth_rental",
    NULL AS "shrink_rate",
    NULL AS "weighted_domestic_inbound",
    NULL AS "weighted_ocean_freight",
    NULL AS "weighted_domestic_outbound",
    NULL AS "weighted_freight_cost",
    NULL AS "weighted_agency_fee",
    NULL AS "weighted_duty_rate",
    NULL AS "freight_join_flag",
    NULL AS "prepaid_share",
    NULL AS "weighted_prepaid_freight",
    NULL AS "prepaid_flag",
    'Omni' AS "channel"
FROM "OMNI_SALES_GRP"
  WHERE ("ORDER_LINE_TYPE" != 'BOPIS' OR "ORDER_LINE_TYPE" IS NULL AND 'BOPIS' IS NOT NULL OR "ORDER_LINE_TYPE" IS NOT NULL AND 'BOPIS' IS NULL) AND ("ORDER_LINE_TYPE" != 'BODFS' OR "ORDER_LINE_TYPE" IS NULL AND 'BODFS' IS NOT NULL OR "ORDER_LINE_TYPE" IS NOT NULL AND 'BODFS' IS NULL);

<br>
--FINAL QUERY FOR ALLOCATION_ROLLUP_JOINED_PREPARED_JOINED 
</br>

CREATE OR REPLACE TEMPORARY TABLE "ALLOCATION_ROLLUP_JOINED_PREPARED_JOINED"
AS
WITH ALLOCATION_ROLLUP_JOINED_PREPARED AS
(
SELECT DISTINCT
    "ARTICLE_NO",
    "MERCH_YEAR",
    "VENDOR_ID",
    "PARENT_VENDOR_ID",
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
    "ARTICLE_TYPE_CODE",
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
    "BRAND_NAME",
    "PVT_LABEL_DESC",
    "VENDOR_NAME",
    "VENDOR_FULL",
    "VENDOR_CO_NAME",
    "REGION_CODE",
    "ORDER_LINE_TYPE",
    "SCAC",
    "OMS_ORDER_LINE_SCHEDULE_SHIP_NODE",
    "SALES_QTY",
    "SALES_AMT",
    "COST_AMT",
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
    "0mni_multi_salvage",
    "SALES_ALLOCATE",
    "COST_ALLOCATE",
    "unsellables",
    "defectives",
    "dc_shrink",
    "theft",
    "defective_allowance",
    "battery_core",
    "mixing_center",
    "shrink_inventory",
    "new_store_discounts",
    "cash_discounts",
    "pop",
    "pdq",
    "marketing",
    "vendor_compliance",
    "booth_rental",
    "shrink_rate",
    "weighted_domestic_inbound",
    "weighted_ocean_freight",
    "weighted_domestic_outbound",
    "weighted_freight_cost",
    "weighted_agency_fee",
    "weighted_duty_rate",
    "freight_join_flag",
    "prepaid_share",
    "weighted_prepaid_freight",
    "prepaid_flag",
     CASE WHEN ("ORDER_LINE_TYPE" = 'BODFS') OR ("ORDER_LINE_TYPE" = 'BOPIS') THEN 'Omni'
                  WHEN "channel" IS NULL THEN NULL ELSE "channel" END AS  "channel1",
    "prepaid_share_modified",
    "cost_allocate_freight",
    "qty_allocate_freight",
    "support_rate_flag",
    "vendor_support_fast_flag",
    "markdown_perc",
    CASE WHEN "GROSS_SALES" > 0 AND "SALES_AMT" > 0 AND "GROSS_SALES" < "SALES_AMT" THEN 0 ELSE CASE WHEN "GROSS_SALES" < 0 AND "SALES_AMT" < 0 THEN 0 ELSE CASE WHEN "markdown_perc" > 0.1 OR "markdown_perc" < -0.1 THEN 1 ELSE 0 END END END AS "markdown_flag"
  FROM (
    SELECT DISTINCT
        CAST("ARTICLE_NO" AS NUMBER) AS "ARTICLE_NO" ,
        CAST("MERCH_YEAR" AS NUMBER) AS "MERCH_YEAR",
        CAST("VENDOR_ID" AS NUMBER) AS "VENDOR_ID",
        CAST("PARENT_VENDOR_ID" AS NUMBER) AS "PARENT_VENDOR_ID",
        CAST("YEAR_PERIOD" AS NUMBER) AS "YEAR_PERIOD",
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
        "ARTICLE_TYPE_CODE",
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
        "BRAND_NAME",
        "PVT_LABEL_DESC",
        "VENDOR_NAME",
        "VENDOR_FULL",
        "VENDOR_CO_NAME",
        "REGION_CODE",
        "ORDER_LINE_TYPE",
        "SCAC",
        "OMS_ORDER_LINE_SCHEDULE_SHIP_NODE",
        "SALES_QTY",
        "SALES_AMT",
        "COST_AMT",
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
        "0mni_multi_salvage",
        "SALES_ALLOCATE",
        "COST_ALLOCATE",
        "unsellables",
        "defectives",
        "dc_shrink",
        "theft",
        "defective_allowance",
        "battery_core",
        "mixing_center",
        "shrink_inventory",
        "new_store_discounts",
        "cash_discounts",
        "pop",
        "pdq",
        "marketing",
        "vendor_compliance",
        "booth_rental",
        "shrink_rate",
        "weighted_domestic_inbound",
        "weighted_ocean_freight",
        "weighted_domestic_outbound",
        "weighted_freight_cost",
        "weighted_agency_fee",
        "weighted_duty_rate",
        "freight_join_flag",
        "prepaid_share",
        "weighted_prepaid_freight",
        "prepaid_flag",
        "channel" AS "channel",
        COALESCE(CASE WHEN "freight_join_flag" IS NULL AND "prepaid_flag" IS NOT NULL THEN 1 ELSE CASE WHEN "freight_join_flag" IS NOT NULL AND "prepaid_flag" IS NULL THEN 0 ELSE CASE WHEN ("freight_join_flag" = 1) AND ("prepaid_flag" = 1) THEN CASE WHEN "weighted_prepaid_freight" = 0 THEN 0 ELSE CASE WHEN "weighted_domestic_inbound" + "weighted_domestic_outbound" = 0 THEN 1 ELSE "prepaid_share" END END ELSE "prepaid_share" END END END, 0) AS "prepaid_share_modified",
        CASE WHEN "COST_AMT" < 0 THEN 0 ELSE "COST_AMT" END AS "cost_allocate_freight",
        CASE WHEN "SALES_QTY" < 0 THEN 0 ELSE "SALES_QTY" END AS "qty_allocate_freight",
        CASE WHEN "YEAR_PERIOD" = 202301 THEN 202301 ELSE CASE WHEN "YEAR_PERIOD" >= 202212 THEN 202212 ELSE CASE WHEN "YEAR_PERIOD" >= 202207 THEN 202207 ELSE CASE WHEN "YEAR_PERIOD" >= 202001 THEN 202001 ELSE 0 END END END END AS "support_rate_flag",
        CASE WHEN UPPER("channel") = 'STORE' THEN CASE WHEN "YEAR_PERIOD" = 202301 THEN 202301 ELSE CASE WHEN "YEAR_PERIOD" >= 202212 THEN 202212 ELSE CASE WHEN "YEAR_PERIOD" >= 202207 THEN 202207 ELSE CASE WHEN "YEAR_PERIOD" >= 202101 THEN 202101 ELSE 0 END END END END ELSE 0 END AS "vendor_support_fast_flag",
        CASE WHEN ("GROSS_SALES" = 0) OR "GROSS_SALES" IS NULL OR ("SALES_AMT" = 0) THEN 0 ELSE "GROSS_SALES" / NULLIF("SALES_AMT", 0) - 1 END AS "markdown_perc"
      FROM "ALLOCATION_ROLLUP_JOINED_STACKED"
    ) A
)
SELECT DISTINCT
    "allocation_rollup_joined_prepared"."ARTICLE_NO" AS "ARTICLE_NO",
    "allocation_rollup_joined_prepared"."MERCH_YEAR" AS "MERCH_YEAR",
    "allocation_rollup_joined_prepared"."VENDOR_ID" AS "VENDOR_ID",
    "allocation_rollup_joined_prepared"."PARENT_VENDOR_ID" AS "PARENT_VENDOR_ID",
    "allocation_rollup_joined_prepared"."YEAR_PERIOD" AS "YEAR_PERIOD",
    "allocation_rollup_joined_prepared"."vendor_support_fast_flag" AS "vendor_support_fast_flag",
    "allocation_rollup_joined_prepared"."support_rate_flag" AS "support_rate_flag",
    "allocation_rollup_joined_prepared"."CO_CODE" AS "CO_CODE",
    "allocation_rollup_joined_prepared"."SUM_TYPE_CODE" AS "SUM_TYPE_CODE",
    "allocation_rollup_joined_prepared"."PRICE_OVRD_FLAG" AS "PRICE_OVRD_FLAG",
    "allocation_rollup_joined_prepared"."SCAN_ITEM_FLAG" AS "SCAN_ITEM_FLAG",
    "allocation_rollup_joined_prepared"."PRICE_TYPE_CODE" AS "PRICE_TYPE_CODE",
    "allocation_rollup_joined_prepared"."BUY_ONLINE_CODE" AS "BUY_ONLINE_CODE",
    "allocation_rollup_joined_prepared"."YEAR_WEEK" AS "YEAR_WEEK",
    "allocation_rollup_joined_prepared"."YEAR_QUARTER" AS "YEAR_QUARTER",
    "allocation_rollup_joined_prepared"."ARTICLE_DESC" AS "ARTICLE_DESC",
    "allocation_rollup_joined_prepared"."ARTICLE_FULL" AS "ARTICLE_FULL",
    "allocation_rollup_joined_prepared"."ARTICLE_TYPE_NAME" AS "ARTICLE_TYPE_NAME",
    "allocation_rollup_joined_prepared"."ARTICLE_TYPE_CODE" AS "ARTICLE_TYPE_CODE",
    "allocation_rollup_joined_prepared"."ARTICLE_TYPE_FULL" AS "ARTICLE_TYPE_FULL",
    "allocation_rollup_joined_prepared"."LABEL_TYPE_CODE" AS "LABEL_TYPE_CODE",
    "allocation_rollup_joined_prepared"."UOM_CODE" AS "UOM_CODE",
    "allocation_rollup_joined_prepared"."UOM_DESC" AS "UOM_DESC",
    "allocation_rollup_joined_prepared"."UOM_FULL" AS "UOM_FULL",
    "allocation_rollup_joined_prepared"."ARTICLE_LENGTH" AS "ARTICLE_LENGTH",
    "allocation_rollup_joined_prepared"."ARTICLE_WIDTH" AS "ARTICLE_WIDTH",
    "allocation_rollup_joined_prepared"."ARTICLE_HEIGHT" AS "ARTICLE_HEIGHT",
    "allocation_rollup_joined_prepared"."ARTICLE_WEIGHT" AS "ARTICLE_WEIGHT",
    "allocation_rollup_joined_prepared"."ARTICLE_SIZE" AS "ARTICLE_SIZE",
    "allocation_rollup_joined_prepared"."ARTICLE_COUNT" AS "ARTICLE_COUNT",
    "allocation_rollup_joined_prepared"."LOC_TYPE_CODE" AS "LOC_TYPE_CODE",
    "allocation_rollup_joined_prepared"."PRIMARY_VENDOR_ID" AS "PRIMARY_VENDOR_ID",
    "allocation_rollup_joined_prepared"."ARTICLE_CAT_CODE" AS "ARTICLE_CAT_CODE",
    "allocation_rollup_joined_prepared"."ARTICLE_CAT_NAME" AS "ARTICLE_CAT_NAME",
    "allocation_rollup_joined_prepared"."ARTICLE_CAT_FULL" AS "ARTICLE_CAT_FULL",
    "allocation_rollup_joined_prepared"."DEPT_CODE" AS "DEPT_CODE",
    "allocation_rollup_joined_prepared"."DEPT_NAME" AS "DEPT_NAME",
    "allocation_rollup_joined_prepared"."DEPT_FULL" AS "DEPT_FULL",
    "allocation_rollup_joined_prepared"."LOB_NO" AS "LOB_NO",
    "allocation_rollup_joined_prepared"."LOB_DESC" AS "LOB_DESC",
    "allocation_rollup_joined_prepared"."LOB_FULL" AS "LOB_FULL",
    "allocation_rollup_joined_prepared"."MAJ_DEPT_NO" AS "MAJ_DEPT_NO",
    "allocation_rollup_joined_prepared"."MAJ_DEPT_DESC" AS "MAJ_DEPT_DESC",
    "allocation_rollup_joined_prepared"."MAJ_DEPT_FULL" AS "MAJ_DEPT_FULL",
    "allocation_rollup_joined_prepared"."BRAND_NAME" AS "BRAND_NAME",
    "allocation_rollup_joined_prepared"."PVT_LABEL_DESC" AS "PVT_LABEL_DESC",
    "allocation_rollup_joined_prepared"."VENDOR_NAME" AS "VENDOR_NAME",
    "allocation_rollup_joined_prepared"."VENDOR_FULL" AS "VENDOR_FULL",
    "allocation_rollup_joined_prepared"."VENDOR_CO_NAME" AS "VENDOR_CO_NAME",
    "allocation_rollup_joined_prepared"."REGION_CODE" AS "REGION_CODE",
    "allocation_rollup_joined_prepared"."ORDER_LINE_TYPE" AS "ORDER_LINE_TYPE",
    "allocation_rollup_joined_prepared"."SCAC" AS "SCAC",
    "allocation_rollup_joined_prepared"."OMS_ORDER_LINE_SCHEDULE_SHIP_NODE" AS "OMS_ORDER_LINE_SCHEDULE_SHIP_NODE",
    "allocation_rollup_joined_prepared"."SALES_QTY" AS "SALES_QTY",
    "allocation_rollup_joined_prepared"."qty_allocate_freight" AS "qty_allocate_freight",
    "allocation_rollup_joined_prepared"."SALES_AMT" AS "SALES_AMT",
    "allocation_rollup_joined_prepared"."COST_AMT" AS "COST_AMT",
    "allocation_rollup_joined_prepared"."cost_allocate_freight" AS "cost_allocate_freight",
    "allocation_rollup_joined_prepared"."DISC_AMT" AS "DISC_AMT",
    "allocation_rollup_joined_prepared"."OVRD_AMT" AS "OVRD_AMT",
    "allocation_rollup_joined_prepared"."TRANS_TAX_AMT" AS "TRANS_TAX_AMT",
    "allocation_rollup_joined_prepared"."GROSS_SALES" AS "GROSS_SALES",
    "allocation_rollup_joined_prepared"."markdown_perc" AS "markdown_perc",
    "allocation_rollup_joined_prepared"."markdown_flag" AS "markdown_flag",
    "allocation_rollup_joined_prepared"."SHIPPING_REVENUE" AS "SHIPPING_REVENUE",
    "allocation_rollup_joined_prepared"."SHIPPING_DISC" AS "SHIPPING_DISC",
    "allocation_rollup_joined_prepared"."ACTUAL_SHIPPING_REVENUE" AS "ACTUAL_SHIPPING_REVENUE",
    "allocation_rollup_joined_prepared"."omni_tsc_delivery" AS "omni_tsc_delivery",
    "allocation_rollup_joined_prepared"."omni_small_package_ltl" AS "omni_small_package_ltl",
    "allocation_rollup_joined_prepared"."omni_roadie" AS "omni_roadie",
    "allocation_rollup_joined_prepared"."0mni_multi_salvage" AS "0mni_multi_salvage",
    "allocation_rollup_joined_prepared"."SALES_ALLOCATE" AS "SALES_ALLOCATE",
    "allocation_rollup_joined_prepared"."COST_ALLOCATE" AS "COST_ALLOCATE",
    "allocation_rollup_joined_prepared"."unsellables" AS "unsellables",
    "allocation_rollup_joined_prepared"."defectives" AS "defectives",
    "allocation_rollup_joined_prepared"."dc_shrink" AS "dc_shrink",
    "allocation_rollup_joined_prepared"."theft" AS "theft",
    "allocation_rollup_joined_prepared"."defective_allowance" AS "defective_allowance",
    "allocation_rollup_joined_prepared"."battery_core" AS "battery_core",
    "allocation_rollup_joined_prepared"."mixing_center" AS "mixing_center",
    "allocation_rollup_joined_prepared"."shrink_inventory" AS "shrink_inventory",
    "allocation_rollup_joined_prepared"."new_store_discounts" AS "new_store_discounts",
    "allocation_rollup_joined_prepared"."cash_discounts" AS "cash_discounts",
    "allocation_rollup_joined_prepared"."pop" AS "pop",
    "allocation_rollup_joined_prepared"."pdq" AS "pdq",
    "allocation_rollup_joined_prepared"."marketing" AS "marketing",
    "allocation_rollup_joined_prepared"."vendor_compliance" AS "vendor_compliance",
    "allocation_rollup_joined_prepared"."booth_rental" AS "booth_rental",
    "allocation_rollup_joined_prepared"."shrink_rate" AS "shrink_rate",
    "allocation_rollup_joined_prepared"."weighted_domestic_inbound" AS "weighted_domestic_inbound",
    "allocation_rollup_joined_prepared"."weighted_ocean_freight" AS "weighted_ocean_freight",
    "allocation_rollup_joined_prepared"."weighted_domestic_outbound" AS "weighted_domestic_outbound",
    "allocation_rollup_joined_prepared"."weighted_freight_cost" AS "weighted_freight_cost",
    "allocation_rollup_joined_prepared"."weighted_agency_fee" AS "weighted_agency_fee",
    "allocation_rollup_joined_prepared"."weighted_duty_rate" AS "weighted_duty_rate",
    "allocation_rollup_joined_prepared"."freight_join_flag" AS "freight_join_flag",
    "allocation_rollup_joined_prepared"."prepaid_share" AS "prepaid_share",
    "allocation_rollup_joined_prepared"."weighted_prepaid_freight" AS "weighted_prepaid_freight",
    "allocation_rollup_joined_prepared"."prepaid_flag" AS "prepaid_flag",
    "allocation_rollup_joined_prepared"."prepaid_share_modified" AS "prepaid_share_modified",
    "allocation_rollup_joined_prepared"."channel1" AS "channel",
    "lm_vendor_support_rates_stacked"."vendor_support_perc" AS "vendor_support_perc",
    "lm_vendor_support_fast_rates_stacked"."vendor_support_fast_perc" AS "vendor_support_fast_perc",
    "TENANT149_DATA_scandowns_flag_clean_by_year_period_new"."scandown_flag" AS "scandown_flag"
  FROM "ALLOCATION_ROLLUP_JOINED_PREPARED" "allocation_rollup_joined_prepared"
  LEFT JOIN "LM_VENDOR_SUPPORT_RATES_STACKED" "lm_vendor_support_rates_stacked"
    ON ("allocation_rollup_joined_prepared"."VENDOR_ID" = "lm_vendor_support_rates_stacked"."VENDOR_ID")
      AND ("allocation_rollup_joined_prepared"."PARENT_VENDOR_ID" = "lm_vendor_support_rates_stacked"."PARENT_VENDOR_ID")
      AND ("allocation_rollup_joined_prepared"."support_rate_flag" = "lm_vendor_support_rates_stacked"."support_rate_flag")
  LEFT JOIN "LM_VENDOR_SUPPORT_FAST_RATES_STACKED" "lm_vendor_support_fast_rates_stacked"
    ON ("allocation_rollup_joined_prepared"."VENDOR_ID" = "lm_vendor_support_fast_rates_stacked"."VENDOR_ID")
      AND ("allocation_rollup_joined_prepared"."vendor_support_fast_flag" = "lm_vendor_support_fast_rates_stacked"."vendor_support_fast_flag")
  LEFT JOIN (
    SELECT DISTINCT
        "year_period_new" AS "year_period_new",
        "year" AS "year",
        "Vendor_No" AS "Vendor_No",
        "article_int" AS "article_int",
        1 AS "scandown_flag"
      FROM (
        SELECT *
          FROM ( SELECT DISTINCT
                "year_period_new" AS "year_period_new",
                LEFT("year_period_new",4) AS "year" ,
                "Vendor_No" AS "Vendor_No",
                "article_int" AS "article_int"
                  FROM SCANDOWNS_FLAG_CLEAN
                  GROUP BY "year_period_new", "year", "Vendor_No", "article_int") "TENANT149_DATA_scandowns_flag_clean_by_year_period_new"
        ) "withoutcomputedcols_query"
    ) "TENANT149_DATA_scandowns_flag_clean_by_year_period_new"
    ON ("allocation_rollup_joined_prepared"."ARTICLE_NO" = "TENANT149_DATA_scandowns_flag_clean_by_year_period_new"."article_int")
      AND ("allocation_rollup_joined_prepared"."VENDOR_ID" = "TENANT149_DATA_scandowns_flag_clean_by_year_period_new"."Vendor_No")
      AND ("allocation_rollup_joined_prepared"."YEAR_PERIOD" = "TENANT149_DATA_scandowns_flag_clean_by_year_period_new"."year_period_new");


<br>
--FINAL QUERY FOR MASTER_SALES_ROUND_P2 
</br>
INSERT INTO "MASTER_SALES_ROUND_P2"
 SELECT 
"ARTICLE_NO",
"MERCH_YEAR",
"VENDOR_ID",
"PARENT_VENDOR_ID",
"YEAR_PERIOD",
"vendor_support_fast_flag",
"support_rate_flag",
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
"ARTICLE_TYPE_CODE",
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
"BRAND_NAME",
"PVT_LABEL_DESC",
"VENDOR_NAME",
"VENDOR_FULL",
"VENDOR_CO_NAME",
"REGION_CODE",
"ORDER_LINE_TYPE",
"SCAC",
"OMS_ORDER_LINE_SCHEDULE_SHIP_NODE",
"SALES_QTY",
"qty_allocate_freight",
"SALES_AMT",
"COST_AMT",
"margin",
"cost_allocate_freight",
DISC_AMT,
"OVRD_AMT",
"TRANS_TAX_AMT",
"GROSS_SALES",
"markdown_perc",
"markdown_flag",
SHIPPING_REVENUE,
"SHIPPING_DISC",
ACTUAL_SHIPPING_REVENUE,
"omni_tsc_delivery",
"omni_small_package_ltl",
"omni_roadie",
"0mni_multi_salvage",
"SALES_ALLOCATE",
"COST_ALLOCATE",
"unsellables",
"defectives",
"dc_shrink",
"theft",
"defective_allowance",
"battery_core",
"mixing_center",
"shrink_inventory",
"new_store_discounts",
"cash_discounts",
"pop",
"pdq",
"marketing",
"vendor_compliance",
"booth_rental",
"shrink_rate",
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
"freight_join_flag",
"prepaid_share",
"weighted_prepaid_freight",
"prepaid_flag",
"prepaid_share_modified",
"prepaid_freight",
"non_prepaid_share",
"domestic_inbound",
"domestic_outbound",
"total_freight",
CASE WHEN "total_freight" = 0 THEN 0 ELSE 1 END AS "freight_flag",
"domestic_outbound_w_prepaid",
"channel",
"vendor_support_perc",
"vendor_support_dol",
"vendor_support_fast_perc",
"vendor_support_fast_dol",
"scandown_flag"
  FROM (
    SELECT 
        *,
        COALESCE("domestic_inbound", 0) + COALESCE("domestic_outbound", 0) + COALESCE("prepaid_freight", 0) + COALESCE("ocean_freight", 0) + COALESCE("agency_fee", 0) + COALESCE("duty_tarrifs", 0) AS "total_freight",
        COALESCE("vendor_support_perc", 0) * COALESCE("COST_AMT", 0) AS "vendor_support_dol",
        COALESCE("vendor_support_fast_perc", 0) * COALESCE("COST_AMT", 0) AS "vendor_support_fast_dol"
      FROM (
        SELECT 
            *,
            COALESCE("domestic_outbound", 0) + COALESCE("prepaid_freight", 0) AS "domestic_outbound_w_prepaid",
            COALESCE("weighted_ocean_freight", 0) * COALESCE("qty_allocate_freight", 0) AS "ocean_freight",
            COALESCE("weighted_duty_rate", 0) * COALESCE("cost_allocate_freight", 0) AS "duty_tarrifs",
            COALESCE("weighted_agency_fee", 0) * COALESCE("cost_allocate_freight", 0) AS "agency_fee",
            COALESCE("SALES_AMT", 0) - COALESCE("COST_AMT", 0) AS "margin",
            COALESCE("unsellables", 0) + COALESCE("defectives", 0) + COALESCE("dc_shrink", 0) + COALESCE("theft", 0) + COALESCE("inventory_shrink", 
             0) AS "total_shrink"
          FROM (
            SELECT DISTINCT *,
                CASE WHEN ("ARTICLE_NO" = 3195163) OR ("ARTICLE_NO" = 1115622) THEN 0 ELSE COALESCE("non_prepaid_share", 0) * 
                           COALESCE("weighted_domestic_outbound", 0) * COALESCE("qty_allocate_freight", 0) END AS "domestic_outbound",
                            COALESCE("non_prepaid_share", 0) * COALESCE("weighted_domestic_inbound", 0) * COALESCE("qty_allocate_freight", 0) AS 
                             "domestic_inbound",
                CASE WHEN ("ARTICLE_NO" = 3195163) OR ("ARTICLE_NO" = 1115622) THEN 0 ELSE COALESCE("prepaid_share_modified", 0) * 
                           COALESCE("weighted_prepaid_freight", 0) * COALESCE("qty_allocate_freight", 0) END AS "prepaid_freight"
              FROM (
                SELECT DISTINCT
                    "ARTICLE_NO",
                    "MERCH_YEAR",
                    "VENDOR_ID",
                    "PARENT_VENDOR_ID",
                    "YEAR_PERIOD",
                    "vendor_support_fast_flag",
                    "support_rate_flag",
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
                    "ARTICLE_TYPE_CODE",
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
                    "BRAND_NAME",
                    "PVT_LABEL_DESC",
                    "VENDOR_NAME",
                    "VENDOR_FULL",
                    "VENDOR_CO_NAME",
                    "REGION_CODE",
                    "ORDER_LINE_TYPE",
                    "SCAC",
                    "OMS_ORDER_LINE_SCHEDULE_SHIP_NODE",
                    "SALES_QTY",
                    "qty_allocate_freight",
                    "SALES_AMT",
                    "COST_AMT",
                    "cost_allocate_freight",
                    "DISC_AMT",
                    "OVRD_AMT",
                    "TRANS_TAX_AMT",
                    "GROSS_SALES",
                    "markdown_perc",
                    "markdown_flag",
                    "SHIPPING_REVENUE",
                    "SHIPPING_DISC",
                    "ACTUAL_SHIPPING_REVENUE",
                    "omni_tsc_delivery",
                    "omni_small_package_ltl",
                    "omni_roadie",
                    "0mni_multi_salvage",
                    "SALES_ALLOCATE",
                    "COST_ALLOCATE",
                    "unsellables",
                    "defectives",
                    "dc_shrink",
                    "theft",
                    "defective_allowance",
                    "battery_core",
                    "mixing_center",
                    "shrink_inventory",
                    "new_store_discounts",
                    "cash_discounts",
                    "pop",
                    "pdq",
                    "marketing",
                    "vendor_compliance",
                    "booth_rental",
                    "shrink_rate",
                    "weighted_domestic_inbound",
                    "weighted_ocean_freight",
                    "weighted_domestic_outbound",
                    "weighted_freight_cost",
                    "weighted_agency_fee",
                    "weighted_duty_rate",
                    "freight_join_flag",
                    "prepaid_share",
                    "weighted_prepaid_freight",
                    "prepaid_flag",
                    "prepaid_share_modified",
                    "channel",
                    "vendor_support_perc",
                    "vendor_support_fast_perc",
                    "scandown_flag",
                    COALESCE("shrink_rate", 0) * COALESCE("SALES_AMT", 0) AS "inventory_shrink",
                    COALESCE(1 - "prepaid_share_modified", 0) AS "non_prepaid_share",
                    COALESCE("weighted_freight_cost", 0) * COALESCE("qty_allocate_freight", 0) AS "freight_cost"
                  FROM "ALLOCATION_ROLLUP_JOINED_PREPARED_JOINED"
                ) A
            ) B
        ) C
    ) D;
	  
<br>
--master table 
</br>
insert into  DC_3PL_OMNI_FULFILLMENT_BASE_FINAL_SF  
select DISTINCT
"ACCT_YEAR",
"YEAR_PERIOD",
"YEAR_WEEK",
"OMS_ORDER_NO",
"OMS_ITEM_ID",
"OMS_ORDER_LINE_KEY",
"OMS_ITEM_PRIMARY_VENDOR_ID",
"OMS_ORDER_LINE_SCHEDULE_SHIP_NODE",
"STORE_NO",
"OMS_SHIP_NODE_DESC",
"ARTICLE_CAT_FULL",
"DEMAND_QTY",
"DEMAND_SALES",
"BOOKED_SALES_QTY",
"BOOKED_SALES_AMT",
"BOOKED_COST_AMT",
"SHIPPING_REVENUE",
"SHIPPING_DISC",
"ACTUAL_SHIPPING_REVENUE",
"ORDER_LINE_TYPE",
"SCAC",
"SHIP_NODE_TYPE",
"MERCH_YEAR",
"CHANNEL"
from TENANT149_DATA_DC_3PL_omni_fulfillment_base_prepared;

<br>
--spark sql for 
--input file= allocation_control_pt2_sp
--master file ALLOCATION_W_INVENTORY_SHRINK_PREPARED_RENAMED for 
--output file =allocation_sp_p2_sp
-- preparing master table
</br>      
insert into ALLOCATION_W_INVENTORY_SHRINK_PREPARED_PRECOMPUTE 
WITH CTE AS (
      SELECT DISTINCT
          ARTICLE_NO,
          MERCH_YEAR,
          VENDOR_ID,
          PARENT_VENDOR_ID,
          YEAR_PERIOD,
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
          cost_allocate_freight,
          DISC_AMT,
          OVRD_AMT,
          TRANS_TAX_AMT,
          GROSS_SALES,
          markdown_perc,
          markdown_flag,
          SHIPPING_REVENUE,
          SHIPPING_DISC,
          ACTUAL_SHIPPING_REVENUE,
          omni_tsc_delivery,
          omni_small_package_ltl,
          omni_roadie,
          "0mni_multi_salvage",
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
          weighted_domestic_inbound,
          weighted_ocean_freight,
          weighted_domestic_outbound,
          weighted_freight_cost,
          weighted_agency_fee,
          weighted_duty_rate,
          freight_join_flag,
          prepaid_share,
          weighted_prepaid_freight,
          prepaid_flag,
          prepaid_share_modified,
          channel,
          vendor_support_perc,
          vendor_support_fast_perc,
          scandown_flag,
          COALESCE(shrink_rate, 0) * COALESCE(SALES_AMT, 0) AS inventory_shrink,
          COALESCE(1 - prepaid_share_modified, 0) AS non_prepaid_share,
          COALESCE(weighted_freight_cost, 0) * COALESCE(qty_allocate_freight, 0) AS freight_cost
        FROM (select DISTINCT
                        ARTICLE_NO,
                        MERCH_YEAR,
                        VENDOR_ID,
                        PARENT_VENDOR_ID,
                        YEAR_PERIOD,
                        "vendor_support_fast_flag" as vendor_support_fast_flag,
                        "support_rate_flag" as support_rate_flag,
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
                        "qty_allocate_freight" as qty_allocate_freight,
                        SALES_AMT,
                        COST_AMT,
                        "cost_allocate_freight" as cost_allocate_freight,
                        DISC_AMT,
                        OVRD_AMT,
                        TRANS_TAX_AMT,
                        GROSS_SALES,
                        "markdown_perc" as markdown_perc,
                        "markdown_flag" as markdown_flag,
                        SHIPPING_REVENUE,
                        SHIPPING_DISC,
                        ACTUAL_SHIPPING_REVENUE,
                        "omni_tsc_delivery" as omni_tsc_delivery,
                        "omni_small_package_ltl" as 	omni_small_package_ltl,
                        "omni_roadie" as 	omni_roadie,
                        "0mni_multi_salvage" as 	"0mni_multi_salvage",
                        "SALES_ALLOCATE" as 	SALES_ALLOCATE,
                        "COST_ALLOCATE" as 	COST_ALLOCATE,
                        "unsellables" as 	unsellables,
                        "defectives" as 	defectives,
                        "dc_shrink" as 	dc_shrink,
                        "theft" as 	theft,
                        "defective_allowance" as 	defective_allowance,
                        "battery_core" as 	battery_core,
                        "mixing_center" as 	mixing_center,
                        "shrink_inventory" as 	shrink_inventory,
                        "new_store_discounts" as 	new_store_discounts,
                        "cash_discounts" as 	cash_discounts,
                        "pop" as 	pop,
                        "pdq" as 	pdq,
                        "marketing" as 	marketing,
                        "vendor_compliance" as 	vendor_compliance,
                        "booth_rental" as 	booth_rental,
                        "shrink_rate" as 	shrink_rate,
                        "weighted_domestic_inbound" as 	weighted_domestic_inbound,
                        "weighted_ocean_freight" as 	weighted_ocean_freight,
                        "weighted_domestic_outbound" as 	weighted_domestic_outbound,
                        "weighted_freight_cost" as 	weighted_freight_cost,
                        "weighted_agency_fee" as 	weighted_agency_fee,
                        "weighted_duty_rate" as 	weighted_duty_rate,
                        "freight_join_flag" as 	freight_join_flag,
                        "prepaid_share" as 	prepaid_share,
                        "weighted_prepaid_freight" as 	weighted_prepaid_freight,
                        "prepaid_flag" as 	prepaid_flag,
                        "prepaid_share_modified" as 	prepaid_share_modified,
                        "channel" as 	channel,
                        "vendor_support_perc" as 	vendor_support_perc,
                        "vendor_support_fast_perc" as 	vendor_support_fast_perc,
                        "scandown_flag" as 	scandown_flag
                    from    "ALLOCATION_ROLLUP_JOINED_PREPARED_JOINED")dku
                        ) 
    SELECT DISTINCT
      ARTICLE_NO,
      MERCH_YEAR,
      VENDOR_ID,
      PARENT_VENDOR_ID,
      YEAR_PERIOD,
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
      cost_allocate_freight,
      DISC_AMT,
      OVRD_AMT,
      TRANS_TAX_AMT,
      GROSS_SALES,
      markdown_perc,
      markdown_flag,
      SHIPPING_REVENUE,
      SHIPPING_DISC,
      ACTUAL_SHIPPING_REVENUE,
      omni_tsc_delivery,
      omni_small_package_ltl,
      omni_roadie,
      "0mni_multi_salvage",
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
      weighted_domestic_inbound,
      weighted_ocean_freight,
      weighted_domestic_outbound,
      weighted_freight_cost,
      weighted_agency_fee,
      weighted_duty_rate,
      freight_join_flag,
      prepaid_share,
      weighted_prepaid_freight,
      prepaid_flag,
      prepaid_share_modified,
      channel,
      vendor_support_perc,
      vendor_support_fast_perc,
      scandown_flag,
      inventory_shrink,
      non_prepaid_share,
      freight_cost,
      CASE WHEN (ARTICLE_NO = 3195163) OR (ARTICLE_NO = 1115622) THEN 0 ELSE COALESCE(non_prepaid_share, 0) * COALESCE(weighted_domestic_outbound, 0) * COALESCE(qty_allocate_freight, 0) END AS domestic_outbound,
      COALESCE(non_prepaid_share, 0) * COALESCE(weighted_domestic_inbound, 0) * COALESCE(qty_allocate_freight, 0) AS domestic_inbound,
      CASE WHEN (ARTICLE_NO = 3195163) OR (ARTICLE_NO = 1115622) THEN 0 ELSE COALESCE(prepaid_share_modified, 0) * COALESCE(weighted_prepaid_freight, 0) * COALESCE(qty_allocate_freight, 0) END AS prepaid_freight
      FROM CTE  ;
  
<br>
--ALLOCATION_W_INVENTORY_SHRINK_PREPARED
</br>
CREATE OR REPLACE TEMP TABLE ALLOCATION_W_INVENTORY_SHRINK_PREPARED
  AS 
  WITH CTE AS (
    SELECT DISTINCT
        ARTICLE_NO,
        MERCH_YEAR,
        VENDOR_ID,
        PARENT_VENDOR_ID,
        YEAR_PERIOD,
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
        cost_allocate_freight,
        DISC_AMT,
        OVRD_AMT,
        TRANS_TAX_AMT,
        GROSS_SALES,
        markdown_perc,
        markdown_flag,
        SHIPPING_REVENUE,
        SHIPPING_DISC,
        ACTUAL_SHIPPING_REVENUE,
        omni_tsc_delivery,
        omni_small_package_ltl,
        omni_roadie,
        "0mni_multi_salvage",
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
        weighted_domestic_inbound,
        weighted_ocean_freight,
        weighted_domestic_outbound,
        weighted_freight_cost,
        weighted_agency_fee,
        weighted_duty_rate,
        freight_join_flag,
        prepaid_share,
        weighted_prepaid_freight,
        prepaid_flag,
        prepaid_share_modified,
        channel,
        vendor_support_perc,
        vendor_support_fast_perc,
        scandown_flag,
        inventory_shrink,
        non_prepaid_share,
        freight_cost,
        domestic_outbound,
        domestic_inbound,
        prepaid_freight,
        domestic_outbound_w_prepaid,
        ocean_freight,
        duty_tarrifs,
        agency_fee,
        margin,
        total_shrink,
        COALESCE(domestic_inbound, 0) + COALESCE(domestic_outbound, 0) + COALESCE(prepaid_freight, 0) + COALESCE(ocean_freight, 0) + COALESCE(agency_fee, 0) + COALESCE(duty_tarrifs, 0) AS total_freight,
        COALESCE(vendor_support_perc, 0) * COALESCE(COST_AMT, 0) AS vendor_support_dol,
        COALESCE(vendor_support_fast_perc, 0) * COALESCE(COST_AMT, 0) AS vendor_support_fast_dol
      FROM (
        SELECT DISTINCT
            ARTICLE_NO,
            MERCH_YEAR,
            VENDOR_ID,
            PARENT_VENDOR_ID,
            YEAR_PERIOD,
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
            cost_allocate_freight,
            DISC_AMT,
            OVRD_AMT,
            TRANS_TAX_AMT,
            GROSS_SALES,
            markdown_perc,
            markdown_flag,
            SHIPPING_REVENUE,
            SHIPPING_DISC,
            ACTUAL_SHIPPING_REVENUE,
            omni_tsc_delivery,
            omni_small_package_ltl,
            omni_roadie,
            "0mni_multi_salvage",
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
            weighted_domestic_inbound,
            weighted_ocean_freight,
            weighted_domestic_outbound,
            weighted_freight_cost,
            weighted_agency_fee,
            weighted_duty_rate,
            freight_join_flag,
            prepaid_share,
            weighted_prepaid_freight,
            prepaid_flag,
            prepaid_share_modified,
            channel,
            vendor_support_perc,
            vendor_support_fast_perc,
            scandown_flag,
            inventory_shrink,
            non_prepaid_share,
            freight_cost,
            domestic_outbound,
            domestic_inbound,
            prepaid_freight,
            COALESCE(domestic_outbound, 0) + COALESCE(prepaid_freight, 0) AS domestic_outbound_w_prepaid,
            COALESCE(weighted_ocean_freight, 0) * COALESCE(qty_allocate_freight, 0) AS ocean_freight,
            COALESCE(weighted_duty_rate, 0) * COALESCE(cost_allocate_freight, 0) AS duty_tarrifs,
            COALESCE(weighted_agency_fee, 0) * COALESCE(cost_allocate_freight, 0) AS agency_fee,
            COALESCE(SALES_AMT, 0) - COALESCE(COST_AMT, 0) AS margin,
            COALESCE(unsellables, 0) + COALESCE(defectives, 0) + COALESCE(dc_shrink, 0) + COALESCE(theft, 0) + COALESCE(inventory_shrink, 0) AS total_shrink
          FROM ALLOCATION_W_INVENTORY_SHRINK_PREPARED_PRECOMPUTE
        )A
        )
  SELECT DISTINCT
    ARTICLE_NO,
    MERCH_YEAR,
    VENDOR_ID,
    PARENT_VENDOR_ID,
    YEAR_PERIOD,
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
    cost_allocate_freight,
    DISC_AMT,
    OVRD_AMT,
    TRANS_TAX_AMT,
    GROSS_SALES,
    markdown_perc,
    markdown_flag,
    SHIPPING_REVENUE,
    SHIPPING_DISC,
    ACTUAL_SHIPPING_REVENUE,
    omni_tsc_delivery,
    omni_small_package_ltl,
    omni_roadie,
    "0mni_multi_salvage",
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
    weighted_domestic_inbound,
    weighted_ocean_freight,
    weighted_domestic_outbound,
    weighted_freight_cost,
    weighted_agency_fee,
    weighted_duty_rate,
    freight_join_flag,
    prepaid_share,
    weighted_prepaid_freight,
    prepaid_flag,
    prepaid_share_modified,
    channel,
    vendor_support_perc,
    vendor_support_fast_perc,
    scandown_flag,
    inventory_shrink,
    non_prepaid_share,
    freight_cost,
    domestic_outbound,
    domestic_inbound,
    prepaid_freight,
    domestic_outbound_w_prepaid,
    ocean_freight,
    duty_tarrifs,
    agency_fee,
    margin,
    total_shrink,
    total_freight,
    vendor_support_dol,
    vendor_support_fast_dol,
    CASE WHEN total_freight = 0 THEN 0 ELSE 1 END AS freight_flag
  FROM CTE  ;

<br>
--ALLOCATION_W_INVENTORY_SHRINK_PREPARED_RENAMED
</br>

insert into ALLOCATION_W_INVENTORY_SHRINK_PREPARED_RENAMED
SELECT DISTINCT
    ARTICLE_NO,
    MERCH_YEAR,
    VENDOR_ID,
    PARENT_VENDOR_ID,
    YEAR_PERIOD,
    vendor_support_fast_flag ,
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
    markdown_flag,
    SHIPPING_REVENUE,
    SHIPPING_DISC,
    ACTUAL_SHIPPING_REVENUE,
    omni_tsc_delivery,
    omni_small_package_ltl,
    omni_roadie,
    "0mni_multi_salvage" AS omni_multi_salvage,
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
    freight_flag,
    domestic_outbound_w_prepaid,
    channel,
    vendor_support_perc,
    vendor_support_dol,
    vendor_support_fast_perc,
    vendor_support_fast_dol,
    scandown_flag
  FROM ALLOCATION_W_INVENTORY_SHRINK_PREPARED ;

<br>
--prepairing input table allocation_control_pt2_sp
</br>
insert into allocation_control_pt2_sp
select DISTINCT
    allocate_flag,
    CASE WHEN ALLOCATION_FILE ='volume_rebate_2022_final' THEN 'TENANT149_DATA_volume_rebate_2022_fold_by_Parent_Vendor_stacked_period' 
        WHEN ALLOCATION_FILE ='markdowns_final' THEN 'markdowns_final_sf' 
        WHEN ALLOCATION_FILE ='vendor_support_fast_exception_final' THEN 'vendor_support_fast_exception' 
        WHEN ALLOCATION_FILE ='volume_rebate_deviation_final' THEN 'volume_rebate_deviation'
        WHEN ALLOCATION_FILE ='scandowns_final' THEN 'scandowns_final_sf'
        WHEN ALLOCATION_FILE ='freight_other_charges_final' THEN 'TENANT149_DATA_freight_other_charges_prepared_by_year'
        WHEN ALLOCATION_FILE ='volume_rebate_2021_2020_final' THEN 'volume_rebate_2021_2020_allocate'
        WHEN ALLOCATION_FILE ='price_cuts_final' THEN 'price_cuts_allocate'
        WHEN ALLOCATION_FILE ='lm_scandown_final' THEN 'lm_scandown_prepared_filtered'
        WHEN ALLOCATION_FILE ='lm_markdowns_final' THEN 'TENANT149_lm_markdowns_new_filtered'
    END AS ALLOCATION_FILE,
    group_by_period_1,
    group_by_period_2,
    l2_group_by_period_1,
    l2_group_by_period_2,
    group_by_1,
    group_by_2,
    group_by_3,
    group_by_4,
    group_by_5,
    l2_group_by_1,
    l2_group_by_2,
    l2_group_by_3,
    l2_group_by_4,
    allocate_by,
    allocation_type,
    rollup_by_1,
    rollup_by_2
from(
    SELECT DISTINCT
      allocate_flag,
      allocation_file,
      group_by_period_1,
      group_by_period_2,
      l2_group_by_period_1,
      l2_group_by_period_2,
      group_by_1,
      group_by_2,
      group_by_3,
      group_by_4,
      group_by_5,
      l2_group_by_1,
      l2_group_by_2,
      l2_group_by_3,
      l2_group_by_4,
      allocate_by,
      allocation_type,
      rollup_by_1,
      rollup_by_2
    from ALLOCATION_CONTROL_PT2
    where allocate_flag = 'Y')dku

<br>
--prepairing input table DC_3PL_OMNI_fulfillment_allocatioin_prepared_sf
</br>      
INSERT INTO DC_3PL_OMNI_fulfillment_allocatioin_prepared_sf
SELECT DISTINCT
    allocate_flag,
    CASE WHEN ALLOCATION_FILE ='dc_3pl_PnL_omni_fulfillment_fixed' THEN 'TENANT149_dc_3pl_PnL_omni_fulfillment_fixed' 
          WHEN ALLOCATION_FILE ='dc_3pl_PnL_omni_fulfillment_variable' THEN 'TENANT149_dc_3pl_PnL_omni_fulfillment_variable'
    END AS ALLOCATION_FILE,
    group_by_period_1,
      group_by_period_2,
      l2_group_by_period_1,
      l2_group_by_period_2,
      group_by_1,
      group_by_2,
      group_by_3,
      group_by_4,
      group_by_5,
      l2_group_by_1,
      l2_group_by_2,
      l2_group_by_3,
      l2_group_by_4,
      allocate_by,
      allocation_type,
      rollup_by_1,
      rollup_by_2
FROM DC_3PL_OMNI_fulfillment_allocatioin
WHERE allocate_flag = 'Y'