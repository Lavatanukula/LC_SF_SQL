
 <br> 
--Deleting SALES_W_CAL_FILTERED data for 13 Months as per dynamic Filter below 
</br>
DELETE FROM  MKT_DB.LANDED_COST.SALES_W_CAL_FILTERED 
WHERE "YEAR_PERIOD" >='202306';


 <br> 
--Deleting SALES_W_HIERARCHY data for 13 Months as per dynamic Filter below 
</br>

DELETE FROM  MKT_DB.LANDED_COST.SALES_W_HIERARCHY 
WHERE "YEAR_PERIOD" >='202306';

<br> 
--Deleting SALES_W_VENDOR_PREP_TMP data for 13 Months as per dynamic Filter below 
</br>

DELETE FROM  MKT_DB.LANDED_COST.SALES_W_VENDOR_PREP_TMP 
WHERE "YEAR_PERIOD" >='202306';

 <br> 
--Deleting SALES_W_LIST_COST_WINDOWS_STG data for 13 Months as per dynamic Filter below 
</br>

DELETE FROM  MKT_DB.LANDED_COST.SALES_W_LIST_COST_WINDOWS_STG 
WHERE "YEAR_PERIOD" >='202306';

 <br> 
--Deleting SALES_W_LIST_COST data for 13 Months as per dynamic Filter below 
</br>
DELETE FROM  MKT_DB.LANDED_COST.SALES_W_LIST_COST
WHERE "YEAR_PERIOD" >='202306';

 <br> 
--Deleting SALES_W_GROSS_SALES_PREPARED_STG_TMP data for 13 Months as per dynamic Filter below 
</br>

DELETE FROM  MKT_DB.LANDED_COST.SALES_W_GROSS_SALES_PREPARED_STG_TMP 
WHERE "YEAR_PERIOD" >='202306';

 <br> 
--Deleting SALES_AGGREGATED_WEEKLY data for 13 Months as per dynamic Filter below 
</br>

DELETE FROM  MKT_DB.LANDED_COST.SALES_AGGREGATED_WEEKLY 
WHERE "YEAR_PERIOD" >='202306' ;
  <br> 
--Deleting PURCHASE_ORDER_LINE_W_CAL data for 13 Months as per dynamic Filter below 
</br>

DELETE FROM  MKT_DB.LANDED_COST.PURCHASE_ORDER_LINE_W_CAL 
WHERE "cal_YEAR_PERIOD" >='202306' ;

 <br> 
--Deleting PURCHASE_ORDER_LINE_W_CAL_FILTERED_JOINED data for 13 Months as per dynamic Filter below 
</br>

DELETE FROM  MKT_DB.LANDED_COST.PURCHASE_ORDER_LINE_W_CAL_FILTERED_JOINED 
WHERE "cal_YEAR_PERIOD" >='202306' ;

 <br> 
--Deleting PURCHASE_ORDER_LINE_W_ARTICLE_PREPARED data for 13 Months as per dynamic Filter below 
</br>

DELETE FROM  MKT_DB.LANDED_COST.PURCHASE_ORDER_LINE_W_ARTICLE_PREPARED 
WHERE "cal_YEAR_PERIOD" >='202306' ;

<br> 
--Deleting PURCHASE_ORDER_LINE_W_ARTICLE_PREPARED_WINDOWS data for 13 Months as per dynamic Filter below 
</br>

DELETE FROM MKT_DB.LANDED_COST.PURCHASE_ORDER_LINE_W_ARTICLE_PREPARED_WINDOWS 
WHERE "cal_YEAR_PERIOD" >='202306' ;

<br> 
--Deleting SMALL_PACKAGE_ORDERS data for 13 Months as per dynamic Filter below 
</br>

DELETE FROM  MKT_DB.LANDED_COST.SMALL_PACKAGE_ORDERS 
WHERE "cal_YEAR_PERIOD" >='202306' ;






