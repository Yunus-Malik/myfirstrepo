/*put in info from complaint (ec_id, sr_id or nectar card number)*/
%let d_ec = ;
%let d_sr = ;
%let FNCN = '98263000XXXXXXXXXXX';


/*find sr_ids/ec_id/FNCN, get customer info*/
/*check contactability, suppression etc. average spend...*/
proc sql;
	create table demo_data 
	as select *
	from saspres.vw_ca_customer_account
/*	where ec_id = &d_ec. */
	where full_nectar_card_num = &FNCN.
	;
quit;

/*Check if they are a new nectar customer*/

proc sql;
	create table NN_APP_CUST 
	as select *
	from sascmp.usr_piano_reg
/*	where ec_id = &d_ec. */
	where sr_id = &d_sr.
	;
quit;


/*pull customer's info from contact history*/
proc sql ;
	create table cont_hist 
	as select *
	from saspres.vw_ch_fact_ec as fact
	left join saspres.vw_chrh_campaign_ec as chrh
	      on fact.flowchartid = chrh.flowchartid
	left join saspres.vw_ch_offer_attribute_ec as offer
	      on fact.flowchartid = offer.offerid
	where fact.ec_id = &d_ec.
/*	or fact.sr_id in (&d_sr.)*/
	       and datepart(fact.contactdatetime) >= today()-180
	order by contactdatetime;
quit;

proc sql;
	create table CH 
	as select a.flowchartID,
				rundatetime,
				campaigncode,
				campaignname,
				flowchartname,
				treatmentcode,
				contactstatuscode,
				contactdatetime,
				count(ec_id) as cts_ec_id,
				count(sr_id) as cts_sr_id,                                                                                  
				count(hh_id) as cts_hh_id,
				count (distinct sr_id) as cts_dist_sr_id,
				count (distinct hh_id) as cts_dist_hh_id
	from saspres.vw_ch_fact_ec a
	inner join saspres.vw_chrh_campaign_ec b
	on a.flowchartID = b.flowchartID
	where ec_id = &d_ec.
/*	or sr_id in (&d_sr.)*/
	group by 1,2,3,4,5,6,7,8;
quit;

proc sql;
	create table CH2 as select * from CH
	order by rundatetime;
quit;

/*Are they in any fallow groups?*/
/*cross-cat fallow*/
proc sql;
	create table Fallow_Xcat 
	as select *
	from sascmp.USR_Crosscat_1718_Fallow
	where ec_id in (&d_ec.) 
	/*or sr_id in (&d_sr)*/
	;
quit;

/*long term fallow group*/
proc sql;
	create table Fallow_long_term 
	as select *
    from sascmp.usr_lt_fallow_1617
    where ENTERPRISE_CUSTOMER_ID in (&d_ec.)
	/*or PARTY_ACCOUNT_ID in (&d_sr)*/
    ;
quit;

proc sql;
    create table Fallow_long_term 
	as select *
    from sascmp.usr_lt_fallow_1617
    where ENTERPRISE_CUSTOMER_ID in (&d_ec.)
	/*or PARTY_ACCOUNT_ID in (&d_sr.)*/
    ;
quit;

/*where are they in the redemption segmentation (group H are non-redeemers, often excluded from campaigns)*/
proc sql;
    create table fallow_red_seg 
	as select *
    from catalina.redemption_seg_current
    where PARTY_ACCOUNT_ID  in (&d_sr.)
	/*and segment_code = 'H'*/
    ;
quit;

/*find points redemptions*/
proc sql;
	connect to teradata (&sasviews);
	create table points_redemptions
	as select * from connection to teradata 
	(select *
		from VW_LOYALTY_COUPON_REDEMPTION
		where party_account_id in (&d_sr.)
		order by transaction_date
	);
	disconnect from teradata;
quit;

/*find transactions*/
proc sql;
	connect to teradata (&sasviews);
	create table trx_total
	as select * from connection to teradata 
	(select *
	from VW_SHOPPING_TRANSACTION
	where party_account_id in (&d_sr.) 
	  and transaction_date  >= date '2018-01-01' 
	order by transaction_date
	);
	disconnect from teradata;
quit;
       

/*transaction line items*/
proc sql;
	connect to teradata (&sasviews);
	create table trx_skus as 
	select * from connection to teradata
	(select 
	txn.party_account_id
	,loc.loc_name
	,case when ltm.location_trait_id in (6100,6102) then 'Superstore' 
	   when ltm.location_trait_id = 6101 then 'Convenience'
	   when ltm.location_trait_id = 6103 then 'PFS'
	    when ltm.location_trait_id = 6104 then 'Online'
	    else 'N/A' end as store_type
	,case when ltm2.location_trait_id = 6140 then 'Superstore' 
	   when ltm2.location_trait_id = 6141 then 'Convenience'
	   when ltm2.location_trait_id = 6142 then 'Online'
	   else 'N/A' end as site_type
	,txn.location_key
	,txn.till_number
	,txn.transaction_number
	,txn.transaction_date
	,txn.transaction_time
	,txn.transaction_element
	,txn.extended_price
	,disc.prom_item_mb_discount_value
	,txn.unit_price
	,txn.item_quantity
	,txn.item_weight
	,txn.unit_of_measure
	,skumap.sku
	,skumap.sku_desc
	from vw_shopping_transaction_line as txn
	inner join vw_location_trait_matrix as ltm on txn.location_key = ltm.location_key and ltm.location_trait_id in (6100,6101,6102,6103,6104)
	inner join vw_location_trait_matrix as ltm2 on txn.location_key = ltm2.location_key and ltm2.location_trait_id in (6140,6141,6142)
	inner join vw_location_map as loc on txn.location_key = loc.location_key
	inner join  vw_ean as ean on ean.ean_key=txn.ean_key
	inner join  vw_c_sku_map as skumap  on ean.sku_key=skumap.sku_key
	inner join  vw_c_sub_category_map as subcat on  skumap.sub_category_key=subcat.sub_category_key
	left join (select
	     party_account_id,
	     location_key,
	     transaction_date,
	     transaction_time,
	     transaction_number,
	     till_number,
	     ean_key,
	     sum(prom_item_mb_discount_value) as prom_item_mb_discount_value
	   from vw_shopping_txn_line_discount
	   group by 1,2,3,4,5,6,7)  as disc
	  on txn.party_account_id = disc.party_account_id
	   and txn.location_key=disc.location_key
	    and txn.transaction_date=disc.transaction_date
	     and txn.transaction_time=disc.transaction_time
	      and txn.transaction_number=disc.transaction_number
	       and txn.till_number=disc.till_number
	        and ean.ean_key=disc.ean_key
	where txn.party_account_id in (&d_sr.)
	  and txn.transaction_date  >= date '2017-01-01' 
	   and txn.refund_reason_code = '000'
	order by txn.transaction_date, txn.party_account_id, txn.transaction_time, txn.transaction_element);
	disconnect from teradata;
quit;

/*find purchases*/
proc sql;
	create table trx_purchases 
	as select
		party_account_id, 
		sku_desc,
		count(*)
	from trx_skus
	group by 1, 2;
quit;

/*find coupon prints*/
proc sql;
	connect to teradata (&sasviews);
	create table cont_print
	as select * from connection to teradata 
	(	select *
		from vw_catalina_print_file
		where party_account_id in (&d_sr.)
		and transaction_date  >= date '2018-01-01' 
		order by transaction_date
	);
	disconnect from teradata;
quit;

/*find payment method*/
proc sql;
	connect to teradata (&sasviews);
	create table payment_method
	as select * from connection to teradata 
	(select *
	from vw_payment_line
	where party_account_id in (&d_sr)
	and transaction_date  >= date '2017-01-01' 
	order by transaction_date
	);
	disconnect from teradata;
quit;

/*find point spend*/
proc sql;
	connect to teradata (&sasviews);
	create table points_spent
	as select * from connection to teradata 
	(select *
	from vw_loyalty_event
	where party_account_id in (&d_sr.)
	and transaction_date  >= date '2017-01-01' 
	order by transaction_date
	);
	disconnect from teradata;
quit;

/*look at coupon lookup raw*/
proc sql;
	create table test 
	as select distinct * 
	from cont_print as a 
	inner join catalina.coupon_lookup_raw b 
	on (a.barcode = b.barcode )
	where (coupon_type ne 'message')
	order by transaction_date
	;
quit;

/*is the customer suppressed- why?*/
proc sql;
	create table suppressions as select * from saspres.VW_EC_RT_SUPRESSION
	where ec_id = &d_ec.;
quit;
