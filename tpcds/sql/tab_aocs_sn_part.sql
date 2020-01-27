DROP SCHEMA IF EXISTS tpcds CASCADE;
CREATE SCHEMA tpcds;

CREATE TABLE tpcds.call_center (
    cc_call_center_sk integer,
    cc_call_center_id character varying(16),
    cc_rec_start_date date,
    cc_rec_end_date date,
    cc_closed_date_sk integer,
    cc_open_date_sk integer,
    cc_name character varying(50),
    cc_class character varying(50),
    cc_employees integer,
    cc_sq_ft integer,
    cc_hours character varying(20),
    cc_manager character varying(40),
    cc_mkt_id integer,
    cc_mkt_class character varying(50),
    cc_mkt_desc character varying(100),
    cc_market_manager character varying(40),
    cc_division text,
    cc_division_name character varying(50),
    cc_company text,
    cc_company_name character varying(50),
    cc_street_number character varying(10),
    cc_street_name character varying(60),
    cc_street_type character varying(15),
    cc_suite_number character varying(10),
    cc_city character varying(60),
    cc_county character varying(30),
    cc_state text,
    cc_zip character varying(10),
    cc_country character varying(20),
    cc_gmt_offset smallnumber, 
    cc_tax_percentage smallnumber 
)
DISTRIBUTED BY (cc_call_center_sk);

CREATE TABLE tpcds.catalog_page (
    cp_catalog_page_sk integer NOT NULL,
    cp_catalog_page_id character varying(16) NOT NULL,
    cp_start_date_sk integer,
    cp_end_date_sk integer,
    cp_department character varying(50),
    cp_catalog_number integer,
    cp_catalog_page_number integer,
    cp_description character varying(100),
    cp_type character varying(100)
)
DISTRIBUTED BY (cp_catalog_page_sk);

CREATE TABLE tpcds.catalog_returns (
    cr_returned_date_sk integer,
    cr_returned_time_sk integer,
    cr_item_sk integer NOT NULL,
    cr_refunded_customer_sk integer,
    cr_refunded_cdemo_sk integer,
    cr_refunded_hdemo_sk integer,
    cr_refunded_addr_sk integer,
    cr_returning_customer_sk integer,
    cr_returning_cdemo_sk integer,
    cr_returning_hdemo_sk integer,
    cr_returning_addr_sk integer,
    cr_call_center_sk integer,
    cr_catalog_page_sk integer,
    cr_ship_mode_sk integer,
    cr_warehouse_sk integer,
    cr_reason_sk integer,
    cr_order_number bigint NOT NULL,
    cr_return_quantity integer,
    cr_return_amount smallnumber, 
    cr_return_tax smallnumber, 
    cr_return_amt_inc_tax smallnumber, 
    cr_fee smallnumber, 
    cr_return_ship_cost smallnumber, 
    cr_refunded_cash smallnumber, 
    cr_reversed_charge smallnumber, 
    cr_store_credit smallnumber, 
    cr_net_loss smallnumber 
)
-- WITH (:E9_LARGE_STORAGE)
WITH (APPENDONLY=true, orientation=column, compresstype=lz4)
DISTRIBUTED BY (cr_item_sk, cr_order_number) 
partition by range(cr_returned_date_sk)
(start(2450815) INCLUSIVE end(2453005) INCLUSIVE every (28),
default partition outliers)
;


CREATE TABLE tpcds.catalog_sales (
    cs_sold_date_sk integer,
    cs_sold_time_sk integer,
    cs_ship_date_sk integer,
    cs_bill_customer_sk integer,
    cs_bill_cdemo_sk integer,
    cs_bill_hdemo_sk integer,
    cs_bill_addr_sk integer,
    cs_ship_customer_sk integer,
    cs_ship_cdemo_sk integer,
    cs_ship_hdemo_sk integer,
    cs_ship_addr_sk integer,
    cs_call_center_sk integer,
    cs_catalog_page_sk integer,
    cs_ship_mode_sk integer,
    cs_warehouse_sk integer,
    cs_item_sk integer NOT NULL,
    cs_promo_sk integer,
    cs_order_number bigint NOT NULL,
    cs_quantity integer,
    cs_wholesale_cost smallnumber,
    cs_list_price smallnumber,
    cs_sales_price smallnumber,
    cs_ext_discount_amt smallnumber,
    cs_ext_sales_price smallnumber,
    cs_ext_wholesale_cost smallnumber,
    cs_ext_list_price smallnumber,
    cs_ext_tax smallnumber,
    cs_coupon_amt smallnumber,
    cs_ext_ship_cost smallnumber,
    cs_net_paid smallnumber,
    cs_net_paid_inc_tax smallnumber,
    cs_net_paid_inc_ship smallnumber,
    cs_net_paid_inc_ship_tax smallnumber,
    cs_net_profit smallnumber
)
-- WITH (:E9_LARGE_STORAGE)
WITH (APPENDONLY=true, orientation=column, compresstype=lz4)
DISTRIBUTED BY (cs_item_sk, cs_order_number)
partition by range(cs_sold_date_sk)
(start(2450815) INCLUSIVE end(2453005) INCLUSIVE every (28),
default partition outliers)
;

CREATE TABLE tpcds.customer (
    c_customer_sk integer NOT NULL,
    c_customer_id character varying(16) NOT NULL,
    c_current_cdemo_sk integer,
    c_current_hdemo_sk integer,
    c_current_addr_sk integer,
    c_first_shipto_date_sk integer,
    c_first_sales_date_sk integer,
    c_salutation character varying(10),
    c_first_name character varying(20),
    c_last_name character varying(30),
    c_preferred_cust_flag character varying(1),
    c_birth_day integer,
    c_birth_month integer,
    c_birth_year integer,
    c_birth_country character varying(20),
    c_login character varying(13),
    c_email_address character varying(50),
    c_last_review_date character varying(10)
)
DISTRIBUTED BY (c_customer_sk);

CREATE TABLE tpcds.customer_address (
    ca_address_sk integer NOT NULL,
    ca_address_id character varying(16) NOT NULL,
    ca_street_number character varying(10),
    ca_street_name character varying(60),
    ca_street_type character varying(15),
    ca_suite_number character varying(10),
    ca_city character varying(60),
    ca_county character varying(30),
    ca_state character varying(2),
    ca_zip character varying(10),
    ca_country character varying(20),
    ca_gmt_offset numeric(5,2),
    ca_location_type character varying(20)
)
DISTRIBUTED BY (ca_address_sk);


CREATE TABLE tpcds.customer_demographics (
    cd_demo_sk integer NOT NULL,
    cd_gender character varying (1),
    cd_marital_status character varying (1),
    cd_education_status character varying(20),
    cd_purchase_estimate integer,
    cd_credit_rating character varying(10),
    cd_dep_count integer,
    cd_dep_employed_count integer,
    cd_dep_college_count integer
)
DISTRIBUTED BY (cd_demo_sk);


CREATE TABLE tpcds.date_dim (
    d_date_sk integer NOT NULL,
    d_date_id character varying(16) NOT NULL,
    d_date date,
    d_month_seq integer,
    d_week_seq integer,
    d_quarter_seq integer,
    d_year integer,
    d_dow integer,
    d_moy integer,
    d_dom integer,
    d_qoy integer,
    d_fy_year integer,
    d_fy_quarter_seq integer,
    d_fy_week_seq integer,
    d_day_name character varying(9),
    d_quarter_name character varying(6),
    d_holiday character varying (1),
    d_weekend character varying (1),
    d_following_holiday character varying (1),
    d_first_dom integer,
    d_last_dom integer,
    d_same_day_ly integer,
    d_same_day_lq integer,
    d_current_day character varying (1),
    d_current_week character varying (1),
    d_current_month character varying (1),
    d_current_quarter character varying (1),
    d_current_year character varying (1)
)
DISTRIBUTED BY (d_date_sk);


CREATE TABLE tpcds.household_demographics (
    hd_demo_sk integer NOT NULL,
    hd_income_band_sk integer,
    hd_buy_potential character varying(15),
    hd_dep_count integer,
    hd_vehicle_count integer
)
DISTRIBUTED BY (hd_demo_sk);


CREATE TABLE tpcds.income_band (
    ib_income_band_sk integer NOT NULL,
    ib_lower_bound integer,
    ib_upper_bound integer
)
DISTRIBUTED BY (ib_income_band_sk);


CREATE TABLE tpcds.inventory (
    inv_date_sk integer NOT NULL,
    inv_item_sk integer NOT NULL,
    inv_warehouse_sk integer NOT NULL,
    inv_quantity_on_hand integer
)
-- WITH (:E9_MEDIUM_STORAGE)
WITH (APPENDONLY=true, orientation=column, compresstype=lz4)
DISTRIBUTED BY(inv_date_sk, inv_item_sk, inv_warehouse_sk)
partition by range(inv_date_sk)
(start(2450815) INCLUSIVE end(2453005) INCLUSIVE every (28),
default partition outliers)
;


CREATE TABLE tpcds.item (
    i_item_sk integer NOT NULL,
    i_item_id character varying(16) NOT NULL,
    i_rec_start_date date,
    i_rec_end_date date,
    i_item_desc character varying(200),
    i_current_price smallnumber,
    i_wholesale_cost smallnumber,
    i_brand_id integer,
    i_brand character varying(50),
    i_class_id integer,
    i_class character varying(50),
    i_category_id integer,
    i_category character varying(50),
    i_manufact_id integer,
    i_manufact character varying(50),
    i_size character varying(20),
    i_formulation character varying(20),
    i_color character varying(20),
    i_units character varying(10),
    i_container character varying(10),
    i_manager_id integer,
    i_product_name character varying(50)
)
DISTRIBUTED BY (i_item_sk);


CREATE TABLE tpcds.promotion (
    p_promo_sk integer NOT NULL,
    p_promo_id character varying(16) NOT NULL,
    p_start_date_sk integer,
    p_end_date_sk integer,
    p_item_sk integer,
    p_cost numeric(15,2),
    p_response_target integer,
    p_promo_name character varying(50),
    p_channel_dmail character varying (1),
    p_channel_email character varying (1),
    p_channel_catalog character varying (1),
    p_channel_tv character varying (1),
    p_channel_radio character varying (1),
    p_channel_press character varying (1),
    p_channel_event character varying (1),
    p_channel_demo character varying (1),
    p_channel_details character varying(100),
    p_purpose character varying(15),
    p_discount_active character varying (1)
)
DISTRIBUTED BY (p_promo_sk);


CREATE TABLE tpcds.reason (
    r_reason_sk integer NOT NULL,
    r_reason_id character varying(16) NOT NULL,
    r_reason_desc character varying(100)
)
DISTRIBUTED BY (r_reason_sk);


CREATE TABLE tpcds.ship_mode (
    sm_ship_mode_sk integer NOT NULL,
    sm_ship_mode_id character varying(16) NOT NULL,
    sm_type character varying(30),
    sm_code character varying(10),
    sm_carrier character varying(20),
    sm_contract character varying(20)
)
DISTRIBUTED BY (sm_ship_mode_sk);


CREATE TABLE tpcds.store (
    s_store_sk integer NOT NULL,
    s_store_id character varying(16) NOT NULL,
    s_rec_start_date date,
    s_rec_end_date date,
    s_closed_date_sk integer,
    s_store_name character varying(50),
    s_number_employees integer,
    s_floor_space integer,
    s_hours character varying(20),
    s_manager character varying(40),
    s_market_id integer,
    s_geography_class character varying(100),
    s_market_desc character varying(100),
    s_market_manager character varying(40),
    s_division_id integer,
    s_division_name character varying(50),
    s_company_id integer,
    s_company_name character varying(50),
    s_street_number character varying(10),
    s_street_name character varying(60),
    s_street_type character varying(15),
    s_suite_number character varying(10),
    s_city character varying(60),
    s_county character varying(30),
    s_state character varying(2),
    s_zip character varying(10),
    s_country character varying(20),
    s_gmt_offset smallnumber, 
    s_tax_precentage smallnumber 
)
DISTRIBUTED BY (s_store_sk);


CREATE TABLE tpcds.store_returns (
    sr_returned_date_sk integer,
    sr_return_time_sk integer,
    sr_item_sk integer NOT NULL,
    sr_customer_sk integer,
    sr_cdemo_sk integer,
    sr_hdemo_sk integer,
    sr_addr_sk integer,
    sr_store_sk integer,
    sr_reason_sk integer,
    sr_ticket_number bigint NOT NULL,
    sr_return_quantity integer,
    sr_return_amt smallnumber,
    sr_return_tax smallnumber,
    sr_return_amt_inc_tax smallnumber,
    sr_fee smallnumber,
    sr_return_ship_cost smallnumber,
    sr_refunded_cash smallnumber,
    sr_reversed_charge smallnumber,
    sr_store_credit smallnumber,
    sr_net_loss smallnumber
)
-- WITH (:E9_LARGE_STORAGE)
WITH (APPENDONLY=true, orientation=column, compresstype=lz4)
DISTRIBUTED BY (sr_item_sk, sr_ticket_number)
partition by range(sr_returned_date_sk)
(start(2450815) INCLUSIVE end(2453005) INCLUSIVE every (28),
default partition outliers)
;


CREATE TABLE tpcds.store_sales (
    ss_sold_date_sk integer,
    ss_sold_time_sk integer,
    ss_item_sk int NOT NULL,
    ss_customer_sk integer,
    ss_cdemo_sk integer,
    ss_hdemo_sk integer,
    ss_addr_sk integer,
    ss_store_sk integer,
    ss_promo_sk integer,
    ss_ticket_number bigint NOT NULL,
    ss_quantity integer,
    ss_wholesale_cost smallnumber,
    ss_list_price smallnumber,
    ss_sales_price smallnumber,
    ss_ext_discount_amt smallnumber,
    ss_ext_sales_price smallnumber,
    ss_ext_wholesale_cost smallnumber,
    ss_ext_list_price smallnumber,
    ss_ext_tax smallnumber,
    ss_coupon_amt smallnumber,
    ss_net_paid smallnumber,
    ss_net_paid_inc_tax smallnumber,
    ss_net_profit smallnumber
)
-- WITH (:E9_LARGE_STORAGE)
WITH (APPENDONLY=true, orientation=column, compresstype=lz4)
DISTRIBUTED BY (ss_item_sk, ss_ticket_number)
partition by range(ss_sold_date_sk)
(start(2450815) INCLUSIVE end(2453005) INCLUSIVE every (28),
default partition outliers)
;


CREATE TABLE tpcds.time_dim (
    t_time_sk integer NOT NULL,
    t_time_id character varying(16) NOT NULL,
    t_time integer,
    t_hour integer,
    t_minute integer,
    t_second integer,
    t_am_pm character varying (2),
    t_shift character varying (20),
    t_sub_shift character varying (20),
    t_meal_time character varying (20)
)
DISTRIBUTED BY (t_time_sk);


CREATE TABLE tpcds.warehouse (
    w_warehouse_sk integer NOT NULL,
    w_warehouse_id character varying(16) NOT NULL,
    w_warehouse_name character varying(20),
    w_warehouse_sq_ft integer,
    w_street_number character varying(10),
    w_street_name character varying(60),
    w_street_type character varying(15),
    w_suite_number character varying(10),
    w_city character varying(60),
    w_county character varying(30),
    w_state character varying(2),
    w_zip character varying(10),
    w_country character varying(20),
    w_gmt_offset smallnumber 
)
DISTRIBUTED BY (w_warehouse_sk);



CREATE TABLE tpcds.web_page (
    wp_web_page_sk integer NOT NULL,
    wp_web_page_id character varying(16) NOT NULL,
    wp_rec_start_date date,
    wp_rec_end_date date,
    wp_creation_date_sk integer,
    wp_access_date_sk integer,
    wp_autogen_flag character varying (1),
    wp_customer_sk integer,
    wp_url character varying(100),
    wp_type character varying(50),
    wp_char_count integer,
    wp_link_count integer,
    wp_image_count integer,
    wp_max_ad_count integer
)
DISTRIBUTED BY (wp_web_page_sk);


CREATE TABLE tpcds.web_returns (
    wr_returned_date_sk integer,
    wr_returned_time_sk integer,
    wr_item_sk integer NOT NULL,
    wr_refunded_customer_sk integer,
    wr_refunded_cdemo_sk integer,
    wr_refunded_hdemo_sk integer,
    wr_refunded_addr_sk integer,
    wr_returning_customer_sk integer,
    wr_returning_cdemo_sk integer,
    wr_returning_hdemo_sk integer,
    wr_returning_addr_sk integer,
    wr_web_page_sk integer,
    wr_reason_sk integer,
    wr_order_number integer NOT NULL,
    wr_return_quantity integer,
    wr_return_amt smallnumber,
    wr_return_tax smallnumber,
    wr_return_amt_inc_tax smallnumber,
    wr_fee smallnumber,
    wr_return_ship_cost smallnumber,
    wr_refunded_cash smallnumber,
    wr_reversed_charge smallnumber,
    wr_account_credit smallnumber,
    wr_net_loss smallnumber
)
-- WITH (:E9_LARGE_STORAGE)
WITH (APPENDONLY=true, orientation=column, compresstype=lz4)
DISTRIBUTED BY (wr_order_number, wr_item_sk)
partition by range(wr_returned_date_sk)
(start(2450815) INCLUSIVE end(2453005) INCLUSIVE every (28),
default partition outliers)
;

CREATE TABLE tpcds.web_sales (
    ws_sold_date_sk integer,
    ws_sold_time_sk integer,
    ws_ship_date_sk integer,
    ws_item_sk integer NOT NULL,
    ws_bill_customer_sk integer,
    ws_bill_cdemo_sk integer,
    ws_bill_hdemo_sk integer,
    ws_bill_addr_sk integer,
    ws_ship_customer_sk integer,
    ws_ship_cdemo_sk integer,
    ws_ship_hdemo_sk integer,
    ws_ship_addr_sk integer,
    ws_web_page_sk integer,
    ws_web_site_sk integer,
    ws_ship_mode_sk integer,
    ws_warehouse_sk integer,
    ws_promo_sk integer,
    ws_order_number integer NOT NULL,
    ws_quantity integer,
    ws_wholesale_cost smallnumber,
    ws_list_price smallnumber,
    ws_sales_price smallnumber,
    ws_ext_discount_amt smallnumber,
    ws_ext_sales_price smallnumber,
    ws_ext_wholesale_cost smallnumber,
    ws_ext_list_price smallnumber,
    ws_ext_tax smallnumber,
    ws_coupon_amt smallnumber,
    ws_ext_ship_cost smallnumber,
    ws_net_paid smallnumber,
    ws_net_paid_inc_tax smallnumber,
    ws_net_paid_inc_ship smallnumber,
    ws_net_paid_inc_ship_tax smallnumber,
    ws_net_profit smallnumber
)
-- WITH (:E9_LARGE_STORAGE)
WITH (APPENDONLY=true, orientation=column, compresstype=lz4)
DISTRIBUTED BY (ws_item_sk, ws_order_number)
partition by range(ws_sold_date_sk)
(start(2450815) INCLUSIVE end(2453005) INCLUSIVE every (28),
default partition outliers)
;

CREATE TABLE tpcds.web_site (
    web_site_sk integer NOT NULL,
    web_site_id character varying(16) NOT NULL,
    web_rec_start_date date,
    web_rec_end_date date,
    web_name character varying(50),
    web_open_date_sk integer,
    web_close_date_sk integer,
    web_class character varying(50),
    web_manager character varying(40),
    web_mkt_id integer,
    web_mkt_class character varying(50),
    web_mkt_desc character varying(100),
    web_market_manager character varying(40),
    web_company_id integer,
    web_company_name character varying(50),
    web_street_number character varying(10),
    web_street_name character varying(60),
    web_street_type character varying(15),
    web_suite_number character varying(10),
    web_city character varying(60),
    web_county character varying(30),
    web_state character varying(2),
    web_zip character varying(10),
    web_country character varying(20),
    web_gmt_offset smallnumber, 
    web_tax_percentage smallnumber 
)
DISTRIBUTED BY (web_site_sk);
