package bench

import (
	"fmt"
	"os"
	"os/exec"
	"testing"
)

func TestSetup(t *testing.T) {
	conf, err := GetConfig()
	if err != nil {
		t.Fatalf("Configuration error: %s", err.Error())
	}

	segs, err := Segs()
	if err != nil {
		t.Fatalf("Cannot get deepgreen segs, error: %s.", err.Error())
	}

	seghosts := make(map[string]bool)
	for _, seg := range segs {
		seghosts[seg.Addr] = true
	}

	t.Run("Step=mkdirgen", func(t *testing.T) {
		cmd := fmt.Sprintf("mkdir -p %s/gen", Dir())
		err = exec.Command("bash", "-c", cmd).Run()
		if err != nil {
			t.Errorf("Cannot create gen dir.  error: %s", err.Error())
		}
	})

	t.Run("Step=xdrtoml", func(t *testing.T) {
		if conf.Ext != "XDR" {
			return
		}

		tomlf := Dir() + "/gen/xdrive.toml"
		xf, err := os.Create(tomlf)
		if err != nil {
			t.Errorf("Cannot create xdrive.toml file.  error: %s", err.Error())
		}

		fmt.Fprintf(xf, "[xdrive]\n")
		fmt.Fprintf(xf, "dir = \"%s\"\n", conf.Staging)
		fmt.Fprintf(xf, "pluginpath = [\"%s/plugin\"]\n", conf.Staging)
		fmt.Fprintf(xf, "host = [")
		prefix := " "
		for k, _ := range seghosts {
			fmt.Fprintf(xf, " %s\"%s:31416\" ", prefix, k)
			prefix = ","
		}
		fmt.Fprintf(xf, " ]\n\n")

		fmt.Fprintf(xf, "[[xdrive.mount]]\n")
		fmt.Fprintf(xf, "name = \"tpcds-scale-%d\"\n", conf.Scale)
		fmt.Fprintf(xf, "argv = [\"xdr_fs/xdr_fs\", \"csv\", \"./tpcds/scale-%d\"]\n", conf.Scale)
		xf.Close()

		err = exec.Command("xdrctl", "deploy", tomlf).Run()
		if err != nil {
			t.Errorf("Cannot deploy xdrive. error: %s", err.Error())
		}
	})

	t.Run("Step=db", func(t *testing.T) {
		conn, err := ConnectTemplate1()
		if err != nil {
			t.Errorf("Cannot connect to template1, error: %s", err.Error())
		}
		defer conn.Disconnect()

		conn.Execute(fmt.Sprintf("drop database %s", conf.Db))
		conn.Execute(fmt.Sprintf("create database %s", conf.Db))
	})

	t.Run("Step=ddl", func(t *testing.T) {
		ddlf := fmt.Sprintf("%s/sql/%s", Dir(), conf.DDL)
		cmd, err := PsqlCmd(ddlf)
		if err != nil {
			t.Errorf("Cannot build psql ddl command. error :%s", err.Error())
		}

		err = exec.Command("bash", "-c", cmd).Run()
		if err != nil {
			t.Errorf("Cannot run ddl.   error: %s", err.Error())
		}

		qf := fmt.Sprintf("%s/sql/mkview.sql", Dir())
		cmd, err = PsqlCmd(qf)
		if err != nil {
			t.Errorf("Cannot build psql query command. error :%s", err.Error())
		}

		err = exec.Command("bash", "-c", cmd).Run()
		if err != nil {
			t.Errorf("Cannot run query view ddl.   error: %s", err.Error())
		}
	})

	t.Run("Step=extddl", func(t *testing.T) {
		conn, err := Connect()
		if err != nil {
			t.Errorf("Cannot connect to database %s, error: %s", err.Error())
		}
		defer conn.Disconnect()

		conn.Execute("DROP SCHEMA IF EXISTS XDR CASCADE")
		conn.Execute("DROP SCHEMA IF EXISTS GPF CASCADE")
		conn.Execute("CREATE SCHEMA XDR")
		conn.Execute("CREATE SCHEMA GPF")

		var locf func(string) string

		if conf.Ext == "XDR" {
			locf = func(t string) string {
				return fmt.Sprintf("'xdrive://localhost:31416/tpcds-scale-%d/seg-#SEGID#/%s_[0-9]*.dat'", conf.Scale, t)
			}
		} else {
			panic("TPCDS only support XDR at this moment.  Please use DeepGreen.")
			locf = func(t string) string {
				prefix := ""
				ret := ""
				for h, _ := range seghosts {
					ret = ret + prefix + fmt.Sprintf("'gpfdist://%s:22222/tpcds/scale-%d/seg-*/%s_[0-9]*.dat'", h, conf.Scale, t)
					prefix = ","
				}
				return ret
			}
		}

		// Create two set of external tables, one for xdrive, one for gpfdist.
		cc := `CREATE EXTERNAL TABLE %s.call_center (
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
			cc_gmt_offset double precision, 
			cc_tax_percentage double precision 
		) LOCATION (%s) FORMAT 'CSV' (DELIMITER '|') 
				   `
		conn.Execute(fmt.Sprintf(cc, conf.Ext, locf("call_center")))

		cp := `CREATE EXTERNAL TABLE %s.catalog_page ( 
			cp_catalog_page_sk integer, 
			cp_catalog_page_id character varying(16), 
			cp_start_date_sk integer,
			cp_end_date_sk integer,
			cp_department character varying(50),
			cp_catalog_number integer,
			cp_catalog_page_number integer,
			cp_description character varying(100),
			cp_type character varying(100)
		) LOCATION (%s) FORMAT 'CSV' (DELIMITER '|') 
				   `
		conn.Execute(fmt.Sprintf(cp, conf.Ext, locf("catalog_page")))

		cr := `CREATE EXTERNAL TABLE %s.catalog_returns ( 
			cr_returned_date_sk integer,
			cr_returned_time_sk integer,
			cr_item_sk integer, 
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
			cr_order_number bigint, 
			cr_return_quantity integer,
			cr_return_amount double precision, 
			cr_return_tax double precision, 
			cr_return_amt_inc_tax double precision, 
			cr_fee double precision, 
			cr_return_ship_cost double precision, 
			cr_refunded_cash double precision, 
			cr_reversed_charge double precision, 
			cr_store_credit double precision, 
			cr_net_loss double precision 
		) LOCATION (%s) FORMAT 'CSV' (DELIMITER '|') 
				   `
		conn.Execute(fmt.Sprintf(cr, conf.Ext, locf("catalog_returns")))

		cs := `CREATE EXTERNAL TABLE %s.catalog_sales ( 
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
			cs_item_sk integer, 
			cs_promo_sk integer,
			cs_order_number bigint,
			cs_quantity integer,
			cs_wholesale_cost double precision,
			cs_list_price double precision,
			cs_sales_price double precision,
			cs_ext_discount_amt double precision,
			cs_ext_sales_price double precision,
			cs_ext_wholesale_cost double precision,
			cs_ext_list_price double precision,
			cs_ext_tax double precision,
			cs_coupon_amt double precision,
			cs_ext_ship_cost double precision,
			cs_net_paid double precision,
			cs_net_paid_inc_tax double precision,
			cs_net_paid_inc_ship double precision,
			cs_net_paid_inc_ship_tax double precision,
			cs_net_profit double precision
		) LOCATION (%s) FORMAT 'CSV' (DELIMITER '|') 
				   `
		conn.Execute(fmt.Sprintf(cs, conf.Ext, locf("catalog_sales")))

		c := `CREATE EXTERNAL TABLE %s.customer ( 
			c_customer_sk integer, 
			c_customer_id character varying(16), 
			c_current_cdemo_sk integer,
			c_current_hdemo_sk integer,
			c_current_addr_sk integer,
			c_first_shipto_date_sk integer,
			c_first_sales_date_sk integer,
			c_salutation character varying(10),
			c_first_name character varying(20),
			c_last_name character varying(30),
			c_preferred_cust_flag character varying (1),
			c_birth_day integer,
			c_birth_month integer,
			c_birth_year integer,
			c_birth_country character varying(20),
			c_login character varying(13),
			c_email_address character varying(50),
			c_last_review_date character varying(10)
		) LOCATION (%s) FORMAT 'CSV' (DELIMITER '|') 
				   `
		conn.Execute(fmt.Sprintf(c, conf.Ext, locf("customer")))

		ca := `CREATE EXTERNAL TABLE %s.customer_address ( 
			ca_address_sk integer, 
			ca_address_id character varying(16), 
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
		) LOCATION (%s) FORMAT 'CSV' (DELIMITER '|') 
				   `
		conn.Execute(fmt.Sprintf(ca, conf.Ext, locf("customer_address")))

		cd := `CREATE EXTERNAL TABLE %s.customer_demographics ( 
			cd_demo_sk integer, 
			cd_gender character varying (1),
			cd_marital_status character varying (1),
			cd_education_status character varying(20),
			cd_purchase_estimate integer,
			cd_credit_rating character varying(10),
			cd_dep_count integer,
			cd_dep_employed_count integer,
			cd_dep_college_count integer
		) LOCATION (%s) FORMAT 'CSV' (DELIMITER '|') 
				   `
		conn.Execute(fmt.Sprintf(cd, conf.Ext, locf("customer_demographics")))

		d := `CREATE EXTERNAL TABLE %s.date_dim ( 
			d_date_sk integer,
			d_date_id character varying(16), 
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
		) LOCATION (%s) FORMAT 'CSV' (DELIMITER '|') 
				   `
		conn.Execute(fmt.Sprintf(d, conf.Ext, locf("date_dim")))

		hd := `CREATE EXTERNAL TABLE %s.household_demographics ( 
			hd_demo_sk integer, 
			hd_income_band_sk integer,
			hd_buy_potential character varying(15),
			hd_dep_count integer,
			hd_vehicle_count integer
		) LOCATION (%s) FORMAT 'CSV' (DELIMITER '|') 
				   `
		conn.Execute(fmt.Sprintf(hd, conf.Ext, locf("household_demographics")))

		ib := `CREATE EXTERNAL TABLE %s.income_band ( 
			ib_income_band_sk integer, 
			ib_lower_bound integer,
			ib_upper_bound integer
		) LOCATION (%s) FORMAT 'CSV' (DELIMITER '|') 
				   `
		conn.Execute(fmt.Sprintf(ib, conf.Ext, locf("income_band")))

		inv := `CREATE EXTERNAL TABLE %s.inventory ( 
			inv_date_sk integer, 
			inv_item_sk integer, 
			inv_warehouse_sk integer, 
			inv_quantity_on_hand integer
		) LOCATION (%s) FORMAT 'CSV' (DELIMITER '|') 
				   `
		conn.Execute(fmt.Sprintf(inv, conf.Ext, locf("inventory")))

		i := `CREATE EXTERNAL TABLE %s.item ( 
			i_item_sk integer, 
			i_item_id character varying(16),
			i_rec_start_date date,
			i_rec_end_date date,
			i_item_desc character varying(200),
			i_current_price double precision,
			i_wholesale_cost double precision,
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
		) LOCATION (%s) FORMAT 'CSV' (DELIMITER '|') 
				   `
		conn.Execute(fmt.Sprintf(i, conf.Ext, locf("item")))

		p := `CREATE EXTERNAL TABLE %s.promotion ( 
			p_promo_sk integer, 
			p_promo_id character varying(16),
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
		) LOCATION (%s) FORMAT 'CSV' (DELIMITER '|') 
				   `
		conn.Execute(fmt.Sprintf(p, conf.Ext, locf("promotion")))

		r := `CREATE EXTERNAL TABLE %s.reason ( 
			r_reason_sk integer, 
			r_reason_id character varying(16), 
			r_reason_desc character varying(100)
		) LOCATION (%s) FORMAT 'CSV' (DELIMITER '|') 
				   `
		conn.Execute(fmt.Sprintf(r, conf.Ext, locf("reason")))

		sm := `CREATE EXTERNAL TABLE %s.ship_mode ( 
			sm_ship_mode_sk integer, 
			sm_ship_mode_id character varying(16), 
			sm_type character varying(30),
			sm_code character varying(10),
			sm_carrier character varying(20),
			sm_contract character varying(20)
		) LOCATION (%s) FORMAT 'CSV' (DELIMITER '|') 
				   `
		conn.Execute(fmt.Sprintf(sm, conf.Ext, locf("ship_mode")))

		s := `CREATE EXTERNAL TABLE %s.store ( 
			s_store_sk integer, 
			s_store_id character varying(16), 
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
			s_gmt_offset double precision, 
			s_tax_precentage double precision 
		) LOCATION (%s) FORMAT 'CSV' (DELIMITER '|') 
				   `
		conn.Execute(fmt.Sprintf(s, conf.Ext, locf("store")))

		sr := `CREATE EXTERNAL TABLE %s.store_returns ( 
			sr_returned_date_sk integer,
			sr_return_time_sk integer,
			sr_item_sk integer, 
			sr_customer_sk integer,
			sr_cdemo_sk integer,
			sr_hdemo_sk integer,
			sr_addr_sk integer,
			sr_store_sk integer,
			sr_reason_sk integer,
			sr_ticket_number bigint, 
			sr_return_quantity integer,
			sr_return_amt double precision,
			sr_return_tax double precision,
			sr_return_amt_inc_tax double precision,
			sr_fee double precision,
			sr_return_ship_cost double precision,
			sr_refunded_cash double precision,
			sr_reversed_charge double precision,
			sr_store_credit double precision,
			sr_net_loss double precision
		) LOCATION (%s) FORMAT 'CSV' (DELIMITER '|') 
				   `
		conn.Execute(fmt.Sprintf(sr, conf.Ext, locf("store_returns")))

		ss := `CREATE EXTERNAL TABLE %s.store_sales ( 
			ss_sold_date_sk integer,
			ss_sold_time_sk integer,
			ss_item_sk int, 
			ss_customer_sk integer,
			ss_cdemo_sk integer,
			ss_hdemo_sk integer,
			ss_addr_sk integer,
			ss_store_sk integer,
			ss_promo_sk integer,
			ss_ticket_number bigint,
			ss_quantity integer,
			ss_wholesale_cost double precision,
			ss_list_price double precision,
			ss_sales_price double precision,
			ss_ext_discount_amt double precision,
			ss_ext_sales_price double precision,
			ss_ext_wholesale_cost double precision,
			ss_ext_list_price double precision,
			ss_ext_tax double precision,
			ss_coupon_amt double precision,
			ss_net_paid double precision,
			ss_net_paid_inc_tax double precision,
			ss_net_profit double precision
		) LOCATION (%s) FORMAT 'CSV' (DELIMITER '|') 
				   `
		conn.Execute(fmt.Sprintf(ss, conf.Ext, locf("store_sales")))

		ttt := `CREATE EXTERNAL TABLE %s.time_dim ( 
			t_time_sk integer, 
			t_time_id character varying(16),
			t_time integer,
			t_hour integer,
			t_minute integer,
			t_second integer,
			t_am_pm character varying (2),
			t_shift character varying (20),
			t_sub_shift character varying (20),
			t_meal_time character varying (20)
		) LOCATION (%s) FORMAT 'CSV' (DELIMITER '|') 
				   `
		conn.Execute(fmt.Sprintf(ttt, conf.Ext, locf("time_dim")))

		w := `CREATE EXTERNAL TABLE %s.warehouse ( 
			w_warehouse_sk integer, 
			w_warehouse_id character varying(16), 
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
			w_gmt_offset double precision 
		) LOCATION (%s) FORMAT 'CSV' (DELIMITER '|') 
				   `
		conn.Execute(fmt.Sprintf(w, conf.Ext, locf("warehouse")))

		wp := `CREATE EXTERNAL TABLE %s.web_page ( 
			wp_web_page_sk integer, 
			wp_web_page_id character varying(16), 
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
		) LOCATION (%s) FORMAT 'CSV' (DELIMITER '|') 
				   `
		conn.Execute(fmt.Sprintf(wp, conf.Ext, locf("web_page")))

		wr := `CREATE EXTERNAL TABLE %s.web_returns ( 
			wr_returned_date_sk integer,
			wr_returned_time_sk integer,
			wr_item_sk integer, 
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
			wr_order_number integer, 
			wr_return_quantity integer,
			wr_return_amt double precision,
			wr_return_tax double precision,
			wr_return_amt_inc_tax double precision,
			wr_fee double precision,
			wr_return_ship_cost double precision,
			wr_refunded_cash double precision,
			wr_reversed_charge double precision,
			wr_account_credit double precision,
			wr_net_loss double precision
		) LOCATION (%s) FORMAT 'CSV' (DELIMITER '|') 
				   `
		conn.Execute(fmt.Sprintf(wr, conf.Ext, locf("web_returns")))

		ws := `CREATE EXTERNAL TABLE %s.web_sales ( 
			ws_sold_date_sk integer,
			ws_sold_time_sk integer,
			ws_ship_date_sk integer,
			ws_item_sk integer, 
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
			ws_order_number integer, 
			ws_quantity integer,
			ws_wholesale_cost double precision,
			ws_list_price double precision,
			ws_sales_price double precision,
			ws_ext_discount_amt double precision,
			ws_ext_sales_price double precision,
			ws_ext_wholesale_cost double precision,
			ws_ext_list_price double precision,
			ws_ext_tax double precision,
			ws_coupon_amt double precision,
			ws_ext_ship_cost double precision,
			ws_net_paid double precision,
			ws_net_paid_inc_tax double precision,
			ws_net_paid_inc_ship double precision,
			ws_net_paid_inc_ship_tax double precision,
			ws_net_profit double precision
		) LOCATION (%s) FORMAT 'CSV' (DELIMITER '|') 
				   `
		conn.Execute(fmt.Sprintf(ws, conf.Ext, locf("web_sales")))

		web := `CREATE EXTERNAL TABLE %s.web_site ( 
			web_site_sk integer, 
			web_site_id character varying(16), 
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
			web_gmt_offset double precision, 
			web_tax_percentage double precision 
		) LOCATION (%s) FORMAT 'CSV' (DELIMITER '|') 
				   `
		conn.Execute(fmt.Sprintf(web, conf.Ext, locf("web_site")))
	})
}
