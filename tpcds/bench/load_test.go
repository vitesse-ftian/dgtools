package bench

import (
	"fmt"
	"testing"
)

func BenchmarkLoadData(b *testing.B) {
	conf, err := GetConfig()
	if err != nil {
		b.Errorf("Configuration error: %s", err.Error())
	}

	conn, err := Connect()
	if err != nil {
		b.Errorf("Cannot connect to database %s, error: %s", err.Error())
	}
	defer conn.Disconnect()

	err = conn.Execute("set gp_autostats_mode = 'none'")
	if err != nil {
		b.Errorf("Cannot set guc gp_autostats_mode.  error: %s", err.Error())
	}

	insSql := func(tab string) {
		sql := fmt.Sprintf("INSERT INTO tpcds.%s SELECT * FROM %s.%s", tab, conf.Ext, tab)
		err := conn.Execute(sql)
		if err != nil {
			b.Errorf("Cannot load table %s.  error: %s", tab, err.Error())
		}
	}

	b.Run("Step=call_center", func(b *testing.B) {
		insSql("call_center")
	})

	b.Run("Step=catalog_page", func(b *testing.B) {
		insSql("catalog_page")
	})

	b.Run("Step=catalog_returns", func(b *testing.B) {
		insSql("catalog_returns")
	})

	b.Run("Step=catalog_sales", func(b *testing.B) {
		insSql("catalog_sales")
	})

	b.Run("Step=customer", func(b *testing.B) {
		insSql("customer")
	})

	b.Run("Step=customer_address", func(b *testing.B) {
		insSql("customer_address")
	})

	b.Run("Step=customer_demographics", func(b *testing.B) {
		insSql("customer_demographics")
	})

	b.Run("Step=date_dim", func(b *testing.B) {
		insSql("date_dim")
	})

	b.Run("Step=household_demographics", func(b *testing.B) {
		insSql("household_demographics")
	})

	b.Run("Step=income_band", func(b *testing.B) {
		insSql("income_band")
	})

	b.Run("Step=inventory", func(b *testing.B) {
		insSql("inventory")
	})

	b.Run("Step=item", func(b *testing.B) {
		insSql("item")
	})

	b.Run("Step=promotion", func(b *testing.B) {
		insSql("promotion")
	})

	b.Run("Step=reason", func(b *testing.B) {
		insSql("reason")
	})

	b.Run("Step=ship_mode", func(b *testing.B) {
		insSql("ship_mode")
	})

	b.Run("Step=store", func(b *testing.B) {
		insSql("store")
	})

	b.Run("Step=store_returns", func(b *testing.B) {
		insSql("store_returns")
	})

	b.Run("Step=store_sales", func(b *testing.B) {
		insSql("store_sales")
	})

	b.Run("Step=time_dim", func(b *testing.B) {
		insSql("time_dim")
	})

	b.Run("Step=warehouse", func(b *testing.B) {
		insSql("warehouse")
	})

	b.Run("Step=web_page", func(b *testing.B) {
		insSql("web_page")
	})

	b.Run("Step=web_returns", func(b *testing.B) {
		insSql("web_returns")
	})

	b.Run("Step=web_sales", func(b *testing.B) {
		insSql("web_sales")
	})

	b.Run("Step=web_site", func(b *testing.B) {
		insSql("web_site")
	})

	b.Run("Step=analyze", func(b *testing.B) {
		err := conn.Execute("VACUUM ANALYZE")
		if err != nil {
			b.Errorf("Cannot vacuum analyze database. error: %s", err.Error())
		}
	})
}
