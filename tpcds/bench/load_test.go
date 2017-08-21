package bench

import (
	"fmt"
	"testing"
)

func insSql(b *testing.B, tab string, ext string) string {
	sql := fmt.Sprintf("INSERT INTO tpcds.%s SELECT * FROM %s.%s", tab, ext, tab)
	err := conn.Execute(sql)
	if err != nil {
		b.Errorf("Cannot load table %s.  error: %s", tab, err.Error())
	}
}

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

	b.Run("Step=call_center", func(b *testing.B) {
		insSql(b, "call_center", conf.Ext)
	})

	b.Run("Step=catalog_page", func(b *testing.B) {
		insSql(b, "catalog_page", conf.Ext)
	})

	b.Run("Step=catalog_returns", func(b *testing.B) {
		insSql(b, "catalog_returns", conf.Ext)
	})

	b.Run("Step=catalog_sales", func(b *testing.B) {
		insSql(b, "catalog_sales", conf.Ext)
	})

	b.Run("Step=customer", func(b *testing.B) {
		insSql(b, "customer", conf.Ext)
	})

	b.Run("Step=customer_address", func(b *testing.B) {
		insSql(b, "customer_address", conf.Ext)
	})

	b.Run("Step=customer_demographics", func(b *testing.B) {
		insSql(b, "customer_demographics", conf.Ext)
	})

	b.Run("Step=date_dim", func(b *testing.B) {
		insSql(b, "date_dim", conf.Ext)
	})

	b.Run("Step=household_demographics", func(b *testing.B) {
		insSql(b, "household_demographics", conf.Ext)
	})

	b.Run("Step=income_band", func(b *testing.B) {
		insSql(b, "income_band", conf.Ext)
	})

	b.Run("Step=inventory", func(b *testing.B) {
		insSql(b, "inventory", conf.Ext)
	})

	b.Run("Step=item", func(b *testing.B) {
		insSql(b, "item", conf.Ext)
	})

	b.Run("Step=promotion", func(b *testing.B) {
		insSql(b, "promotion", conf.Ext)
	})

	b.Run("Step=reason", func(b *testing.B) {
		insSql(b, "reason", conf.Ext)
	})

	b.Run("Step=ship_mode", func(b *testing.B) {
		insSql(b, "ship_mode", conf.Ext)
	})

	b.Run("Step=store", func(b *testing.B) {
		insSql(b, "store", conf.Ext)
	})

	b.Run("Step=store_returns", func(b *testing.B) {
		insSql(b, "store_returns", conf.Ext)
	})

	b.Run("Step=store_sales", func(b *testing.B) {
		insSql(b, "store_sales", conf.Ext)
	})

	b.Run("Step=time_dim", func(b *testing.B) {
		insSql(b, "time_dim", conf.Ext)
	})

	b.Run("Step=warehouse", func(b *testing.B) {
		insSql(b, "warehouse", conf.Ext)
	})

	b.Run("Step=web_page", func(b *testing.B) {
		insSql(b, "web_page", conf.Ext)
	})

	b.Run("Step=web_returns", func(b *testing.B) {
		insSql(b, "web_returns", conf.Ext)
	})

	b.Run("Step=web_sales", func(b *testing.B) {
		insSql(b, "web_sales", conf.Ext)
	})

	b.Run("Step=web_site", func(b *testing.B) {
		insSql(b, "web_site", conf.Ext)
	})

	b.Run("Step=analyze", func(b *testing.B) {
		err := conn.Execute("VACUUM ANALYZE")
		if err != nil {
			b.Errorf("Cannot vacuum analyze database. error: %s", err.Error())
		}
	})
}
