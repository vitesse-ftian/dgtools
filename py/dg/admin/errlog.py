import dg.conn
import dg.xtable

def xt_errlog(conn, lv="ERROR+", limit=None):
    sql = "select * from gp_toolkit.gp_log_system "
    if lv != None:
        if lv == "ERROR+":
            sql += "where logseverity in ('ERROR', 'FATAL', 'PANIC') "
        elif lv == "FATAL+":
            sql += "where logseverity in ('FATAL', 'PANIC') "
        else:
            sql += "where logseverity = '{0}' ".format(lv)

    sql += " order by logtime desc "
    if limit != None:
        sql += " limit {0}".format(limit)

    return dg.xtable.fromQuery(conn, sql)
    
if __name__ == '__main__':
    c = dg.conn.Conn("host=localhost")
    xt = xt_errlog(c, limit = 10)
    rows = xt.execute()
    print(rows)


