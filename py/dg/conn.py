import psycopg2

class Conn:
    def __init__(self, connstr):
        self.conn = psycopg2.connect(connstr)
        self.setversion()
        self.nexttmp = 0
    
    def setversion(self):
        cur = self.conn.cursor()
        cur.execute("select version()")
        verstr = cur.fetchone()
        if "Greenplum Database 4" in verstr[0]:
            self.ver = 4
        elif "Greenplum Database 5" in verstr[0]:
            self.ver = 5
        else:
            raise RuntimeError('Unknown Deepgreen Version')
       
        self.typemap = {}
        cur.execute("select oid, typname from pg_type")
        rows = cur.fetchall()
        for row in rows:
            self.typemap[row[0]] = row[1]

        cur.close()
        self.conn.commit()

    def close(self):
        self.conn.close()

    def next_tmpname(self):
        self.nexttmp += 1
        return "tmp_{0}".format(self.nexttmp)

    def execute(self, sql):
        cur = self.conn.cursor()
        cur.execute(sql) 
        rows = cur.fetchall()
        cur.close()
        self.conn.commit()
        return rows

    def cursor(self, sql):
        cur = self.conn.cursor()
        cur.execute(sql) 
        return cur


if __name__ == '__main__':
    conn = Conn("host=localhost user=ftian dbname=ftian")
    print("Connected to deepgreen database, version is ", conn.ver)

