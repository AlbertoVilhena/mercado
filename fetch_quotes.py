"""Coleta cotacoes (Yahoo Finance, atraso ~15 min) e grava no Postgres do Render.
Roda como Cron Job. Variavel obrigatoria: DATABASE_URL."""
import os, datetime as dt
import requests, psycopg2

SYMBOLS = {  # simbolo Yahoo -> (id, nome)
    "^BVSP": ("ibov", "Ibovespa"),
    "BRL=X": ("dolar", "Dólar (USD/BRL)"),
    "BZ=F": ("brent", "Brent (US$)"),
}
HEAD = {"User-Agent": "Mozilla/5.0"}

def quote(sym):
    r = requests.get(f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}",
                     params={"interval": "5m", "range": "1d"}, headers=HEAD, timeout=20)
    r.raise_for_status()
    m = r.json()["chart"]["result"][0]["meta"]
    return m["regularMarketPrice"], m.get("chartPreviousClose"), m["regularMarketTime"]

def main():
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS quotes(
        id text, name text, price double precision, prev_close double precision,
        change_pct double precision, quote_time timestamptz, fetched_at timestamptz DEFAULT now(),
        source text DEFAULT 'Yahoo Finance (atraso ~15 min)')""")
    cur.execute("CREATE INDEX IF NOT EXISTS quotes_id_time ON quotes(id, fetched_at DESC)")
    for sym, (qid, name) in SYMBOLS.items():
        try:
            p, prev, ts = quote(sym)
            chg = (p / prev - 1) * 100 if prev else None
            cur.execute("INSERT INTO quotes(id,name,price,prev_close,change_pct,quote_time) VALUES(%s,%s,%s,%s,%s,%s)",
                        (qid, name, p, prev, chg, dt.datetime.fromtimestamp(ts, dt.timezone.utc)))
            print("ok", qid, p)
        except Exception as e:
            print("erro", sym, e)
    conn.commit(); cur.close(); conn.close()

if __name__ == "__main__":
    main()
