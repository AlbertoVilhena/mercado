"""Coletor gratuito do painel de mercado (Render Cron Job -> Postgres Neon).

- Cotacoes: Yahoo Finance (atraso ~15 min) para indices, cambio, petroleo e ~20 acoes.
- Noticias: feeds RSS publicos (InfoMoney, Money Times, Exame, G1).
- Analises automaticas: calculadas por regra a partir das cotacoes (sem IA, sem custo).

Variavel obrigatoria: DATABASE_URL.
Cada etapa e independente: se uma fonte falhar, as demais seguem.
"""
import os
import datetime as dt
import email.utils
import xml.etree.ElementTree as ET

import psycopg2
import requests

HEAD = {"User-Agent": "Mozilla/5.0 (compatible; frame-mercado/1.0)"}

# simbolo Yahoo -> (id, nome, grupo)
SYMBOLS = {
    "^BVSP": ("ibov", "Ibovespa", "Indice"),
    "BRL=X": ("dolar", "Dólar (USD/BRL)", "Indice"),
    "BZ=F": ("brent", "Brent (US$)", "Indice"),
    "ITUB4.SA": ("itub4", "Itaú", "Bancos"),
    "BBDC4.SA": ("bbdc4", "Bradesco", "Bancos"),
    "BBAS3.SA": ("bbas3", "Banco do Brasil", "Bancos"),
    "SANB11.SA": ("sanb11", "Santander", "Bancos"),
    "BPAC11.SA": ("bpac11", "BTG Pactual", "Bancos"),
    "VALE3.SA": ("vale3", "Vale", "Exportadora"),
    "SUZB3.SA": ("suzb3", "Suzano", "Exportadora"),
    "KLBN11.SA": ("klbn11", "Klabin", "Exportadora"),
    "GGBR4.SA": ("ggbr4", "Gerdau", "Exportadora"),
    "EMBJ3.SA": ("embj3", "Embraer", "Exportadora"),
    "PETR4.SA": ("petr4", "Petrobras", "Energia"),
    "WEGE3.SA": ("wege3", "WEG", "Indústria"),
    "MGLU3.SA": ("mglu3", "Magazine Luiza", "Varejo/consumo"),
    "VAMO3.SA": ("vamo3", "Vamos", "Varejo/consumo"),
    "CEAB3.SA": ("ceab3", "C&A", "Varejo/consumo"),
    "HAPV3.SA": ("hapv3", "Hapvida", "Saúde"),
    "RDOR3.SA": ("rdor3", "Rede D'Or", "Saúde"),
    "CYRE3.SA": ("cyre3", "Cyrela", "Construção"),
    "MRVE3.SA": ("mrve3", "MRV", "Construção"),
    "TEND3.SA": ("tend3", "Tenda", "Construção"),
    "CSAN3.SA": ("csan3", "Cosan", "Outros"),
    "SBSP3.SA": ("sbsp3", "Sabesp", "Outros"),
}

FEEDS = [
    ("InfoMoney", "https://www.infomoney.com.br/mercados/feed/"),
    ("InfoMoney", "https://www.infomoney.com.br/economia/feed/"),
    ("Money Times", "https://www.moneytimes.com.br/feed/"),
    ("Exame", "https://exame.com/invest/feed/"),
    ("G1 Economia", "https://g1.globo.com/rss/g1/economia/"),
]


def quote(sym):
    r = requests.get(
        f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}",
        params={"interval": "5m", "range": "1d"}, headers=HEAD, timeout=20)
    r.raise_for_status()
    m = r.json()["chart"]["result"][0]["meta"]
    return m["regularMarketPrice"], m.get("chartPreviousClose"), m["regularMarketTime"]


def setup(cur):
    cur.execute("""CREATE TABLE IF NOT EXISTS quotes(
        id text, name text, price double precision, prev_close double precision,
        change_pct double precision, quote_time timestamptz,
        fetched_at timestamptz DEFAULT now(),
        source text DEFAULT 'Yahoo Finance (atraso ~15 min)')""")
    cur.execute("ALTER TABLE quotes ADD COLUMN IF NOT EXISTS grp text")
    cur.execute("CREATE INDEX IF NOT EXISTS quotes_id_time ON quotes(id, fetched_at DESC)")
    cur.execute("""CREATE TABLE IF NOT EXISTS news(
        url text PRIMARY KEY, title text, source text,
        published_at timestamptz, fetched_at timestamptz DEFAULT now())""")
    cur.execute("""CREATE TABLE IF NOT EXISTS analyses_auto(
        key text PRIMARY KEY, title text, finding text, why text,
        computed_at timestamptz DEFAULT now())""")


def collect_quotes(conn, cur):
    got = {}
    for sym, (qid, name, grp) in SYMBOLS.items():
        try:
            p, prev, ts = quote(sym)
            chg = (p / prev - 1) * 100 if prev else None
            cur.execute(
                "INSERT INTO quotes(id,name,price,prev_close,change_pct,quote_time,grp) VALUES(%s,%s,%s,%s,%s,%s,%s)",
                (qid, name, p, prev, chg, dt.datetime.fromtimestamp(ts, dt.timezone.utc), grp))
            got[qid] = dict(name=name, grp=grp, price=p, prev=prev, chg=chg)
            print("ok", qid, p)
        except Exception as e:  # noqa: BLE001
            print("erro", sym, e)
    conn.commit()
    return got


def collect_news(conn, cur):
    total = 0
    for source, url in FEEDS:
        try:
            r = requests.get(url, headers=HEAD, timeout=20)
            r.raise_for_status()
            root = ET.fromstring(r.content)
            for it in root.iter("item"):
                title = (it.findtext("title") or "").strip()
                link = (it.findtext("link") or "").strip()
                pub = it.findtext("pubDate")
                if not title or not link.startswith("http"):
                    continue
                try:
                    when = email.utils.parsedate_to_datetime(pub) if pub else dt.datetime.now(dt.timezone.utc)
                except Exception:  # noqa: BLE001
                    when = dt.datetime.now(dt.timezone.utc)
                cur.execute(
                    "INSERT INTO news(url,title,source,published_at) VALUES(%s,%s,%s,%s) ON CONFLICT (url) DO NOTHING",
                    (link, title, source, when))
                total += cur.rowcount
            print("feed ok", source, url)
        except Exception as e:  # noqa: BLE001
            print("feed erro", url, e)
    cur.execute("DELETE FROM news WHERE published_at < now() - interval '3 days'")
    conn.commit()
    print("noticias novas:", total)


def num(v, d=0):
    """Formata numero em pt-BR (ponto de milhar, virgula decimal)."""
    return f"{v:,.{d}f}".replace(",", "#").replace(".", ",").replace("#", ".")


def pct(v):
    return f"{v:+.2f}".replace(".", ",").replace("-", "−") + "%"


def upsert(cur, key, title, finding, why):
    cur.execute(
        """INSERT INTO analyses_auto(key,title,finding,why,computed_at) VALUES(%s,%s,%s,%s,now())
           ON CONFLICT (key) DO UPDATE SET title=EXCLUDED.title, finding=EXCLUDED.finding,
           why=EXCLUDED.why, computed_at=now()""",
        (key, title, finding, why))


def analyses(conn, cur, got):
    stocks = {k: v for k, v in got.items() if v["grp"] != "Indice" and v["chg"] is not None}
    # 1) amplitude
    if len(stocks) >= 6:
        up = [v for v in stocks.values() if v["chg"] > 0]
        down = [v for v in stocks.values() if v["chg"] < 0]
        best = max(stocks.values(), key=lambda v: v["chg"])
        worst = min(stocks.values(), key=lambda v: v["chg"])
        upsert(cur, "1_amplitude", "Amplitude: quantas ações sobem",
               f"{len(up)} de {len(stocks)} ações monitoradas sobem e {len(down)} caem. "
               f"Maior alta: {best['name']} {pct(best['chg'])}; maior queda: {worst['name']} {pct(worst['chg'])} "
               f"(diferença de {num(best['chg'] - worst['chg'], 1)} p.p.).",
               "Quando a maioria sobe, o movimento é um repique amplo; quando poucas lideram, é concentrado e mais frágil. "
               "Cálculo automático sobre as ações monitoradas, com atraso de ~15 min.")
        # 2) grupos
        groups = {}
        for v in stocks.values():
            groups.setdefault(v["grp"], []).append(v["chg"])
        avg = sorted(((g, sum(x) / len(x)) for g, x in groups.items()), key=lambda t: -t[1])
        txt = "; ".join(f"{g} {pct(a)}" for g, a in avg)
        upsert(cur, "2_grupos", "Média por grupo",
               "Variação média no dia: " + txt + ".",
               "Exportadoras costumam subir com dólar alto; bancos, construção e varejo refletem juros e risco fiscal. "
               "A distância entre os grupos mostra o que o mercado está precificando.")
    # 3) circuit breaker
    ib = got.get("ibov")
    if ib and ib["prev"]:
        p, prev = ib["price"], ib["prev"]
        l10, l15, l20 = prev * 0.90, prev * 0.85, prev * 0.80
        d10 = (p / l10 - 1) * 100
        upsert(cur, "3_circuit", "Distância do circuit breaker",
               f"Ibovespa em {num(p)} pts ({pct(ib['chg'])}). Linha de −10%: {num(l10)} (a {num(abs(d10), 1)}% de distância); "
               f"−15%: {num(l15)}; −20%: {num(l20)}.",
               "Regra da B3, sobre o fechamento anterior: −10% pausa o pregão por 30 min; −15% (após a reabertura) por 1 h; "
               "−20% a B3 pode suspender. Cálculo automático.")
    # 4) dolar e bolsa
    dl = got.get("dolar")
    if ib and dl and dl["chg"] is not None:
        upsert(cur, "4_dolar", "Dólar e bolsa",
               f"Dólar R$ {num(dl['price'], 3)} ({pct(dl['chg'])}); Ibovespa {pct(ib['chg'])}.",
               "Dólar em alta com bolsa em queda indica aversão a risco local; dólar cedendo com bolsa subindo "
               "indica alívio. Cálculo automático, atraso de ~15 min.")
    conn.commit()


def main():
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    cur = conn.cursor()
    setup(cur)
    conn.commit()
    got = collect_quotes(conn, cur)
    try:
        collect_news(conn, cur)
    except Exception as e:  # noqa: BLE001
        conn.rollback()
        print("noticias erro", e)
    try:
        analyses(conn, cur, got)
    except Exception as e:  # noqa: BLE001
        conn.rollback()
        print("analises erro", e)
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
