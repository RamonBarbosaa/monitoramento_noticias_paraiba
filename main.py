#!/usr/bin/env python3
# main.py
# Coleta notícias via Google News RSS, filtra por keywords/cidades, gera resumo local e grava CSV.
# Versão: gratuita, roda em GitHub Actions sem chaves externas.

import feedparser
import requests
import json
import pandas as pd
import os
import time
from datetime import datetime
from bs4 import BeautifulSoup

# Sumy para sumarização
from sumy.parsers.plaintext import PlaintextParser
from sumy.nlp.tokenizers import Tokenizer
from sumy.summarizers.lex_rank import LexRankSummarizer

# Config
KW_FILE = "keywords.json"
OUTPUT_CSV = "noticias_paraiba.csv"
SEARCH_TOP = 10  # resultados por query

# Funções utilitárias
def load_keywords():
    with open(KW_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def build_google_news_rss(query):
    # Monta a URL de busca do Google News RSS em pt-BR (sem chave).
    # when:7d limita a última semana (opcional). Ajuste se quiser mais/menos.
    q = requests.utils.requote_uri(query + " paraíba")
    url = f"https://news.google.com/rss/search?q={q}&hl=pt-BR&gl=BR&ceid=BR:pt-PT"
    return url

def fetch_rss_entries(rss_url):
    feed = feedparser.parse(rss_url)
    return feed.entries

def extract_text_from_link(url):
    # Tentativa simples de pegar texto da página (fallback para summary do RSS)
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(url, headers=headers, timeout=10)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html5lib")
        # junta paragrafos <p>
        paragraphs = soup.find_all("p")
        text = "\n".join([p.get_text() for p in paragraphs])
        # limite pra evitar textos gigantes
        if len(text) < 200:
            # fallback: usar meta description
            meta = soup.find("meta", {"name": "description"})
            if meta and meta.get("content"):
                text = meta.get("content")
        return text.strip()
    except Exception:
        return ""

def simple_summarize(text, sentences_count=3):
    if not text or len(text.split()) < 30:
        # texto curto: retorna texto original
        return text.strip()
    try:
        parser = PlaintextParser.from_string(text, Tokenizer("portuguese"))
        summarizer = LexRankSummarizer()
        summary = summarizer(parser.document, sentences_count)
        return " ".join([str(s) for s in summary])
    except Exception:
        # fallback: primeiras sentenças
        return " ".join(text.split(".")[:sentences_count]).strip()

def classify_by_keywords(text, keywords):
    txt = text.lower()
    # heurística simples: procura palavras-chave
    if any(k in txt for k in ["homicid", "morte", "assassin"]):
        return "Homicídio"
    if any(k in txt for k in ["tráfico", "trafic", "droga"]):
        return "Tráfico"
    if any(k in txt for k in ["operação policial", "operação", "polícia", "policial"]):
        return "Operação Policial"
    if any(k in txt for k in ["roubo", "assalto"]):
        return "Roubo"
    return "Outro"

def extract_cities(text, cities):
    found = []
    t = text.lower()
    for c in cities:
        if c.lower() in t:
            found.append(c)
    return list(dict.fromkeys(found))

def extract_names_heuristic(text):
    # heurística simples: pega sequências de palavras com inicial maiúscula (>1 palavra)
    names = set()
    words = text.split()
    i = 0
    while i < len(words):
        w = words[i]
        if w.istitle() and len(w) > 2:
            # check next word(s)
            name_parts = [w]
            j = i+1
            while j < len(words) and words[j].istitle():
                name_parts.append(words[j])
                j += 1
            if len(name_parts) >= 2:
                name = " ".join(name_parts)
                names.add(name)
                i = j
            else:
                i += 1
        else:
            i += 1
    return list(names)

def load_existing_csv(path):
    if os.path.exists(path):
        try:
            return pd.read_csv(path)
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()

def save_csv(df, path):
    df.to_csv(path, index=False, encoding="utf-8-sig")

def normalize_date(d):
    # tenta parsear data do feed
    try:
        parsed = datetime(*d[:6])
        return parsed.isoformat()
    except Exception:
        return str(d)

def main():
    cfg = load_keywords()
    keywords = cfg.get("keywords", [])
    cities = cfg.get("cities", [])
    existing = load_existing_csv(OUTPUT_CSV)
    rows = []
    seen_keys = set()
    # preenche seen_keys para evitar duplicatas (titulo+date)
    if not existing.empty:
        for _, r in existing.iterrows():
            key = f"{r.get('title','')}_{r.get('published','')}"
            seen_keys.add(key)

    for kw in keywords:
        rss = build_google_news_rss(kw)
        entries = fetch_rss_entries(rss)[:SEARCH_TOP]
        for e in entries:
            title = e.get("title", "")
            link = e.get("link", "")
            published = e.get("published", "")
            summary_rss = e.get("summary", "") or e.get("description", "")
            key = f"{title}_{published}"
            if key in seen_keys:
                continue

            # tenta extrair texto do link; fallback para summary do RSS
            article_text = extract_text_from_link(link)
            if not article_text:
                article_text = summary_rss

            # detecta se tem Paraíba (garante foco)
            lower = (title + " " + article_text).lower()
            if "paraíba" not in lower and "paraiba" not in lower:
                # descarta se não menciona Paraíba
                continue

            # resumo
            resumo = simple_summarize(article_text, sentences_count=3)
            tipo = classify_by_keywords(title + " " + article_text, keywords)
            local = extract_cities(title + " " + article_text, cities)
            nomes = extract_names_heuristic(article_text)

            # relevância simples: quantas keywords aparecem
            relevance = sum(1 for k in keywords if k.lower() in lower)

            row = {
                "title": title,
                "link": link,
                "published": published,
                "fetched_at": datetime.utcnow().isoformat(),
                "summary": resumo,
                "category": tipo,
                "cities": ";".join(local),
                "names": ";".join(nomes),
                "relevance": relevance,
                "raw_text_snippet": (article_text[:800].replace("\n"," ")).strip()
            }
            rows.append(row)
            seen_keys.add(key)
            # pequeno delay pra ser gentil com servidores
            time.sleep(0.5)

    # montar DataFrame final: concat com existentes
    new_df = pd.DataFrame(rows)
    if existing is not None and not existing.empty:
        df_final = pd.concat([existing, new_df], ignore_index=True)
    else:
        df_final = new_df

    # salvar CSV
    if not df_final.empty:
        save_csv(df_final, OUTPUT_CSV)
        print(f"Saved {len(df_final)} records to {OUTPUT_CSV}")
    else:
        print("No new records found.")

if __name__ == "__main__":
    main()

