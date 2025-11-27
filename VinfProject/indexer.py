import re
import math
import csv
from collections import defaultdict, Counter
import sys
import tiktoken
import os
import json
max_limit = sys.maxsize

while True:
    try:
        csv.field_size_limit(max_limit)
        break
    except OverflowError:
        max_limit = int(max_limit / 10)

class Indexer:
    def __init__(self):
        self.docs = {}
        self.index = defaultdict(list)
        self.num_docs = 0
        self.used_fields = []
        self.enc = tiktoken.get_encoding("cl100k_base")
        self.token_counts = {}

    def load_from_tsv(self, path):
        with open(path, encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter="\t")
            self.used_fields = reader.fieldnames
            for i, row in enumerate(reader):
                parts = []
                for field in self.used_fields:
                    val = str(row.get(field, "")).strip()
                    if val and val.lower() != "nan":
                        parts.append(f"{field} is {val}")
                text = " ".join(parts)
                tokens = self.enc.encode(text)
                self.token_counts[i] = len(tokens)
                self.docs[i] = text.strip()

        self.num_docs = len(self.docs)
        total_tokens = sum(self.token_counts.values())
        avg_tokens = total_tokens / len(self.token_counts)
        max_tokens = max(self.token_counts.values())
        min_tokens = min(self.token_counts.values())

        print(f"Loaded {self.num_docs} documents from {path}")
        print(f"Average tokens per document: {avg_tokens:.1f}")
        print(f"Min tokens: {min_tokens} | Max tokens: {max_tokens}")


    def tokenize(self, text):
        return re.findall(r"\b\w+\b", text.lower())

    def build_index(self):
        for doc_id, text in self.docs.items():
            tokens = self.tokenize(text)
            counts = Counter(tokens)
            for term, freq in counts.items():
                self.index[term].append((doc_id, freq))

        for term in self.index:
            self.index[term].sort(key=lambda x: x[0])
        print(f"Indexed {len(self.index)} unique terms")

    def idf_classic(self, df):
        return math.log(self.num_docs / (df or 1))

    def idf_smooth(self, df):
        return math.log(1 + self.num_docs / (1 + df))

    def idf_probabilistic(self, df):

        return math.log((self.num_docs - df + 0.5) / (df + 0.5) + 1e-10)

    def search(self, query, idf_type="classic", top_n=5):
        query_terms = self.tokenize(query)
        if not query_terms:
            return []

        scores = defaultdict(float)
        doc_sets = []

        for term in query_terms:
            postings = self.index.get(term, [])
            df = len(postings)
            if df == 0:
                continue
            if idf_type == "classic":
                idf = self.idf_classic(df)
            elif idf_type == "smooth":
                idf = self.idf_smooth(df)
            else:
                idf = self.idf_probabilistic(df)

            docs_with_term = set()

            for doc_id, tf in postings:
                scores[doc_id] += tf * idf
                docs_with_term.add(doc_id)

            doc_sets.append(docs_with_term)

        if doc_sets:
            common_docs = set.intersection(*doc_sets)
            scores = {d: s for d, s in scores.items() if d in common_docs}
        else:
            scores = {}

        ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        return [(doc_id, score, self.docs[doc_id]) for doc_id, score in ranked[:top_n]]

    def show_results(self, query, idf_type="classic", top_n=5):
        print(f"\nQuery: '{query}' ({idf_type} IDF)")
        results = self.search(query, idf_type, top_n)
        if not results:
            print("No results found.")
        for doc_id, score, text in results:
            print(f"{score:.3f} | {text}...")

    def save_index(self, path="index_data.json"):
        data = {
            "docs": self.docs,
            "index": {term: postings for term, postings in self.index.items()},
            "num_docs": self.num_docs,
            "token_counts": self.token_counts,
        }

        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        print(f"Index saved to {os.path.abspath(path)}")

    def load_index(self, path="index_data.json"):

        if not os.path.exists(path):
            print("No saved index found. Run build_index() first.")
            return False

        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        self.docs = data.get("docs", {})
        self.index = defaultdict(list, {term: postings for term, postings in data.get("index", {}).items()})
        self.num_docs = data.get("num_docs", 0)
        self.token_counts = data.get("token_counts", {})

        print(f"Loaded index from {os.path.abspath(path)} ({len(self.index)} terms, {self.num_docs} docs)")
        return True




