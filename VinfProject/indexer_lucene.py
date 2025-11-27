import csv
import lucene
from java.nio.file import Paths
from org.apache.lucene.store import FSDirectory
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.index import IndexWriter, IndexWriterConfig
from org.apache.lucene.document import Document, TextField, StringField, Field
from org.apache.lucene.queryparser.classic import QueryParser
from org.apache.lucene.search import IndexSearcher, TermQuery, BooleanQuery, BooleanClause, PhraseQuery, FuzzyQuery
from org.apache.lucene.index import DirectoryReader
from org.apache.lucene.index import Term


class LuceneIndexer:
    def __init__(self, index_path="lucene_index"):
        lucene.initVM(vmargs=['-Djava.awt.headless=true'])
        self.index_path = index_path
        self.directory = FSDirectory.open(Paths.get(index_path))
        self.analyzer = StandardAnalyzer()

    def create_writer(self):
        config = IndexWriterConfig(self.analyzer)
        return IndexWriter(self.directory, config)


    def doc_to_dict(self, doc):
        """Convert a Lucene Document to a Python dictionary"""
        result = {}
        for field in doc.getFields():
            field_name = field.name()
            field_value = field.stringValue()
            result[field_name] = field_value
        return result


    def load_from_tsv(self, path):
        self.loaded_docs = []

        with open(path, encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter="\t")
            fields = reader.fieldnames

            for row in reader:
                parts = []
                for field in fields:
                    val = str(row.get(field, "")).strip()
                    if val:
                        parts.append(f"{field}: {val}")

                text = " ".join(parts)
                self.loaded_docs.append((row, text))

        print(f"Loaded {len(self.loaded_docs)} rows from {path}")

   
    def build_index(self):
        writer = self.create_writer()
        count = 0

        for row, text in self.loaded_docs:
            doc = Document()

            for k, v in row.items():
                doc.add(StringField(k, v, Field.Store.YES))

            doc.add(TextField("_fulltext", text, Field.Store.YES))

            writer.addDocument(doc)
            count += 1

        writer.commit()
        writer.close()
        print(f"Indexed {count} documents")

   
    def get_searcher(self):
        reader = DirectoryReader.open(self.directory)
        return IndexSearcher(reader)

    def search_keyword(self, query_text, top_n=5):
        parser = QueryParser("_fulltext", self.analyzer)
        query = parser.parse(query_text)
        searcher = self.get_searcher()

        hits = searcher.search(query, top_n).scoreDocs
        results = []
        for hit in hits:
            doc = searcher.doc(hit.doc)
            results.append((hit.score, self.doc_to_dict(doc)))
        return results

    def search_phrase(self, phrase, top_n=5):
        terms = phrase.lower().split()
        pq = PhraseQuery.Builder()

        for term in terms:
            pq.add(Term("_fulltext", term))

        query = pq.build()
        searcher = self.get_searcher()
        hits = searcher.search(query, top_n).scoreDocs
        return [(hit.score, self.doc_to_dict(searcher.doc(hit.doc))) for hit in hits]

    def search_fuzzy(self, term, top_n=5):
        fq = FuzzyQuery(Term("_fulltext", term), 2)
        searcher = self.get_searcher()
        hits = searcher.search(fq, top_n).scoreDocs
        return [(hit.score, self.doc_to_dict(searcher.doc(hit.doc))) for hit in hits]

    def search_boolean_and(self, t1, t2, top_n=5):
        bq = BooleanQuery.Builder()
        bq.add(TermQuery(Term("_fulltext", t1)), BooleanClause.Occur.MUST)
        bq.add(TermQuery(Term("_fulltext", t2)), BooleanClause.Occur.MUST)

        query = bq.build()
        searcher = self.get_searcher()
        hits = searcher.search(query, top_n).scoreDocs
        return [(hit.score, self.doc_to_dict(searcher.doc(hit.doc))) for hit in hits]



def main():
    print("Initializing Lucene Indexer...")

    indexer = LuceneIndexer(index_path="lucene_index")

    tsv_path = "VinfProject/data.csv"   
    indexer.load_from_tsv(tsv_path)

    indexer.build_index()

    print("\nKeyword Search:")
    for score, doc in indexer.search_keyword("trnava"):
        print(f"Score: {score:.4f}")
        print(f"Document: {doc}")
        print("-" * 50)

    print("\nPhrase Search:")
    for score, doc in indexer.search_phrase("rd na predaj lokalita spiegelsalle"):
        print(f"Score: {score:.4f}")
        print(f"Document: {doc}")
        print("-" * 50)

    print("\nFuzzy Search:")
    for score, doc in indexer.search_fuzzy("tranva"):
        print(f"Score: {score:.4f}")
        print(f"Document: {doc}")
        print("-" * 50)

    print("\nBoolean AND Search: ")
    for score, doc in indexer.search_boolean_and("trnava", "house"):

        print(f"Score: {score:.4f}")
        print(f"Document: {doc}")
        print("-" * 50)


if __name__ == "__main__":
    main()