import csv
import lucene
from java.nio.file import Paths
from org.apache.lucene.store import FSDirectory
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.index import IndexWriter, IndexWriterConfig
from org.apache.lucene.document import Document, TextField, StringField, Field, LongPoint, StoredField
from org.apache.lucene.queryparser.classic import QueryParser
from org.apache.lucene.search import IndexSearcher, TermQuery, BooleanQuery, BooleanClause, PhraseQuery, FuzzyQuery
from org.apache.lucene.index import DirectoryReader
from org.apache.lucene.index import Term
import os
import shutil
try:
    from org.apache.lucene.queries.intervals import Intervals, IntervalsSource
    from org.apache.lucene.search import IntervalQuery
    INTERVALS_AVAILABLE = True
except ImportError:
    INTERVALS_AVAILABLE = False
    print("Warning: Interval queries not available in this Lucene version")


class LuceneIndexer:
    def __init__(self, index_path="lucene_index"):
        lucene.initVM(vmargs=['-Djava.awt.headless=true'])
        self.index_path = index_path
        self.directory = FSDirectory.open(Paths.get(index_path))
        self.analyzer = StandardAnalyzer()
        
        self.boolean_fields = [
            'train_station',
            'sports_facilities'
        ]

    def create_writer(self):
        config = IndexWriterConfig(self.analyzer)
        return IndexWriter(self.directory, config)

    def doc_to_dict(self, doc):
        result = {}
        for field in doc.getFields():
            field_name = field.name()
            field_value = field.stringValue()
            result[field_name] = field_value
        return result

    def normalize_slovak(self, text):
        normalized = text
        replacements = {
            'á': 'a', 'ä': 'a', 'é': 'e', 'í': 'i', 
            'ó': 'o', 'ô': 'o', 'ú': 'u', 'ý': 'y',
            'č': 'c', 'ď': 'd', 'ľ': 'l', 'ň': 'n',
            'ŕ': 'r', 'š': 's', 'ť': 't', 'ž': 'z'
        }
        for sk_char, en_char in replacements.items():
            normalized = normalized.replace(sk_char, en_char)
        
        return f"{text} {normalized}"  
    def load_from_tsv(self, path, indexable_fields=None):
        self.loaded_docs = []
        
        with open(path, encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter="\t")
            
            for row in reader:
                cleaned_row = {}
                for field, val in row.items():
                    if val in ['NaN', 'nan', '', None]:
                        cleaned_row[field] = ''
                    else:
                        cleaned_row[field] = str(val).strip()
                
                parts = []
                for field in indexable_fields or cleaned_row.keys():
                    val = cleaned_row.get(field, '')
                    if val and val not in ['NaN', 'nan']:
                        if field in self.boolean_fields:
                            if val.lower() == 'yes':
                                parts.append(field.replace('_', ' '))
                        else:
                            parts.append(f"{field}: {val}")
                
                text = " ".join(parts)
                text = self.normalize_slovak(text)
                
                self.loaded_docs.append((cleaned_row, text))
   
    def build_index(self):
        writer = self.create_writer()
        count = 0

        for row, text in self.loaded_docs:
            doc = Document()

            for k, v in row.items():
                doc.add(StringField(k, v, Field.Store.YES))
                
                if k in ['price', 'area', 'rooms', 'year_built', 'price_per_m2', 
                         'rent_price', 'deposit', 'population', 'elevation_m']:
                    try:
                        clean_val = v.replace(',', '').replace('€', '').replace(' ', '').strip()
                        if clean_val:
                            numeric_val = float(clean_val)
                            doc.add(LongPoint(k + "_num", int(numeric_val)))
                            doc.add(StoredField(k + "_num", int(numeric_val)))
                    except:
                        pass

            doc.add(TextField("_fulltext", text, Field.Store.YES))

            writer.addDocument(doc)
            count += 1

        writer.commit()
        writer.close()
        print(f"Indexed {count} documents")

    def get_searcher(self):
        reader = DirectoryReader.open(self.directory)
        return IndexSearcher(reader)

    def search(self, query_text, top_n=10, fuzzy_edits=1):
        
        terms = query_text.lower().split()
        bq = BooleanQuery.Builder()
        
        for term in terms:
            term_bq = BooleanQuery.Builder()
            
            term_bq.add(TermQuery(Term("_fulltext", term)), BooleanClause.Occur.SHOULD)
            
            if len(term) > 3:  
                fq = FuzzyQuery(Term("_fulltext", term), fuzzy_edits)
                term_bq.add(fq, BooleanClause.Occur.SHOULD)
            
            bq.add(term_bq.build(), BooleanClause.Occur.MUST)
        
        query = bq.build()
        searcher = self.get_searcher()
        hits = searcher.search(query, top_n).scoreDocs
        return [(hit.score, self.doc_to_dict(searcher.doc(hit.doc))) for hit in hits]

    def search_phrase_aware(self, query_text, top_n=10, fuzzy_edits=1):
        
        terms = query_text.lower().split()
        main_bq = BooleanQuery.Builder()
        
        for term in terms:
            term_bq = BooleanQuery.Builder()
            term_bq.add(TermQuery(Term("_fulltext", term)), BooleanClause.Occur.SHOULD)
            
            if len(term) > 3:
                fq = FuzzyQuery(Term("_fulltext", term), fuzzy_edits)
                term_bq.add(fq, BooleanClause.Occur.SHOULD)
            
            main_bq.add(term_bq.build(), BooleanClause.Occur.MUST)
        
        if len(terms) > 1:
            pq = PhraseQuery.Builder()
            pq.setSlop(5)  
            for term in terms:
                pq.add(Term("_fulltext", term))
            
            main_bq.add(pq.build(), BooleanClause.Occur.SHOULD)
        
        query = main_bq.build()
        searcher = self.get_searcher()
        hits = searcher.search(query, top_n).scoreDocs
        return [(hit.score, self.doc_to_dict(searcher.doc(hit.doc))) for hit in hits]

    def search_with_filters(self, query_text, filters=None, top_n=10, fuzzy_edits=1):
        
        bq = BooleanQuery.Builder()
        
        if query_text:
            terms = query_text.lower().split()
            
            for term in terms:
                term_bq = BooleanQuery.Builder()
                term_bq.add(TermQuery(Term("_fulltext", term)), BooleanClause.Occur.SHOULD)
                
                if len(term) > 3:
                    fq = FuzzyQuery(Term("_fulltext", term), fuzzy_edits)
                    term_bq.add(fq, BooleanClause.Occur.SHOULD)
                
                bq.add(term_bq.build(), BooleanClause.Occur.MUST)
        
        if filters:
            for field, value in filters.items():
                if field.endswith('_range') and isinstance(value, tuple):
                    base_field = field.replace('_range', '')
                    range_query = LongPoint.newRangeQuery(
                        base_field + "_num", 
                        int(value[0]), 
                        int(value[1])
                    )
                    bq.add(range_query, BooleanClause.Occur.MUST)
                else:
                    bq.add(TermQuery(Term(field, str(value).lower())), BooleanClause.Occur.MUST)
        
        query = bq.build()
        searcher = self.get_searcher()
        hits = searcher.search(query, top_n).scoreDocs
        return [(hit.score, self.doc_to_dict(searcher.doc(hit.doc))) for hit in hits]

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

    def search_phrase(self, phrase, slop=0, top_n=5):
        terms = phrase.lower().split()
        pq = PhraseQuery.Builder()
        pq.setSlop(slop)

        for term in terms:
            pq.add(Term("_fulltext", term))

        query = pq.build()
        searcher = self.get_searcher()
        hits = searcher.search(query, top_n).scoreDocs
        return [(hit.score, self.doc_to_dict(searcher.doc(hit.doc))) for hit in hits]

    def search_fuzzy(self, term, max_edits=2, top_n=5):
        fq = FuzzyQuery(Term("_fulltext", term), max_edits)
        searcher = self.get_searcher()
        hits = searcher.search(fq, top_n).scoreDocs
        return [(hit.score, self.doc_to_dict(searcher.doc(hit.doc))) for hit in hits]

    def search_range(self, field, min_val, max_val, top_n=10):
        query = LongPoint.newRangeQuery(field + "_num", int(min_val), int(max_val))
        searcher = self.get_searcher()
        hits = searcher.search(query, top_n).scoreDocs
        return [(hit.score, self.doc_to_dict(searcher.doc(hit.doc))) for hit in hits]


def print_property_details(score, doc):
    print("="*80)
    print(f"SCORE: {score:.4f}")
    print("="*80)
    
    print("\nBASIC INFORMATION:")
    print(f"  Title: {doc.get('title', 'N/A')}")
    print(f"  Property Type: {doc.get('property_type', 'N/A')}")
    print(f"  Offer Type: {doc.get('offer_type', 'N/A')}")
    print(f"  Condition: {doc.get('condition', 'N/A')}")
    
    print("\nLOCATION:")
    print(f"  City: {doc.get('city', 'N/A')}")
    print(f"  Address: {doc.get('address', 'N/A')}")
    print(f"  Country: {doc.get('source_country', 'N/A')}")
    
    print("\nPRICE & SIZE:")
    print(f"  Price: {doc.get('price', 'N/A')}")
    print(f"  Rent Price: {doc.get('rent_price', 'N/A')}")
    print(f"  Deposit: {doc.get('deposit', 'N/A')}")
    print(f"  Area: {doc.get('area', 'N/A')} m²")
    print(f"  Price per m²: {doc.get('price_per_m2', 'N/A')}")
    print(f"  Rooms: {doc.get('rooms', 'N/A')}")
    
    print("\nPROPERTY DETAILS:")
    print(f"  Year Built: {doc.get('year_built', 'N/A')}")
    print(f"  Energy Class: {doc.get('energy_class', 'N/A')}")
    print(f"  Availability: {doc.get('availability', 'N/A')}")
    
    print("\nFEATURES:")
    print(f"  Train Station: {doc.get('train_station', 'N/A')}")
    print(f"  Sports Facilities: {doc.get('sports_facilities', 'N/A')}")
    
    print("\nNEIGHBORHOOD:")
    print(f"  Population: {doc.get('population', 'N/A')}")
    print(f"  Elevation: {doc.get('elevation_m', 'N/A')} m")
    print(f"  Universities: {doc.get('universities', 'N/A')}")
    print(f"  Historic Sites: {doc.get('historic_sites', 'N/A')}")
    print(f"  Shopping Centers: {doc.get('shopping_centers', 'N/A')}")
    print(f"  Hospitals: {doc.get('hospitals', 'N/A')}")
    print(f"  Parks: {doc.get('parks', 'N/A')}")
    print(f"  Nicknames: {doc.get('nicknames', 'N/A')}")
    
    print("\n" + "-"*80 + "\n")


def main():
    print("Initializing Lucene Indexer...")
    index_path = "lucene_index"

    if os.path.exists(index_path):
            print(f"Removing old index at {index_path}...")
            shutil.rmtree(index_path)
   
    indexer = LuceneIndexer(index_path="lucene_index")
    indexable_fields = [
        "title", "property_type", "location", "price", 
        "offer_type", "address", "area", "energy_class", "rooms", 
        "year_built", "condition", "price_per_m2", "rent_price", 
        "deposit", "availability", "source_country", "population", 
        "city", "elevation_m", "historic_sites", "nicknames",
        "universities", "train_station", "shopping_centers", "hospitals", 
        "parks", "sports_facilities"
    ]
    tsv_path = "VinfProject/data_deduped.csv"   
    indexer.load_from_tsv(tsv_path, indexable_fields=indexable_fields)

    indexer.build_index()


    # print("\nQuery: buy flat 3 rooms vienna with price between 400000 and 600000")

    # for score, doc in indexer.search_with_filters(
    #     query_text="vienna flat 3 rooms buy",
    #     filters={
    #         'price_range': (0, 1500000),
    #     },
    #     top_n=5
    # ):
    #     print_property_details(score, doc)
   

    print("\nQuery: flat rent bratislava 3 rooms")
    for score, doc in indexer.search_google_style("flat rent bratislava 3 rooms", top_n=5):
        print_property_details(score, doc)

    print("\nQuery: house train station buy")
    for score, doc in indexer.search_google_style("house train station buy", top_n=5):
        print_property_details(score, doc)

    print("\nQuery: flat buy beauty on danube")
    for score, doc in indexer.search_google_style("flat buy beauty on danube", top_n=5):
        print_property_details(score, doc)
    
    print("\nQuery: rent flat trnava novostavba")
    for score, doc in indexer.search_google_style("rent flat trnava novostavba", top_n=5):
        print_property_details(score, doc)



if __name__ == "__main__":
    main()