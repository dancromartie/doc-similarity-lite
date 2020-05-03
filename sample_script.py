import random
import re

import similarity_lite

stop_words = ["the", "a"]
tokenizer_func = lambda x: x.strip().split()


def main():
    similarity_obj = similarity_lite.SimilarityLite(
        db_path='/tmp/simlite.db',
        stop_words=stop_words,
        tokenizer_func=tokenizer_func,
        idf_cutoff=.2,
        delete_existing_table=True
    )

    sentences = [
        "apple sauce is a watery mix of apples and other stuff",
        "apple juice is like water but made from apples",
        "pie can be made from apple, pumpkin, cherry, etc",
        "apple pie is considered very american",
        "apple is a very rich company",
        "a pumpkin is a kind of gourd that grows in the ground",
        "smashing a pumpkin is a beloved activity in the country",
        "pie can refer to pizza or the dessert",
        "some say pizza is better than apple pie, because pie is too rich",
        "pizza pie has a very rich history",
    ]

    docs = []
    docs_by_id = {}
    for i, text in enumerate(sentences):
        doc = {"id": str(i), "doc_text": text}
        docs.append(doc)
        docs_by_id[str(i)] = doc

    similarity_obj.add_or_update_docs(docs, update_stats=True)

    search_query = "rich apple pie"
    similar_docs = similarity_obj.get_similar_docs(search_query)
    print("***********SEARCHED***************")
    print(search_query)
    print("***********RANKED RESULTS***************")
    for id_, similarity in similar_docs[0:10]:
        print(docs_by_id[id_], similarity)


if __name__ == "__main__":
    main()
