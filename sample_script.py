import random
import re

import similarity_lite

stop_words = ["the", "a"]
tokenizer_func = lambda x: x["doc_text"].strip().split()

# For fake doc generation
candidate_words = """
    Amdahl's law is often used in parallel computing to predict the theoretical
    speedup when using multiple processors. For example, if a program needs 20
    hours using a single processor core, and a particular part of the program which
    takes one hour to execute cannot be parallelized, while the remaining 19 hours
    (p = 0.95) of execution time can be parallelized, then regardless of how many
    processors are devoted to a parallelized execution of this program, the minimum
    execution time cannot be less than that critical one hour. Hence, the
    theoretical speedup is limited to at most 20 times (1/(1 - p) = 20). For this
    reason parallel computing is relevant only for a low number of processors and
    very parallelizable programs
""".lower().split()
candidate_words = [c for c in candidate_words if re.match('^[a-zA-Z0-9]+$', c)]


def random_fake_doc_text():
    doc_words = [random.choice(candidate_words) for _ in xrange(5)]
    doc_words.sort()
    doc_text = " ".join(doc_words)
    return doc_text
    

def main():
    similarity_obj = similarity_lite.SimilarityLite(
        db_path='/tmp/simlite.db',
        stop_words=stop_words,
        tokenizer_func=tokenizer_func,
        idf_cutoff=.2,
        delete_existing_table=True
    )

    docs = []
    docs_by_id = {}
    num_docs = 100 * 1000
    for i in xrange(num_docs):
        doc = {"id": str(i), "doc_text": random_fake_doc_text()}
        docs.append(doc)
        docs_by_id[str(i)] = doc

    similarity_obj.add_or_update_docs(docs, update_everything=True)

    similar_docs = similarity_obj.get_similar_docs("500", num_results=num_docs)
    print "***********SEARCHED***************"
    print docs_by_id["500"]
    print "***********TOP RESULTS***************"
    for result in similar_docs[0:10]:
        print docs_by_id[result[0]], result[1]
    print "***********BOTTOM RESULTS***************"
    for result in similar_docs[-10:]:
        print docs_by_id[result[0]], result[1]


if __name__ == "__main__":
    main()
