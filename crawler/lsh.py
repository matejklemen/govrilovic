import numpy as np


# close enough
SENTINEL_INFINITY = 2 ** 31 - 1


class LocalitySensitiveHashing:
    def __init__(self, vocab,
                 num_bands,
                 num_hash=None,
                 hash_funcs=None,
                 band_hasher=None,
                 repr_func=None,
                 random_state=None):
        """ Implementation of locality sensitive hashing for document deduplication.

        How it works:
        (1.) Compute a set of kmers (words/...) representation of document based on a vocabulary of
        words.
        (2.) Change this sparse representation into a dense one using `num_hash` hash functions
        (specified by `hash_funcs`).
        (3.) Divide dense representation into equally sized bands (`num_bands` of these) and hash
        each band using `band_hasher` to obtain the final signature.
        (4.) This final signature is then compared between documents to produce candidates for
        duplicates. These should then be checked using some similarity measure (note that the
        number of these candidates should be small compared to the database size).

        A more thorough description of the algorithm can be found in [1].

        Parameters
        ----------
        vocab: list of str
            List of words in our vocabulary

        num_bands: int
            Number of "groups" that the dense representation is split into. Needs to be a
            divisor of `num_hash`

        num_hash: int, optional
            Number of hash functions to be used. Only required if `hash_funcs` is not provided

        hash_funcs: list of functions, optional
            Hash functions to be used to obtain dense representation of documents

        band_hasher: function, optional
            Hash function to be used for hashing bands of the dense representation

        repr_func: function, optional
            Function to turn document into smaller units. Takes in document and should return
            an iterable of units (e.g. list of words or kmers)

        random_state: int, optional
            Random state so that random things in this algorithm are reproducible. Currently
            unused


        References
        ----------
        [1] Leskovec, Jure, Anand Rajaraman, and Jeffrey David Ullman. Mining of massive datasets.
            Cambridge university press, 2014.
        """
        # create a mapping from words to indices
        self.vocab = {item: idx for idx, item in enumerate(vocab)}
        vocab_size = len(vocab)

        if not hash_funcs:
            if num_hash is None:
                raise ValueError("If hash functions are not provided, number of hash functions "
                                 "to be used (num_hash) should be given")

            # TODO: better default hash functions (e.g. md5(idx) XOR random number)
            # construct random hashing functions for signature computation
            self.hash_funcs = [(lambda idx: np.random.randint(1, vocab_size + 1) * idx % vocab_size)
                               for _ in range(num_hash)]
        else:
            self.hash_funcs = hash_funcs

        self.num_hash = len(self.hash_funcs)
        if self.num_hash % num_bands > 0:
            # also handles the case that `num_bands` needs to be <= number of hash functions
            raise ValueError("Dense vector representation needs to be equally divisible between "
                             "bands ('num_hash' needs to be a multiple of 'num_bands')")

        self.num_bands = num_bands

        # default to single characters
        self.repr_func = repr_func if repr_func is not None else (lambda doc: set(doc))

        if band_hasher is None:
            # vectorized hashing of bands in a dense representation of the document
            self.band_hash_func = lambda arr: \
                np.bitwise_xor(np.sum(np.reshape(arr, [self.num_bands, -1]), axis=1), 6)
        else:
            self.band_hash_func = band_hasher

    def get_repr(self, doc):
        """ Converts document into indices (ints) according to chosen vocabulary and representation
        function.

        Parameters
        ----------
        doc: str
            Query document

        Returns
        -------
        set:
            Unique elements in the document, encoded as ints
        """
        encoded_repr = set()
        for el in self.repr_func(doc):
            encoded = self.vocab.get(el, None)
            if encoded is not None:
                encoded_repr.add(encoded)

        return encoded_repr

    def compute_dense_repr(self, encoded_doc):
        """ Converts sparsely encoded document (should be encoded using `self.get_repr(...)`) into
        a dense vector using hashing magic.

        Parameters
        ----------
        encoded_doc: iterable
            Encoded document

        Returns
        -------
        np.array:
            Dense vector representation
        """
        dense_repr = np.zeros(self.num_hash, dtype=np.int32)
        dense_repr[:] = SENTINEL_INFINITY

        # for each word present in the document, compute the hash functions of the current word
        # and update the dense representation of the document accordingly
        for idx_word in encoded_doc:
            for idx_hash_func in range(self.num_hash):
                hashed_idx_word = self.hash_funcs[idx_hash_func](idx_word)
                if hashed_idx_word < dense_repr[idx_hash_func]:
                    dense_repr[idx_hash_func] = hashed_idx_word

        return dense_repr

    def compute_signature(self, doc):
        """ Computes a signature for a document, which is then used to query the database to
        select CANDIDATES for duplicates.

        Parameters
        ----------
        doc: str
            Query document

        Returns
        -------
        np.array:
            Signature for the document
        """
        encoded_doc = self.get_repr(doc)
        dense_repr = self.compute_dense_repr(encoded_doc)
        return self.band_hash_func(dense_repr)


if __name__ == "__main__":
    # sample use-case from the book, noted above
    s1 = "ad"
    s2 = "c"
    s3 = "bde"
    s4 = "acd"

    lsh_obj = LocalitySensitiveHashing(vocab=["a", "b", "c", "d", "e"],
                                       num_hash=2,
                                       hash_funcs=[
                                           lambda idx: (idx + 1) % 5,
                                           lambda idx: (3 * idx + 1) % 5
                                       ],
                                       num_bands=2)

    # `s1` and `s4` should have same representation with a high probability because they are almost
    # duplicates
    print(lsh_obj.compute_signature(s1))
    print(lsh_obj.compute_signature(s2))
    print(lsh_obj.compute_signature(s3))
    print(lsh_obj.compute_signature(s4))
