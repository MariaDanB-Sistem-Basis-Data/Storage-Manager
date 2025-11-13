class Statistic:
    def __init__(self, n_r, b_r, l_r, f_r, v_a_r):
        """
        nr: number of tuples in a relation r.
        br: number of blocks containing tuples of r.
        lr: size of tuple of r.
        fr: blocking factor of r – i.e., the number of tuples of r that fit into one block.
        V(A,r): number of distinct values that appear in r for attribute A; same as the size of A(r).
        """
        
        self.n_r = n_r
        self.b_r = b_r
        self.l_r = l_r
        self.f_r = f_r
        self.v_a_r = v_a_r