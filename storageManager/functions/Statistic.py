class Statistic:
    def __init__(self, n_r, b_r, l_r, f_r, V_a_r):
        self.n_r = n_r # Number of tuples (rows)
        self.b_r = b_r # Number of blocks
        self.l_r = l_r # Tuple size
        self.f_r = f_r # Blocking factor
        self.V_a_r = V_a_r # Distinct values in columns

    def __str__(self):  
        return (
            f"Statistic:\n"
            f"  Number of Tuples (n_r): {self.n_r}\n"
            f"  Number of Blocks (b_r): {self.b_r}\n"
            f"  Tuple Size (l_r): {self.l_r} bytes\n"
            f"  Blocking Factor (f_r): {self.f_r}\n"
            f"  Distinct Values (V(A,r)):\n    {self.V_a_r}"
        )
