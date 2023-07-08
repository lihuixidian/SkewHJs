import numpy as np
import random
from main import save_plot


#generates boundless zipf data in batches
def batch_generator(a, batch_size=100):
    yield np.random.zipf(a, batch_size)

def sample_batch(batch, lower_bound, upper_bound):
    res = []
    for x in batch:
        if x >= lower_bound and x <= upper_bound:
            res.append(x)
    
    return res

#generates N zipf rows thar range in [lo,up]
class Bounding_zipf_generator:
    def __init__(self, a, upper_bound, N, lower_bound = 0) -> None:
        self.a = a
        self.upper_bound = upper_bound
        self.lower_bound = lower_bound
        self.N = N

    def generate(self):
        data = []
        expect_num = self.N
        cur_count = 0
        while cur_count < expect_num:
            batch = next(batch_generator(self.a))
            temp =  sample_batch(batch, self.lower_bound, self.upper_bound)
            data.extend(temp)
            cur_count += len(temp)

        res = np.array(data[0:self.N])
        hashtable = dict()
        # for i in range (len(res)):
        #     newkey = hashtable.get(res[i], random.randint(self.lower_bound, self.upper_bound))
        #     res[i] = newkey
        return res


class bzg_factory:
    def __init__(self):
        pass

    def create(self, a, upper_bound, N, lower_bound = 0):
        return Bounding_zipf_generator(a, upper_bound, N, lower_bound)


if __name__ == "__main__":
    a = 1.2
    bzg = Bounding_zipf_generator(a,1000,1000)
    res = bzg.generate()
    
    save_plot(res,a )
    



 



