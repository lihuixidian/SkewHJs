
import numpy as np
#import matplotlib.pyplot as plt
from scipy import special
import sys
import logging
import os
from zipf_generator import *


"""
{
How to use:
    cd zipf
    python main.py + argvs
}
{
Parameters
    argv[1] : num_row_1
    argv[2] : a_1
    argv[3] : num_row_2 
    args[4] : a_2
    args[5] : upper_bound
    args[6] : offset(optional, by default 0)
}
Data scope: [0, upper_bound]
"""

logging.basicConfig(level=logging.NOTSET)

def save_plot(raw_data, a, write_dir):
    plt.clf()
    count, bins, ignored = plt.hist(raw_data[raw_data<50], 50)
    x = np.arange(1., 50.)
    y = (x**(-a) / special.zetac(a)) 
    
    plt.plot(x, y/max(y), linewidth=2, color='r')
    plt.title("num_row : {}".format(len(raw_data)))
    plt.savefig(write_dir + "{}_{}.jpg".format(str(len(raw_data)),str(a)))
    


def write_data(raw_data, a, num_row, out_dir):
    csv_name = "t{}_{}".format(str(num_row),a).replace('.','_')
    with open (out_dir + csv_name + ".csv", 'w') as f:
        for i, x in enumerate(raw_data):
            f.write("%s,%s"%(str(x), str(i)))
            f.write('\n')


def write_skew(raw_data, a, threshold, write_dir):
    skew_name = "t{}_{}".format(str(len(raw_data)),a).replace('.','_')
    threshold_count = threshold * len(raw_data)
    map = dict()
    res = dict()
    for x in raw_data:
        map[x] = map.get(x,0) + 1

    for key in map.keys():
        if map[key] > threshold_count:
            res[key] = map[key]

    with open (write_dir + skew_name + ".skew", 'w') as f:
        for skew_data in res.keys():
            f.write("%s,%s"%(str(skew_data), str(map[skew_data])))
            f.write('\n')

    

if __name__ == "__main__":
    out_dir = os.getcwd() + "/csv/"
    # get argvs
    num_row_1 = int(sys.argv[1])   
    a_1 = float(sys.argv[2])
    num_row_2 = int(sys.argv[3])
    a_2 = float(sys.argv[4])
    upper_bound = int(sys.argv[5])
    if len(sys.argv) <= 6:
        offset = 0
    else:
        offset =  sys.argv[6]

    small_rows = min(num_row_1, num_row_2)
    big_rows = max(num_row_1, num_row_2)

    small_a = a_1 if small_rows == num_row_1 else a_2
    big_a = a_1 if big_rows == num_row_1 else a_2

    small_tname = "t{}_{}".format(str(small_rows), str(small_a)).replace('.','_')
    big_tname = "t{}_{}".format(str(big_rows), str(big_a)).replace('.','_')
    folder_name = "%s_%s"%(small_tname, big_tname)
    write_dir = out_dir + folder_name + '/'
    if not os.path.exists(write_dir):
        os.mkdir(write_dir)
    


    bzg_factory_instance = bzg_factory()
    bzg_1 = bzg_factory_instance.create(a_1,upper_bound, num_row_1)
    bzg_2 = bzg_factory_instance.create(a_2,upper_bound,num_row_2)

    raw_data_1 = bzg_1.generate()
    raw_data_2 = bzg_2.generate()

    write_skew(raw_data_1,a_1, 0.001,write_dir)
    write_skew(raw_data_2,a_2, 0.001,write_dir)

    logging.info("writing data_1, num_row = {}...".format(num_row_1))
    write_data(raw_data_1, a_1,num_row_1, write_dir)

    logging.info("writing data_2, num_row = {}...".format(num_row_2))
    write_data(raw_data_2, a_2,num_row_2, write_dir)

 #   save_plot(raw_data_1, a_1, write_dir)
  #  save_plot(raw_data_2, a_2, write_dir)

            
    
