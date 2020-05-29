import os, sys
import random

file_threshold = 0.5 # >0.5, create a file

class Disk(object):
    header_addr = 0
    cost_transfer = 1 # per byte
    time_transfer = 0
    cost_header_move = 1 # per byte
    cost_header_move_inv = 1000 # constant for move inversely
    time_header_move = 0
    def issue(self, addr, length, is_write):
        if addr < self.header_addr:
            self.time_header_move += self.cost_header_move_inv
        else:
            self.time_header_move += (addr - self.header_addr) * self.cost_header_move
        self.time_transfer += length * self.cost_transfer
        self.header_addr = addr + length

    def print(self):
        print("time_trans %d\ttime_header_move %d" % (self.time_transfer, self.time_header_move))

def new_file(filename):
    count_lst = [2, 4, 8, 16, 32, 128, 256, 1024, 4096]
    size_idx = random.randint(0, len(count_lst) - 1)
    os.system("dd if=/dev/zero of=" + filename + " bs=1k count=" + str(count_lst[size_idx]))

def new_dir(dirname):
    os.makedirs(dirname)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: %s rootpath" % (sys.argv[0]))
        sys.exit(0)
    N = 5
    dir_set = [sys.argv[1]]
    file_set = []
    os.system("rm -r " + sys.argv[1])
    os.makedirs(sys.argv[1])

    for i in range(N):
        file_p = random.random()
        if file_p > file_threshold:
            parent_id = random.randint(0, len(dir_set) - 1)
            new_file_path = dir_set[parent_id] + '/' + 'file' + str(i)
            new_file(new_file_path)
            file_set.append(new_file_path)
        else:
            parent_id = random.randint(0, len(dir_set) - 1)
            new_dir_path = dir_set[parent_id] + '/' + 'dir' + str(i)
            dir_set.append(new_dir_path)
            new_dir(new_dir_path)

    print(file_set)
    print(dir_set)
