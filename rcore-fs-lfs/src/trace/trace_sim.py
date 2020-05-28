import os, sys

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

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: %s trace.txt" % (sys.argv[0]))
        sys.exit(0)
    with open(sys.argv[1]) as tracef:
        disk = Disk()
        for line in tracef:
            arr = line.split('\t')
            addr = int(arr[1])
            length = int(arr[3])
            is_write = bool(int(arr[4]))
            # if is_write:
            disk.issue(addr, length, is_write)

        disk.print()