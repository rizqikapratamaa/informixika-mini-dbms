from datetime import datetime
from pathlib import Path
import os

activefile = None
last_checkpoint = None

# contoh log item 
logitem = "Timestamp: " + str(datetime.now()) +  " \nLog: (T0,A,2000)\nLog: (T0,B,1000)\nLog: (T1,C,2000)\n"

def __save_checkpoint():
    # Sebenernya cuman set activefile
    global activefile
    global last_checkpoint
    
    last_checkpoint = datetime.now()
    activefile = None
    pass

def maketime():
    now = datetime.now()
    Y = str(now.year).zfill(4)
    M = str(now.month).zfill(2)
    D = str(now.day).zfill(2)
    h = str(now.hour).zfill(2)
    m = str(now.minute).zfill(2)
    s = str(now.second).zfill(2)
    return Y + M + D + h + m + s

def makefile_list():
    
    # Get the directory of the current script
    base_dir = Path(__file__).resolve().parent
    log_dir = base_dir / '../log'

    # Ensure the log directory exists
    log_dir.mkdir(parents=True, exist_ok=True)
    
    log_files = [f for f in os.listdir(log_dir) if f.startswith('logfile_')]
    return log_files

#tulis log ke activefile, if active_file is none, buka/buat file baru,
def write_log(logitem):
    global activefile
    
    # Get the directory of the current script
    base_dir = Path(__file__).resolve().parent
    log_dir = base_dir / '../log'

    # Ensure the log directory exists
    log_dir.mkdir(parents=True, exist_ok=True)
    
    if activefile == None:
        time = maketime()
        activefile = log_dir / f'logfile_{time}.txt'
        with open(activefile, 'w') as f:
            f.write(logitem)
    else:
        with open(activefile, 'a') as f:
            f.write('\n' + logitem)
    pass

def read_log(filename):
    # Get the directory of the current script
    base_dir = Path(__file__).resolve().parent
    log_dir = base_dir / '../log' / filename
    
    with open(log_dir, 'r') as f:
        return f.readlines()


#class untuk membantu iterasi log
class log_iterator:
    def __init__(self, timestamp):
        self.filelist = makefile_list()
        self.filenumber = len(self.filelist)-1
        self.curtime = timestamp
        self.logbuffer = read_log(self.filelist[self.filenumber])
        self.line = len(self.logbuffer)-1
        pass
    
    #returns the next log in log
    def next(self):
        if self.filenumber == -1:
            self.logbuffer= None
        if self.filenumber == len(self.filelist):
            self.filenumber = len(self.logbuffer)-1
            self.logbuffer = read_log(self.filelist[self.filenumber])
            self.line = len(self.logbuffer)-1

        # Kalau misalnya langkah masih di dalam log file sekarang/checkpoint ini
        if True and self.logbuffer is not None:
            if self.line == -1 :
                self.logbuffer = read_log(self.filelist[self.filenumber])
                self.line = len(self.logbuffer)-1
            line = self.line
            self.line = self.line -1
            if self.line == -1 :
                self.filenumber = self.filenumber - 1
            return self.logbuffer[line].strip("\n")
        # Kalau langkah berikutnya ada di checkpoint sebelumnya, bisa pake last_checkpoint
        else:
            return "no logfile available"
            #buka file sebelumnya, masukin log_buffer

    def previous(self):
        if self.filenumber == len(self.filelist):
            self.logbuffer= None
        if self.filenumber == -1:
            self.filenumber = 0
            self.logbuffer = read_log(self.filelist[self.filenumber])
            self.line = 0
        # Kalau misalnya langkah masih di dalam log file sekarang/checkpoint ini
        if True and self.logbuffer is not None:
            if self.line == len(self.logbuffer) :
                print(self.filenumber, len(self.filelist))
                self.logbuffer = read_log(self.filelist[self.filenumber])
                self.line = 0
            line = self.line
            self.line = self.line +1
            if self.line == len(self.logbuffer) :
                self.filenumber = self.filenumber + 1
            return self.logbuffer[line].strip("\n")
        # Kalau langkah berikutnya ada di checkpoint sebelumnya, bisa pake last_checkpoint
        else:
            return "no logfile available"