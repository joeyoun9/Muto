#!/usr/bin/env python
"""
A self contained package which can be used to decode CT12 observations from a 
text file, when the text file includes both control characters and time stamps.

Parameters should be specified at the top of the file, or when executing the 
code in a remote file.

When used in tandem with the Muto package (from github.com/jsyoung/muto) the 
potentially more up-to-date CT12 message reader can be used. Otherwise one is
contained within this code. 

This code does require numpy, and should be run in at least python 2.3, though
earlier operation may be successful.

Note: this assumes that the timestamp for the observation read is AFTER the 
data message that came from the ceilometer. If this is not the case, then this 
code will produce files which are slightly offset in time. 

"""


# 1. Set some important parameters

# specify how many bytes to read at a single time (larger values use more memory)
READ_CHUNK = 100000
# Should an informative header be inserted into the output CSV file?'
PRINT_HEADER = True
# Provide information regarding the timestamp and timezone. Note that
#    TIMEZONE_STRING is appended onto the end of the time stamp.
#    For more information, see in the code itself in the file_read() method.
TIMESTAMP_FORMAT = "%m/%d/%Y %H:%M:%S%Z"
# NOTE: the timestamp format is specified in: http://docs.python.org/2/library/time.html
TIMEZONE_STRING = "UTC"

# Note, you can no longer specify if power is to be saved vs DD. Only power
#    values are computed or saved.


# '2. import numpy for computation and array structures, and othe packages
import numpy as np
import sys, calendar, time
# for outputs we are going to use the standard logging library.
import logging as l
l.basicConfig(level=l.DEBUG, format='%(asctime)s %(levelname)s %(message)s')

#### YOU SHOULD NOT NEED TO MAKE FURTHER MODIFICATIONS FOR BASIC OPERATION ####

# 3. Create a read function to
def read(ob):
    'split the data by line, and create a list called dls with each element being one line'
    dls = ob.split("\n")
    'initialize a blank simple list to hold the corrected lines array'
    dl = []
    'check if the received ob is long enough, if not, then skip, this ob is not formatted right...'
    if len(dls) < 15 or ":" in ob:
        return False
    'To catch all possibilities, fill the dl list with all the lines greater than 3 characters'
    for l in dls:
        if len(l.strip()) > 3:
            'the line appears to be long enough, so append it to our holder list,'
            dl.append(l)
    'now dl has replaced dls as the data lines variable, so remove the old one'
    del dls
    'Now read the status information lines, dl[0] and dl[1] for cloud info, gain, temp, etc'
    '''
    This is done by knowing the points in each line for each inidividual relevant piece of 
    status information. This can be found in the data message reading guide.
    '''
    cl = dl[0].strip()
    il = dl[1].strip()
    status = [cl[0:1], cl[4:9], cl[10:15], cl[16:21], cl[22:27]] + [x for x in cl[28:39]]
    status += [il[0:1], il[2:3], il[4:8], il[9:12], il[13:16], il[17:20], il[21:25], il[26:31],
    il[32:34], il[35:37], ]
    '''
    Then run a quick process that turns this list back into a string, pipe (|) separated, then replace
    some of the overflow/ASOS codes ////, ** with 0's or 9's respectively.
    
    This also creates an efficient numpy array of the values, as a list of floating point numbers
    '''
    status = np.fromstring('|'.join(status).replace("/", '0').replace('*', '9'), sep="|")
    'Create a numpy array to hold the backscattered power values, there are 250 range bins in a profile'
    values = np.zeros((250), dtype=np.float32)
    'Convert the 15 lines of data into one line, and replace spaces with 0s so the splitting works better'
    string = ob[len(dl[0]) + len(dl[1]) + 2:].replace(' ', '0').replace("\n", "").replace("\r", "").strip()
    'create an index to keep track of which bin we are in'
    index = 0
    '''
    Loop through this backscatter string jumping every two characters. 
    Noting that every 42nd character is a height index, and therefore should be skipped.
    
    First, simply the DD value is saved, for computational efficiency. Once the profile is read
    the power is computed from the list of values in a more efficient manner (showing the power of numpy).
    '''
    for i in xrange(2, len(string[2:]) + 1, 2):
        if i % 42 == 0: continue
        'Translate the hexidecimal string to an integer using the hex int function int(x,16)'
        val = int(string[i:i + 2], 16)
        values[index] = val
        index += 1
    'derive the gain value from the status lines before, and the manual, gain = 0 or 2, so let it pick as the index'
    gain = [250., 0, 930.][int(status[-10])]
    'now use the formula to make a vector calculation for power over the entire length'
    values = np.exp((values - 1) / 50) * 0.188 / gain

    'simply return the two arrays, since the outside function knows everything else'
    return {'height':None, 'bs':values, 'status':status}

def s2t(string, time_format):
    '''
    Convert a textual string time representation to a unix epoch time using the standard time format string
    provided. Identical to
    
        >>> calendar.timegm(time.strptime(string,time_format) 
        
    Parameters
    ----------
    string: str
        a time stamp of whatever format, as long as it is information that can be interpreted by the 
        time.strptime() function
        
    
    
    Note
    ----
    Specify UTC in the string, and %Z in the format to ensure the data is properly 
    interpreted as UTC/GMT
    '''
    return calendar.timegm(time.strptime(string, time_format))

def create_csv_headers(bshandle, sthandle):
    '''
    Create header lines for both CSV files craeted by this code by passing
    the file handles to both the BS file and STATUS file. 
    '''
    bsh = 'time,' + ','.join(map(str, range(0, 3750, 15)))
    sth = 'time,cld#,layer 1,bs range 1,layer 2,bs range 2, hardware alarm,'
    sth += 'supply voltage alarm, laser power low, temperature alarm,solar shutter on,'
    sth += 'blower on, heater on,cloud report unit, normalization, fast heater, gain,'
    sth += 'laser pulse frequency,noise RMS,bs sum,internal, laser power, transmitter temp,'
    sth += 'zero offset, internal, extinction coefficient'
    bshandle.write(bsh + '\n')
    sthandle.write(sth + '\n')


def read_file(source, READ_CHUNK, PRINT_HEADER, TIMESTAMP_FORMAT, TIMEZONE_STRING):
    '''
    Read a single CT12 log file, with timestamps formatted in the defined way
    and create two CSV documents from that using the reader if possible. 
    
    All 5 inputs are required for this function. 
    
    '''
    try:
        from muto.accessories.decoders.profile.vaisala_ct12 import read
    except:
        l.warning('The Muto package is not installed/accessible'\
                  + ' using self-contained version instead')
    # open the file for reading.
    readhandle = open(source, 'r')
    # open the two output files for writing, meaning we OVERWRITE AND CLEAR these files.'
    bshandle = open(source + '.backscatter.csv', 'w')
    sthandle = open(source + '.status.csv', 'w')
    # write headers, if we are supposed to'
    if PRINT_HEADER:
        # backscatter hedaer is simply time and then heights in meters'
        create_csv_headers(bshandle, sthandle)
    # define variables for control structures, these are non printing unichar characters'
    B = unichr(002)
    C = unichr(003)
    # begin a controlled infinite loop to read the file in chunks, the lazy way'
    while True:
        # read a single chunk
        data = readhandle.read(READ_CHUNK)
        # if that returned nothing, we are done reading.
        if not data:
            break
        # break out individual obs by splitting the file by the first ob.
        data = data.split(B)
        for ob in data:
            'grab the time by splitting by the second control, taking the end value, and stripping whitespace'
            tmstring = ob.split(C)[-1].strip()
            'now translate the time, and wrap in a try statement, to catch bad times = bad obs'
            try:
                tm = s2t(tmstring[:-4] + TIMEZONE_STRING, TIMESTAMP_FORMAT)
                # 'Note, this will fail in 2100, assumes 012 == 2012 (only uses 2 digit year)'
            except:
                # 'the time was not in the right format
                l.warning('I could not read this timestamp!')
                continue
            # 'now grab just the observation text'
            try:
                out = read(ob.split(C)[0].strip())
            except:
                # 'again, failed to read, = bad ob'
                l.warning('ob read failed. (occassional failure ok, frequent '\
                          + ' failure bad)')
                continue
            if not out:
                continue
            'if we made it to this point, the ob has been read successfully! So, now just save it'
            l.debug('ob: ' + time.ctime(tm) + ' (success)')

            'Now we will write this to the file, using join and map to convert values to strings'
            bs = ','.join(map(str, out['bs']))
            st = ','.join(map(str, out['status']))

            bshandle.write(str(tm) + ',' + bs + '\n')
            sthandle.write(str(tm) + ',' + st + '\n')

    'and close everything'
    readhandle.close()
    bshandle.close()
    sthandle.close()


'3. Now that the functions exist, all we have to do is read the file, find the obs, and save them'
'check for a file provided in the arguments'

if __name__ == "__main__":
    '''
    Execute this code from the terminal, reading data source file provided
    as a string argument to the call.
    '''
    if len(sys.argv) < 2:
        raise ValueError('You must provide an argument.')
    'grab the source file from the terminal arguments'
    source = sys.argv[1]

    l.info('reading: ' + source)
    read_file(source, READ_CHUNK, PRINT_HEADER, TIMESTAMP_FORMAT, TIMEZONE_STRING)
    l.info('Reading Complete')







