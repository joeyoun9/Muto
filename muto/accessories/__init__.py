'''
This will hold useful side functions and methods
'''
import time, calendar
def s2t(str, fmt):
    '''
    Use functions to grab an epoch time from a 
    string formatted by fmt. Formatstrings can be 
    identified in the strtotime module/guide
    
    str = time string
    fmt = format string
    '''
    return calendar.timegm(time.strptime(str, fmt))
