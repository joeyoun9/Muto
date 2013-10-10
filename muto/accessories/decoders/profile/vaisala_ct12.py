'''
Created on Jan 27, 2013

@author: jyoung
'''
from numpy import exp, zeros, float32, array, arange, uint8

def read(ob, doFilter=True):
    """
        Process a CT12 data message. This will read both the message and 
        status information, and output a dict of the values similar to the 
        method for reading CL31 messages.
    """
    # check if the text you have been given is a proper ct12 message:

    dls = ob.split("\n")
    dl = []
    if len(dls) < 15 or ":" in ob:
        return False

    # purify the ob so that i dont have to care about formatting
    for l in dls:
        if len(l.strip()) > 3:
            # then i guess there is content
            dl.append(l)  # NOT STRIP!!
    del dls

    'cloud line'
    cl = dl[0].strip()
    'info line'
    il = dl[1].strip()
    'that x for x is NECESSARY.'
    data = [cl[0:1], cl[4:9], cl[10:15], cl[16:21], cl[22:27]] + [x for x in cl[28:39]]
    data += [ il[0:1], il[2:3], il[4:8], il[9:12], il[13:16], il[17:20],
             il[21:25], il[26:31], il[32:34], il[35:37], ]

    'Translate these values into a single numpy array'
    def to(x):
        if '/' in x: return 0.
        return float(x)
    status = array(map(to, data), dtype=float32)
    if doFilter:
        values = zeros((250), dtype=float32)
    else:
        values = zeros((250), dtype=uint8)
    '250 is the only length this msg can be'
    'Join the split up data lines back together to read in more easily and reformat'
    string = '\n'.join(dl[2:]).replace(' ', '0').replace("\n", "").replace("\r", "").strip()
    index = 0
    for i in xrange(2, len(string[2:]) + 1, 2):
        if i % 42 == 0: continue  # height indices
        if doFilter:
            val = (int(string[i:i + 2], 16) - 1) / 50.  # compute the SS value...
        else:
            val = int(string[i:i + 2], 16)
        values[index] = val
        index += 1
    if doFilter:
        values = exp(values)
    out = {
           'height':arange(250) * 15,
           'bs':values,
           'status':status,
           }  # 15 m vertical resolution is the only reportable form!

    return out

def decode_hex_string(string):
    '''
    This is a compacted code for simply reading the hexadecimal string provided
    which comes from a CT12k. This should be a 13-lines string with height 
    indices and nothing else.
    
    This is also used in the computation of the full observation value. 
    
    Parameters
    ----------
    string: str
        the string which will be read for individual 2-digit
        hexadeceimal values. Standard extreme-value logic is applied.
        
    Returns
    -------
    numpy array 
    '''
    pass
    'This method will not be implemented until a reason to do so exists.'

