'''
Tools for working with and creating the muto HDF5 file archive format.

Creates a quick and official interface for managing and manipulating 
HDF5 data files. Attempts to be generic, but probably fails at this point.

Much of this is based off my thesis work, except completely using tables 
instead of v arrays.
'''
import tables 
import fcntl
import muto
import numpy as np
import logging as l
l.basicConfig(level=l.DEBUG,
    format='%(asctime)s: %(levelname)s: %(message)s', datefmt='%m/%d/%Y %H:%M:%S')



class h5(object):
    '''
    Class for interacting with HDF5 files in the format created by this class
    '''

    def __init__(self, fname):
        """
        Create the object for interaction by simply providing the location of 
        the HDF5 document
        
        Parameters
        ----------    
        fname : str
            String location of HDF5 source file (.h5,hdf5,etc...)
            
        """
        self.filename = fname
        self.doc = NullDoc()

    def create(self, close=True, clear=False, indices=False, group='/', **variables):
        # create an hdf5 table for use with datasets, ideally of known size...
        # variables specifies what variables will be filled in this archive
        """
        Create an HDF5 document formatted for the provided variables
        
        Parameters
        ----------
            close : bool
                tell if the file should be returned or closed
            indexes : dict
                A dict of {'name':[length (integer),]} values to save as 
                single valued indices (Carrays)
            group: str,opt
                The textual representation of the group the dataset will 
                reside in
            **variables:
                name=[length,length,...] values to state the expandable 
                variables for the dataset
        """
        
        #FIXME  - only one category available.??
        if clear:
            'if the doc has been opened before, then reopen it for appending.'
            self.doc, self.lock = h5openw(self.filename)
        else:
            self.doc, self.lock = h5opena(self.filename)
            
        'Create the group the data is going to sit in'
        if group  is not '/':
            if '/' not in group:
                group = group + '/'
            gp = group.split('/')
            'then create the group'
            drop = '/'.join(gp[:-1]) + '/'
            try:
                self.doc.createGroup(drop, name=gp[-1])
            except:
                'in this case, the group already exists'
                pass
        'Lets see if there is a way to clear the group soon'
        
        
        
        'Identify the compression filters we are going to want to use'
        filters = tables.Filters(complevel=6, complib='zlib')#blosc
        '''
        for now, index information is expected to be fixed, or else it should 
        be a variable, so I will leave it as a Carray.
        '''
        
        if indices:
            for k in indices:
                if type(indices[k]) == tuple:
                    dims = ()
                    for i in indices[k]:
                        dims = dims + (i,)
                else:
                    dims = (indices[k],)
                    
                self.doc.createCArray(group, k,
                                      tables.Float32Atom(), dims,
                                      filters=filters.copy())
        '''
        By reading the variable information, we can create the description
        for this data table. 
        '''
        table_description = {'time': tables.FloatCol(pos=1)}
        'define the starting variable position'
        i = 2
        for k in variables:
            'data shape is equivalent to all the additional dimensions'
            table_description[k] = tables.Float32Col(shape=variables[k], pos=i,
                                                   dflt= -9999.)
            i += 1
        'create the table, disregard that it returns a table object'
        self.doc.createTable(group, 'data', table_description,
                                    filters=filters.copy())
        l.info('table created')
        'NOTE -- EXPECTED ROWS HAS BEEN REMOVED!!!'
        
        'Set file attributes'        
        self.doc.setNodeAttr('/', 'creator', 'Muto v' + muto.version())
        self.doc.setNodeAttr('/', 'version', '1.3')
        'set any group attributes. - a list of available attributes'
        self.doc.setNodeAttr(group, 'indices', indices.keys())
        'now we need to add the time values as a CS index'
        self.doc.getNode(group).data.cols.time.createCSIndex(filters=filters.copy())
        'and instruct the table to auto-index'
        self.doc.getNode(group).data.autoIndex = True
        if close:
            self.doc.close()
        return True

    def slice(self, variables, begin=False, end=False, duration=False,
              timetup=False, indices=False, group='/', persist=False,
              limit=None):
        """
        Read a specific temporal subset of various variables, as well as fetch 
        indices
        
        Parameters
        ----------
        variables: list
            a list of strings indicating all the variables which should be read.
        timetup: tuple
            Tuple of (begin,end) epoch timestamps which specify the begin/end 
            times of the slice.
        begin: int, opt
            epoch time stamp to begin. If only given, then dataset will be read 
            to the end.
        end: int, opt
            epoch time of slice end. If no other value given dataset is read 
            from beginning to this specified point.
        duration: int, opt
            Number of seconds to observe relative to either END of dataset, or 
            one of the provided begin/end values (begin takes priority).
        indices: list, opt
            indices are same-shape time independent data, of which only one 
            'ob' is pulled duration in seconds
        group: str/group, opt
            specify the HDF5 group where this dataset exists.
            
        Returns
        -------
        out: dict
            a dictionary keyed by the variables and indices given, their values
            are numpy arrays corresponding to the time sliced and ordered 
            datasets requested.
            
        Note
        ----
        If no time information (timetuple,begin,etc.) is given, the entire 
        dataset will be returned, similar to a multivariable dump.
        
        Time is ALWAYS returned as an index on the output variable from a 
         slice operation
        
        See Also
        --------
        index : grab only dataset indices.
        dump : fetch data independent of time.
        
        
        """
        if not self.doc or not self.doc.isopen:
            self.doc, self.lock = h5openr(self.filename)
        'Determine specified time limits'
        table = self.doc.getNode(group).data
        if timetup:
            begin = timetup[0]
            end = timetup[1]
        elif duration and not end and not begin:
            'assuming that the max time is in the last 100'
            end = np.max([r['time'] for r in table[-100:]])
            begin = end - duration
        elif duration and begin:
            end = begin + duration
        elif duration and end:
            begin = end - duration
        elif not duration and not begin and not end:
            # you gave me nothing
            raise Exception('You must specify a time tuple (timetup), begin/end'\
                            + ' or a duration in order to slice. Use dump() so see'\
                            + ' an entire dataset') 
        out = {}
        
        if type(variables) == str:
            'Only one variable is requested, so we can use a prebuilt hack'
            try:
                varlen = table[-1][variables].shape[0]
            except:
                'This probably means the variable had no shape'
                varlen = 1
            
            'a quick hack to make the most frequent requests faster'
            out = np.array([(r['time'], r[variables]) for r in table.where('(time >= ' + str(begin) + ') & (time <= ' + str(end) + ')')],
                           dtype=[('time', float), (variables, 'f4', (varlen,))])
            'IN THE FUTURE, this can be modified to account for all the variables '
        elif len(variables) == 2:
            'Account for the SECOND most common request'
            varlen1 = table[-1][variables[0]].shape[0]
            varlen2 = table[-1][variables[1]].shape[0]
            #NOTE this will break if time or any single-valued variable is requested...
            
            out = np.array([(r['time'], r[variables[0]], r[variables[1]]) for r in table.where('(time >= ' + str(begin) + ') & (time <= ' + str(end) + ')')],
                           dtype=[('time', float), (variables[0], 'f4', (varlen1,), (variables[1], 'f4', (varlen2,)))])
            
        else:
            result = self.doc.getNode(group).data.getWhereList('(time >= ' + str(begin) + ')&(time <= ' + str(end) + ')')
            if len(result['time']) > 0:
                rstart, rstop = result[0], result[-1] + 1
                if rstop - rstart == len(result):
                    'then we can just return all the values (skipping more refined check)'
                    out['time'] = self.doc.getNode(group).data.read(rstart, rstop, field='time')
                    for v in variables:
                        out[v] = self.doc.getNode(group).data.read(rstart, rstop, field=v)
                else:
                    out['time'] = self.doc.getNode(group).data.readCoordinates(result, field='time')
                    for v in variables:
                        out[v] = self.doc.getNode(group).data.readCoordinates(result, field=v)
            else:
                'result length is 0'
                self.doc.close()
                'FIXME - raising an exception may not really be nice/necessary'
                raise Exception('This data set does not have any data within the'\
                                + ' times specified')

        
        if not type(indices) == bool:
            if type(indices) == str:
                indices = [indices] 
            for i in indices:
                out[i] = self.doc.getNode(group, name=i)[:] 

        if not persist:
            self.doc.close()
        return out

    def index(self, index, group='/'): # DEPRECATED
        """
        Grab the value of a specific index from the document/group
        
        Parameters
        ----------
        index: str
            string representation of the index name.
        group: str/group, opt
            string representation of the group where the indices are read from.
        """
        if not self.doc or not self.doc.isopen:
            self.doc, self.lock = h5openr(self.filename)
        print 'WARNING - THIS IS A DEPRECATED FUNCTION'
        return self.doc.getNode(group, name=index)[0]
        # take the first value ([0]) because indices are time invariant in that 
        #dimension
    def stat(self):
        '''
        Quickly print the file object to stdout
        '''
        if not self.doc or not self.doc.isopen:
            self.doc, self.lock = h5openr(self.filename)
        print self.doc
        self.close()

    def save_indices(self, group='/', **indices):
        """ 
        Insert index variable arrays into the document
        
        Parameters
        ---------- 
        group: str, opt
            Specify the group in the file where the data are read from
        **indices:
            name=value pairs of the values to assign (and create) the indices for
            any specific dataset.
        
        """
        # simply stick the values of indices into their places
        if not self.doc or not self.doc.isopen:
            self.doc, self.lock = h5opena(self.filename)

        for i in indices:
            self.doc.getNode(group, name=i)[:] = indices[i] # that is all!

        self.close()


    def append(self, time, persist=False, group='/',
               filter=lambda x, y, z: True, **data):
        """
        Adds a single entry (row) for multiple variables as well as updates 
        the metadata attributes for the referred group. Appends only one row at
        a time.
        
        FIXME - this ought to be updated to allow multiple rows to 
        simultaneously be added!
        
        
        Parameters
        ----------
        time: int
            Unix timestamp of entry
        persist: bool
            set to true for the file to be left open between append rounds,
            this is recommended, as it will be much easier on the filesystem, 
            and faster.
        group: str,group Object
            textual or objective reference to the group branch where the 
            variable array is located
        filter: function, opt
            A method which will be passed the time and data and table object
            and should make a determination if an insertion is valid.
            An example would be to check if a time already exists in the dataset
            or a time-azimuth combination
        **data: 
            keyword arguments of variable=value
    
    
        Note
        ----
        This does not check that you are appending to every array, so
        if you don't, your indices will get all mucked up, be warned
        -- this does mean you can contain variables which are not actually 
        functions of time like height, x, y, etc. you must specify these 
        as indices for readout purposes
        
        """

        if not self.doc or not self.doc.isopen:
            self.doc, self.lock = h5opena(self.filename)
        
        if not filter(self.doc, time, data):
            'Then the append does not pass their test, and should end'
            return False
        'Grab the table\'s row operator.'
        row = self.doc.getNode(group).data.row
        'create a tuple from the given data for the given variables'

        row['time'] = time
        for v in data:
            'Naturally, this will fail if the data is not the right shape!'
            row[v] = data[v]
        'Or this might be where it fails'
        row.append()
        

        if not persist:
            self.doc.close()
        return True


    def dump(self, variable, group='/'):
        """
        A method to quickly output the entire contents of any specific variable/index
        array. 
            
        Warning
        -------
        This does not currently make any checks for dataset size, so if you dump too large
        a dataset, a significant amount of memory can be used accidentally
            
        Parameters
        ----------
        variable: str
            The variable or index array to be output.
        group: str/group, opt
            The group the dataset is stored in.
        """
        if not self.doc or not self.doc.isopen:
            self.doc, self.lock = h5openr(self.filename)
        indices = self.doc.getNodeAttr(group, 'indices')
        if variable in indices:
            out = self.doc.getNode(group, name=variable)[:]
        else:
            table = self.doc.getNode(group, name='data')
            out = table.col(variable)[:]
        self.close()
        return out

    def close(self):
        
        if self.doc.isopen:
            self.doc.close()
            'unlock'
            fcntl.lockf(self.lock, fcntl.LOCK_UN)
            self.lock.close()

'create the null document object class so the isopen property can be assigned'
class NullDoc(object):
    def __init__(self):
        self.isopen = False


"These functions can have the lockout functionality applied to them later"
def h5openr(f):
    lkf = open(f + '.lock', 'w')
    #fcntl.lockf(lkf,fcntl.LOCK_EX)
    f = tables.openFile(f, mode='r')
    'and lock the file'
    #FIXME - this locking does not seem to be effective
    return f, lkf

def h5opena(f):
    lkf = open(f + '.lock', 'w')
    #fcntl.lockf(lkf,fcntl.LOCK_EX)
    # open the file f for appending
    f = tables.openFile(f, mode='a')
    return f, lkf

def h5openw(f):
    lkf = open(f + '.lock', 'w')
    #fcntl.lockf(lkf,fcntl.LOCK_EX)
    #this will destroy whatever file it is opening, note
    f = tables.openFile(f, mode='w', title='ms')
    return f, lkf

'''
Here I include an example append filter, to filter if a time already exists in the data
'''

