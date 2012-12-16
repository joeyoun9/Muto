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

class H5(object):
    '''
    Class for interacting with HDF5 files in the format created by this class
    '''

    def __init__(self,fname):
        """
        Create the object for interaction by simply providing the location of 
        the HDF5 document
        
        Parameters
        ----------    
        fname : str
            String location of HDF5 source file (.h5,hdf5,etc...)
            
        """
        self.filename = fname
        self.doc = False

    def create(self, close=True, indices=False, group='/', size_guess=2000000, **variables):
        # create an hdf5 table for use with datasets, ideally of known size...
        # variables specifies what variables will be filled in this archive
        """
        Create 
        
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
        if not self.doc:
            'if the doc has been opened before, then reopen it for appending.'
            self.doc = h5openw(self.filename)
        elif not self.doc.isopen:
            self.doc = h5opena(self.filename)
            
        'Create the group the data is going to sit in'
        if group  is not '/':
            if '/' not in group:
                group = group+'/'
            gp = group.split('/')
            'then create the group'
            drop = '/'.join(gp[:-1])+'/'
            self.doc.createGroup(drop,name=gp[-1])
            
        'Identify the compression filters we are going to want to use'
        filters =  tables.Filters(complevel=6, complib='zlib')#blosc
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

                self.doc.createCArray(group,k,
                                      tables.Float32Atom(),dims,
                                      filters=filters.copy())
        '''
        By reading the variable information, we can create the description
        for this data table. 
        '''
        
        table_description = {'time': tables.FloatCol(pos=1)}
        'define the starting variable position'
        i=2
        for k in variables:
            dims = (0,)
#            if type(variables[k]) == tuple:
#                # then this is multidimensional
#                for i in variables[k]: #it had better be a list
#                    dims = dims + (i,)
#            else:
#                dims = (0,variables[k])
            table_description[k]=tables.Float32Col(shape=variables[k],pos=i,
                                                   dflt=-9999.)
            i+=1
        data_table = self.doc.createTable(group,'data',table_description,
                                    filters=filters.copy(),expectedrows=size_guess)
        'Set file attributes'        
        self.doc.setNodeAttr('/','creator', 'Muto v'+muto.version())
        self.doc.setNodeAttr('/','version', '1.1')
        'set any group attributes. - a list of available attributes'
        self.doc.setNodeAttr(group,'indices',indices.keys())
        
        if close:
            self.doc.close()
        return True

    def slice(self,variables,begin=False,end=False,duration=False,
              timetup=False,indices=False,group='/'):
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
            self.doc = h5openr(self.filename)
        'Determine specified time limits'
        if timetup:
            begin = timetup[0]
            end = timetup[1]
        elif duration and not end and not begin:
            # then 
            end = np.max(self.doc.getNode(group).data.col('time')[-10000:])
            begin = end - duration # duration is in seconds?
        elif duration and begin:
            end = begin + duration
        elif duration and end:
            begin = end - duration
        elif not duration and not begin and not end:
            # you gave me nothing
            raise Exception('You must specify a time tuple (timetup), begin/end'\
                            +' or a duration in order to slice. Use dump() so see'\
                            +' an entire dataset') 
        out = {}
        table = self.doc.getNode(group).data
        result = table.getWhereList('(time >= '+str(begin)+')&(time <= '+str(end)+')')
        if len(result) > 0:
            if not type(variables) == list:
                variables = [variables]
            out['time'] = table.readCoordinates(result,field='time')
            for v in variables:
                out[v] = table.readCoordinates(result,field=v)
        else:
            self.doc.close()
            'FIXME - raising an exception may not really be nice/necessary'
            raise Exception('This data set does not have any data within the'\
                            +' times specified')

        
        if not type(indices) == bool:
            for i in indices:
                out[i] = self.doc.getNode(group,name=i)[0] 

        self.doc.close()
        return out

    def index(self,index,group='/'): # DEPRECATED
        """
        CURRENTLY DEPRECATED - UNKNOWN UTILITY
        Read out the entire value of the specified data indexing value. 
        No searching is performed
        
        Deprecated? - will break
        
        Parameters
        ----------
        index: str
            string representation of the index name.
        group: str/group, opt
            string representation of the group where the indices are read from.
        """
        if not self.doc or not self.doc.isopen:
            self.doc=h5openr(self.filename)
        print 'WARNING - THIS IS A DEPRECATED FUNCTION'
        return self.doc.getNode(group,name=index)[0]
        # take the first value ([0]) because indices are time invariant in that 
        #dimension

    def save_indices(self,group='/', **indices):
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
            self.doc = h5opena(self.filename)

        for i in indices:
            self.doc.getNode(group,name=i)[:] = indices[i] # that is all!

        self.close()


    def append(self, time, persist=False, group='/', filter=lambda x, y, z: True, **data):
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
            self.doc = h5opena(self.filename)
        
        if not filter(self.doc,time,data):
            'Then the append does not pass their test, and should end'
            return False
        'Grab the table\'s row operator.'
        row = self.doc.getNode(group).data.row
        'create a tuple from the given data for the given variables'

        row['time']= time
        for v in data:
            'Naturally, this will fail if the data is not the right shape!'
            row[v]=data[v]
        'Or this might be where it fails'
        row.append()
        

        if not persist:
            self.doc.close()
        return True


    def dump(self,variable,group='/'):
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
            self.doc = h5openr(self.filename)
        indices = self.doc.getNodeAttr(group,'indices')
        if variable in indices:
            out = self.doc.getNode(group,name=variable)[:]
        else:
            table = self.doc.getNode(group,name='data')
            out=table.col(variable)[:]
        self.close()
        return out

    def close(self):
        if self.doc.isopen:
            fcntl.lockf(self.doc,fcntl.LOCK_UN)

            self.doc.close()
            'unlock'






"These functions can have the lockout functionality applied to them later"
def h5openr(f):
    f = tables.openFile(f,mode='r')
    'and lock the file'
    fcntl.lockf(f,fcntl.LOCK_SH)
    return f

def h5opena(f):
    # open the file f for appending
    f = tables.openFile(f,mode='a')
    fcntl.lockf(f,fcntl.LOCK_EX)
    return f

def h5openw(f):
    #this will destroy whatever file it is opening, note
    f = tables.openFile(f,mode='w', title='ms')
    fcntl.lockf(f,fcntl.LOCK_EX)
    return f

'''
Here I include an example append filter, to filter if a time already exists in the data
'''
