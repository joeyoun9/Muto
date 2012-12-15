class GenericProfile(DataObject):
	'''
	The profile class extends the DataObject class by adding a couple 
	methods to handle processing generic profile data, where what is passed
	is a numpy array of data values, meant to be stored.
	'''
	
class PointProfie(DataObject):
	'''
	The PointProfile class extends the DataObject for measurements like soundings.
	
	A profile composed of numerous points in the atmosphere, therefore spatial 
	position is also included as important information. Numerous variables are 
	recorded about each point, as well, both the profile overall has a timestamp
	as well as each individual observation. 
	
	Variable collections are dynamic, and will have default values such that they
	can be absent.
	'''



class DataObject(object):
	'''
	The DataObject forms the backbone of individual data messages. Simple
	metadata and values can be recorded, along with more comprehensive
	descriptive data.

	The class is designed to hold generic methods for archiving the files
	within muto-formatted HDF5 files. Ideally these file formats will be 
	simple to expand beyond muto applications.

	Finally the class will be expandable for different observation types
	by expanding upon this base class.
	
	This whole operation should be very light, as it may get called 
	repeatedly by any ingestion operation
	'''
	'Initialization mehod for any ob, '
	def __init__(self,name,raw):
		'''
		Initialize the object class by simply recording attribution data
		
		Parameters
		----------
		
		name: str
			represents a fundamental identifying characteristic of the observation.
			Will be checked during the writing process. Branches are of a fixed
			type
			
		'''
		self.raw = raw
		self.name=name
	def write(self):
		'Ensure the branch written to is of the same type as this ob. '
		
	
	
		
	
