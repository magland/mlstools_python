import os
import json
import shlex
import subprocess
import mlstools as mls
import hashlib

class MLProcessorPIO: #parameter, input, or output
	def __init__(self,obj):
		self._obj=obj
	def name(self):
		return self._obj.get('name','')
	def description(self):
		return self._obj.get('description','')
	def isOptional(self):
		return self._obj.get('optional',False)
	def defaultValue(self):
		return self._obj.get('default_value','')

class Empty:
	pass

class MLProcessor:
	def __init__(self,processor_name,package_uri=''):
		self._processor_name=processor_name
		self._package_uri=package_uri
		self._spec=None
		self._mlconfig=None
	def spec(self):
		if not self._spec:
			cmd='mp-spec {} --_package_uri={}'.format(self._processor_name,self._package_uri)
			output=os.popen(cmd).read()
			obj=json.loads(output)
			self._spec=obj
		return self._spec
	def name(self):
		return self._processor_name
	def packageUri(self):
		return self._package_uri
	def version(self):
		return self.spec().get('version','')
	def description(self):
		return self.spec().get('description','')
	def inputNames(self):
		inputs0=self.spec().get('inputs',[])
		ret=[]
		for input0 in inputs0:
			ret.append(input0['name'])
		return ret
	def input(self,name):
		inputs0=self.spec().get('inputs',[])
		for input0 in inputs0:
			if input0['name'] == name:
				return MLProcessorPIO(input0)
		raise Exception('Input not found in spec: {}'.format(name))
	def outputNames(self):
		outputs0=self.spec().get('outputs',[])
		ret=[]
		for output0 in outputs0:
			ret.append(output0['name'])
		return ret
	def output(self,name):
		outputs0=self.spec().get('outputs',[])
		for output0 in outputs0:
			if output0['name'] == name:
				return MLProcessorPIO(output0)
		raise Exception('Output not found in spec: {}'.format(name))
	def parameterNames(self):
		parameters0=self.spec().get('parameters',[])
		ret=[]
		for parameter0 in parameters0:
			ret.append(parameter0['name'])
		return ret
	def parameter(self,name):
		parameters0=self.spec().get('parameters',[])
		for parameter0 in parameters0:
			if parameter0['name'] == name:
				return MLProcessorPIO(parameter0)
		raise Exception('Parameter not found in spec: {}'.format(name))

	def _run_command_and_print_output(self,command):
	    process = subprocess.Popen(shlex.split(command), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	    while True:
	        output = process.stderr.readline()
	        output2= process.stdout.readline()
	        if (not output) and (not output2) and (process.poll() is not None):
	            break
	        if output:
	            print (output.strip().decode())
	        if output2:
	            print (output2.strip().decode())
	    rc = process.poll()
	    return rc

	def run(self,**kwargs):
		inames=set(self.inputNames())
		for iname in inames:
			if not self.input(iname).isOptional():
				if not iname in kwargs:
					raise Exception('Missing input argument: {}'.format(iname))
		onames=set(self.outputNames())
		for oname in onames:
			if not self.output(oname).isOptional():
				if not oname in kwargs:
					raise Exception('Missing output argument: {}'.format(oname))
		pnames=set(self.parameterNames())
		for pname in pnames:
			if not self.parameter(pname).isOptional():
				if not pname in kwargs:
					raise Exception('Missing parameter argument: {}'.format(pname))
		for argname in kwargs:
			if not argname.startswith('_'):
				if (not argname in inames) and (not argname in onames) and (not argname in pnames):
					raise Exception('Unexpected argument: {}'.format(argname))
		cmd='mp-run-process {} --_package_uri={}'.format(self.name(),self.packageUri())
		argnames_sorted=sorted(kwargs.keys())
		for argname in argnames_sorted:
			if argname.startswith('_'):
				cmd=cmd+' --{}={}'.format(argname,kwargs[argname])
			else:
				if argname in inames:
					val=kwargs[argname]
					if type(val)==tuple:
						for val0 in val:
							path0=self._get_path_for_input(argname,val0)
							cmd=cmd+' --{}={}'.format(argname,path0)
					else:
						path0=self._get_path_for_input(argname,val)
						cmd=cmd+' --{}={}'.format(argname,path0)
				elif argname in onames:
					pass #for now
				elif argname in pnames:
					val=kwargs[argname]
					if type(val)==tuple:
						for val0 in val:
							cmd=cmd+' --{}={}'.format(argname,val0)
					else:
						cmd=cmd+' --{}={}'.format(argname,val)
				else:
					raise Exception('Unexpected argument: {}'.format(argname))
		process_signature=self._get_signature_from_cmd(cmd)
		output_paths={}
		for argname in argnames_sorted:
			if (not argname.startswith('_')) and (argname in onames):
				path0=self._create_path_for_output(argname,kwargs[argname],process_signature)
				output_paths[argname]=path0
				cmd=cmd+' --{}={}'.format(argname,path0)
		print ('RUNNING:::::: '+cmd)
		#process = Popen(shlex.split(cmd), stdout=PIPE)
		#process.communicate()
		#exit_code = process.wait()
		exit_code = self._run_command_and_print_output(cmd)
		if exit_code != 0:
			raise Exception('Non-zero exit code for {}'.format(self.name()))
		ret=Empty()
		for argname in kwargs:
			if argname in onames:
				if kwargs[argname]:
					setattr(ret,argname,output_paths[argname])
		return ret


	def _get_mlconfig(self):
		if not self._mlconfig:
			self._mlconfig=json.loads(os.popen('mlconfig print').read())
		return self._mlconfig

	def _get_temp_path(self):
		return self._get_mlconfig()['general']['temporary_path']

	def _get_path_for_input(self,iname,val):
		if (type(val)==str):
			return val
		return mls.getFilePath(val)

	def _get_signature_from_cmd(self,cmd):
		return hashlib.sha1(cmd.encode()).hexdigest()

	def _create_path_for_output(self,oname,val,signature):
		if (type(val)==str):
			return val
		temporary_path=self._get_temp_path()
		if not os.path.exists(temporary_path+'/mountainlab'):
			os.makedirs(temporary_path+'/mountainlab')
		if not os.path.exists(temporary_path+'/mountainlab/tmp_short_term'):
			os.makedirs(temporary_path+'/mountainlab/tmp_short_term')
		return temporary_path+'/mountainlab/tmp_short_term/output_'+oname+'_'+signature

def runProcess(processor_name,package_uri='',**kwargs):
	P=MLProcessor(processor_name,package_uri=package_uri)
	if not P.spec():
		raise Exception('Unable to find processor: {} {}'.format(processor_name,package_uri))
	return P.run(**kwargs)