from org.apache.nifi.processor.io import StreamCallback
from org.apache.nifi.flowfile import FlowFile
import subprocess

class MyProcessor(StreamCallback):
    def __init__(self):
        pass

    def process(self, inputStream, outputStream):
        # Execute the Python 3.9 script that processes Reddit data
        try:
            # Run internal Python 3.9 script, which collects the data
            process = subprocess.Popen(['/usr/bin/python3.9', '/home/vagrant/BigDataAnalytics/scripts/reddit_script_internal.py'], stdout=subprocess.PIPE)
            output, _ = process.communicate()

            # Write the output (JSON data) to the flow file
            outputStream.write(bytearray(output))

        except Exception as e:
            raise Exception("Failed to execute Python script: " + str(e))

flowFile = session.get()
if flowFile != None:
    flowFile = session.write(flowFile, MyProcessor())
session.transfer(flowFile, REL_SUCCESS)
session.commit()

