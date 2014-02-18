from subprocess import call
import json
import sys
import os

config_file = sys.argv[1]

with open(config_file) as f:
    json_config = json.load(f)

json_config = json_config['loader']

my_dir = os.path.dirname(os.path.realpath(__file__))
os.chdir(my_dir)

cur_classpath = ""
if('CLASSPATH' in os.environ):
	cur_classpath = os.environ['CLASSPATH']

mainclass = json_config['mainclass']
os.environ['SPARK_HOME'] = json_config['sparkhome']
os.environ['CLASSPATH'] = cur_classpath + ':'.join(json_config['classpath'])

print os.environ

call(['java', mainclass, '-c', config_file, '--start'])