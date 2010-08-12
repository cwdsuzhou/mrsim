
from os.path import exists, isfile,getsize, dirname
import sys
import os, errno

from django.template import Template, Context
from django.conf import settings

def mkdir_p(path):
    if exists(path) and not isfile(path):
        removeDirecoty(path)
    try:
        os.makedirs(path)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST:
            pass
        else: raise

def removeDirecoty(path):
    import shutil
    shutil.rmtree(path)
    
def generate_form_templates(templ_file='./templates/job.json',
                            variable_file='./vars/variables'):
    result={}
    settings.configure()
    t = Template(open(templ_file).read())

    variables = eval(open(variable_file).read())

    for oneVar in variables:
        jobName=oneVar['jobName']
        c = Context(oneVar)
        result[oneVar['jobName']]=t.render(c)

    return result

def save_json(dir_name, file_name, txt):
    outfile =open(dir_name+'/'+file_name,'w')
    outfile.write(txt)
    outfile.close()

def process_var(txt):
    a=txt[1:]
    para=a.split('=')
    return (para[0],para[1])

if __name__ == "__main__":

  
    args=[]
  

    for a in sys.argv:
        args.append(a)

    print args

    if len(args) < 4 :
        print 'not enough parameters'
        print 'gen_jobs.py template_file variable_file output_dir'
        exit()
    else:
        dir_name = args[3]
        templ_file= args[1]
        variable_file=args[2]
        
        mkdir_p(dir_name)
        for k,v in generate_form_templates(templ_file,variable_file ).items():
            save_json(dir_name, k+'.json', v)
        print 'done'
