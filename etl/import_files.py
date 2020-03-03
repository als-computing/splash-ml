import shutil
import fnmatch
#import pyFAI
def get_all_filepaths(root_path, ext):

   import os
   all_files = []
   nt = 1
   na = 1
   ngisaxs = 1
   ngiwaxs = 1
   nauto = 1
   nbeam = 1

   for root, dirs, files in os.walk(root_path):
       for filename in files:
           if filename.lower().endswith(ext):
               all_files.append(os.path.join(root, filename))
              # filename_new = filename[:-4]

               if filename.lower().startswith("agb"):
                   zahl = "000000" + str(na)
                   file_new = "AgB" + zahl.zfill(5) + ".edf"
                   shutil.copy(os.path.join(root, filename), os.path.join('D:AgB', file_new))
                   na = na + 1
               elif fnmatch.fnmatch(os.path.join(root, filename).lower(),"*gisaxs*"):
                   print os.path.join(root, filename)
                   zahl = "000000" + str(ngisaxs)
                   file_new = "gisaxs" + zahl.zfill(5) + ".edf"
                   shutil.copy(os.path.join(root, filename), os.path.join('D:Learning\GISAXS', file_new))
                   ngisaxs = ngisaxs + 1
               elif fnmatch.fnmatch(os.path.join(root, filename).lower(),"*giwaxs*"):
                   print os.path.join(root, filename)
                   zahl = "000000" + str(ngiwaxs)
                   file_new = "giwaxs" + zahl.zfill(5) + ".edf"
                   shutil.copy(os.path.join(root, filename), os.path.join('D:Learning\GIWAXS', file_new))
                   ngiwaxs = ngiwaxs + 1
               elif filename.lower().startswith("auto"):
                   zahl = "000000" + str(nauto)
                   file_new = "auto" + zahl.zfill(5) + ".edf"
                   shutil.copy(os.path.join(root, filename), os.path.join('D:Learning\auto', file_new))
                   nauto = nauto + 1
               elif filename.lower().startswith("beam"):
                   zahl = "000000" + str(nbeam)
                   file_new = "beam" + zahl.zfill(5) + ".edf"
                   shutil.copy(os.path.join(root, filename), os.path.join('D:Learning\beam', file_new))
                   nbeam = nbeam + 1
               else:
                   zahl = "000000" + str(nt)
                   file_new = "Learn" + zahl.zfill(5) + ".edf"
                   nt = nt + 1
                   shutil.copy(os.path.join(root, filename),os.path.join("D:Learning\Transmission", file_new))

   return all_files
# Example: print all the .py files in './python'
a = get_all_filepaths("D:", "gb")
#b = pyFAI.load("AgB.edf")
#print b
