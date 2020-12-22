import hashlib
import os
import shutil


def _hash_file(path):
    BLOCKSIZE = 65536
    hasher = hashlib.md5()
    with open(path, 'rb') as afile:
        buf = afile.read(BLOCKSIZE)
        while len(buf) > 0:
            hasher.update(buf)
            buf = afile.read(BLOCKSIZE)
    return hasher.hexdigest()


def _hash_string(s):
    hasher = hashlib.md5()
    hasher.update(s.encode('utf-8'))
    return hasher.hexdigest()


def anonymize_copy(src_file_path, src_root, dest_root) -> str:
    src_relative_path = os.path.relpath(src_root, src_file_path)
    dest_relative_path = _hash_string(os.path.dirname(src_relative_path))
    dest_folder = os.path.join(dest_root, dest_relative_path)
    src_file_ext = os.path.splitext(os.path.basename(src_file_path))[1]
    file_name_root_no_ext = _hash_file(src_file_path)
    os.makedirs(dest_folder, exist_ok=True)
    dest_file_path = os.path.join(
                        dest_folder,
                        file_name_root_no_ext)
    dest_file = dest_file_path + src_file_ext
    shutil.copyfile(src_file_path, dest_file)
    return dest_file
