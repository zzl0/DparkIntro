# coding: utf-8
import re
import os
import glob
import shutil


def get_file_map():
    files = glob.glob('posts/*.md')
    return {os.path.basename(f): get_outfile(f) for f in files}


def get_outfile(filename):
    with open(filename) as f:
        for line in f:
            if line.startswith('date:'):
                date = line.strip().split(": ")[1]
                break
        else:
            raise Exception("No date info in %s" % filename)

        basename = os.path.basename(filename)
        return date + basename[2:]


def sync_markdown(blog_dir):
    pat = re.compile('(\./[0-9]{2}-[\w-]*\.md)')
    filemap = get_file_map()
    for filename in filemap:
        outfile = os.path.join(blog_dir, '_posts', filemap[filename])
        infile = os.path.join('posts', filename)
        with open(outfile, 'w') as fout, open(infile) as fin:
            content = fin.read()
            referer_name = pat.findall(content)
            for n in referer_name:
                content = content.replace(n, gen_url(filemap[n[2:]]))
            fout.write(content)

def gen_url(filename):
    "2014-10-13-dpark-basic.md -> /2014/10/13/dpark-basic/"
    parts = filename[:-3].split('-')
    date, name = '/'.join(parts[:3]), '-'.join(parts[3:])
    return '/' + date + '/' + name + '/'


def sync_img(blog_dir):
    imgs = [f for f in glob.glob('img/dpark/*')
            if not f.endswith('dot')]
    dest_dir = os.path.join(blog_dir, 'img', 'dpark')
    for f in imgs:
        shutil.copy(f, dest_dir)


def main():
    import sys
    blog_dir = '/Users/zzl/projects/blog/zzl0.github.com'

    sync_markdown(blog_dir)
    sync_img(blog_dir)


if __name__ == '__main__':
    main()
