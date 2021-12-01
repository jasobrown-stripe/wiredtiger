#!/usr/bin/env python

import re
from dist import compare_srcfile

filename = "../src/include/wiredtiger.in"
verbose_categories = []

# Retrieve all verbose categories
with open(filename, 'r') as f:
    in_section = False
    pattern = re.compile("^WT_VERB_[A-Z_]+$")
    for line in f:
        if line.find('VERBOSE ENUM START') != -1:
            # The next line is in the section
            in_section = True
            continue
        if line.find('VERBOSE ENUM STOP') != -1:
            break
        if in_section:
            # Remove any leading and trailing whitespaces 
            line = line.strip()
            content = line.split(',')

            # Sanity checks
            assert(len(content) > 1)
            verbose_category = content[0]
            assert pattern.match(verbose_category), "The category "  + verbose_category + " does not follow the expected syntax."

            # Save the category
            verbose_categories.append(content[0])

    assert in_section, "No verbose categories have been found in " + filename
f.close()

filename = "../src/include/verbose.h"

# Generate all verbose categories as strings
tmp_file = '__tmp_verbose'
with open(filename, 'r') as f:

    tfile = open(tmp_file, 'w')
    in_section = False

    for line in f:

        line_tmp = line

        if line.find('AUTOMATIC VERBOSE ENUM STRING GENERATION START') != -1:
            # The next line is in the section
            in_section = True
        elif line.find('AUTOMATIC VERBOSE ENUM STRING GENERATION STOP') != -1:
            in_section = False
        elif in_section == True:
            line_tmp = ""
            for category in verbose_categories:
                line_tmp += "\"" + category + "\", "
            line_tmp += '\n'

        tfile.write(line_tmp)

    tfile.close()
    compare_srcfile(tmp_file, filename)

f.close()
