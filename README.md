# MrjobRecommander
Build Recommander system with mrjob in python

- Install with pip:

     pip install mrjob

or from a git clone of the source code:

- python setup.py test && python setup.py install

   Writing your first job

   Open a file called mr_word_count.py and type this into it:

# from mrjob.job import MRJob


class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):
        yield "chars", len(line)
        yield "words", len(line.split())
        yield "lines", 1

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    MRWordFrequencyCount.run()

Now go back to the command line, find your favorite body of text (such mrjob’s README.rst, or even your new file mr_word_count.py), and try this:

$ python mr_word_count.py my_file.txt

You should see something like this:

"chars" 3654
"lines" 123
"words" 417

Congratulations! You’ve just written and run your first program with mrjob.
