
# ABBY MCCOLLAM
# CHALLENGE 1

# python challenge1.py -r hadoop hdfs:///user/abbymccollam/googlebooks.csv --output-dir=hdfs:///user/abbymccollam/challenge1output --conf-path=mrjob.conf

from mrjob.job import MRJob

class Challenge1(MRJob):
    def mapper(self, _, line):
        parts = line.split('\t') #separating .csv by tabs
        bigram = parts[0] #assigning bigrams to index 0
        year = int(parts[1]) #assigning years to index 1
        yield bigram, year # return bigram, year

    def reducer(self, bigram, years):
        min_year = min(years) #calculating minimum year among bigrams
        if min_year > 1991: #finding minimum years less than 1991
            yield bigram, min_year #returning those remaining

# run main
if __name__ == '__main__':
    Challenge1.run()