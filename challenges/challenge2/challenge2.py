# ABBY MCCOLLAM
# CHALLENGE 2

# python challenge2.py -r hadoop hdfs:///user/abbymccollam/googlebooks.csv --output-dir=hdfs:///user/abbymccollam/challenge2output --conf-path=mrjob.conf

from mrjob.job import MRJob

class Challenge2(MRJob):
    def mapper(self, _, line): 
        parts = line.split('\t') #separating .csv by tabs
        bigram = parts[0] #assigning variables to indexes
        count1 = int(parts[2])
        count2 = int(parts[4])
        yield (bigram, (count1, count2)) #yielding bigram, count1/2

    def reducer(self, bigram, counts):
        # initializing variables
        bigram_occur = 0
        in_book = 0
        for num, books in counts: #calculating bigram occurances in books
            bigram_occur += num
            in_book += books
        avg = bigram_occur / in_book #average number of matches per book
        yield bigram, avg #returns bigram and average num it appears in a book

# run main
if __name__ == '__main__':
    Challenge2.run()